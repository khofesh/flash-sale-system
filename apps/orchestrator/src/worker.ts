import os from 'node:os';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  HELLO_GROUP,
  HELLO_STREAM,
  isHelloResult,
  makeHop,
  type HelloHop,
  type HelloStatus,
  type HelloRequestMessage,
} from '@fss/core';
import {
  resolveMigrationsDir,
  runMigrations,
  type Logger,
  type MessageBus,
  type Pool,
  type Redis,
  type StopFn,
} from '@fss/infra';

export interface OrchestratorDeps {
  redis: Redis;
  pool: Pool;
  bus: MessageBus;
  logger: Logger;
  migrationsDir?: string;
  /** Pending stream entries idle longer than this are reclaimed (SPEC §9: 30 s). */
  claimIdleMs?: number;
  claimIntervalMs?: number;
}

interface HelloStreamFields {
  id: string;
  name: string;
  hostname: string;
  ts: string;
}

type StreamEntry = [string, string[]];

class InvalidEntryError extends Error {}

export async function startOrchestrator(deps: OrchestratorDeps): Promise<StopFn> {
  const { redis, pool, bus } = deps;
  const migrationsDir = await resolveMigrationsDir(deps.migrationsDir);
  await runMigrations(pool, migrationsDir);
  await ensureGroup(redis);

  const consumer = `${os.hostname()}-${process.pid}`;
  const stops: StopFn[] = [
    await bus.subscribe('order-events', (message) => handleOrderEvent(deps, message.body), {
      concurrency: 10,
    }),
    consumeReservations(deps, consumer),
  ];

  return async () => {
    await Promise.all(stops.map((stop) => stop()));
  };
}

async function ensureGroup(redis: Redis): Promise<void> {
  try {
    await redis.xgroup('CREATE', HELLO_STREAM, HELLO_GROUP, '0', 'MKSTREAM');
  } catch (error) {
    if (!(error instanceof Error) || !error.message.includes('BUSYGROUP')) {
      throw error;
    }
  }
}

function consumeReservations(deps: OrchestratorDeps, consumer: string): StopFn {
  const { redis, logger } = deps;
  const claimIdleMs = deps.claimIdleMs ?? 30_000;
  const claimIntervalMs = deps.claimIntervalMs ?? 5_000;
  let running = true;
  let lastClaim = 0;

  const handleEntries = async (entries: StreamEntry[]) => {
    for (const [entryId, rawFields] of entries) {
      if (!running) return;
      try {
        await handleReservation(deps, fieldsToObject(rawFields));
        await redis.xack(HELLO_STREAM, HELLO_GROUP, entryId);
      } catch (error) {
        if (error instanceof InvalidEntryError) {
          logger.error({ err: error, entryId }, 'dropping invalid hello stream entry');
          await redis.xack(HELLO_STREAM, HELLO_GROUP, entryId);
          continue;
        }
        // Left pending; reclaimed by XAUTOCLAIM once idle for claimIdleMs.
        logger.error({ err: error, entryId }, 'failed to process hello reservation');
      }
    }
  };

  const loop = (async () => {
    while (running) {
      try {
        if (Date.now() - lastClaim >= claimIntervalMs) {
          lastClaim = Date.now();
          // Reclaims entries left pending by crashed consumers (including a previous
          // incarnation of this one) and entries whose processing failed.
          const [, claimed] = (await redis.xautoclaim(
            HELLO_STREAM,
            HELLO_GROUP,
            consumer,
            claimIdleMs,
            '0-0',
            'COUNT',
            100,
          )) as [string, Array<StreamEntry | null>];
          const live = claimed.filter((entry): entry is StreamEntry => entry !== null);
          if (live.length) {
            logger.warn({ count: live.length }, 'reclaimed pending hello stream entries');
            await handleEntries(live);
          }
        }

        const batches = (await redis.xreadgroup(
          'GROUP',
          HELLO_GROUP,
          consumer,
          'COUNT',
          10,
          'BLOCK',
          2_000,
          'STREAMS',
          HELLO_STREAM,
          '>',
        )) as Array<[string, StreamEntry[]]> | null;

        for (const [, entries] of batches ?? []) {
          await handleEntries(entries);
        }
      } catch (error) {
        if (!running) return;
        logger.error({ err: error }, 'hello stream read failed');
        await sleep(1_000);
      }
    }
  })();

  return async () => {
    running = false;
    await loop;
  };
}

const STATUS_RANK: Record<HelloStatus, number> = { QUEUED: 0, PAID: 1, DONE: 2 };

/**
 * Persists the hello, then requests payment. Commit happens before publish so a
 * downstream reply can never observe uncommitted state. If the publish fails, or
 * the entry is redelivered, the row is still QUEUED and payment is requested
 * again under the same messageId; the conditional QUEUED -> PAID transition
 * absorbs any duplicate `hello.paid`.
 */
async function handleReservation(
  { pool, bus, logger }: OrchestratorDeps,
  fields: HelloStreamFields,
): Promise<void> {
  const apiHop: HelloHop = { service: 'api', hostname: fields.hostname, at: fields.ts };
  const orchestratorHop = makeHop('orchestrator', os.hostname());

  const inserted = await pool.query(
    `INSERT INTO hello_messages (id, name, hops, status)
     VALUES ($1, $2, $3::jsonb, 'QUEUED')
     ON CONFLICT (id) DO NOTHING`,
    [fields.id, fields.name, JSON.stringify([apiHop, orchestratorHop])],
  );
  if (inserted.rowCount !== 1) {
    const status = await currentStatus(pool, fields.id);
    if (status !== 'QUEUED') {
      logger.info({ helloId: fields.id, status }, 'hello already past QUEUED; skipping');
      return;
    }
    logger.warn({ helloId: fields.id }, 'redelivered hello still QUEUED; re-requesting payment');
  }

  const message: HelloRequestMessage = {
    messageId: `${fields.id}:payment`,
    type: 'hello.payment',
    helloId: fields.id,
    name: fields.name,
  };
  await bus.publish('payment-requests', message);
  logger.info({ helloId: fields.id }, 'hello reservation persisted and payment requested');
}

/**
 * Transitions are conditional (`WHERE status = <expected>`). A duplicate event
 * changes nothing; a duplicate `hello.paid` while still PAID re-requests shipping
 * (covers a publish that failed after commit). An event that arrives before the
 * row reaches the expected state throws, so SQS redelivers it later.
 */
async function handleOrderEvent(deps: OrchestratorDeps, body: unknown): Promise<void> {
  const { pool, bus, logger } = deps;
  if (!isHelloResult(body)) {
    logger.warn({ body }, 'ignoring unknown order event');
    return;
  }

  const hops = JSON.stringify([body.hop, makeHop('orchestrator', os.hostname())]);
  const [from, to]: [HelloStatus, HelloStatus] =
    body.type === 'hello.paid' ? ['QUEUED', 'PAID'] : ['PAID', 'DONE'];

  const updated = await pool.query(
    `UPDATE hello_messages
        SET hops = hops || $1::jsonb,
            status = $2,
            updated_at = now()
      WHERE id = $3 AND status = $4`,
    [hops, to, body.helloId, from],
  );

  if (updated.rowCount !== 1) {
    const status = await currentStatus(pool, body.helloId);
    if (status === undefined || STATUS_RANK[status] < STATUS_RANK[from]) {
      throw new Error(
        `${body.type} for ${body.helloId} arrived early (status ${status}); retrying`,
      );
    }
    if (!(body.type === 'hello.paid' && status === 'PAID')) {
      logger.info(
        { helloId: body.helloId, type: body.type, status },
        'duplicate order event; ignoring',
      );
      return;
    }
    logger.warn(
      { helloId: body.helloId },
      'duplicate hello.paid while PAID; re-requesting shipping',
    );
  }

  if (body.type === 'hello.paid') {
    const message: HelloRequestMessage = {
      messageId: `${body.helloId}:ship`,
      type: 'hello.ship',
      helloId: body.helloId,
      name: body.name,
    };
    await bus.publish('shipping-requests', message);
    logger.info({ helloId: body.helloId }, 'hello paid; shipping requested');
  } else {
    logger.info({ helloId: body.helloId }, 'hello shipped; flow done');
  }
}

async function currentStatus(pool: Pool, id: string): Promise<HelloStatus | undefined> {
  const result = await pool.query<{ status: HelloStatus }>(
    'SELECT status FROM hello_messages WHERE id = $1',
    [id],
  );
  return result.rows[0]?.status;
}

function fieldsToObject(raw: string[]): HelloStreamFields {
  const fields: Record<string, string> = {};
  for (let index = 0; index < raw.length; index += 2) {
    const key = raw[index];
    const value = raw[index + 1];
    if (key !== undefined && value !== undefined) {
      fields[key] = value;
    }
  }

  if (!fields.id || !fields.name) {
    throw new InvalidEntryError(`Invalid hello stream entry: ${JSON.stringify(fields)}`);
  }

  return {
    id: fields.id,
    name: fields.name,
    hostname: fields.hostname ?? 'unknown',
    ts: fields.ts ?? new Date().toISOString(),
  };
}

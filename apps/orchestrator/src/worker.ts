import { randomUUID } from 'node:crypto';
import os from 'node:os';
import {
  makeHop,
  type HelloHop,
  type HelloRequestMessage,
  type HelloResultMessage,
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

export const HELLO_STREAM = 'hello:stream';
export const HELLO_GROUP = 'orchestrator';

export interface OrchestratorDeps {
  redis: Redis;
  pool: Pool;
  bus: MessageBus;
  logger: Logger;
  migrationsDir?: string;
}

interface HelloStreamFields {
  id: string;
  name: string;
  hostname: string;
  ts: string;
}

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
  let running = true;

  const loop = (async () => {
    while (running) {
      let batches: unknown;
      try {
        batches = await redis.xreadgroup(
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
        );
      } catch (error) {
        if (!running) return;
        logger.error({ err: error }, 'xreadgroup failed');
        await sleep(1_000);
        continue;
      }

      if (!batches) continue;

      for (const [, entries] of batches as Array<[string, Array<[string, string[]]>]>) {
        for (const [entryId, rawFields] of entries) {
          try {
            await handleReservation(deps, fieldsToObject(rawFields));
            await redis.xack(HELLO_STREAM, HELLO_GROUP, entryId);
          } catch (error) {
            logger.error({ err: error, entryId }, 'failed to process hello reservation');
          }
        }
      }
    }
  })();

  return async () => {
    running = false;
    await loop;
  };
}

async function handleReservation(
  { pool, bus, logger }: OrchestratorDeps,
  fields: HelloStreamFields,
): Promise<void> {
  const apiHop: HelloHop = {
    service: 'api',
    hostname: fields.hostname,
    at: fields.ts,
  };
  const orchestratorHop = makeHop('orchestrator', os.hostname());

  await pool.query(
    `INSERT INTO hello_messages (id, name, hops, status)
     VALUES ($1, $2, $3::jsonb, 'QUEUED')
     ON CONFLICT (id) DO NOTHING`,
    [fields.id, fields.name, JSON.stringify([apiHop, orchestratorHop])],
  );

  const message: HelloRequestMessage = {
    messageId: randomUUID(),
    type: 'hello.payment',
    helloId: fields.id,
    name: fields.name,
  };
  await bus.publish('payment-requests', message);
  logger.info({ helloId: fields.id }, 'hello reservation persisted and payment requested');
}

async function handleOrderEvent(deps: OrchestratorDeps, body: unknown): Promise<void> {
  const { pool, bus, logger } = deps;
  if (!isHelloResult(body)) {
    logger.warn({ body }, 'ignoring unknown order event');
    return;
  }

  const orchestratorHop = makeHop('orchestrator', os.hostname());

  if (body.type === 'hello.paid') {
    await pool.query(
      `UPDATE hello_messages
          SET hops = hops || $1::jsonb,
              updated_at = now()
        WHERE id = $2`,
      [JSON.stringify([body.hop, orchestratorHop]), body.helloId],
    );

    const message: HelloRequestMessage = {
      messageId: randomUUID(),
      type: 'hello.ship',
      helloId: body.helloId,
      name: body.name,
    };
    await bus.publish('shipping-requests', message);
    logger.info({ helloId: body.helloId }, 'hello paid; shipping requested');
    return;
  }

  await pool.query(
    `UPDATE hello_messages
        SET hops = hops || $1::jsonb,
            status = 'DONE',
            updated_at = now()
      WHERE id = $2`,
    [JSON.stringify([body.hop, orchestratorHop]), body.helloId],
  );
  logger.info({ helloId: body.helloId }, 'hello shipped; flow done');
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
    throw new Error(`Invalid hello stream entry: ${JSON.stringify(fields)}`);
  }

  return {
    id: fields.id,
    name: fields.name,
    hostname: fields.hostname ?? 'unknown',
    ts: fields.ts ?? new Date().toISOString(),
  };
}

function isHelloResult(body: unknown): body is HelloResultMessage {
  if (typeof body !== 'object' || body === null) return false;
  const candidate = body as Partial<HelloResultMessage>;
  return (
    (candidate.type === 'hello.paid' || candidate.type === 'hello.shipped') &&
    typeof candidate.helloId === 'string' &&
    typeof candidate.name === 'string' &&
    typeof candidate.hop === 'object' &&
    candidate.hop !== null &&
    typeof candidate.hop.service === 'string' &&
    typeof candidate.hop.hostname === 'string' &&
    typeof candidate.hop.at === 'string'
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

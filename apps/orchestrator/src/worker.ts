import os from 'node:os';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  completeOrderStub,
  persistReservation,
  resolveMigrationsDir,
  runMigrations,
  saleRedisKeys,
  type Logger,
  type Pool,
  type Redis,
  type StopFn,
} from '@fss/infra';

export const RESERVATIONS_GROUP = 'orchestrator';

export interface OrchestratorDeps {
  redis: Redis;
  pool: Pool;
  logger: Logger;
  saleId: string;
  migrationsDir?: string;
  /** Pending stream entries idle longer than this are reclaimed (SPEC §9: 30 s). */
  claimIdleMs?: number;
  claimIntervalMs?: number;
}

interface ReservationFields {
  reservationId: string;
  userId: string;
  ts: number;
}

type StreamEntry = [string, string[]];

class InvalidEntryError extends Error {}

export async function startOrchestrator(deps: OrchestratorDeps): Promise<StopFn> {
  const { redis, pool } = deps;
  const migrationsDir = await resolveMigrationsDir(deps.migrationsDir);
  await runMigrations(pool, migrationsDir);
  await ensureGroup(redis, deps.saleId);

  const consumer = `${os.hostname()}-${process.pid}`;
  return consumeReservations(deps, consumer);
}

async function ensureGroup(redis: Redis, saleId: string): Promise<void> {
  const { reservations } = saleRedisKeys(saleId);
  try {
    await redis.xgroup('CREATE', reservations, RESERVATIONS_GROUP, '0', 'MKSTREAM');
  } catch (error) {
    if (!(error instanceof Error) || !error.message.includes('BUSYGROUP')) {
      throw error;
    }
  }
}

function consumeReservations(deps: OrchestratorDeps, consumer: string): StopFn {
  const { redis, logger } = deps;
  const stream = saleRedisKeys(deps.saleId).reservations;
  const claimIdleMs = deps.claimIdleMs ?? 30_000;
  const claimIntervalMs = deps.claimIntervalMs ?? 5_000;
  let running = true;
  let lastClaim = 0;

  const handleEntries = async (entries: StreamEntry[]) => {
    for (const [entryId, rawFields] of entries) {
      if (!running) return;
      try {
        await handleReservation(deps, fieldsToObject(rawFields));
        await redis.xack(stream, RESERVATIONS_GROUP, entryId);
      } catch (error) {
        if (error instanceof InvalidEntryError) {
          logger.error({ err: error, entryId }, 'dropping invalid reservation stream entry');
          await redis.xack(stream, RESERVATIONS_GROUP, entryId);
          continue;
        }
        // Left pending; reclaimed by XAUTOCLAIM once idle for claimIdleMs.
        logger.error({ err: error, entryId }, 'failed to process reservation');
      }
    }
  };

  const loop = (async () => {
    while (running) {
      try {
        if (Date.now() - lastClaim >= claimIntervalMs) {
          lastClaim = Date.now();
          // Reclaims entries left pending by crashed consumers (including a
          // previous incarnation of this one) and entries whose handling failed.
          const [, claimed] = (await redis.xautoclaim(
            stream,
            RESERVATIONS_GROUP,
            consumer,
            claimIdleMs,
            '0-0',
            'COUNT',
            100,
          )) as [string, Array<StreamEntry | null>];
          const live = claimed.filter((entry): entry is StreamEntry => entry !== null);
          if (live.length) {
            logger.warn({ count: live.length }, 'reclaimed pending reservation stream entries');
            await handleEntries(live);
          }
        }

        const batches = (await redis.xreadgroup(
          'GROUP',
          RESERVATIONS_GROUP,
          consumer,
          'COUNT',
          10,
          'BLOCK',
          2_000,
          'STREAMS',
          stream,
          '>',
        )) as Array<[string, StreamEntry[]]> | null;

        for (const [, entries] of batches ?? []) {
          await handleEntries(entries);
        }
      } catch (error) {
        if (!running) return;
        logger.error({ err: error }, 'reservation stream read failed');
        await sleep(1_000);
      }
    }
  })();

  return async () => {
    running = false;
    await loop;
  };
}

/**
 * Persists the reservation, then applies the Phase 2 payment stub. Commit
 * happens before the stream entry is acked; if the orchestrator dies in
 * between, XAUTOCLAIM redelivers and both steps are idempotent.
 */
async function handleReservation(deps: OrchestratorDeps, fields: ReservationFields): Promise<void> {
  const { pool, logger, saleId } = deps;

  const inserted = await persistReservation(pool, {
    saleId,
    orderId: fields.reservationId,
    userId: fields.userId,
    reservedAt: fields.ts,
  });

  const completed = await completeOrderStub(pool, fields.reservationId);

  logger.info(
    {
      saleId,
      orderId: fields.reservationId,
      userId: fields.userId,
      inserted,
      completed,
    },
    inserted ? 'reservation persisted' : 'reservation redelivered',
  );
}

function fieldsToObject(raw: string[]): ReservationFields {
  const fields: Record<string, string> = {};
  for (let index = 0; index < raw.length; index += 2) {
    const key = raw[index];
    const value = raw[index + 1];
    if (key !== undefined && value !== undefined) {
      fields[key] = value;
    }
  }

  const reservationId = fields.reservationId;
  const userId = fields.userId;
  if (!reservationId || !userId) {
    throw new InvalidEntryError(`Invalid reservation stream entry: ${JSON.stringify(fields)}`);
  }
  if (!isUuid(reservationId)) {
    throw new InvalidEntryError(`Reservation id is not a UUID: ${reservationId}`);
  }

  const ts = Number(fields.ts ?? Date.now());
  return {
    reservationId,
    userId,
    ts: Number.isFinite(ts) ? ts : Date.now(),
  };
}

const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function isUuid(value: string): boolean {
  return uuidPattern.test(value);
}

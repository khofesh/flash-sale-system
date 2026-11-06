import os from 'node:os';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  QUEUE_FOR_MESSAGE,
  isSagaEnvelope,
  isSagaResultEvent,
  makeEnvelope,
  nextAction,
  type SagaMessageType,
  type SagaStep,
} from '@fss/core';
import {
  ORDER_EVENTS_CONSUMER,
  RedisReservationStore,
  cancelTimedOutPayments,
  getOrderById,
  isMessageProcessed,
  markMessageProcessed,
  persistReservation,
  recordSagaEvent,
  resolveMigrationsDir,
  runMigrations,
  saleRedisKeys,
  transition,
  type Logger,
  type MessageBus,
  type OrderRecord,
  type Pool,
  type Redis,
  type ReservationStore,
  type StopFn,
} from '@fss/infra';

export const RESERVATIONS_GROUP = 'orchestrator';

export interface OrchestratorDeps {
  redis: Redis;
  pool: Pool;
  logger: Logger;
  bus: MessageBus;
  saleId: string;
  migrationsDir?: string;
  /** Pending stream entries idle longer than this are reclaimed (SPEC §9: 30 s). */
  claimIdleMs?: number;
  claimIntervalMs?: number;
  /** Orders stuck in PAYMENT_PENDING longer than this are cancelled (SPEC §4.2). */
  paymentTimeoutMs?: number;
  /** How often the payment-timeout sweeper runs. */
  sweepIntervalMs?: number;
}

interface ReservationFields {
  reservationId: string;
  userId: string;
  ts: number;
}

type StreamEntry = [string, string[]];

class InvalidEntryError extends Error {}

export async function startOrchestrator(deps: OrchestratorDeps): Promise<StopFn> {
  const { redis, pool, logger } = deps;
  const migrationsDir = await resolveMigrationsDir(deps.migrationsDir);
  await runMigrations(pool, migrationsDir);
  await ensureGroup(redis, deps.saleId);

  const reservations = new RedisReservationStore(redis, logger);
  await reservations.loadScripts().catch((error: unknown) => {
    logger.warn({ err: error }, 'failed to preload reservation scripts');
  });

  const consumer = `${os.hostname()}-${process.pid}`;
  const stopStream = consumeReservations(deps, consumer, reservations);
  const stopEvents = await deps.bus.subscribe('order-events', (message) =>
    handleOrderEvent(deps, reservations, message.body),
  );
  const stopSweeper = startPaymentSweeper(deps, reservations);

  return async () => {
    await Promise.allSettled([stopStream(), stopEvents(), stopSweeper()]);
  };
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

function consumeReservations(
  deps: OrchestratorDeps,
  consumer: string,
  reservations: ReservationStore,
): StopFn {
  const { redis, logger } = deps;
  const stream = saleRedisKeys(deps.saleId).reservations;
  const claimIdleMs = deps.claimIdleMs ?? 30_000;
  const claimIntervalMs = deps.claimIntervalMs ?? 5_000;
  let running = true;
  let lastClaim = 0;
  // XAUTOCLAIM scans the pending list in pages; keep the cursor so entries past
  // the first page are reclaimed too.
  let claimCursor = '0-0';

  const handleEntries = async (entries: StreamEntry[]) => {
    for (const [entryId, rawFields] of entries) {
      if (!running) return;
      try {
        await handleReservation(deps, reservations, fieldsToObject(rawFields));
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
          const [next, claimed] = (await redis.xautoclaim(
            stream,
            RESERVATIONS_GROUP,
            consumer,
            claimIdleMs,
            claimCursor,
            'COUNT',
            100,
          )) as [string, Array<StreamEntry | null>];
          claimCursor = next;
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
        if (error instanceof Error && error.message.includes('NOGROUP')) {
          // The stream key was deleted (e.g. by hand); recreate the group.
          logger.warn('reservation consumer group is missing; recreating it');
          claimCursor = '0-0';
          await ensureGroup(redis, deps.saleId).catch((cause: unknown) => {
            logger.error({ err: cause }, 'failed to recreate reservation consumer group');
          });
          continue;
        }
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
 * Persists the reservation and starts the saga. Commit happens before the
 * stream entry is acked; if the orchestrator dies in between, XAUTOCLAIM
 * redelivers and every step is idempotent:
 * - the order insert is deduplicated by the reservation id,
 * - RESERVED -> PAYMENT_PENDING is a conditional UPDATE,
 * - payment.requested is at-least-once and payment is deterministic per order.
 *
 * A reservation that conflicts with a live order for the same user (the second
 * layer of the one-per-user rule) is never persisted; its unit goes back to
 * stock through `release.lua`, so `sold + available == initial_stock` keeps
 * holding.
 */
async function handleReservation(
  deps: OrchestratorDeps,
  reservations: ReservationStore,
  fields: ReservationFields,
): Promise<void> {
  const { pool, logger, saleId } = deps;
  const log = { saleId, orderId: fields.reservationId, userId: fields.userId };

  const outcome = await persistReservation(pool, {
    saleId,
    orderId: fields.reservationId,
    userId: fields.userId,
    reservedAt: fields.ts,
  });

  if (outcome === 'conflict') {
    const released = await reservations.release(saleId, fields.userId);
    logger.error(
      { ...log, released },
      'reservation conflicts with a live order for the user; unit returned',
    );
    return;
  }

  const order = await getOrderById(pool, fields.reservationId);
  if (!order) {
    logger.error(log, 'reservation persisted but the order could not be read');
    return;
  }

  await applySteps(
    deps,
    reservations,
    order,
    nextAction({ status: order.status }, { type: 'reservation.persisted' }),
  );
  logger.info(
    { ...log, outcome, status: order.status },
    outcome === 'inserted' ? 'reservation persisted' : 'reservation redelivered',
  );
}

/** Handles `payment.succeeded/failed/refunded` and `shipping.succeeded/failed`. */
async function handleOrderEvent(
  deps: OrchestratorDeps,
  reservations: ReservationStore,
  body: unknown,
): Promise<void> {
  const { pool, logger } = deps;

  if (!isSagaEnvelope(body)) {
    logger.warn('ignoring malformed order event');
    return;
  }
  if (!isSagaResultEvent(body.type)) {
    logger.warn({ type: body.type }, 'ignoring unsupported order event type');
    return;
  }

  if (await isMessageProcessed(pool, ORDER_EVENTS_CONSUMER, body.messageId)) {
    logger.debug({ messageId: body.messageId }, 'order event already processed');
    return;
  }

  const order = await getOrderById(pool, body.orderId);
  if (!order) {
    logger.error({ orderId: body.orderId }, 'order event references an unknown order');
    await markMessageProcessed(pool, ORDER_EVENTS_CONSUMER, body.messageId);
    return;
  }

  const steps = nextAction(
    { status: order.status },
    { type: body.type, payload: body.payload as Record<string, unknown> },
  );

  await applySteps(deps, reservations, order, steps);
  // Marked only after every side effect (Redis release, SQS publish) succeeded,
  // so a crash mid-handling leaves the message for redelivery.
  await markMessageProcessed(pool, ORDER_EVENTS_CONSUMER, body.messageId);
}

async function applySteps(
  deps: OrchestratorDeps,
  reservations: ReservationStore,
  initial: OrderRecord,
  steps: SagaStep[],
): Promise<void> {
  const { pool, logger } = deps;
  let order = initial;

  for (const step of steps) {
    switch (step.kind) {
      case 'transition': {
        const applied = await transition(pool, {
          orderId: order.id,
          from: step.from,
          to: step.to,
          type: step.type,
          payload: { orderId: order.id, from: step.from, to: step.to, ...step.patch },
          patch: step.patch,
        });

        if (!applied) {
          // The order moved on concurrently (or this is a redelivery). Stop
          // applying the rest of this plan: its preconditions are gone.
          logger.debug(
            { orderId: order.id, from: step.from, to: step.to },
            'conditional transition did not apply',
          );
          return;
        }

        order = {
          ...order,
          status: step.to,
          ...(step.patch?.paymentRef !== undefined ? { paymentRef: step.patch.paymentRef } : {}),
          ...(step.patch?.shipmentRef !== undefined ? { shipmentRef: step.patch.shipmentRef } : {}),
          ...(step.patch?.failureReason !== undefined
            ? { failureReason: step.patch.failureReason }
            : {}),
        };
        break;
      }

      case 'publish': {
        await publish(deps, order, step.type);
        break;
      }

      case 'release': {
        const released = await reservations.release(order.saleId, order.userId);
        logger.info(
          { orderId: order.id, userId: order.userId, reason: step.reason, released },
          released ? 'unit returned to stock' : 'unit was already released',
        );
        break;
      }
    }
  }
}

async function publish(deps: OrchestratorDeps, order: OrderRecord, type: SagaMessageType) {
  const payload: Record<string, unknown> = {
    saleId: order.saleId,
    userId: order.userId,
  };
  if (type === 'payment.refund_requested' && order.paymentRef) {
    payload.paymentRef = order.paymentRef;
  }

  const envelope = makeEnvelope(order.id, type, payload);
  await recordSagaEvent(deps.pool, order.id, type, payload);
  await deps.bus.publish(QUEUE_FOR_MESSAGE[type], envelope);
  deps.logger.info({ orderId: order.id, type }, 'saga message published');
}

/** Cancels PAYMENT_PENDING orders that outlived PAYMENT_TIMEOUT_MS. */
function startPaymentSweeper(deps: OrchestratorDeps, reservations: ReservationStore): StopFn {
  const { pool, logger } = deps;
  const timeoutMs = deps.paymentTimeoutMs ?? 60_000;
  const intervalMs = deps.sweepIntervalMs ?? 5_000;
  let running = true;

  const loop = (async () => {
    while (running) {
      try {
        const expired = await cancelTimedOutPayments(pool, timeoutMs);
        for (const order of expired) {
          const released = await reservations.release(order.saleId, order.userId);
          logger.warn(
            { orderId: order.id, saleId: order.saleId, released },
            'payment timed out; order cancelled and unit released',
          );
        }
      } catch (error) {
        if (!running) return;
        logger.error({ err: error }, 'payment timeout sweep failed');
      }

      await sleep(intervalMs);
    }
  })();

  return async () => {
    running = false;
    await loop;
  };
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

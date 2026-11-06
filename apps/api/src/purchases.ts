import { createHash, randomUUID } from 'node:crypto';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import type { FastifyInstance, RawServerDefault } from 'fastify';
import {
  FINAL_ORDER_STATUSES,
  SECURED_ORDER_STATUSES,
  normalizeUserId,
  purchaseResponse,
  type OrderStatus,
  type PurchaseCode,
  type PurchaseResponse,
  type ReserveOutcome,
  type ReserveResult,
} from '@fss/core';
import {
  getOrderByUser,
  type Logger,
  type Pool,
  type Redis,
  type ReservationStore,
} from '@fss/infra';
import type { AppMetrics } from './metrics.js';

export interface PurchaseRouteDeps {
  redis: Redis;
  pool: Pool;
  logger: Logger;
  saleId: string;
  reservations: ReservationStore;
  idempotencyTtlSeconds?: number;
  metrics?: AppMetrics;
  /**
   * How long a replica trusts its in-memory sold-out flag before checking Redis
   * again. Zero disables the short circuit (used by tests that reset a sale).
   */
  soldOutRecheckMs?: number;
}

export interface PurchaseRoutes {
  /** Forgets the sold-out flag, e.g. after an admin reset restores stock. */
  clearSoldOut(): void;
}

interface CachedResponse {
  status: number;
  body: { code: PurchaseCode; orderId?: string };
}

const PENDING = 'PENDING';
/**
 * Shorter than a retrying client's patience: a replica that dies holding a claim
 * must not lock the key for the whole response TTL. Should a slow request outlive
 * this, reserve.lua still prevents a second unit.
 */
const PENDING_TTL_SECONDS = 5;

/** Registers `POST /api/purchases` and `GET /api/purchases/:userId` (SPEC §6.2, §6.3). */
export function registerPurchaseRoutes(
  app: FastifyInstance<RawServerDefault, IncomingMessage, ServerResponse, Logger>,
  deps: PurchaseRouteDeps,
): PurchaseRoutes {
  const ttl = deps.idempotencyTtlSeconds ?? 600;
  const recheckMs = deps.soldOutRecheckMs ?? 1_000;
  // Every replica learns about sell-out from the script, then answers 410 here
  // until the next recheck. A release or refund can restore stock in between,
  // so the flag is only trusted for a second (SPEC §9.2).
  let soldOutAt: number | null = null;
  const isSoldOut = () =>
    recheckMs > 0 && soldOutAt !== null && performance.now() - soldOutAt < recheckMs;

  const markSoldOut = () => {
    soldOutAt = performance.now();
  };

  app.post<{ Body: { userId?: unknown } }>('/api/purchases', async (request, reply) => {
    const rawUserId = request.body?.userId;
    if (typeof rawUserId !== 'string') {
      return reply.code(400).send({ code: 'VALIDATION_ERROR', message: 'userId is required' });
    }

    const normalized = normalizeUserId(rawUserId);
    if (!normalized.ok) {
      return reply.code(400).send({ code: 'VALIDATION_ERROR', message: normalized.message });
    }
    const userId = normalized.value;

    const idempotencyKey = idempotencyCacheKey(
      deps.saleId,
      userId,
      request.headers['idempotency-key'],
    );

    if (idempotencyKey) {
      let cached: CachedResponse | null;
      try {
        cached = await readCachedResponse(deps.redis, idempotencyKey);
      } catch (error) {
        deps.logger.error({ err: error }, 'failed to read idempotency key');
        return reply.code(503).header('Retry-After', '1').send({ code: 'UNAVAILABLE' });
      }

      if (cached) {
        return reply.code(cached.status).send(cached.body);
      }
    }

    if (isSoldOut()) {
      // The script already said SOLD_OUT recently; every replica sheds the herd
      // without a Redis round trip (SPEC §9.2). No idempotency key is claimed:
      // the next request after the recheck gets the real answer.
      return reply.code(410).send({ code: 'SOLD_OUT' });
    }

    if (idempotencyKey) {
      const claimed = await deps.redis
        .set(idempotencyKey, PENDING, 'EX', Math.min(ttl, PENDING_TTL_SECONDS), 'NX')
        .catch((error: unknown) => {
          deps.logger.error({ err: error }, 'failed to claim idempotency key');
          return undefined;
        });

      if (claimed === undefined) {
        return reply.code(503).header('Retry-After', '1').send({ code: 'UNAVAILABLE' });
      }

      if (claimed !== 'OK') {
        // Another request with this key is still running. Wait briefly for its
        // response instead of reserving a second unit.
        const pending = await waitForCachedResponse(deps.redis, idempotencyKey);
        if (pending) {
          return reply.code(pending.status).send(pending.body);
        }
        return reply.code(503).header('Retry-After', '1').send({ code: 'UNAVAILABLE' });
      }
    }

    const { response, cacheable, outcome } = await executeReserve(deps, userId);
    deps.metrics?.reserveOutcomes.inc({ outcome });
    if (outcome === 'SOLD_OUT') markSoldOut();

    if (idempotencyKey) {
      if (cacheable) {
        await storeCachedResponse(deps.redis, deps.logger, idempotencyKey, response, ttl);
      } else {
        await deps.redis.del(idempotencyKey).catch((error: unknown) => {
          deps.logger.warn({ err: error }, 'failed to release idempotency key');
        });
      }
    }

    if (response.retryAfter !== undefined) {
      reply.header('Retry-After', String(response.retryAfter));
    }
    return reply.code(response.statusCode).send(response.body);
  });

  app.get<{ Params: { userId: string } }>('/api/purchases/:userId', async (request, reply) => {
    const normalized = normalizeUserId(request.params.userId);
    if (!normalized.ok) {
      return reply.code(400).send({ code: 'VALIDATION_ERROR', message: normalized.message });
    }
    const userId = normalized.value;

    let order;
    try {
      order = await getOrderByUser(deps.pool, deps.saleId, userId);
    } catch (error) {
      // Postgres is allowed to be down on the read path: fall back to Redis,
      // which knows every reservation the orchestrator has not persisted yet.
      deps.logger.warn({ err: error }, 'failed to read order from postgres; falling back to redis');
    }

    // If a different reservation holds the user's unit, they bought again and the
    // orchestrator has not persisted it yet: Redis knows first. A holder equal to
    // this order only means its compensation is still in progress.
    if (order && !SECURED_ORDER_STATUSES.has(order.status)) {
      try {
        const holder = await deps.reservations.holder(deps.saleId, userId);
        if (holder && holder !== order.id) {
          return { userId, secured: true, status: 'RESERVED' as OrderStatus };
        }
      } catch (error) {
        deps.logger.warn({ err: error }, 'failed to read reservation holder from redis');
      }
    }

    if (order) {
      return {
        userId,
        secured: SECURED_ORDER_STATUSES.has(order.status),
        orderId: order.id,
        status: order.status,
        updatedAt: order.updatedAt.toISOString(),
        ...(order.failureReason ? { failureReason: order.failureReason } : {}),
        ...(FINAL_ORDER_STATUSES.has(order.status) ? { final: true } : {}),
      };
    }

    let isBuyer: boolean;
    try {
      isBuyer = await deps.reservations.isBuyer(deps.saleId, userId);
    } catch (error) {
      deps.logger.error({ err: error }, 'failed to read buyers set from redis');
      return reply.code(503).header('Retry-After', '1').send({ code: 'UNAVAILABLE' });
    }

    if (isBuyer) {
      // The orchestrator has not persisted the reservation yet.
      return { userId, secured: true, status: 'RESERVED' as OrderStatus };
    }

    return reply.code(404).send({ userId, secured: false });
  });

  return {
    clearSoldOut: () => {
      soldOutAt = null;
    },
  };
}

async function executeReserve(
  deps: PurchaseRouteDeps,
  userId: string,
): Promise<{ response: PurchaseResponse; cacheable: boolean; outcome: ReserveOutcome }> {
  let result: ReserveResult;
  try {
    result = await deps.reservations.reserve(deps.saleId, userId, randomUUID());
  } catch (error) {
    // Fail closed: without Redis there is no safe way to decide a purchase.
    deps.logger.error({ err: error }, 'reserve script failed');
    return {
      response: purchaseResponse({ status: 'UNAVAILABLE' }),
      cacheable: false,
      outcome: 'UNAVAILABLE',
    };
  }

  let orderId: string | undefined;
  if (result.status === 'ALREADY_PURCHASED') {
    try {
      orderId = (await getOrderByUser(deps.pool, deps.saleId, userId))?.id;
    } catch (error) {
      // The reservation is still valid; the order id is only a convenience.
      deps.logger.warn({ err: error }, 'failed to look up existing order id');
    }
  }

  const response = purchaseResponse(result, orderId);
  return { response, cacheable: response.statusCode !== 503, outcome: result.status };
}

function idempotencyCacheKey(
  saleId: string,
  userId: string,
  header: string | string[] | undefined,
): string | null {
  const value = Array.isArray(header) ? header[0] : header;
  if (typeof value !== 'string' || value.length === 0) return null;

  const digest = createHash('sha256')
    .update(`${saleId}\u0000${userId}\u0000${value}`)
    .digest('hex');
  return `idempotency:${digest}`;
}

async function readCachedResponse(redis: Redis, key: string): Promise<CachedResponse | null> {
  const raw = await redis.get(key);
  if (!raw || raw === PENDING) return null;

  try {
    const parsed = JSON.parse(raw) as Partial<CachedResponse>;
    if (typeof parsed.status === 'number' && parsed.body && typeof parsed.body.code === 'string') {
      return { status: parsed.status, body: parsed.body };
    }
  } catch {
    // Handled below.
  }
  // Corrupt cache entry: drop it so this request can claim the key instead of
  // waiting on a response that will never arrive. The reserve script still
  // guards against a second unit for the same user.
  await redis.del(key);
  return null;
}

async function waitForCachedResponse(
  redis: Redis,
  key: string,
  attempts = 20,
): Promise<CachedResponse | null> {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    await sleep(100);
    try {
      const cached = await readCachedResponse(redis, key);
      if (cached) return cached;
    } catch {
      // Redis is unavailable; the caller will fail closed with 503.
      return null;
    }
  }
  return null;
}

async function storeCachedResponse(
  redis: Redis,
  logger: Logger,
  key: string,
  response: PurchaseResponse,
  ttl: number,
): Promise<void> {
  try {
    await redis.set(
      key,
      JSON.stringify({ status: response.statusCode, body: response.body }),
      'EX',
      ttl,
    );
  } catch (error) {
    // The purchase already happened; a cache failure must not turn it into an
    // error. Drop the PENDING claim so retries are not stuck behind it; the
    // reserve script answers them with ALREADY_PURCHASED.
    logger.warn({ err: error }, 'failed to store idempotent response');
    await redis.del(key).catch(() => undefined);
  }
}

import { createHash, timingSafeEqual } from 'node:crypto';
import Fastify from 'fastify';
import { deriveSaleStatus, type SaleStatus } from '@fss/core';
import {
  MAX_TOTAL_STOCK,
  RedisReservationStore,
  SaleHasOrdersError,
  getSaleSnapshot,
  reconcileSale,
  resetSale,
  type Logger,
  type Pool,
  type Redis,
  type ReservationStore,
  type SaleSnapshot,
} from '@fss/infra';
import type { AppMetrics } from './metrics.ts';
import { registerPurchaseRoutes } from './purchases.ts';

interface BuildAppDeps {
  redis: Redis;
  pool: Pool;
  logger: Logger;
  saleId?: string;
  productId?: string;
  /** Enables `POST /admin/sale`. When unset, the admin routes are not registered. */
  adminToken?: string;
  saleCacheMs?: number;
  /** Used by the purchase routes; defaults to the Redis-backed store. */
  reservations?: ReservationStore;
  /** When set, request metrics and the `/metrics` scrape route are registered. */
  metrics?: AppMetrics;
  /** Refreshes the operational gauges (stream lag, DLQ depth) before a scrape. */
  refreshMetrics?: () => Promise<void>;
  /** How long a replica answers 410 from its sold-out flag without touching Redis. */
  soldOutRecheckMs?: number;
}

interface SaleView {
  saleId: string;
  status: SaleStatus;
  startsAt: string;
  endsAt: string;
  remainingStock: number;
  totalStock: number;
  serverTime: string;
}

interface AdminSaleBody {
  saleId?: string;
  productId?: string;
  startsAt: string;
  endsAt: string;
  totalStock: number;
}

export function buildApp({
  redis,
  pool,
  logger,
  saleId = 'launch-2026',
  productId = 'flash-tshirt',
  adminToken,
  saleCacheMs = 250,
  reservations = new RedisReservationStore(redis),
  metrics,
  refreshMetrics,
  soldOutRecheckMs,
}: BuildAppDeps) {
  const app = Fastify({ loggerInstance: logger });

  if (metrics) {
    app.addHook('onResponse', async (request, reply) => {
      const labels = {
        method: request.method,
        // The pattern (`/api/purchases/:userId`) keeps the label set bounded.
        route: request.routeOptions.url ?? 'unmatched',
        status: String(reply.statusCode),
      };
      metrics.httpRequests.inc(labels);
      metrics.httpRequestDuration.observe(labels, reply.elapsedTime / 1_000);
    });

    app.get('/metrics', async (_request, reply) => {
      if (refreshMetrics) {
        await refreshMetrics().catch((error: unknown) => {
          logger.warn({ err: error }, 'failed to refresh operational metrics');
        });
      }
      return reply
        .header('Cache-Control', 'no-store')
        .type(metrics.registry.contentType)
        .send(await metrics.registry.metrics());
    });
  }

  // `at` is a local monotonic timestamp, used only to age the cached Redis time.
  interface CachedSale {
    at: number;
    sale: SaleSnapshot | null;
  }
  let saleCache: CachedSale | null = null;
  let saleInflight: Promise<CachedSale> | null = null;
  let saleGeneration = 0;

  // Concurrent cache misses share one Redis round trip (SPEC §6.1).
  const recentSale = (): Promise<CachedSale> => {
    if (saleCache && performance.now() - saleCache.at < saleCacheMs) {
      return Promise.resolve(saleCache);
    }
    if (saleInflight) return saleInflight;

    const generation = saleGeneration;
    const inflight = getSaleSnapshot(redis, saleId)
      .then((sale) => {
        const cached = { at: performance.now(), sale };
        if (generation === saleGeneration) saleCache = cached;
        return cached;
      })
      .finally(() => {
        if (saleInflight === inflight) saleInflight = null;
      });
    saleInflight = inflight;
    return inflight;
  };

  const purchases = registerPurchaseRoutes(app, {
    redis,
    pool,
    logger,
    saleId,
    reservations,
    metrics,
    soldOutRecheckMs,
  });

  const invalidateSale = () => {
    saleGeneration += 1;
    saleCache = null;
    saleInflight = null;
    // A reset can restore stock on this replica, so its sold-out flag is stale.
    purchases.clearSoldOut();
  };

  app.get('/healthz', async () => ({ status: 'ok' }));

  app.get('/readyz', async (_request, reply) => {
    let redisOk = false;
    let postgresOk = false;

    try {
      await redis.ping();
      redisOk = true;
    } catch (error) {
      logger.warn({ err: error }, 'redis readiness check failed');
    }

    try {
      await pool.query('SELECT 1');
      postgresOk = true;
    } catch (error) {
      logger.warn({ err: error }, 'postgres readiness check failed');
    }

    if (!redisOk || !postgresOk) {
      reply.code(503);
    }

    return {
      redis: redisOk ? 'ok' : 'error',
      postgres: postgresOk ? 'ok' : 'error',
    };
  });

  app.get('/api/sale', async (_request, reply) => {
    let cached: CachedSale;

    try {
      cached = await recentSale();
    } catch (error) {
      logger.error({ err: error }, 'failed to read sale from redis');
      return reply.code(503).header('Retry-After', '1').send({ code: 'UNAVAILABLE' });
    }

    const { sale } = cached;
    if (!sale) {
      return reply.code(404).send({ code: 'SALE_NOT_FOUND' });
    }

    // Redis TIME, aged by how long the snapshot has been cached, so every
    // replica agrees with reserve.lua regardless of host clock drift (SPEC §4.1).
    const now = sale.serverTime + Math.round(performance.now() - cached.at);

    const view: SaleView = {
      saleId: sale.id,
      status: deriveSaleStatus(
        { startsAt: sale.startsAt, endsAt: sale.endsAt },
        sale.remainingStock,
        now,
      ),
      startsAt: new Date(sale.startsAt).toISOString(),
      endsAt: new Date(sale.endsAt).toISOString(),
      remainingStock: sale.remainingStock,
      totalStock: sale.totalStock,
      serverTime: new Date(now).toISOString(),
    };

    return reply.header('Cache-Control', 'max-age=1').send(view);
  });

  if (adminToken) {
    const expectedTokenHash = sha256(adminToken);

    app.post<{ Body: AdminSaleBody }>(
      '/admin/sale',
      {
        onRequest: async (request, reply) => {
          if (!tokenMatches(request.headers['x-admin-token'], expectedTokenHash)) {
            return reply.code(401).send({ code: 'UNAUTHORIZED' });
          }
        },
        schema: {
          body: {
            type: 'object',
            additionalProperties: false,
            required: ['startsAt', 'endsAt', 'totalStock'],
            properties: {
              saleId: { type: 'string', minLength: 1, maxLength: 100 },
              productId: { type: 'string', minLength: 1, maxLength: 100 },
              startsAt: { type: 'string', format: 'date-time' },
              endsAt: { type: 'string', format: 'date-time' },
              totalStock: { type: 'integer', minimum: 0, maximum: MAX_TOTAL_STOCK },
            },
          },
        },
      },
      async (request, reply) => {
        const { startsAt, endsAt, totalStock } = request.body;
        const start = new Date(startsAt);
        const end = new Date(endsAt);
        if (
          Number.isNaN(start.getTime()) ||
          Number.isNaN(end.getTime()) ||
          end.getTime() <= start.getTime()
        ) {
          return reply
            .code(400)
            .send({ code: 'VALIDATION_ERROR', message: 'endsAt must be after startsAt' });
        }

        const targetSaleId = request.body.saleId ?? saleId;
        try {
          await resetSale(pool, redis, {
            id: targetSaleId,
            productId: request.body.productId ?? productId,
            startsAt: start,
            endsAt: end,
            totalStock,
          });
        } catch (error) {
          if (error instanceof SaleHasOrdersError) {
            return reply.code(409).send({ code: 'SALE_HAS_ORDERS', message: error.message });
          }
          if (isPgDataError(error)) {
            return reply.code(400).send({ code: 'VALIDATION_ERROR', message: error.message });
          }
          logger.error({ err: error, saleId: targetSaleId }, 'failed to reset sale');
          return reply.code(503).header('Retry-After', '1').send({ code: 'UNAVAILABLE' });
        }

        if (targetSaleId === saleId) {
          invalidateSale();
        }

        logger.info(
          { saleId: targetSaleId, startsAt, endsAt, totalStock },
          'sale created or reset',
        );

        return reply.code(200).send({
          saleId: targetSaleId,
          startsAt: start.toISOString(),
          endsAt: end.toISOString(),
          totalStock,
          remainingStock: totalStock,
        });
      },
    );

    app.get('/admin/reconcile', {
      onRequest: async (request, reply) => {
        if (!tokenMatches(request.headers['x-admin-token'], expectedTokenHash)) {
          return reply.code(401).send({ code: 'UNAUTHORIZED' });
        }
      },
      handler: async (_request, reply) => {
        try {
          const report = await reconcileSale(pool, redis, saleId);
          if (!report) {
            return reply.code(404).send({ code: 'SALE_NOT_FOUND' });
          }
          if (!report.settled) {
            // Traffic never paused long enough for a consistent read: not drift.
            return reply.code(503).header('Retry-After', '1').send(report);
          }
          return reply.code(report.ok ? 200 : 409).send(report);
        } catch (error) {
          logger.error({ err: error, saleId }, 'reconcile failed');
          return reply.code(503).header('Retry-After', '1').send({ code: 'UNAVAILABLE' });
        }
      },
    });
  }

  return app;
}

function sha256(value: string): Buffer {
  return createHash('sha256').update(value).digest();
}

/** Constant-time token check; hashing first makes the lengths equal. */
function tokenMatches(given: unknown, expectedHash: Buffer): boolean {
  return typeof given === 'string' && timingSafeEqual(sha256(given), expectedHash);
}

/** Postgres SQLSTATE class 22 (data exception), e.g. a value out of range: a client error. */
function isPgDataError(error: unknown): error is Error {
  if (!(error instanceof Error)) return false;
  const { code } = error as { code?: unknown };
  return typeof code === 'string' && code.startsWith('22');
}

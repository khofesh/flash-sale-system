import { loadConfig } from '@fss/config';
import {
  onShutdown,
  createLogger,
  createPgPool,
  createRedis,
  createSqsClient,
  getSaleSnapshot,
  readDeadLetterDepths,
  redisReady,
  RedisReservationStore,
  saleRedisKeys,
  seedSale,
} from '@fss/infra';
import { buildApp } from './app.js';
import { createAppMetrics } from './metrics.js';

const config = loadConfig();
const service = config.SERVICE_NAME === 'unknown' ? 'api' : config.SERVICE_NAME;
const logger = createLogger(service, config.LOG_LEVEL);

const redis = createRedis(config.REDIS_URL, { failFast: true, autoPipelining: true });
redis.on('error', (error) => logger.warn({ err: error }, 'redis connection error'));
const pool = createPgPool(config.DATABASE_URL, logger, { max: config.PG_POOL_MAX });
const sqs = createSqsClient({
  endpoint: config.SQS_ENDPOINT,
  region: config.AWS_REGION,
  accessKeyId: config.AWS_ACCESS_KEY_ID,
  secretAccessKey: config.AWS_SECRET_ACCESS_KEY,
});
const metrics = createAppMetrics();

try {
  await redisReady(redis);
} catch (error) {
  // Exits so the restart policy retries; without Redis no request can be served.
  logger.error({ err: error }, 'redis is unavailable at startup');
  await pool.end();
  process.exit(1);
}

try {
  const { sale, created, stockMissing } = await seedSale(pool, redis, {
    id: config.SALE_ID,
    productId: config.SALE_PRODUCT_ID,
    startsAt: config.SALE_STARTS_AT,
    endsAt: config.SALE_ENDS_AT,
    totalStock: config.SALE_TOTAL_STOCK,
  });
  logger.info(
    {
      saleId: sale.id,
      startsAt: sale.startsAt,
      endsAt: sale.endsAt,
      totalStock: sale.totalStock,
      created,
    },
    'sale seeded',
  );
  if (stockMissing) {
    // Fail closed: re-initialising stock here could restore sold units.
    logger.error(
      { saleId: sale.id },
      'redis stock is missing for an existing sale; it stays unavailable until reset or the redis rebuild runs',
    );
  }
} catch (error) {
  // Purchases only need Redis (SPEC §9.3), so a replica restarted during a
  // Postgres outage can still serve a sale that is already there.
  const existing = await getSaleSnapshot(redis, config.SALE_ID).catch(() => null);
  if (!existing) {
    logger.error({ err: error }, 'failed to seed sale');
    await redis.quit().catch(() => undefined);
    await pool.end();
    process.exit(1);
  }
  logger.warn({ err: error, saleId: existing.id }, 'seed failed; serving the sale in redis');
}

const reservations = new RedisReservationStore(redis, logger);
try {
  await reservations.loadScripts();
} catch (error) {
  logger.error({ err: error }, 'failed to preload redis scripts');
}

/**
 * The orchestrator owns the stream and the broker, but a scrape reaches any
 * replica; reading them here shows the same global state from every replica.
 */
async function refreshMetrics(): Promise<void> {
  const { reservations } = saleRedisKeys(config.SALE_ID);
  try {
    const groups = (await redis.xinfo('GROUPS', reservations)) as unknown[][];
    metrics.streamLag.reset();
    metrics.streamPending.reset();
    for (const fields of groups) {
      const field = (name: string) => fields[fields.indexOf(name) + 1];
      const group = String(field('name') ?? 'unknown');
      metrics.streamLag.set({ group }, Number(field('lag') ?? 0));
      metrics.streamPending.set({ group }, Number(field('pending') ?? 0));
    }
  } catch (error) {
    // No stream yet, or Redis is down: the other gauges are still useful.
    logger.warn({ err: error, saleId: config.SALE_ID }, 'failed to read reservation stream lag');
  }

  try {
    const depths = await readDeadLetterDepths(sqs);
    metrics.dlqDepth.reset();
    for (const [queue, depth] of Object.entries(depths)) {
      metrics.dlqDepth.set({ queue }, depth);
    }
  } catch (error) {
    logger.warn({ err: error }, 'failed to read dead-letter queue depth');
  }
}

const app = buildApp({
  redis,
  pool,
  logger,
  saleId: config.SALE_ID,
  productId: config.SALE_PRODUCT_ID,
  adminToken: config.ADMIN_TOKEN,
  reservations,
  metrics,
  refreshMetrics,
  soldOutRecheckMs: config.SOLD_OUT_RECHECK_MS,
});

onShutdown(logger, async () => {
  await app.close();
  await redis.quit().catch(() => undefined);
  await pool.end();
  sqs.destroy();
});

try {
  await app.listen({ host: config.HOST, port: config.PORT });
} catch (error) {
  logger.error({ err: error }, 'failed to start api');
  process.exit(1);
}

import { loadConfig } from '@fss/config';
import {
  onShutdown,
  createLogger,
  createPgPool,
  createRedis,
  getSaleSnapshot,
  redisReady,
  RedisReservationStore,
  seedSale,
} from '@fss/infra';
import { buildApp } from './app.js';

const config = loadConfig();
const service = config.SERVICE_NAME === 'unknown' ? 'api' : config.SERVICE_NAME;
const logger = createLogger(service, config.LOG_LEVEL);

const redis = createRedis(config.REDIS_URL, { failFast: true });
redis.on('error', (error) => logger.warn({ err: error }, 'redis connection error'));
const pool = createPgPool(config.DATABASE_URL, logger);

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

const app = buildApp({
  redis,
  pool,
  logger,
  saleId: config.SALE_ID,
  productId: config.SALE_PRODUCT_ID,
  adminToken: config.ADMIN_TOKEN,
  reservations,
});

onShutdown(logger, async () => {
  await app.close();
  await redis.quit().catch(() => undefined);
  await pool.end();
});

try {
  await app.listen({ host: config.HOST, port: config.PORT });
} catch (error) {
  logger.error({ err: error }, 'failed to start api');
  process.exit(1);
}

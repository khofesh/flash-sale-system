import { loadConfig } from '@fss/config';
import { onShutdown, createLogger, createPgPool, createRedis, seedSale } from '@fss/infra';
import { buildApp } from './app.js';

const config = loadConfig();
const service = config.SERVICE_NAME === 'unknown' ? 'api' : config.SERVICE_NAME;
const logger = createLogger(service, config.LOG_LEVEL);

const redis = createRedis(config.REDIS_URL);
const pool = createPgPool(config.DATABASE_URL);

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
  logger.error({ err: error }, 'failed to seed sale');
  await redis.quit();
  await pool.end();
  process.exit(1);
}

const app = buildApp({
  redis,
  pool,
  logger,
  saleId: config.SALE_ID,
  productId: config.SALE_PRODUCT_ID,
  adminToken: config.ADMIN_TOKEN,
});

onShutdown(logger, async () => {
  await app.close();
  await redis.quit();
  await pool.end();
});

try {
  await app.listen({ host: config.HOST, port: config.PORT });
} catch (error) {
  logger.error({ err: error }, 'failed to start api');
  process.exit(1);
}

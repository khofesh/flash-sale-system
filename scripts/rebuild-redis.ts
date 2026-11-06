import { loadConfig } from '@fss/config';
import { createPgPool, createRedis, rebuildRedis } from '@fss/infra';

const config = loadConfig();
const saleId = process.argv[2] ?? config.SALE_ID;
const redis = createRedis(config.REDIS_URL);
const pool = createPgPool(config.DATABASE_URL);

try {
  const report = await rebuildRedis(pool, redis, saleId);
  console.log(
    `rebuilt ${saleId}: stock=${report.stock}/${report.totalStock} buyers=${report.buyers} ` +
      `live=${report.liveOrders} pendingReleases=${report.pendingReleases} ` +
      `unpersisted=${report.unpersisted}`,
  );
} catch (error) {
  console.error(`\nFAIL — ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
} finally {
  await redis.quit().catch(() => undefined);
  await pool.end().catch(() => undefined);
}

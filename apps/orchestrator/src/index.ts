import { loadConfig } from '@fss/config';
import { onShutdown, createLogger, createPgPool, createRedis } from '@fss/infra';
import { startOrchestrator } from './worker.js';

const config = loadConfig();
const service = config.SERVICE_NAME === 'unknown' ? 'orchestrator' : config.SERVICE_NAME;
const logger = createLogger(service, config.LOG_LEVEL);

const redis = createRedis(config.REDIS_URL);
const pool = createPgPool(config.DATABASE_URL);

const stop = await startOrchestrator({
  redis,
  pool,
  logger,
  saleId: config.SALE_ID,
});

onShutdown(logger, async () => {
  await stop();
  await redis.quit();
  await pool.end();
});

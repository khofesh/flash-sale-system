import { loadConfig } from '@fss/config';
import { onShutdown, createLogger, createPgPool, createRedis } from '@fss/infra';
import { buildApp } from './app.js';

const config = loadConfig();
const service = config.SERVICE_NAME === 'unknown' ? 'api' : config.SERVICE_NAME;
const logger = createLogger(service, config.LOG_LEVEL);

const redis = createRedis(config.REDIS_URL);
const pool = createPgPool(config.DATABASE_URL);
const app = buildApp({ redis, pool, logger });

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

import { loadConfig } from '@fss/config';
import { createLogger, createPgPool, createRedis } from '@fss/infra';
import { buildApp } from './app.js';

const config = loadConfig();
const service = config.SERVICE_NAME === 'unknown' ? 'api' : config.SERVICE_NAME;
const logger = createLogger(service, config.LOG_LEVEL);

const redis = createRedis(config.REDIS_URL);
const pool = createPgPool(config.DATABASE_URL);
const app = buildApp({ redis, pool, logger });

let shuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.info({ signal }, 'shutting down');

  const timeout = setTimeout(() => {
    logger.error('graceful shutdown timed out');
    process.exit(1);
  }, 10_000);

  try {
    await app.close();
    await redis.quit();
    await pool.end();
  } catch (error) {
    logger.error({ err: error }, 'shutdown failed');
    process.exitCode = 1;
  } finally {
    clearTimeout(timeout);
    process.exit();
  }
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

try {
  await app.listen({ host: config.HOST, port: config.PORT });
} catch (error) {
  logger.error({ err: error }, 'failed to start api');
  process.exit(1);
}

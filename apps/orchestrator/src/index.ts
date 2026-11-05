import { loadConfig } from '@fss/config';
import { createLogger, createPgPool, createRedis, createSqsClient, SqsBus } from '@fss/infra';
import { startOrchestrator } from './worker.js';

const config = loadConfig();
const service = config.SERVICE_NAME === 'unknown' ? 'orchestrator' : config.SERVICE_NAME;
const logger = createLogger(service, config.LOG_LEVEL);

const redis = createRedis(config.REDIS_URL);
const pool = createPgPool(config.DATABASE_URL);
const bus = new SqsBus(
  createSqsClient({
    endpoint: config.SQS_ENDPOINT,
    region: config.AWS_REGION,
    accessKeyId: config.AWS_ACCESS_KEY_ID,
    secretAccessKey: config.AWS_SECRET_ACCESS_KEY,
  }),
  logger,
);

const stop = await startOrchestrator({ redis, pool, bus, logger });

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
    await stop();
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

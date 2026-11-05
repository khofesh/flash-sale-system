import { loadConfig } from '@fss/config';
import { createLogger, createSqsClient, SqsBus } from '@fss/infra';
import { startPayment } from './worker.js';

const config = loadConfig();
const service = config.SERVICE_NAME === 'unknown' ? 'payment' : config.SERVICE_NAME;
const logger = createLogger(service, config.LOG_LEVEL);

const bus = new SqsBus(
  createSqsClient({
    endpoint: config.SQS_ENDPOINT,
    region: config.AWS_REGION,
    accessKeyId: config.AWS_ACCESS_KEY_ID,
    secretAccessKey: config.AWS_SECRET_ACCESS_KEY,
  }),
  logger,
);

const stop = await startPayment({ bus, logger, latencyMs: config.PAYMENT_LATENCY_MS });

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

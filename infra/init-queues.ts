import { setTimeout as sleep } from 'node:timers/promises';
import { loadConfig } from '@fss/config';
import { createLogger, createSqsClient, initQueues } from '@fss/infra';

const config = loadConfig();
const logger = createLogger('queue-init', config.LOG_LEVEL);

const client = createSqsClient({
  endpoint: config.SQS_ENDPOINT,
  region: config.AWS_REGION,
  accessKeyId: config.AWS_ACCESS_KEY_ID,
  secretAccessKey: config.AWS_SECRET_ACCESS_KEY,
});

const maxAttempts = 30;

for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
  try {
    const queues = await initQueues(client);
    logger.info({ queues }, 'queues and DLQs ready');
    process.exit(0);
  } catch (error) {
    logger.warn({ err: error, attempt, maxAttempts }, 'queue initialization failed; retrying');
    await sleep(1_000);
  }
}

logger.error('queue initialization failed after all attempts');
process.exit(1);

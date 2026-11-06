import { loadConfig } from '@fss/config';
import {
  onShutdown,
  createLogger,
  createPgPool,
  createRedis,
  createSqsClient,
  SqsBus,
} from '@fss/infra';
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

const stop = await startOrchestrator({
  redis,
  pool,
  logger,
  bus,
  saleId: config.SALE_ID,
  paymentTimeoutMs: config.PAYMENT_TIMEOUT_MS,
});

onShutdown(logger, async () => {
  await stop();
  await redis.quit();
  await pool.end();
});

import { loadConfig } from '@fss/config';
import { onShutdown, createLogger, createSqsClient, SqsBus } from '@fss/infra';
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

const stop = await startPayment({
  bus,
  logger,
  latencyMs: config.PAYMENT_LATENCY_MS,
  failureRate: config.PAYMENT_FAILURE_RATE,
  refundLatencyMs: Math.min(config.PAYMENT_LATENCY_MS, 200),
  concurrency: config.PAYMENT_CONCURRENCY,
});

onShutdown(logger, async () => {
  await stop();
});

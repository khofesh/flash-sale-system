import { loadConfig } from '@fss/config';
import { onShutdown, createLogger, createSqsClient, SqsBus } from '@fss/infra';
import { startShipping } from './worker.ts';

const config = loadConfig();
const service = config.SERVICE_NAME === 'unknown' ? 'shipping' : config.SERVICE_NAME;
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

const stop = await startShipping({
  bus,
  logger,
  latencyMs: config.SHIPPING_LATENCY_MS,
  failureRate: config.SHIPPING_FAILURE_RATE,
  concurrency: config.SHIPPING_CONCURRENCY,
});

onShutdown(logger, async () => {
  await stop();
});

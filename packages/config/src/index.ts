import { z } from 'zod';

const logLevels = ['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'] as const;

const schema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  LOG_LEVEL: z.enum(logLevels).default('info'),
  SERVICE_NAME: z.string().min(1).default('unknown'),
  HOST: z.string().min(1).default('0.0.0.0'),
  PORT: z.coerce.number().int().positive().default(3000),
  REDIS_URL: z.string().min(1).default('redis://localhost:6379'),
  DATABASE_URL: z.string().min(1).default('postgres://flash:flash@localhost:5432/flash'),
  SQS_ENDPOINT: z.url().default('http://localhost:4566'),
  AWS_REGION: z.string().min(1).default('us-east-1'),
  AWS_ACCESS_KEY_ID: z.string().min(1).default('test'),
  AWS_SECRET_ACCESS_KEY: z.string().min(1).default('test'),
  PAYMENT_LATENCY_MS: z.coerce.number().int().nonnegative().default(200),
  PAYMENT_FAILURE_RATE: z.coerce.number().min(0).max(1).default(0.1),
  PAYMENT_TIMEOUT_MS: z.coerce.number().int().positive().default(60_000),
  SHIPPING_LATENCY_MS: z.coerce.number().int().nonnegative().default(100),
  SHIPPING_FAILURE_RATE: z.coerce.number().min(0).max(1).default(0.02),
  SALE_ID: z.string().min(1).default('launch-2026'),
  SALE_PRODUCT_ID: z.string().min(1).default('flash-tshirt'),
  SALE_STARTS_AT: optionalDate(),
  SALE_ENDS_AT: optionalDate(),
  SALE_TOTAL_STOCK: z.coerce.number().int().nonnegative().max(2_147_483_647).default(100),
  // No default: admin routes are only enabled when a token is set explicitly.
  ADMIN_TOKEN: z.preprocess((v) => (v === '' ? undefined : v), z.string().min(1).optional()),
  MIGRATIONS_DIR: z.string().min(1).optional(),
});

function optionalDate() {
  return z.preprocess(
    (value) => (value === '' ? undefined : value),
    z
      .string()
      .refine((value) => !Number.isNaN(Date.parse(value)), 'must be an ISO date')
      .optional(),
  );
}

export type Config = z.infer<typeof schema>;

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = schema.safeParse(env);
  if (!parsed.success) {
    const details = parsed.error.issues
      .map((issue) => `${issue.path.join('.') || '(root)'}: ${issue.message}`)
      .join('; ');
    throw new Error(`Invalid configuration: ${details}`);
  }
  return parsed.data;
}

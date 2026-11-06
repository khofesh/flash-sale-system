import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import { resolve } from 'node:path';
import { loadConfig } from '@fss/config';
import { createPgPool, createRedis, resetSale, saleRedisKeys } from '@fss/infra';
import { verifyStack } from '../stress/verify.js';

const scenario = process.argv.slice(2).find((argument) => argument !== '--') ?? 'smoke';
const scenarios = {
  smoke: { stock: 100_000, iterations: 0 },
  spike: { stock: 1_000, iterations: 10_000 },
  'same-user': { stock: 100, iterations: 2_000 },
  soak: { stock: 100_000, iterations: 0, rate: 2_000, duration: '5m' },
} as const;

type ScenarioName = keyof typeof scenarios;

if (!(scenario in scenarios)) {
  console.error(
    `Unknown stress scenario "${scenario}". Available: ${Object.keys(scenarios).join(', ')}`,
  );
  process.exit(1);
}

const name = scenario as ScenarioName;
const settings = scenarios[name];
const config = loadConfig();
const saleId = config.SALE_ID;
const baseUrl = process.env.BASE_URL ?? 'http://host.docker.internal:8080';
const scriptPath = resolve(process.cwd(), 'stress', `${name}.js`);
const resultsDir = resolve(process.cwd(), 'stress/results');
const stamp = new Date().toISOString().replace(/[:.]/g, '-');
const resultBase = `${resultsDir}/${stamp}-${name}`;

await prepareSale();

mkdirSync(resultsDir, { recursive: true });
writeFileSync(
  `${resultBase}-meta.json`,
  `${JSON.stringify(
    {
      scenario: name,
      startedAt: new Date().toISOString(),
      platform: `${os.platform()} ${os.release()} ${os.arch()}`,
      cpu: os.cpus()[0]?.model ?? 'unknown',
      cpuCount: os.cpus().length,
      totalMemoryBytes: os.totalmem(),
      node: process.version,
      settings,
    },
    null,
    2,
  )}\n`,
);

const k6Summary = `${resultsDir}/k6-summary.json`;
rmSync(k6Summary, { force: true });

const args = [
  'run',
  '--rm',
  '-i',
  // k6's image runs as an unprivileged user that cannot write to the host
  // directory; under rootless Podman container root is the host user.
  '--user',
  '0:0',
  // Only the results directory is mounted; the script still arrives on stdin.
  // :z lets the container write under SELinux (this is not a socket mount).
  '-v',
  `${resultsDir}:/results:z`,
  '--add-host',
  'host.docker.internal:host-gateway',
  '-e',
  `BASE_URL=${baseUrl}`,
  '-e',
  `STOCK=${settings.stock}`,
];

if (settings.iterations > 0) {
  args.push('-e', `ITERATIONS=${settings.iterations}`);
}
if (process.env.MIN_RPS) args.push('-e', `MIN_RPS=${process.env.MIN_RPS}`);
if (process.env.MAX_P99_MS) args.push('-e', `MAX_P99_MS=${process.env.MAX_P99_MS}`);
if ('rate' in settings) {
  args.push('-e', `RATE=${settings.rate}`, '-e', `DURATION=${settings.duration}`);
}

const result = spawnSync(
  'docker',
  [...args, 'grafana/k6:0.57.0', 'run', '-'],
  // The script goes in on stdin, so only the results directory needs a mount.
  { input: readFileSync(scriptPath), stdio: ['pipe', 'inherit', 'inherit'] },
);

if (existsSync(k6Summary)) {
  renameSync(k6Summary, `${resultBase}-k6.json`);
}

let exitCode = result.status ?? 1;

console.log('\nverifying the stack…');
try {
  const timeoutMs = name === 'soak' ? 900_000 : undefined;
  const report = await verifyStack({ requireOrders: name === 'soak', timeoutMs });
  writeFileSync(`${resultBase}-verify.txt`, `${report}\n`);
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  writeFileSync(`${resultBase}-verify.txt`, `FAIL — ${message}\n`);
  console.error('\nverify FAIL —', message);
  exitCode = 1;
}

console.log(`\nresults written to ${resultBase}-*.json / .txt`);
process.exit(exitCode);

/**
 * Clears previous reservations so a spike is repeatable without recreating the
 * stack, then seeds an active sale with the scenario's stock.
 */
async function prepareSale(): Promise<void> {
  const redis = createRedis(config.REDIS_URL);
  const pool = createPgPool(config.DATABASE_URL);

  try {
    const keys = saleRedisKeys(saleId);
    // Empty the stream first (keeping its consumer groups), so the orchestrator
    // cannot re-insert an old reservation between the TRUNCATE and the reset.
    await redis.xtrim(keys.reservations, 'MAXLEN', 0).catch(() => undefined);
    await redis.del(keys.buyers, keys.holders);
    await pool.query('TRUNCATE orders, saga_events, processed_messages RESTART IDENTITY CASCADE');

    const now = Date.now();
    await resetSale(pool, redis, {
      id: saleId,
      productId: config.SALE_PRODUCT_ID,
      startsAt: new Date(now - 60_000),
      endsAt: new Date(now + 15 * 60_000),
      totalStock: settings.stock,
    });

    console.log(`prepared ${saleId}: ACTIVE with ${settings.stock} units`);
  } finally {
    await redis.quit();
    await pool.end();
  }
}

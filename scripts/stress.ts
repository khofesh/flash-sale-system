import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { loadConfig } from '@fss/config';
import { createPgPool, createRedis, resetSale, saleRedisKeys } from '@fss/infra';
import { verifyStack } from '../stress/verify.js';

const scenario = process.argv.slice(2).find((argument) => argument !== '--') ?? 'smoke';
const scenarios = {
  smoke: { stock: 100_000, iterations: 0 },
  spike: { stock: 1_000, iterations: 10_000 },
  'same-user': { stock: 100, iterations: 2_000 },
} as const;

type ScenarioName = keyof typeof scenarios;

if (!(scenario in scenarios)) {
  console.error(
    `Unknown stress scenario "${scenario}". Available: ${Object.keys(scenarios).join(', ')}`,
  );
  process.exit(1);
}

const name = scenario as ScenarioName;
const { stock, iterations } = scenarios[name];
const config = loadConfig();
const saleId = config.SALE_ID;
const baseUrl = process.env.BASE_URL ?? 'http://host.docker.internal:8080';
const scriptPath = resolve(process.cwd(), 'stress', `${name}.js`);

await prepareSale();

const args = [
  'run',
  '--rm',
  '-i',
  '--add-host',
  'host.docker.internal:host-gateway',
  '-e',
  `BASE_URL=${baseUrl}`,
  '-e',
  `STOCK=${stock}`,
];

if (iterations > 0) {
  args.push('-e', `ITERATIONS=${iterations}`);
}

const result = spawnSync(
  'docker',
  [...args, 'grafana/k6:0.57.0', 'run', '-'],
  // The script goes in on stdin: no bind mount, so no SELinux relabeling under Podman.
  { input: readFileSync(scriptPath), stdio: ['pipe', 'inherit', 'inherit'] },
);

let exitCode = result.status ?? 1;

try {
  await verifyStack();
} catch (error) {
  console.error('\nverify FAIL —', error instanceof Error ? error.message : error);
  exitCode = 1;
}

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
      totalStock: stock,
    });

    console.log(`prepared ${saleId}: ACTIVE with ${stock} units`);
  } finally {
    await redis.quit();
    await pool.end();
  }
}

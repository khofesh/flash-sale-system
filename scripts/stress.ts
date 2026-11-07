import { spawn, spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import { resolve } from 'node:path';
import { loadConfig } from '@fss/config';
import { createPgPool, createRedis, resetSale, saleRedisKeys } from '@fss/infra';
import { verifyStack } from '../stress/verify.ts';

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
const composeFile = 'infra/docker-compose.yml';
// Only touch our own nginx: a custom BASE_URL points at something else.
const ownsNginx = !process.env.BASE_URL;
// Every k6 request comes from one address, which the per-IP limit would throttle.
const stressRateLimit = { RATE_LIMIT_RPS: '100000', RATE_LIMIT_BURST: '50000' };
// From the host, unlike BASE_URL, which is how the k6 container sees the stack.
const metricsUrl = process.env.METRICS_URL ?? 'http://localhost:8080/metrics';
const SAMPLE_INTERVAL_MS = 15_000;

await prepareSale();
if (ownsNginx) recreateNginx(stressRateLimit);
if (ownsNginx) await warmUpNginx();

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
      overrides: Object.fromEntries(
        ['RATE', 'DURATION', 'ITERATIONS', 'VUS', 'MIN_RPS', 'MAX_P99_MS'].flatMap((key) =>
          process.env[key] ? [[key, process.env[key]]] : [],
        ),
      ),
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
  args.push('-e', `ITERATIONS=${process.env.ITERATIONS ?? settings.iterations}`);
}
for (const option of ['MIN_RPS', 'MAX_P99_MS', 'VUS'] as const) {
  const value = process.env[option];
  if (value) args.push('-e', `${option}=${value}`);
}
const rate = process.env.RATE ?? ('rate' in settings ? String(settings.rate) : undefined);
if (rate) args.push('-e', `RATE=${rate}`);
const duration = process.env.DURATION ?? ('duration' in settings ? settings.duration : undefined);
if (duration) args.push('-e', `DURATION=${duration}`);

// Spike and soak write their own richer summary from handleSummary, and this flag
// would overwrite it with the legacy format, which has no thresholds or tagged windows.
const summaryFlags =
  name === 'spike' || name === 'soak' ? [] : ['--summary-export=/results/k6-summary.json'];

const samples: Sample[] = [];
const sampler = name === 'soak' ? setInterval(() => void takeSample(), SAMPLE_INTERVAL_MS) : null;

const k6Status = await new Promise<number>((done) => {
  const child = spawn(
    'docker',
    [...args, 'grafana/k6:0.57.0', 'run', ...summaryFlags, '-'],
    // The script goes in on stdin, so only the results directory needs a mount.
    { stdio: ['pipe', 'inherit', 'inherit'] },
  );
  child.on('error', () => done(1));
  child.on('close', (code) => done(code ?? 1));
  child.stdin.end(readFileSync(scriptPath));
});
if (sampler) clearInterval(sampler);

if (existsSync(k6Summary)) {
  renameSync(k6Summary, `${resultBase}-k6.json`);
}

let exitCode = k6Status;

if (name === 'soak') {
  const failures = checkSoak();
  for (const failure of failures) console.error(`soak FAIL — ${failure}`);
  if (failures.length > 0) exitCode = 1;
}

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

if (ownsNginx) recreateNginx({});

console.log(`\nresults written to ${resultBase}-*.json / .txt`);
process.exit(exitCode);

/** Applies a rate limit by recreating nginx, the only place it is configured. */
function recreateNginx(limit: Record<string, string>): void {
  const result = spawnSync(
    'docker',
    ['compose', '-f', composeFile, 'up', '-d', '--no-deps', '--force-recreate', '--wait', 'nginx'],
    { encoding: 'utf8', env: { ...process.env, ...limit } },
  );
  if (result.status !== 0) {
    console.error(`failed to recreate nginx with the rate limit for this run\n${result.stderr}`);
    process.exit(1);
  }
}

/**
 * A new nginx has no upstream connections yet, and opening them under load costs
 * tens of milliseconds of p99. A real sale has pre-sale status polling doing this,
 * so a run starts from that state rather than timing a restart this script caused.
 */
async function warmUpNginx(): Promise<void> {
  const url = new URL('/api/sale', metricsUrl).toString();
  const worker = async () => {
    for (let request = 0; request < 50; request += 1) {
      await fetch(url)
        .then((response) => response.arrayBuffer())
        .catch(() => undefined);
    }
  };
  await Promise.all(Array.from({ length: 200 }, worker));
}

interface Sample {
  at: string;
  /** Resident memory per API replica, keyed by the upstream nginx reports. */
  rssBytes: Record<string, number>;
  /** Used V8 heap per API replica, as of its latest major GC. */
  heapAfterGcBytes: Record<string, number>;
  /** Highest backlog any replica reported for the orchestrator group. */
  streamLag: number | null;
}

/**
 * Round-robin spreads a few scrapes across the replicas, and X-Upstream says which
 * one answered. A failed scrape only loses a data point, never fails the run.
 */
async function takeSample(): Promise<void> {
  const sample: Sample = {
    at: new Date().toISOString(),
    rssBytes: {},
    heapAfterGcBytes: {},
    streamLag: null,
  };

  for (let attempt = 0; attempt < 8; attempt += 1) {
    try {
      const response = await fetch(metricsUrl);
      if (!response.ok) continue;
      const body = await response.text();
      const upstream = response.headers.get('x-upstream') ?? 'unknown';
      const rss = /^process_resident_memory_bytes (\S+)$/m.exec(body)?.[1];
      if (rss) sample.rssBytes[upstream] = Number(rss);
      const heap = /^heap_used_after_major_gc_bytes (\S+)$/m.exec(body)?.[1];
      if (heap) sample.heapAfterGcBytes[upstream] = Number(heap);
      const lag = /^reservation_stream_lag_entries\{[^}]*\} (\S+)$/m.exec(body)?.[1];
      if (lag) sample.streamLag = Math.max(sample.streamLag ?? 0, Number(lag));
    } catch {
      // Handled by the sample simply having fewer replicas in it.
    }
  }
  samples.push(sample);
}

/**
 * The soak's remaining pass criteria (SPEC §12.3), which k6 cannot express: latency
 * and memory late in the run against the same measurements early in it. The first
 * fifth of the samples is warm-up, while each replica's heap grows to its working size.
 */
function checkSoak(): string[] {
  writeFileSync(`${resultBase}-samples.json`, `${JSON.stringify(samples, null, 2)}\n`);
  const failures: string[] = [];

  const k6Path = `${resultBase}-k6.json`;
  if (existsSync(k6Path)) {
    const metrics = (
      JSON.parse(readFileSync(k6Path, 'utf8')) as {
        metrics: Record<string, { values?: Record<string, number> } | undefined>;
      }
    ).metrics;
    const p99 = (window: string) =>
      metrics[`http_req_duration{window:${window}}`]?.values?.['p(99)'];
    const early = p99('early');
    const late = p99('late');
    if (early === undefined || late === undefined) {
      failures.push('the k6 summary has no early/late latency windows');
    } else {
      console.log(`latency p99: early ${early.toFixed(2)} ms, late ${late.toFixed(2)} ms`);
      // The floor keeps sub-millisecond noise from counting as a trend.
      if (late > Math.max(early * 2, 5)) {
        failures.push(`p99 grew from ${early.toFixed(2)} ms to ${late.toFixed(2)} ms`);
      }
    }
  }

  const warmedUp = samples.slice(Math.floor(samples.length / 5));
  const replicas = new Set(warmedUp.flatMap((sample) => Object.keys(sample.heapAfterGcBytes)));
  if (replicas.size === 0) {
    failures.push('too few /metrics samples to judge memory growth');
    return failures;
  }

  for (const replica of replicas) {
    const readings = warmedUp.flatMap((sample) => sample.heapAfterGcBytes[replica] ?? []);
    // The leading readings may predate the load, until the first major GC during it.
    const fresh = readings.slice(readings.findIndex((value) => value !== readings[0]));
    const third = Math.floor(fresh.length / 3);
    if (fresh.length === readings.length || third === 0) {
      failures.push(`${replica} had too few major GCs to judge memory growth`);
      continue;
    }

    const start = Math.min(...fresh.slice(0, third));
    const end = Math.min(...fresh.slice(-third));
    const growth = (end - start) / start;
    const rss = warmedUp.flatMap((sample) => sample.rssBytes[replica] ?? []);
    // RSS is reported but not judged: V8 grows it in steps and rarely gives it back,
    // so a rising RSS alone cannot tell a larger heap from a leak.
    console.log(
      `memory ${replica}: heap after major GC ${toMb(start)} MB -> ${toMb(end)} MB ` +
        `(${(growth * 100).toFixed(1)}%), rss ${toMb(rss[0] ?? 0)} MB -> ${toMb(rss.at(-1) ?? 0)} MB`,
    );
    if (growth > 0.25)
      failures.push(`${replica} heap after major GC grew ${(growth * 100).toFixed(1)}%`);
  }
  return failures;
}

function toMb(bytes: number): string {
  return (bytes / 1024 / 1024).toFixed(0);
}

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

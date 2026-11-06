import { setTimeout as sleep } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';
import { FINAL_ORDER_STATUSES } from '@fss/core';
import { loadConfig } from '@fss/config';
import {
  countDeadLetters,
  createPgPool,
  createRedis,
  createSqsClient,
  reconcileSale,
  type Pool,
  type ReconcileReport,
  type Redis,
} from '@fss/infra';

/**
 * Checks the SPEC §5.3 invariant through the same `reconcileSale` that backs
 * `/admin/reconcile`. Waits for reservations and compensation to drain first,
 * since k6 finishes before the workers do.
 */
export async function verifyStack({
  requireOrders = false,
  timeoutMs,
}: { requireOrders?: boolean; timeoutMs?: number } = {}): Promise<string> {
  const config = loadConfig();
  const redis = createRedis(config.REDIS_URL);
  const pool = createPgPool(config.DATABASE_URL);
  const sqs = createSqsClient({
    endpoint: config.SQS_ENDPOINT,
    region: config.AWS_REGION,
    accessKeyId: config.AWS_ACCESS_KEY_ID,
    secretAccessKey: config.AWS_SECRET_ACCESS_KEY,
  });

  try {
    const totalStock = await readTotalStock(pool, config.SALE_ID);
    // Chaos and soak wait for every saga: a reclaim alone takes XAUTOCLAIM's
    // 30 s idle time, and a soak leaves tens of thousands of orders behind.
    const deadline = Date.now() + (timeoutMs ?? (requireOrders ? 300_000 : 45_000));
    let report = await readReport(redis, pool, config.SALE_ID);
    let orders = await readOrderCounts(pool, config.SALE_ID);

    while (
      (!isDrained(report) || (requireOrders && orders.unfinished > 0)) &&
      Date.now() < deadline
    ) {
      await sleep(500);
      report = await readReport(redis, pool, config.SALE_ID);
      orders = await readOrderCounts(pool, config.SALE_ID);
    }

    const lines = [
      `verify ${config.SALE_ID}: total=${totalStock} stock=${report.stock} ` +
        `live=${report.liveOrders} buyers=${report.buyers} unpersisted=${report.unpersisted} ` +
        `pendingReleases=${report.pendingReleases}`,
    ];
    console.log(`\n${lines[0]}`);

    if (requireOrders) {
      // Otherwise a run where no request got through would pass the invariant.
      assert(orders.total > 0, 'no orders were created, so the run proved nothing');
      assert(orders.unfinished === 0, `${orders.unfinished} orders never reached a final status`);
    }
    assert(report.settled, 'Redis never stopped changing, so the invariant could not be read');
    assert(report.unpersisted === 0, `${report.unpersisted} reservations were never persisted`);
    assert(report.pendingReleases === 0, `${report.pendingReleases} units were never released`);
    assert(
      report.totalStock === totalStock,
      `Redis total ${report.totalStock} disagrees with Postgres total ${totalStock}`,
    );
    assert(
      Number.isFinite(report.stock) && report.stock >= 0 && report.stock <= totalStock,
      `Redis stock ${report.stock} is outside 0..${totalStock}`,
    );
    assert(report.liveOrders <= totalStock, 'more live orders than stock');
    assert(
      report.liveUsers === report.liveOrders,
      `duplicate live orders: ${report.liveOrders} orders for ${report.liveUsers} users`,
    );
    assert(
      report.buyersConsistent,
      `Redis buyers set (${report.buyers}) disagrees with live orders (${report.liveOrders})`,
    );
    assert(
      report.drift === 0,
      `invariant broken: ${report.stock} + ${report.liveOrders} != ${totalStock}`,
    );

    const deadLetters = await countDeadLetters(sqs);
    assert(deadLetters === 0, `${deadLetters} messages are sitting in the DLQs`);

    lines.push('PASS — no oversell, one live order per user, invariant holds, DLQs empty');
    console.log(lines[1]);
    return lines.join('\n');
  } finally {
    await redis.quit();
    await pool.end();
    sqs.destroy();
  }
}

async function readTotalStock(pool: Pool, saleId: string): Promise<number> {
  const result = await pool.query<{ total_stock: number }>(
    'SELECT total_stock FROM sales WHERE id = $1',
    [saleId],
  );
  const row = result.rows[0];
  if (!row) throw new Error(`Sale ${saleId} does not exist`);
  return row.total_stock;
}

async function readOrderCounts(pool: Pool, saleId: string) {
  const result = await pool.query<{ total: number; unfinished: number }>(
    `SELECT count(*)::int AS total,
            (count(*) FILTER (WHERE NOT (status = ANY($2))))::int AS unfinished
       FROM orders
      WHERE sale_id = $1`,
    [saleId, [...FINAL_ORDER_STATUSES]],
  );
  return result.rows[0] ?? { total: 0, unfinished: 0 };
}

async function readReport(redis: Redis, pool: Pool, saleId: string): Promise<ReconcileReport> {
  const report = await reconcileSale(pool, redis, saleId);
  if (!report) throw new Error(`Sale ${saleId} does not exist in Redis`);
  return report;
}

function isDrained(report: ReconcileReport): boolean {
  return report.settled && report.unpersisted === 0 && report.pendingReleases === 0;
}

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

const invokedDirectly =
  process.argv[1] !== undefined && pathToFileURL(process.argv[1]).href === import.meta.url;

if (invokedDirectly) {
  verifyStack({ requireOrders: process.argv.includes('--require-orders') }).catch(
    (error: unknown) => {
      console.error('\nFAIL —', error instanceof Error ? error.message : error);
      process.exit(1);
    },
  );
}

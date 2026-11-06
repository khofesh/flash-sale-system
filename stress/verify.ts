import { setTimeout as sleep } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';
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
export async function verifyStack(): Promise<void> {
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
    const deadline = Date.now() + 45_000;
    let report = await readReport(redis, pool, config.SALE_ID);

    while (!isDrained(report) && Date.now() < deadline) {
      await sleep(500);
      report = await readReport(redis, pool, config.SALE_ID);
    }

    console.log(
      `\nverify ${config.SALE_ID}: total=${totalStock} stock=${report.stock} ` +
        `live=${report.liveOrders} buyers=${report.buyers} unpersisted=${report.unpersisted} ` +
        `pendingReleases=${report.pendingReleases}`,
    );

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

    console.log('PASS — no oversell, one live order per user, invariant holds, DLQs empty');
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
  verifyStack().catch((error: unknown) => {
    console.error('\nFAIL —', error instanceof Error ? error.message : error);
    process.exit(1);
  });
}

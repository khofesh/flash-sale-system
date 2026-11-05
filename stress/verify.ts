import { setTimeout as sleep } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';
import { loadConfig } from '@fss/config';
import {
  countDeadLetters,
  createPgPool,
  createRedis,
  createSqsClient,
  saleRedisKeys,
  type Pool,
  type Redis,
} from '@fss/infra';

interface Snapshot {
  stock: number;
  liveOrders: number;
  liveUsers: number;
  allOrders: number;
  unpersisted: number;
}

/**
 * Checks the SPEC §5.3 invariant: Redis stock plus live orders plus stream
 * entries the orchestrator has not persisted equals the sale's total stock.
 * Waits for the stream to drain first, since k6 finishes before the workers do.
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
    let snapshot = await readSnapshot(redis, pool, config.SALE_ID, totalStock);

    while (snapshot.unpersisted > 0 && Date.now() < deadline) {
      await sleep(500);
      snapshot = await readSnapshot(redis, pool, config.SALE_ID, totalStock);
    }

    console.log(
      `\nverify ${config.SALE_ID}: total=${totalStock} stock=${snapshot.stock} ` +
        `live=${snapshot.liveOrders} unpersisted=${snapshot.unpersisted}`,
    );

    assert(snapshot.unpersisted === 0, `${snapshot.unpersisted} reservations were never persisted`);
    assert(snapshot.liveOrders <= totalStock, 'more live orders than stock');
    assert(
      snapshot.liveUsers === snapshot.liveOrders,
      `duplicate live orders: ${snapshot.liveOrders} orders for ${snapshot.liveUsers} users`,
    );
    assert(
      snapshot.stock + snapshot.liveOrders + snapshot.unpersisted === totalStock,
      `invariant broken: ${snapshot.stock} + ${snapshot.liveOrders} + ${snapshot.unpersisted} != ${totalStock}`,
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

async function readSnapshot(
  redis: Redis,
  pool: Pool,
  saleId: string,
  totalStock: number,
): Promise<Snapshot> {
  const keys = saleRedisKeys(saleId);
  const stockRaw = await redis.get(keys.stock);
  if (stockRaw === null) {
    throw new Error(`Redis stock for ${saleId} is missing`);
  }
  const stock = Number(stockRaw);

  const live = await pool.query<{ live: number; users: number; all_orders: number }>(
    `SELECT
       count(*) FILTER (WHERE status NOT IN ('CANCELLED', 'REFUNDED'))::int AS live,
       count(DISTINCT user_id) FILTER (WHERE status NOT IN ('CANCELLED', 'REFUNDED'))::int AS users,
       count(*)::int AS all_orders
     FROM orders
     WHERE sale_id = $1`,
    [saleId],
  );
  const row = live.rows[0] ?? { live: 0, users: 0, all_orders: 0 };

  const orderIds = await pool.query<{ id: string }>('SELECT id FROM orders WHERE sale_id = $1', [
    saleId,
  ]);
  const persisted = new Set(orderIds.rows.map((order) => order.id));

  const entries = (await redis.xrange(keys.reservations, '-', '+')) as Array<[string, string[]]>;
  let unpersisted = 0;
  for (const [, fields] of entries) {
    const index = fields.indexOf('reservationId');
    const reservationId = index >= 0 ? fields[index + 1] : undefined;
    if (!reservationId || !persisted.has(reservationId)) {
      unpersisted += 1;
    }
  }

  if (!Number.isFinite(stock) || stock < 0 || stock > totalStock) {
    throw new Error(`Redis stock ${stockRaw} is outside 0..${totalStock}`);
  }

  return {
    stock,
    liveOrders: row.live,
    liveUsers: row.users,
    allOrders: row.all_orders,
    unpersisted,
  };
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

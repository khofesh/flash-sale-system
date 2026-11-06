import type { Pool } from 'pg';
import { RESERVATION_CONSUMER } from './orderRepo.js';
import type { Redis } from './redis.js';
import { saleRedisKeys } from './saleRepo.js';

export interface RebuildReport {
  saleId: string;
  totalStock: number;
  /** Units available again, recomputed from Postgres. */
  stock: number;
  /** Orders that are not CANCELLED/REFUNDED. */
  liveOrders: number;
  /** Dead orders still holding a unit, which the sweeper will return. */
  pendingReleases: number;
  /** Stream entries the orchestrator has yet to handle that hold a unit. */
  unpersisted: number;
  /** Buyers in the rebuilt set (= units currently held). */
  buyers: number;
}

interface SaleRow {
  id: string;
  starts_at: Date;
  ends_at: Date;
  total_stock: number;
}

interface OrderRow {
  id: string;
  user_id: string;
  status: string;
  released_at: Date | null;
  created_at: Date;
}

const deadStatuses = new Set(['CANCELLED', 'REFUNDED']);
const REBUILD_LOCK_SECONDS = 60;

/** Another process holds the rebuild lock; it will reopen the sale. */
export class RebuildInProgressError extends Error {
  constructor(saleId: string) {
    super(`A rebuild of sale ${saleId} is already running`);
    this.name = 'RebuildInProgressError';
  }
}

const FENCE_SCRIPT = `
if redis.call('GET', KEYS[1]) == ARGV[1] then return 0 end
redis.call('DEL', KEYS[2])
return 1
`;

/**
 * Closes the sale unless a rebuild for this Redis run has already finished, and
 * returns whether it did. Atomic because replicas race: a plain GET then DEL could
 * delete the stock another replica's rebuild just restored, after which `run_id`
 * matches and nothing would ever reopen the sale.
 */
export async function fenceRestartedSale(
  redis: Redis,
  saleId: string,
  runId: string,
): Promise<boolean> {
  const keys = saleRedisKeys(saleId);
  return (await redis.eval(FENCE_SCRIPT, 2, keys.redisRunId, keys.stock, runId)) === 1;
}

/** Redis assigns a new `run_id` every time the server starts. */
export async function readRedisRunId(redis: Redis): Promise<string> {
  const info = await redis.info('server');
  const runId = /^run_id:(\w+)/m.exec(info)?.[1];
  if (!runId) throw new Error('Redis INFO did not report a run_id');
  return runId;
}

/**
 * Recomputes the Redis sale state from durable data (SPEC §9.3, Redis crashes).
 * A user's unit belongs to, in order: their live order, else the first stream
 * entry the orchestrator has yet to handle, else a dead order whose release was
 * never confirmed. The sale metadata is rewritten from Postgres too, so a flushed
 * Redis is usable again.
 *
 * Safe to run with the stack up. Deleting the stock key fences the sale first:
 * `reserve.lua` answers UNAVAILABLE and `release.lua` refuses without it, so
 * Redis cannot move under the rebuild. If this fails partway the sale stays
 * closed until it is run again. The stream is left untouched: pending entries
 * still belong to their consumer group.
 */
export async function rebuildRedis(
  pool: Pool,
  redis: Redis,
  saleId: string,
): Promise<RebuildReport> {
  const saleResult = await pool.query<SaleRow>(
    'SELECT id, starts_at, ends_at, total_stock FROM sales WHERE id = $1',
    [saleId],
  );
  const sale = saleResult.rows[0];
  if (!sale) throw new Error(`Sale ${saleId} does not exist in Postgres`);

  const keys = saleRedisKeys(saleId);
  const locked = await redis.set(keys.rebuildLock, '1', 'EX', REBUILD_LOCK_SECONDS, 'NX');
  if (locked !== 'OK') throw new RebuildInProgressError(saleId);

  try {
    await redis.del(keys.stock);

    const entries = (await redis.xrange(keys.reservations, '-', '+')) as Array<[string, string[]]>;
    const entryIds = entries
      .map(([, fields]) => fieldValue(fields, 'reservationId'))
      .filter((id): id is string => id !== undefined);

    // Before the orders: persisting writes both in one transaction, so an entry
    // persisted between the two reads is still found through its order row.
    const processedResult = await pool.query<{ message_id: string }>(
      `SELECT message_id FROM processed_messages WHERE consumer = $1 AND message_id = ANY($2)`,
      [RESERVATION_CONSUMER, entryIds],
    );
    const handled = new Set(processedResult.rows.map((row) => row.message_id));

    const orderResult = await pool.query<OrderRow>(
      `SELECT id, user_id, status, released_at, created_at
         FROM orders
        WHERE sale_id = $1
        ORDER BY created_at, id`,
      [saleId],
    );

    const holders = new Map<string, string>();
    let liveOrders = 0;
    for (const order of orderResult.rows) {
      handled.add(order.id);
      if (!deadStatuses.has(order.status)) {
        liveOrders += 1;
        holders.set(order.user_id, order.id);
      }
    }

    // The orchestrator handles entries in order, so the user's first unhandled
    // entry becomes their order and a later one is released as a conflict.
    let unpersisted = 0;
    for (const [, fields] of entries) {
      const reservationId = fieldValue(fields, 'reservationId');
      const userId = fieldValue(fields, 'userId');
      if (!reservationId || !userId || handled.has(reservationId) || holders.has(userId)) continue;
      holders.set(userId, reservationId);
      unpersisted += 1;
    }

    // After the stream entries: a user with a newer reservation got it because
    // this order's unit was already returned, even if that was never recorded.
    // Its pending release is then a no-op rather than taking the new unit.
    let pendingReleases = 0;
    for (const order of orderResult.rows) {
      if (deadStatuses.has(order.status) && order.released_at === null) {
        if (holders.has(order.user_id)) continue;
        holders.set(order.user_id, order.id);
        pendingReleases += 1;
      }
    }

    const stock = sale.total_stock - holders.size;
    if (stock < 0) {
      throw new Error(
        `Cannot rebuild sale ${saleId}: ${holders.size} held units exceed total_stock ${sale.total_stock}`,
      );
    }

    const runId = await readRedisRunId(redis);
    const multi = redis
      .multi()
      .del(keys.buyers)
      .del(keys.holders)
      .hset(keys.meta, {
        startsAt: String(sale.starts_at.getTime()),
        endsAt: String(sale.ends_at.getTime()),
        totalStock: String(sale.total_stock),
      })
      .set(keys.redisRunId, runId);

    if (holders.size > 0) {
      multi.hset(keys.holders, Object.fromEntries(holders));
      multi.sadd(keys.buyers, [...holders.keys()]);
    }
    // Reopens the sale, in the same transaction as the state it depends on.
    multi.set(keys.stock, String(stock));

    const results = await multi.exec();
    if (!results) throw new Error(`Redis rebuild transaction for sale ${saleId} was aborted`);
    for (const [error] of results) {
      if (error) throw error;
    }

    return {
      saleId,
      totalStock: sale.total_stock,
      stock,
      liveOrders,
      pendingReleases,
      unpersisted,
      buyers: holders.size,
    };
  } finally {
    await redis.del(keys.rebuildLock).catch(() => undefined);
  }
}

function fieldValue(fields: string[], name: string): string | undefined {
  const index = fields.indexOf(name);
  return index >= 0 ? fields[index + 1] : undefined;
}

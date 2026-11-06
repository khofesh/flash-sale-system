import type { Pool } from 'pg';
import type { Redis } from './redis.js';
import { saleRedisKeys } from './saleRepo.js';

export interface RebuildReport {
  saleId: string;
  totalStock: number;
  /** Units available again, recomputed from Postgres. */
  stock: number;
  /** Orders that are not CANCELLED/REFUNDED. */
  liveOrders: number;
  /** Dead orders whose unit has not been confirmed back in Redis. */
  pendingReleases: number;
  /** Stream entries with no order row that still hold a unit. */
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

/**
 * Recomputes the Redis sale state from durable data (SPEC §9.3, Redis crashes):
 * `stock`, the buyers set and the holders hash are rebuilt from Postgres live
 * orders, dead orders whose release was never confirmed, and reservation stream
 * entries the orchestrator has not persisted yet. The sale metadata is rewritten
 * from Postgres too, so a flushed Redis is usable again.
 *
 * The stream itself is left untouched: pending entries still belong to their
 * consumer group, and the orchestrator will recreate a missing group. Run this
 * with traffic stopped; it is not safe to interleave with live reservations.
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

  const orderResult = await pool.query<OrderRow>(
    `SELECT id, user_id, status, released_at, created_at
       FROM orders
      WHERE sale_id = $1
      ORDER BY created_at, id`,
    [saleId],
  );

  const allOrderIds = new Set<string>();
  const holders = new Map<string, string>();

  let liveOrders = 0;
  for (const order of orderResult.rows) {
    allOrderIds.add(order.id);
    if (!deadStatuses.has(order.status)) {
      liveOrders += 1;
      holders.set(order.user_id, order.id);
    }
  }

  // A CANCELLED/REFUNDED order with released_at NULL still holds its unit; the
  // sweeper will return it later, so rebuilding must keep it accounted for.
  let pendingReleases = 0;
  for (const order of orderResult.rows) {
    if (deadStatuses.has(order.status) && order.released_at === null) {
      pendingReleases += 1;
      if (!holders.has(order.user_id)) holders.set(order.user_id, order.id);
    }
  }

  // Reservations that were XADDed but never persisted. Only a user without a
  // holder can still be holding one, and the latest entry is the one that does.
  const entries = (await redis.xrange(saleRedisKeys(saleId).reservations, '-', '+')) as Array<
    [string, string[]]
  >;
  const unpersistedHolders = new Map<string, string>();
  for (const [, fields] of entries) {
    const reservationId = fieldValue(fields, 'reservationId');
    const userId = fieldValue(fields, 'userId');
    if (!reservationId || !userId) continue;
    if (allOrderIds.has(reservationId)) continue;
    if (holders.has(userId)) continue;
    unpersistedHolders.set(userId, reservationId);
  }
  for (const [userId, reservationId] of unpersistedHolders) {
    holders.set(userId, reservationId);
  }

  const stock = sale.total_stock - holders.size;
  if (stock < 0) {
    throw new Error(
      `Cannot rebuild sale ${saleId}: ${holders.size} held units exceed total_stock ${sale.total_stock}`,
    );
  }

  const keys = saleRedisKeys(saleId);
  const multi = redis
    .multi()
    .del(keys.buyers)
    .del(keys.holders)
    .hset(keys.meta, {
      startsAt: String(sale.starts_at.getTime()),
      endsAt: String(sale.ends_at.getTime()),
      totalStock: String(sale.total_stock),
    })
    .set(keys.stock, String(stock));

  if (holders.size > 0) {
    multi.hset(keys.holders, Object.fromEntries(holders));
    multi.sadd(keys.buyers, [...holders.keys()]);
  }

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
    unpersisted: unpersistedHolders.size,
    buyers: holders.size,
  };
}

function fieldValue(fields: string[], name: string): string | undefined {
  const index = fields.indexOf(name);
  return index >= 0 ? fields[index + 1] : undefined;
}

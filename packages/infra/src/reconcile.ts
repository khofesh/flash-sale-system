import type { Pool } from 'pg';
import type { Redis } from './redis.js';
import { getSaleSnapshot, saleRedisKeys } from './saleRepo.js';

export interface ReconcileReport {
  saleId: string;
  totalStock: number;
  stock: number;
  liveOrders: number;
  liveUsers: number;
  unpersisted: number;
  buyers: number;
  /** `stock + liveOrders + unpersisted - totalStock`. Zero means no drift. */
  drift: number;
  /** The Redis buyers set holds exactly the live orders plus in-flight reservations. */
  buyersConsistent: boolean;
  ok: boolean;
}

/**
 * Checks the SPEC §5.3 invariant against a live stack:
 * `redis stock + live orders + reservations not yet persisted = total_stock`.
 * Returns null when the sale does not exist in Redis.
 */
export async function reconcileSale(
  pool: Pool,
  redis: Redis,
  saleId: string,
): Promise<ReconcileReport | null> {
  const snapshot = await getSaleSnapshot(redis, saleId);
  if (!snapshot) return null;

  const keys = saleRedisKeys(saleId);
  const buyers = await redis.scard(keys.buyers);

  const live = await pool.query<{ live: number; users: number }>(
    `SELECT
       count(*) FILTER (WHERE status NOT IN ('CANCELLED', 'REFUNDED'))::int AS live,
       count(DISTINCT user_id) FILTER (WHERE status NOT IN ('CANCELLED', 'REFUNDED'))::int AS users
     FROM orders
     WHERE sale_id = $1`,
    [saleId],
  );
  const liveOrders = live.rows[0]?.live ?? 0;
  const liveUsers = live.rows[0]?.users ?? 0;

  const persisted = await pool.query<{ id: string }>('SELECT id FROM orders WHERE sale_id = $1', [
    saleId,
  ]);
  const known = new Set(persisted.rows.map((row) => row.id));

  const entries = (await redis.xrange(keys.reservations, '-', '+')) as Array<[string, string[]]>;
  let unpersisted = 0;
  for (const [, fields] of entries) {
    const index = fields.indexOf('reservationId');
    const reservationId = index >= 0 ? fields[index + 1] : undefined;
    if (!reservationId || !known.has(reservationId)) unpersisted += 1;
  }

  const drift = snapshot.remainingStock + liveOrders + unpersisted - snapshot.totalStock;
  const buyersConsistent = buyers === liveOrders + unpersisted;

  return {
    saleId,
    totalStock: snapshot.totalStock,
    stock: snapshot.remainingStock,
    liveOrders,
    liveUsers,
    unpersisted,
    buyers,
    drift,
    buyersConsistent,
    ok: drift === 0 && buyersConsistent,
  };
}

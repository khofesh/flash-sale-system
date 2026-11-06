import { setTimeout as sleep } from 'node:timers/promises';
import type { Pool } from 'pg';
import type { Redis } from './redis.js';
import { getSaleSnapshot, saleRedisKeys } from './saleRepo.js';

export interface ReconcileReport {
  saleId: string;
  totalStock: number;
  stock: number;
  liveOrders: number;
  liveUsers: number;
  /** Stream entries with no order row whose reservation still holds a unit. */
  unpersisted: number;
  /** Dead orders whose unit has not gone back to Redis yet. */
  pendingReleases: number;
  buyers: number;
  /** `stock + liveOrders + pendingReleases + unpersisted - totalStock`. Zero means no drift. */
  drift: number;
  /** The buyers set holds exactly the holders hash, and each holder is accounted for. */
  buyersConsistent: boolean;
  /**
   * Redis did not change while Postgres was read. When false, traffic never
   * paused long enough for a consistent read, so drift says nothing.
   */
  settled: boolean;
  ok: boolean;
}

interface RedisState {
  stock: string | null;
  buyers: number;
  holders: Record<string, string>;
  entries: Array<[string, string[]]>;
}

const READ_ATTEMPTS = 5;
const READ_RETRY_MS = 100;

/**
 * Checks the SPEC §5.3 invariant against a live stack:
 * `redis stock + units held by orders + reservations not yet persisted = total_stock`.
 * Redis is read in one MULTI and Postgres in one statement, and the read is
 * repeated if Redis moved in between, so in-flight work is never reported as
 * drift. Returns null when the sale does not exist in Redis.
 */
export async function reconcileSale(
  pool: Pool,
  redis: Redis,
  saleId: string,
): Promise<ReconcileReport | null> {
  const snapshot = await getSaleSnapshot(redis, saleId);
  if (!snapshot) return null;

  let report: ReconcileReport | undefined;
  for (let attempt = 0; attempt < READ_ATTEMPTS; attempt += 1) {
    if (attempt > 0) await sleep(READ_RETRY_MS);

    const state = await readRedisState(redis, saleId);
    const orders = await readOrders(pool, saleId, state.holders);
    const settled = await redisUnchanged(redis, saleId, state);

    const unpersisted = countUnpersisted(state, orders.known);
    const stock = Number(state.stock);
    const held = orders.live + orders.pendingReleases + unpersisted;
    const drift = stock + held - snapshot.totalStock;
    const buyersConsistent =
      state.buyers === held && state.buyers === Object.keys(state.holders).length;

    report = {
      saleId,
      totalStock: snapshot.totalStock,
      stock,
      liveOrders: orders.live,
      liveUsers: orders.liveUsers,
      unpersisted,
      pendingReleases: orders.pendingReleases,
      buyers: state.buyers,
      drift,
      buyersConsistent,
      settled,
      ok: settled && drift === 0 && buyersConsistent,
    };
    if (settled) break;
  }
  return report as ReconcileReport;
}

async function readRedisState(redis: Redis, saleId: string): Promise<RedisState> {
  const keys = saleRedisKeys(saleId);
  const results = await redis
    .multi()
    .get(keys.stock)
    .scard(keys.buyers)
    .hgetall(keys.holders)
    .xrange(keys.reservations, '-', '+')
    .exec();
  if (!results) throw new Error(`Redis transaction for sale ${saleId} was aborted`);
  for (const [error] of results) {
    if (error) throw error;
  }
  return {
    stock: results[0]?.[1] as string | null,
    buyers: results[1]?.[1] as number,
    holders: results[2]?.[1] as Record<string, string>,
    entries: results[3]?.[1] as Array<[string, string[]]>,
  };
}

/** Every reservation XADDs and every release INCRs, so these two catch any change. */
async function redisUnchanged(redis: Redis, saleId: string, before: RedisState): Promise<boolean> {
  const keys = saleRedisKeys(saleId);
  const [stock, length] = await Promise.all([redis.get(keys.stock), redis.xlen(keys.reservations)]);
  return stock === before.stock && length === before.entries.length;
}

async function readOrders(pool: Pool, saleId: string, holders: Record<string, string>) {
  const result = await pool.query<{ id: string; user_id: string; live: boolean }>(
    `SELECT id, user_id, status NOT IN ('CANCELLED', 'REFUNDED') AS live
       FROM orders
      WHERE sale_id = $1`,
    [saleId],
  );

  const known = new Set<string>();
  const liveUsers = new Set<string>();
  let live = 0;
  let pendingReleases = 0;
  for (const row of result.rows) {
    known.add(row.id);
    if (row.live) {
      live += 1;
      liveUsers.add(row.user_id);
    } else if (holders[row.user_id] === row.id) {
      pendingReleases += 1;
    }
  }
  return { known, live, liveUsers: liveUsers.size, pendingReleases };
}

/**
 * A stream entry with no order row counts only while it holds a unit. A conflict
 * reservation stays in the stream forever, but its unit has gone back to stock.
 */
function countUnpersisted(state: RedisState, known: Set<string>): number {
  let unpersisted = 0;
  for (const [, fields] of state.entries) {
    const reservationId = fieldValue(fields, 'reservationId');
    const userId = fieldValue(fields, 'userId');
    if (!reservationId || !userId || known.has(reservationId)) continue;
    if (state.holders[userId] === reservationId) unpersisted += 1;
  }
  return unpersisted;
}

function fieldValue(fields: string[], name: string): string | undefined {
  const index = fields.indexOf(name);
  return index >= 0 ? fields[index + 1] : undefined;
}

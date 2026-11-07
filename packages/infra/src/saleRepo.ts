import type { ChainableCommander } from 'ioredis';
import type { Pool } from 'pg';
import type { Redis } from './redis.ts';

/** Largest value a Postgres INTEGER column (`sales.total_stock`) can hold. */
export const MAX_TOTAL_STOCK = 2_147_483_647;

export interface SaleRecord {
  id: string;
  productId: string;
  startsAt: Date;
  endsAt: Date;
  totalStock: number;
}

/** A sale as mirrored in Redis, with times in epoch milliseconds. */
export interface SaleSnapshot {
  id: string;
  startsAt: number;
  endsAt: number;
  totalStock: number;
  remainingStock: number;
  /** Redis `TIME` when the snapshot was read, the same clock reserve.lua uses. */
  serverTime: number;
}

export interface SaleSeed {
  id: string;
  productId: string;
  startsAt?: string | Date;
  endsAt?: string | Date;
  totalStock: number;
}

export interface SeedResult {
  sale: SaleRecord;
  /** True when this call created the sale and initialised its Redis stock. */
  created: boolean;
  /**
   * True when the sale already existed but Redis has no stock key (for example
   * after Redis lost its data). Stock is deliberately not re-initialised, since
   * that would restore sold units; the sale stays unavailable (fail closed)
   * until it is reset or Redis is rebuilt from Postgres.
   */
  stockMissing: boolean;
}

/** The sale is defined in Redis but its stock key is missing. Callers must fail closed. */
class SaleStateMissingError extends Error {
  constructor(saleId: string) {
    super(`Redis stock for sale ${saleId} is missing`);
    this.name = 'SaleStateMissingError';
  }
}

/** A reset was refused because the sale still has live reservations or orders. */
export class SaleHasOrdersError extends Error {
  constructor(saleId: string) {
    super(`Sale ${saleId} still has active reservations or orders`);
    this.name = 'SaleHasOrdersError';
  }
}

interface SaleRow {
  id: string;
  product_id: string;
  starts_at: Date;
  ends_at: Date;
  total_stock: number;
}

const saleColumns = 'id, product_id, starts_at, ends_at, total_stock';

/** All keys for one sale share a hash tag, so they stay in one Redis Cluster slot. */
export function saleRedisKeys(saleId: string) {
  const tag = `{${saleId}}`;
  return {
    meta: `sale:${tag}:meta`,
    stock: `sale:${tag}:stock`,
    buyers: `sale:${tag}:buyers`,
    holders: `sale:${tag}:holders`,
    reservations: `sale:${tag}:reservations`,
    /** The Redis `run_id` this sale's state was last known good on. */
    redisRunId: `sale:${tag}:redis_run_id`,
    rebuildLock: `sale:${tag}:rebuild_lock`,
  } as const;
}

/**
 * Creates the sale in Postgres if it is missing, then mirrors it into Redis.
 * Safe to run from every API replica on every start. An existing sale is never
 * revalidated against the env seed, so a sale that has already ended still
 * boots. Redis stock is only initialised by the call that created the sale, so
 * a restart never restores already-sold units.
 */
export async function seedSale(
  pool: Pool,
  redis: Redis,
  seed: SaleSeed,
  now = Date.now(),
): Promise<SeedResult> {
  let row = await selectSale(pool, seed.id);
  let created = false;

  if (!row) {
    const sale = resolveSeed(seed, now);
    const inserted = await pool.query<SaleRow>(
      `INSERT INTO sales (id, product_id, starts_at, ends_at, total_stock)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (id) DO NOTHING
       RETURNING ${saleColumns}`,
      [sale.id, sale.productId, sale.startsAt, sale.endsAt, sale.totalStock],
    );
    row = inserted.rows[0];
    created = row !== undefined;
    // Another replica inserted it first.
    row ??= await selectSale(pool, sale.id);
  }

  if (!row) {
    throw new Error(`Failed to seed sale ${seed.id}`);
  }

  const keys = saleRedisKeys(row.id);
  const multi = redis.multi();
  queueSaleState(multi, row, created ? 'init' : 'keep');
  multi.exists(keys.stock);
  const results = await execAll(multi, row.id);
  const stockExists = results.at(-1) === 1;

  return { sale: toRecord(row), created, stockMissing: !stockExists };
}

/**
 * Creates or resets a sale. Postgres is written first, so Redis can always be
 * rebuilt from the durable sale definition. A reset is refused while the sale
 * has live reservations (buyers set) or active orders, because restoring full
 * stock then would break `sold + available == initial_stock`. Resetting clears
 * the buyers set, the holders hash and the reservations stream, and recreates any consumer groups
 * the stream had, so running consumers keep working.
 */
export async function resetSale(pool: Pool, redis: Redis, sale: SaleRecord): Promise<void> {
  const keys = saleRedisKeys(sale.id);

  if ((await redis.scard(keys.buyers)) > 0 || (await countActiveOrders(pool, sale.id)) > 0) {
    throw new SaleHasOrdersError(sale.id);
  }

  const result = await pool.query<SaleRow>(
    `INSERT INTO sales (id, product_id, starts_at, ends_at, total_stock)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (id) DO UPDATE
       SET product_id = EXCLUDED.product_id,
           starts_at = EXCLUDED.starts_at,
           ends_at = EXCLUDED.ends_at,
           total_stock = EXCLUDED.total_stock
     RETURNING ${saleColumns}`,
    [sale.id, sale.productId, sale.startsAt, sale.endsAt, sale.totalStock],
  );

  const row = result.rows[0];
  if (!row) {
    throw new Error(`Failed to reset sale ${sale.id}`);
  }

  const groups = await streamGroups(redis, keys.reservations);
  const multi = redis.multi().del(keys.buyers).del(keys.holders).del(keys.reservations);
  for (const group of groups) {
    multi.xgroup('CREATE', keys.reservations, group, '$', 'MKSTREAM');
  }
  queueSaleState(multi, row, 'set');
  await execAll(multi, row.id);
}

/**
 * Reads the sale and Redis `TIME` in one round trip. Returns null when the sale
 * is not defined in Redis, and throws SaleStateMissingError when it is defined
 * but its stock key is gone.
 */
export async function getSaleSnapshot(redis: Redis, saleId: string): Promise<SaleSnapshot | null> {
  const keys = saleRedisKeys(saleId);
  const results = await redis.pipeline().hgetall(keys.meta).get(keys.stock).time().exec();
  if (!results) {
    throw new Error(`Redis pipeline for sale ${saleId} was aborted`);
  }
  for (const [error] of results) {
    if (error) throw error;
  }

  const meta = results[0]?.[1] as Record<string, string>;
  const stock = results[1]?.[1] as string | null;
  const [seconds, micros] = results[2]?.[1] as [string, string];

  if (Object.keys(meta).length === 0) {
    return null;
  }
  if (stock === null) {
    throw new SaleStateMissingError(saleId);
  }

  const startsAt = Number(meta.startsAt);
  const endsAt = Number(meta.endsAt);
  const totalStock = Number(meta.totalStock);
  const remainingStock = Number(stock);

  if (
    !Number.isFinite(startsAt) ||
    !Number.isFinite(endsAt) ||
    !Number.isFinite(totalStock) ||
    !Number.isFinite(remainingStock)
  ) {
    throw new Error(`Redis state for sale ${saleId} is malformed`);
  }

  return {
    id: saleId,
    startsAt,
    endsAt,
    totalStock,
    remainingStock,
    serverTime: Number(seconds) * 1000 + Math.floor(Number(micros) / 1000),
  };
}

function resolveSeed(seed: SaleSeed, now = Date.now()): SaleRecord {
  const startsAt = seed.startsAt
    ? parseDate(seed.startsAt, 'startsAt')
    : new Date(seed.endsAt ? now : now + 60_000);
  const endsAt = seed.endsAt
    ? parseDate(seed.endsAt, 'endsAt')
    : new Date(startsAt.getTime() + 30 * 60_000);

  if (endsAt.getTime() <= startsAt.getTime()) {
    throw new Error(`Sale ${seed.id} must end after it starts`);
  }
  if (
    !Number.isInteger(seed.totalStock) ||
    seed.totalStock < 0 ||
    seed.totalStock > MAX_TOTAL_STOCK
  ) {
    throw new Error(`Sale ${seed.id} has an invalid totalStock`);
  }

  return { ...seed, startsAt, endsAt };
}

/**
 * Queues the Redis mirror of a sale: the meta hash, plus stock according to
 * `stock`: 'set' overwrites it, 'init' sets it only if missing, 'keep' leaves it.
 */
function queueSaleState(
  chain: ChainableCommander,
  row: SaleRow,
  stock: 'set' | 'init' | 'keep',
): void {
  const keys = saleRedisKeys(row.id);
  chain.hset(keys.meta, {
    startsAt: String(row.starts_at.getTime()),
    endsAt: String(row.ends_at.getTime()),
    totalStock: String(row.total_stock),
  });
  if (stock === 'set') {
    chain.set(keys.stock, String(row.total_stock));
  } else if (stock === 'init') {
    chain.set(keys.stock, String(row.total_stock), 'NX');
  }
}

async function execAll(multi: ChainableCommander, saleId: string): Promise<unknown[]> {
  const results = await multi.exec();
  if (!results) {
    throw new Error(`Redis transaction for sale ${saleId} was aborted`);
  }
  return results.map(([error, value]) => {
    if (error) throw error;
    return value;
  });
}

async function selectSale(pool: Pool, saleId: string): Promise<SaleRow | undefined> {
  const result = await pool.query<SaleRow>(`SELECT ${saleColumns} FROM sales WHERE id = $1`, [
    saleId,
  ]);
  return result.rows[0];
}

/** Active orders for a sale. The `orders` table arrives in PLAN Phase 2; before that there are none. */
async function countActiveOrders(pool: Pool, saleId: string): Promise<number> {
  const table = await pool.query<{ exists: boolean }>(
    `SELECT to_regclass('public.orders') IS NOT NULL AS exists`,
  );
  if (!table.rows[0]?.exists) return 0;

  const result = await pool.query<{ count: string }>(
    `SELECT count(*) AS count FROM orders
      WHERE sale_id = $1 AND status NOT IN ('CANCELLED', 'REFUNDED')`,
    [saleId],
  );
  return Number(result.rows[0]?.count ?? 0);
}

async function streamGroups(redis: Redis, stream: string): Promise<string[]> {
  if ((await redis.exists(stream)) === 0) return [];
  const groups = (await redis.xinfo('GROUPS', stream)) as unknown[][];
  return groups.map((fields) => {
    const index = fields.indexOf('name');
    return String(fields[index + 1]);
  });
}

function parseDate(value: string | Date, field: string): Date {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid ${field}: ${String(value)}`);
  }
  return date;
}

function toRecord(row: SaleRow): SaleRecord {
  return {
    id: row.id,
    productId: row.product_id,
    startsAt: row.starts_at,
    endsAt: row.ends_at,
    totalStock: row.total_stock,
  };
}

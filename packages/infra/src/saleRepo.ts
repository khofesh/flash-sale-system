import type { Pool } from 'pg';
import type { Redis } from './redis.js';

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
}

export interface SaleSeed {
  id: string;
  productId: string;
  startsAt?: string | Date;
  endsAt?: string | Date;
  totalStock: number;
}

interface SaleRow {
  id: string;
  product_id: string;
  starts_at: Date;
  ends_at: Date;
  total_stock: number;
}

/** All keys for one sale share a hash tag, so they stay in one Redis Cluster slot. */
export function saleRedisKeys(saleId: string) {
  const tag = `{${saleId}}`;
  return {
    meta: `sale:${tag}:meta`,
    stock: `sale:${tag}:stock`,
    buyers: `sale:${tag}:buyers`,
    reservations: `sale:${tag}:reservations`,
  } as const;
}

/**
 * Creates the sale in Postgres if it is missing, then mirrors it into Redis.
 * Safe to run from every API replica on every start: the insert is conditional
 * and the Redis stock is only initialised when the key does not exist yet, so a
 * restart never restores already-sold units.
 */
export async function seedSale(
  pool: Pool,
  redis: Redis,
  seed: SaleSeed,
  now = Date.now(),
): Promise<SaleRecord> {
  const sale = resolveSeed(seed, now);

  const inserted = await pool.query<SaleRow>(
    `INSERT INTO sales (id, product_id, starts_at, ends_at, total_stock)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (id) DO NOTHING
     RETURNING id, product_id, starts_at, ends_at, total_stock`,
    [sale.id, sale.productId, sale.startsAt, sale.endsAt, sale.totalStock],
  );

  const row =
    inserted.rows[0] ??
    (
      await pool.query<SaleRow>(
        `SELECT id, product_id, starts_at, ends_at, total_stock FROM sales WHERE id = $1`,
        [sale.id],
      )
    ).rows[0];

  if (!row) {
    throw new Error(`Failed to seed sale ${sale.id}`);
  }

  await mirrorSale(redis, row, { keepStock: true });
  return toRecord(row);
}

/**
 * Creates or resets a sale. Postgres is written first, so Redis can always be
 * rebuilt from the durable sale definition. Resetting clears the buyers set and
 * the reservations stream, which also drops the stream consumer group.
 */
export async function resetSale(pool: Pool, redis: Redis, sale: SaleRecord): Promise<void> {
  const row = await pool.query<SaleRow>(
    `INSERT INTO sales (id, product_id, starts_at, ends_at, total_stock)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (id) DO UPDATE
       SET product_id = EXCLUDED.product_id,
           starts_at = EXCLUDED.starts_at,
           ends_at = EXCLUDED.ends_at,
           total_stock = EXCLUDED.total_stock
     RETURNING id, product_id, starts_at, ends_at, total_stock`,
    [sale.id, sale.productId, sale.startsAt, sale.endsAt, sale.totalStock],
  );

  const saved = row.rows[0];
  if (!saved) {
    throw new Error(`Failed to reset sale ${sale.id}`);
  }

  const keys = saleRedisKeys(saved.id);
  const results = await redis
    .multi()
    .del(keys.buyers)
    .del(keys.reservations)
    .hset(keys.meta, {
      startsAt: String(saved.starts_at.getTime()),
      endsAt: String(saved.ends_at.getTime()),
      totalStock: String(saved.total_stock),
    })
    .set(keys.stock, String(saved.total_stock))
    .exec();

  if (!results) {
    throw new Error(`Redis transaction for sale ${saved.id} was aborted`);
  }
  for (const [error] of results) {
    if (error) throw error;
  }
}

export async function getSaleSnapshot(redis: Redis, saleId: string): Promise<SaleSnapshot | null> {
  const keys = saleRedisKeys(saleId);
  const [meta, stock] = await Promise.all([redis.hgetall(keys.meta), redis.get(keys.stock)]);

  const startsAt = Number(meta.startsAt);
  const endsAt = Number(meta.endsAt);
  const totalStock = Number(meta.totalStock);
  const remainingStock = Number(stock);

  if (
    !Number.isFinite(startsAt) ||
    !Number.isFinite(endsAt) ||
    !Number.isFinite(totalStock) ||
    stock === null ||
    !Number.isFinite(remainingStock)
  ) {
    return null;
  }

  return { id: saleId, startsAt, endsAt, totalStock, remainingStock };
}

export function resolveSeed(seed: SaleSeed, now = Date.now()): SaleRecord {
  const startsAt = seed.startsAt
    ? parseDate(seed.startsAt, 'startsAt')
    : new Date(seed.endsAt ? now : now + 60_000);
  const endsAt = seed.endsAt
    ? parseDate(seed.endsAt, 'endsAt')
    : new Date(startsAt.getTime() + 30 * 60_000);

  if (endsAt.getTime() <= startsAt.getTime()) {
    throw new Error(`Sale ${seed.id} must end after it starts`);
  }
  if (!Number.isInteger(seed.totalStock) || seed.totalStock < 0) {
    throw new Error(`Sale ${seed.id} has an invalid totalStock`);
  }

  return { ...seed, startsAt, endsAt };
}

async function mirrorSale(
  redis: Redis,
  row: SaleRow,
  options: { keepStock: boolean },
): Promise<void> {
  const keys = saleRedisKeys(row.id);
  await redis.hset(keys.meta, {
    startsAt: String(row.starts_at.getTime()),
    endsAt: String(row.ends_at.getTime()),
    totalStock: String(row.total_stock),
  });

  if (options.keepStock) {
    await redis.set(keys.stock, String(row.total_stock), 'NX');
  } else {
    await redis.set(keys.stock, String(row.total_stock));
  }
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

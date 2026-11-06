import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer } from '@testcontainers/redis';
import type { StartedTestContainer } from 'testcontainers';
import {
  RedisReservationStore,
  createPgPool,
  createRedis,
  resetSale,
  resolveMigrationsDir,
  runMigrations,
  saleRedisKeys,
  type Pool,
  type Redis,
} from '@fss/infra';

const images = {
  redis: 'redis:7.4.2-alpine',
  postgres: 'postgres:16.4-alpine',
};

describe('reserve/release Lua scripts', () => {
  const saleId = 'reservation-test';
  let redisContainer: StartedTestContainer;
  let postgresContainer: StartedTestContainer;
  let redis: Redis;
  let pool: Pool;
  let store: RedisReservationStore;

  beforeAll(async () => {
    [redisContainer, postgresContainer] = await Promise.all([
      new RedisContainer(images.redis).start(),
      new PostgreSqlContainer(images.postgres)
        .withDatabase('flash')
        .withUsername('flash')
        .withPassword('flash')
        .start(),
    ]);

    redis = createRedis(
      `redis://${redisContainer.getHost()}:${redisContainer.getMappedPort(6379)}`,
    );
    pool = createPgPool(postgresContainer.getConnectionUri());
    await runMigrations(pool, await resolveMigrationsDir());

    store = new RedisReservationStore(redis);
    await store.loadScripts();
  });

  afterAll(async () => {
    await redis?.quit();
    await pool?.end();
    await Promise.all([redisContainer?.stop(), postgresContainer?.stop()]);
  });

  beforeEach(async () => {
    await pool.query('TRUNCATE orders, saga_events, processed_messages CASCADE');
    await redis.del(saleRedisKeys(saleId).buyers);
  });

  async function useSale(options: { startsAt?: Date; endsAt?: Date; totalStock: number }) {
    const now = Date.now();
    await resetSale(pool, redis, {
      id: saleId,
      productId: 'flash-tshirt',
      startsAt: options.startsAt ?? new Date(now - 60_000),
      endsAt: options.endsAt ?? new Date(now + 60_000),
      totalStock: options.totalStock,
    });
  }

  it('reserves one unit, rejects the same user, and then sells out', async () => {
    await useSale({ totalStock: 2 });
    const { stock } = saleRedisKeys(saleId);

    const first = await store.reserve(saleId, 'alice', randomUUID());
    expect(first.status).toBe('RESERVED');
    expect(await redis.get(stock)).toBe('1');

    expect(await store.reserve(saleId, 'alice', randomUUID())).toEqual({
      status: 'ALREADY_PURCHASED',
    });

    expect((await store.reserve(saleId, 'bob', randomUUID())).status).toBe('RESERVED');
    expect(await store.reserve(saleId, 'carol', randomUUID())).toEqual({ status: 'SOLD_OUT' });
    expect(await redis.get(stock)).toBe('0');
  });

  it('rejects reservations outside the sale window', async () => {
    await useSale({
      startsAt: new Date(Date.now() + 60_000),
      endsAt: new Date(Date.now() + 120_000),
      totalStock: 5,
    });
    expect(await store.reserve(saleId, 'early', randomUUID())).toEqual({ status: 'NOT_STARTED' });

    await useSale({
      startsAt: new Date(Date.now() - 120_000),
      endsAt: new Date(Date.now() - 60_000),
      totalStock: 5,
    });
    expect(await store.reserve(saleId, 'late', randomUUID())).toEqual({ status: 'ENDED' });
  });

  it('returns NOT_FOUND for an unknown sale and UNAVAILABLE when the stock key is gone', async () => {
    expect(await store.reserve('missing-sale', 'alice', randomUUID())).toEqual({
      status: 'NOT_FOUND',
    });

    await useSale({ totalStock: 5 });
    await redis.del(saleRedisKeys(saleId).stock);
    expect(await store.reserve(saleId, 'alice', randomUUID())).toEqual({
      status: 'UNAVAILABLE',
    });
  });

  it('releases a unit only once', async () => {
    await useSale({ totalStock: 1 });
    const { stock } = saleRedisKeys(saleId);
    expect((await store.reserve(saleId, 'alice', randomUUID())).status).toBe('RESERVED');
    expect(await redis.get(stock)).toBe('0');

    expect(await store.release(saleId, 'alice')).toBe(true);
    expect(await redis.get(stock)).toBe('1');

    expect(await store.release(saleId, 'alice')).toBe(false);
    expect(await redis.get(stock)).toBe('1');

    expect(await store.release(saleId, 'never-reserved')).toBe(false);
    expect(await redis.get(stock)).toBe('1');
  });

  it('reloads its scripts after SCRIPT FLUSH', async () => {
    await useSale({ totalStock: 1 });
    await redis.script('FLUSH');

    const result = await store.reserve(saleId, 'alice', randomUUID());
    expect(result.status).toBe('RESERVED');
    expect(await redis.get(saleRedisKeys(saleId).stock)).toBe('0');
  });

  it('reserves exactly the stock under 1,000 parallel requests', async () => {
    const totalStock = 100;
    await useSale({ totalStock });

    const results = await Promise.all(
      Array.from({ length: 1_000 }, (_, index) =>
        store.reserve(saleId, `user-${index}`, randomUUID()),
      ),
    );

    const reserved = results.filter((result) => result.status === 'RESERVED');
    const soldOut = results.filter((result) => result.status === 'SOLD_OUT');
    expect(reserved).toHaveLength(totalStock);
    expect(soldOut).toHaveLength(1_000 - totalStock);

    const { stock, buyers } = saleRedisKeys(saleId);
    expect(await redis.get(stock)).toBe('0');
    expect(await redis.scard(buyers)).toBe(totalStock);
  });

  it('allows exactly one reservation when one user hammers the endpoint 500 times', async () => {
    await useSale({ totalStock: 50 });

    const results = await Promise.all(
      Array.from({ length: 500 }, () => store.reserve(saleId, 'same-user', randomUUID())),
    );

    expect(results.filter((result) => result.status === 'RESERVED')).toHaveLength(1);
    expect(results.filter((result) => result.status === 'ALREADY_PURCHASED')).toHaveLength(499);

    const { stock, buyers } = saleRedisKeys(saleId);
    expect(await redis.get(stock)).toBe('49');
    expect(await redis.scard(buyers)).toBe(1);
  });
});

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer } from '@testcontainers/redis';
import type { StartedTestContainer } from 'testcontainers';
import {
  createLogger,
  createPgPool,
  createRedis,
  resetSale as resetSaleRepo,
  resolveMigrationsDir,
  runMigrations,
  saleRedisKeys,
  seedSale,
  type Logger,
  type Pool,
  type Redis,
} from '@fss/infra';
import { buildApp } from '../apps/api/src/app.ts';

const images = {
  redis: 'redis:7.4.2-alpine',
  postgres: 'postgres:16.4-alpine',
};

const saleId = 'launch-2026';
const adminToken = 'test-admin-token';

describe('/api/sale', () => {
  let redisContainer: StartedTestContainer;
  let postgresContainer: StartedTestContainer;
  let redis: Redis;
  let pool: Pool;
  let logger: Logger;
  let app: ReturnType<typeof buildApp>;

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
    logger = createLogger('sale-integration', process.env.INT_LOG ?? 'silent');

    await runMigrations(pool, await resolveMigrationsDir());

    app = buildApp({
      redis,
      pool,
      logger,
      saleId,
      productId: 'flash-tshirt',
      adminToken,
      saleCacheMs: 0,
    });
    await app.ready();
  });

  afterAll(async () => {
    await app?.close();
    await redis?.quit();
    await pool?.end();
    await Promise.all([redisContainer?.stop(), postgresContainer?.stop()]);
  });

  async function resetSale(body: { startsAt: string; endsAt: string; totalStock: number }) {
    return app.inject({
      method: 'POST',
      url: '/admin/sale',
      headers: { 'x-admin-token': adminToken },
      payload: body,
    });
  }

  it('returns 404 before a sale has been configured', async () => {
    const response = await app.inject({ method: 'GET', url: '/api/sale' });
    expect(response.statusCode).toBe(404);
  });

  it('rejects an admin reset without the token', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/admin/sale',
      payload: {
        startsAt: new Date(Date.now() + 30_000).toISOString(),
        endsAt: new Date(Date.now() + 60_000).toISOString(),
        totalStock: 10,
      },
    });
    expect(response.statusCode).toBe(401);
  });

  it('reports an upcoming sale and the server time', async () => {
    const startsAt = new Date(Date.now() + 30_000).toISOString();
    const endsAt = new Date(Date.now() + 60_000).toISOString();
    const reset = await resetSale({ startsAt, endsAt, totalStock: 5 });
    expect(reset.statusCode).toBe(200);

    const response = await app.inject({ method: 'GET', url: '/api/sale' });
    expect(response.statusCode).toBe(200);
    const view = response.json();

    expect(view).toMatchObject({
      saleId,
      status: 'UPCOMING',
      startsAt,
      endsAt,
      remainingStock: 5,
      totalStock: 5,
    });
    expect(Number.isNaN(Date.parse(view.serverTime))).toBe(false);
  });

  it('reports an active sale with its remaining stock', async () => {
    const startsAt = new Date(Date.now() - 10_000).toISOString();
    const endsAt = new Date(Date.now() + 60_000).toISOString();
    expect((await resetSale({ startsAt, endsAt, totalStock: 3 })).statusCode).toBe(200);

    const response = await app.inject({ method: 'GET', url: '/api/sale' });
    expect(response.json()).toMatchObject({
      status: 'ACTIVE',
      remainingStock: 3,
      totalStock: 3,
    });
    // Polling storms are absorbed by the browser/proxy cache (SPEC §9.2).
    expect(response.headers['cache-control']).toBe('max-age=1');
  });

  it('reports SOLD_OUT when the window is open but stock is zero', async () => {
    const startsAt = new Date(Date.now() - 10_000).toISOString();
    const endsAt = new Date(Date.now() + 60_000).toISOString();
    expect((await resetSale({ startsAt, endsAt, totalStock: 1 })).statusCode).toBe(200);
    await redis.set(saleRedisKeys(saleId).stock, '0');

    const response = await app.inject({ method: 'GET', url: '/api/sale' });
    expect(response.json()).toMatchObject({ status: 'SOLD_OUT', remainingStock: 0 });
  });

  it('reports ENDED once the window has closed', async () => {
    const startsAt = new Date(Date.now() - 60_000).toISOString();
    const endsAt = new Date(Date.now() - 1_000).toISOString();
    expect((await resetSale({ startsAt, endsAt, totalStock: 4 })).statusCode).toBe(200);

    const response = await app.inject({ method: 'GET', url: '/api/sale' });
    expect(response.json()).toMatchObject({ status: 'ENDED', remainingStock: 4 });
  });

  it('rejects a totalStock that does not fit the database column', async () => {
    const response = await resetSale({
      startsAt: new Date(Date.now() + 30_000).toISOString(),
      endsAt: new Date(Date.now() + 60_000).toISOString(),
      totalStock: 3_000_000_000,
    });
    expect(response.statusCode).toBe(400);
  });

  it('refuses a reset while the sale has live reservations', async () => {
    const startsAt = new Date(Date.now() - 10_000).toISOString();
    const endsAt = new Date(Date.now() + 60_000).toISOString();
    expect((await resetSale({ startsAt, endsAt, totalStock: 2 })).statusCode).toBe(200);
    await redis.sadd(saleRedisKeys(saleId).buyers, 'user-1');

    const response = await resetSale({ startsAt, endsAt, totalStock: 2 });
    expect(response.statusCode).toBe(409);
    expect(response.json()).toMatchObject({ code: 'SALE_HAS_ORDERS' });

    await redis.del(saleRedisKeys(saleId).buyers);
  });

  it('keeps stream consumer groups across a reset', async () => {
    const { reservations } = saleRedisKeys(saleId);
    await redis.xgroup('CREATE', reservations, 'orchestrator', '$', 'MKSTREAM');
    await redis.xadd(reservations, '*', 'userId', 'user-1');

    const startsAt = new Date(Date.now() + 30_000).toISOString();
    const endsAt = new Date(Date.now() + 60_000).toISOString();
    expect((await resetSale({ startsAt, endsAt, totalStock: 2 })).statusCode).toBe(200);

    expect(await redis.xlen(reservations)).toBe(0);
    const groups = (await redis.xinfo('GROUPS', reservations)) as unknown[][];
    expect(groups.map((g) => g[g.indexOf('name') + 1])).toEqual(['orchestrator']);
  });

  it('returns 503 when the sale is defined but its stock key is gone', async () => {
    const startsAt = new Date(Date.now() - 10_000).toISOString();
    const endsAt = new Date(Date.now() + 60_000).toISOString();
    expect((await resetSale({ startsAt, endsAt, totalStock: 2 })).statusCode).toBe(200);
    await redis.del(saleRedisKeys(saleId).stock);

    const response = await app.inject({ method: 'GET', url: '/api/sale' });
    expect(response.statusCode).toBe(503);
  });

  it('does not register admin routes without a token', async () => {
    const bare = buildApp({ redis, pool, logger, saleId, saleCacheMs: 0 });
    await bare.ready();
    const response = await bare.inject({
      method: 'POST',
      url: '/admin/sale',
      headers: { 'x-admin-token': 'dev-admin-token' },
      payload: {
        startsAt: new Date().toISOString(),
        endsAt: new Date().toISOString(),
        totalStock: 1,
      },
    });
    expect(response.statusCode).toBe(404);
    await bare.close();
  });
});

describe('seedSale', () => {
  let redisContainer: StartedTestContainer;
  let postgresContainer: StartedTestContainer;
  let redis: Redis;
  let pool: Pool;

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
  });

  afterAll(async () => {
    await redis?.quit();
    await pool?.end();
    await Promise.all([redisContainer?.stop(), postgresContainer?.stop()]);
  });

  it('boots after the sale has ended when only endsAt is configured', async () => {
    const seed = {
      id: 'ended-sale',
      productId: 'p',
      endsAt: '2026-01-01T12:00:00Z',
      totalStock: 5,
    };
    const first = await seedSale(pool, redis, seed, Date.parse('2026-01-01T11:00:00Z'));
    expect(first.created).toBe(true);

    const later = await seedSale(pool, redis, seed, Date.parse('2026-01-01T13:00:00Z'));
    expect(later).toMatchObject({ created: false, stockMissing: false });
    expect(later.sale.endsAt.toISOString()).toBe('2026-01-01T12:00:00.000Z');
  });

  it('never restores stock for an existing sale when Redis lost it', async () => {
    const seed = { id: 'lost-redis', productId: 'p', totalStock: 10 };
    await seedSale(pool, redis, seed);
    const { stock } = saleRedisKeys(seed.id);
    await redis.set(stock, '4');

    expect((await seedSale(pool, redis, seed)).stockMissing).toBe(false);
    expect(await redis.get(stock)).toBe('4');

    await redis.flushall();
    const result = await seedSale(pool, redis, seed);
    expect(result).toMatchObject({ created: false, stockMissing: true });
    expect(await redis.get(stock)).toBeNull();
  });

  it('a direct reset restores full stock when there are no reservations', async () => {
    const sale = {
      id: 'lost-redis',
      productId: 'p',
      startsAt: new Date(),
      endsAt: new Date(Date.now() + 60_000),
      totalStock: 10,
    };
    await resetSaleRepo(pool, redis, sale);
    expect(await redis.get(saleRedisKeys(sale.id).stock)).toBe('10');
  });
});

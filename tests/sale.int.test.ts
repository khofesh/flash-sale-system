import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer } from '@testcontainers/redis';
import type { StartedTestContainer } from 'testcontainers';
import {
  createLogger,
  createPgPool,
  createRedis,
  resolveMigrationsDir,
  runMigrations,
  saleRedisKeys,
  type Logger,
  type Pool,
  type Redis,
} from '@fss/infra';
import { buildApp } from '../apps/api/src/app.js';

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
});

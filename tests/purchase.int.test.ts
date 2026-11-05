import { setTimeout as sleep } from 'node:timers/promises';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer } from '@testcontainers/redis';
import type { StartedTestContainer } from 'testcontainers';
import {
  RedisReservationStore,
  createLogger,
  createPgPool,
  createRedis,
  resetSale,
  resolveMigrationsDir,
  runMigrations,
  saleRedisKeys,
  type Logger,
  type Pool,
  type Redis,
  type StopFn,
} from '@fss/infra';
import { buildApp } from '../apps/api/src/app.js';
import { startOrchestrator } from '../apps/orchestrator/src/worker.js';

const images = {
  redis: 'redis:7.4.2-alpine',
  postgres: 'postgres:16.4-alpine',
};

describe('purchases', () => {
  const saleId = 'purchase-test';
  let redisContainer: StartedTestContainer;
  let postgresContainer: StartedTestContainer;
  let redis: Redis;
  let workerRedis: Redis;
  let pool: Pool;
  let logger: Logger;
  let app: ReturnType<typeof buildApp>;
  let stop: StopFn;

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
    // The orchestrator blocks on XREADGROUP, so it needs its own connection.
    workerRedis = createRedis(
      `redis://${redisContainer.getHost()}:${redisContainer.getMappedPort(6379)}`,
    );
    pool = createPgPool(postgresContainer.getConnectionUri());
    logger = createLogger('purchase-integration', process.env.INT_LOG ?? 'silent');

    await runMigrations(pool, await resolveMigrationsDir());

    const reservations = new RedisReservationStore(redis, logger);
    await reservations.loadScripts();

    app = buildApp({
      redis,
      pool,
      logger,
      saleId,
      productId: 'flash-tshirt',
      adminToken: 'test-admin-token',
      saleCacheMs: 0,
      reservations,
    });
    await app.ready();

    stop = await startOrchestrator({
      redis: workerRedis,
      pool,
      logger,
      saleId,
      claimIdleMs: 200,
      claimIntervalMs: 200,
    });
  });

  afterAll(async () => {
    await stop?.();
    await app?.close();
    await redis?.quit();
    await workerRedis?.quit();
    await pool?.end();
    await Promise.all([redisContainer?.stop(), postgresContainer?.stop()]);
  });

  beforeEach(async () => {
    await pool.query('TRUNCATE orders, saga_events, processed_messages CASCADE');
    await redis.del(saleRedisKeys(saleId).buyers);
  });

  async function useSale(options: {
    totalStock: number;
    startsAt?: Date;
    endsAt?: Date;
  }): Promise<void> {
    const now = Date.now();
    await resetSale(pool, redis, {
      id: saleId,
      productId: 'flash-tshirt',
      startsAt: options.startsAt ?? new Date(now - 60_000),
      endsAt: options.endsAt ?? new Date(now + 60_000),
      totalStock: options.totalStock,
    });
  }

  function postPurchase(userId: string, idempotencyKey?: string) {
    return app.inject({
      method: 'POST',
      url: '/api/purchases',
      headers: idempotencyKey ? { 'idempotency-key': idempotencyKey } : undefined,
      payload: { userId },
    });
  }

  async function getPurchase(userId: string) {
    return app.inject({
      method: 'GET',
      url: `/api/purchases/${encodeURIComponent(userId)}`,
    });
  }

  async function waitForStatus(userId: string, expected: string): Promise<Record<string, unknown>> {
    const deadline = Date.now() + 15_000;
    let view: Record<string, unknown> = {};

    while (Date.now() < deadline) {
      const response = await getPurchase(userId);
      if (response.statusCode === 200) {
        view = response.json();
        if (view.status === expected) return view;
      }
      await sleep(100);
    }

    throw new Error(
      `Timed out waiting for ${userId} to reach ${expected}: ${JSON.stringify(view)}`,
    );
  }

  it('reserves, persists and completes an order', async () => {
    await useSale({ totalStock: 3 });

    const response = await postPurchase('  Alice.Example@Example.COM ');
    expect(response.statusCode).toBe(202);
    const body = response.json();
    expect(body.code).toBe('RESERVED');
    expect(body.orderId).toMatch(/^[0-9a-f-]{36}$/);

    const view = await waitForStatus('alice.example@example.com', 'COMPLETED');
    expect(view).toMatchObject({
      userId: 'alice.example@example.com',
      secured: true,
      orderId: body.orderId,
      status: 'COMPLETED',
    });

    const order = await pool.query('SELECT sale_id, user_id, status FROM orders WHERE id = $1', [
      body.orderId,
    ]);
    expect(order.rows[0]).toEqual({
      sale_id: saleId,
      user_id: 'alice.example@example.com',
      status: 'COMPLETED',
    });

    const events = await pool.query(
      'SELECT type FROM saga_events WHERE order_id = $1 ORDER BY id',
      [body.orderId],
    );
    expect(events.rows.map((row) => row.type)).toEqual(['order.reserved', 'order.completed']);
    expect(await redis.get(saleRedisKeys(saleId).stock)).toBe('2');
  });

  it('returns 409 ALREADY_PURCHASED with the existing order id', async () => {
    await useSale({ totalStock: 5 });
    const first = await postPurchase('alice');
    expect(first.statusCode).toBe(202);
    const { orderId } = first.json();
    await waitForStatus('alice', 'COMPLETED');

    const second = await postPurchase('ALICE');
    expect(second.statusCode).toBe(409);
    expect(second.json()).toEqual({ code: 'ALREADY_PURCHASED', orderId });
    expect(await redis.get(saleRedisKeys(saleId).stock)).toBe('4');
  });

  it('returns 410 when the stock is gone', async () => {
    await useSale({ totalStock: 1 });
    expect((await postPurchase('first')).statusCode).toBe(202);

    const soldOut = await postPurchase('second');
    expect(soldOut.statusCode).toBe(410);
    expect(soldOut.json()).toEqual({ code: 'SOLD_OUT' });
  });

  it('rejects purchases outside the sale window', async () => {
    await useSale({
      totalStock: 5,
      startsAt: new Date(Date.now() + 60_000),
      endsAt: new Date(Date.now() + 120_000),
    });
    const early = await postPurchase('early');
    expect(early.statusCode).toBe(403);
    expect(early.json()).toEqual({ code: 'NOT_STARTED' });

    await useSale({
      totalStock: 5,
      startsAt: new Date(Date.now() - 120_000),
      endsAt: new Date(Date.now() - 60_000),
    });
    const late = await postPurchase('late');
    expect(late.statusCode).toBe(403);
    expect(late.json()).toEqual({ code: 'ENDED' });
  });

  it('validates the userId', async () => {
    await useSale({ totalStock: 5 });

    const missing = await app.inject({ method: 'POST', url: '/api/purchases', payload: {} });
    expect(missing.statusCode).toBe(400);
    expect(missing.json()).toMatchObject({ code: 'VALIDATION_ERROR' });

    for (const userId of ['', '   ', 'alice smith', 'alice/smith', 'a'.repeat(255)]) {
      const response = await postPurchase(userId);
      expect(response.statusCode, userId).toBe(400);
      expect(response.json()).toMatchObject({ code: 'VALIDATION_ERROR' });
    }
  });

  it('replays a response for a repeated Idempotency-Key', async () => {
    await useSale({ totalStock: 2 });

    const first = await postPurchase('alice', 'idem-1');
    expect(first.statusCode).toBe(202);
    const firstBody = first.json();

    const second = await postPurchase('alice', 'idem-1');
    expect(second.statusCode).toBe(202);
    expect(second.json()).toEqual(firstBody);

    await waitForStatus('alice', 'COMPLETED');
    expect(await redis.get(saleRedisKeys(saleId).stock)).toBe('1');
    const count = await pool.query<{ count: number }>('SELECT count(*)::int AS count FROM orders');
    expect(count.rows[0]?.count).toBe(1);
  });

  it('falls back to the Redis buyers set before the orchestrator persists', async () => {
    await useSale({ totalStock: 1 });
    await redis.sadd(saleRedisKeys(saleId).buyers, 'ghost');

    const response = await getPurchase('ghost');
    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ userId: 'ghost', secured: true, status: 'RESERVED' });

    const missing = await getPurchase('nobody');
    expect(missing.statusCode).toBe(404);
    expect(missing.json()).toEqual({ userId: 'nobody', secured: false });
  });

  it('is idempotent when the same reservation is delivered twice', async () => {
    await useSale({ totalStock: 2 });
    const response = await postPurchase('alice');
    const { orderId } = response.json();
    await waitForStatus('alice', 'COMPLETED');

    const { reservations } = saleRedisKeys(saleId);
    await redis.xadd(reservations, '*', 'reservationId', orderId, 'userId', 'alice', 'ts', '1');
    await sleep(500);

    const orders = await pool.query<{ count: number }>(
      'SELECT count(*)::int AS count FROM orders WHERE id = $1',
      [orderId],
    );
    expect(orders.rows[0]?.count).toBe(1);
    expect(await redis.get(saleRedisKeys(saleId).stock)).toBe('1');
  });
});

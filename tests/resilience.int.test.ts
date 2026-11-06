import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer } from '@testcontainers/redis';
import type { StartedTestContainer } from 'testcontainers';
import {
  InMemoryBus,
  RESERVATION_CONSUMER,
  RedisReservationStore,
  createLogger,
  createPgPool,
  createRedis,
  rebuildRedis,
  reconcileSale,
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
import { RESERVATIONS_GROUP, startOrchestrator } from '../apps/orchestrator/src/worker.js';
import { startPayment } from '../apps/payment/src/worker.js';
import { startShipping } from '../apps/shipping/src/worker.js';

const images = {
  redis: 'redis:7.4.2-alpine',
  postgres: 'postgres:16.4-alpine',
};

const saleId = 'resilience-test';
const adminToken = 'test-admin-token';
const totalStock = 5;

interface Workers {
  stopOrchestrator: StopFn;
  stopPayment: StopFn;
  stopShipping: StopFn;
}

describe('resilience', () => {
  let redisContainer: StartedTestContainer;
  let postgresContainer: StartedTestContainer;
  let redis: Redis;
  let workerRedis: Redis;
  let pool: Pool;
  let logger: Logger;
  let app: ReturnType<typeof buildApp>;
  let bus: InMemoryBus;
  let workers: Workers | undefined;

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
    workerRedis = createRedis(
      `redis://${redisContainer.getHost()}:${redisContainer.getMappedPort(6379)}`,
    );
    pool = createPgPool(postgresContainer.getConnectionUri());
    logger = createLogger('resilience-integration', process.env.INT_LOG ?? 'silent');

    await runMigrations(pool, await resolveMigrationsDir());

    const reservations = new RedisReservationStore(redis, logger);
    await reservations.loadScripts();

    app = buildApp({
      redis,
      pool,
      logger,
      saleId,
      productId: 'flash-tshirt',
      adminToken,
      saleCacheMs: 0,
      reservations,
    });
    await app.ready();
  });

  afterAll(async () => {
    await app?.close();
    await redis?.quit();
    await workerRedis?.quit();
    await pool?.end();
    await Promise.all([redisContainer?.stop(), postgresContainer?.stop()]);
  });

  beforeEach(async () => {
    await stopWorkers();
    await pool.query('TRUNCATE orders, saga_events, processed_messages RESTART IDENTITY CASCADE');
    await redis.flushall();
    await resetSale(pool, redis, {
      id: saleId,
      productId: 'flash-tshirt',
      startsAt: new Date(Date.now() - 60_000),
      endsAt: new Date(Date.now() + 60_000),
      totalStock,
    });
    bus = new InMemoryBus();
  });

  afterEach(async () => {
    await stopWorkers();
  });

  async function stopWorkers(): Promise<void> {
    if (!workers) return;
    const current = workers;
    workers = undefined;
    await current.stopPayment();
    await current.stopShipping();
    await current.stopOrchestrator();
  }

  async function startWorkers(): Promise<void> {
    const stopOrchestrator = await startOrchestrator({
      redis: workerRedis,
      pool,
      logger,
      bus,
      saleId,
      claimIdleMs: 100,
      claimIntervalMs: 50,
      paymentTimeoutMs: 60_000,
      sweepIntervalMs: 50,
    });
    const stopPayment = await startPayment({ bus, logger, latencyMs: 0, failureRate: 0 });
    const stopShipping = await startShipping({ bus, logger, latencyMs: 0, failureRate: 0 });
    workers = { stopOrchestrator, stopPayment, stopShipping };
  }

  function purchase(userId: string, idempotencyKey?: string) {
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
      await sleep(50);
    }

    throw new Error(
      `Timed out waiting for ${userId} to reach ${expected}: ${JSON.stringify(view)}`,
    );
  }

  it('persists a crashed consumer’s pending reservation exactly once', async () => {
    // Create the consumer group, then stop the orchestrator: this is the state a
    // crash leaves behind when it dies between persisting and acknowledging.
    await startWorkers();
    await stopWorkers();

    const response = await purchase('crash');
    expect(response.statusCode).toBe(202);
    const { orderId } = response.json();

    const stream = saleRedisKeys(saleId).reservations;
    const batches = (await workerRedis.xreadgroup(
      'GROUP',
      RESERVATIONS_GROUP,
      'crashed-consumer',
      'COUNT',
      10,
      'STREAMS',
      stream,
      '>',
    )) as Array<[string, Array<[string, string[]]>]> | null;
    expect(batches?.[0]?.[1]).toHaveLength(1);

    // The entry stays pending, as it would after a crash before XACK.
    await sleep(150);

    await startWorkers();
    const view = await waitForStatus('crash', 'COMPLETED');
    expect(view).toMatchObject({ orderId, status: 'COMPLETED' });

    const orders = await pool.query<{ count: number }>(
      'SELECT count(*)::int AS count FROM orders WHERE id = $1',
      [orderId],
    );
    expect(orders.rows[0]?.count).toBe(1);

    const processed = await pool.query<{ count: number }>(
      'SELECT count(*)::int AS count FROM processed_messages WHERE consumer = $1 AND message_id = $2',
      [RESERVATION_CONSUMER, orderId],
    );
    expect(processed.rows[0]?.count).toBe(1);

    const reservedEvents = await pool.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM saga_events
        WHERE order_id = $1 AND type = 'order.reserved'`,
      [orderId],
    );
    expect(reservedEvents.rows[0]?.count).toBe(1);
    expect(await redis.get(saleRedisKeys(saleId).stock)).toBe(String(totalStock - 1));

    await waitUntil(async () => {
      const groups = (await workerRedis.xinfo('GROUPS', stream)) as unknown[][];
      return groups.every((fields) => Number(fields[fields.indexOf('pending') + 1]) === 0);
    }, 'the pending entry to be acknowledged');
  });

  it('fails closed with 503 and Retry-After, never 500, while Redis is down', async () => {
    redis.disconnect();
    await waitUntil(async () => redis.status === 'end', 'the redis client to disconnect');

    try {
      const sale = await app.inject({ method: 'GET', url: '/api/sale' });
      const buy = await purchase('redis-down');
      const buyIdempotent = await purchase('redis-down', 'redis-down-key');
      const status = await getPurchase('nobody');
      const ready = await app.inject({ method: 'GET', url: '/readyz' });
      const reconcile = await app.inject({
        method: 'GET',
        url: '/admin/reconcile',
        headers: { 'x-admin-token': adminToken },
      });

      for (const response of [sale, buy, buyIdempotent, status, ready, reconcile]) {
        expect(response.statusCode).toBe(503);
      }

      expect(sale.headers['retry-after']).toBe('1');
      expect(buy.headers['retry-after']).toBe('1');
      expect(buyIdempotent.headers['retry-after']).toBe('1');
      expect(status.headers['retry-after']).toBe('1');
      expect(reconcile.headers['retry-after']).toBe('1');
    } finally {
      await redis.connect();
      await waitUntil(async () => redis.status === 'ready', 'the redis client to reconnect');
    }

    // Redis is back: purchases work again, and nothing was lost while it was down.
    const recovered = await purchase('redis-recovered');
    expect(recovered.statusCode).toBe(202);
    expect(await redis.get(saleRedisKeys(saleId).stock)).toBe(String(totalStock - 1));
  });

  it('falls back to Redis for purchase status when Postgres is down', async () => {
    await redis.sadd(saleRedisKeys(saleId).buyers, 'pg-down-buyer');

    const failingPool = new Proxy(pool, {
      get(target, key) {
        if (key === 'query') {
          return async () => {
            throw new Error('postgres is down');
          };
        }
        const value = Reflect.get(target, key) as unknown;
        return typeof value === 'function' ? value.bind(target) : value;
      },
    }) as Pool;

    const pgDownApp = buildApp({ redis, pool: failingPool, logger, saleId, saleCacheMs: 0 });
    await pgDownApp.ready();

    try {
      const response = await pgDownApp.inject({
        method: 'GET',
        url: '/api/purchases/pg-down-buyer',
      });
      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({
        userId: 'pg-down-buyer',
        secured: true,
        status: 'RESERVED',
      });

      // The sale view only needs Redis.
      const sale = await pgDownApp.inject({ method: 'GET', url: '/api/sale' });
      expect(sale.statusCode).toBe(200);
    } finally {
      await pgDownApp.close();
    }
  });

  it('rebuilds stock, buyers and holders from Postgres and unconsumed entries', async () => {
    const alice = randomUUID();
    const bob = randomUUID();
    const carol = randomUUID();
    const dave = randomUUID();
    const erin = randomUUID();
    const erinLater = randomUUID();
    const conflict = randomUUID();

    await pool.query(
      `INSERT INTO orders (id, sale_id, user_id, status, released_at) VALUES
         ($1, $5, 'alice', 'COMPLETED', NULL),
         ($2, $5, 'bob',   'RESERVED',  NULL),
         ($3, $5, 'carol', 'CANCELLED', NULL),
         ($4, $5, 'dave',  'REFUNDED',  now())`,
      [alice, bob, carol, dave, saleId],
    );

    // Simulate a Redis restart that lost the sale state but kept the stream.
    const keys = saleRedisKeys(saleId);
    await redis.del(keys.meta, keys.stock, keys.buyers, keys.holders);
    await redis.xadd(keys.reservations, '*', 'reservationId', erin, 'userId', 'erin', 'ts', '1');
    await redis.xadd(
      keys.reservations,
      '*',
      'reservationId',
      erinLater,
      'userId',
      'erin',
      'ts',
      '2',
    );
    // A conflicting reservation for a user who already has an order is ignored.
    await redis.xadd(
      keys.reservations,
      '*',
      'reservationId',
      conflict,
      'userId',
      'alice',
      'ts',
      '3',
    );

    const report = await rebuildRedis(pool, redis, saleId);
    expect(report).toMatchObject({
      saleId,
      totalStock,
      liveOrders: 2,
      pendingReleases: 1,
      unpersisted: 1,
      buyers: 4,
      stock: 1,
    });

    expect(await redis.get(keys.stock)).toBe('1');
    expect(await redis.hgetall(keys.holders)).toEqual({
      alice,
      bob,
      carol,
      erin: erinLater,
    });
    expect(new Set(await redis.smembers(keys.buyers))).toEqual(
      new Set(['alice', 'bob', 'carol', 'erin']),
    );
    // The stream is left alone so pending entries keep their consumer group.
    expect(await redis.xlen(keys.reservations)).toBe(3);
    expect(await redis.hget(keys.meta, 'totalStock')).toBe(String(totalStock));

    const reconcile = await reconcileSale(pool, redis, saleId);
    expect(reconcile).toMatchObject({
      stock: 1,
      liveOrders: 2,
      pendingReleases: 1,
      unpersisted: 1,
      drift: 0,
      buyersConsistent: true,
      ok: true,
    });
  });
});

async function waitUntil(check: () => Promise<boolean>, what: string): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (await check()) return;
    await sleep(25);
  }
  throw new Error(`Timed out waiting for ${what}`);
}

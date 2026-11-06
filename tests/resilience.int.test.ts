import { randomUUID } from 'node:crypto';
import { connect, createServer, type Server, type Socket } from 'node:net';
import { setTimeout as sleep } from 'node:timers/promises';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer } from '@testcontainers/redis';
import type { StartedTestContainer } from 'testcontainers';
import {
  InMemoryBus,
  RESERVATION_CONSUMER,
  RebuildInProgressError,
  RedisReservationStore,
  SaleStockMissingError,
  createLogger,
  createPgPool,
  createRedis,
  fenceRestartedSale,
  persistReservation,
  readRedisRunId,
  rebuildRedis,
  redisReady,
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

  async function startWorkers(orchestratorPool: Pool = pool): Promise<void> {
    const stopOrchestrator = await startOrchestrator({
      redis: workerRedis,
      pool: orchestratorPool,
      logger,
      bus,
      saleId,
      claimIdleMs: 100,
      claimIntervalMs: 50,
      paymentTimeoutMs: 60_000,
      sweepIntervalMs: 50,
      restartCheckMs: 50,
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
    // Create the consumer group, then stop the orchestrator so the crash below is
    // the only consumer of the entry.
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

    // The crash: the order is committed, but the entry is never acknowledged. The
    // redelivery below therefore hits the duplicate path, not a first insert.
    await persistReservation(pool, {
      saleId,
      orderId,
      userId: 'crash',
      reservedAt: Date.now(),
    });
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
      // The first entry becomes the order; the later one is released as a conflict.
      erin,
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

  it('does not rebuild a hold for a reservation that was already handled', async () => {
    const dead = randomUUID();
    const conflict = randomUUID();
    const keys = saleRedisKeys(saleId);

    // The conflict was handled while the order was live, and the order has since
    // died and been released. Nothing will ever process that entry again.
    await pool.query(
      `INSERT INTO orders (id, sale_id, user_id, status, released_at)
       VALUES ($1, $2, 'alice', 'CANCELLED', now())`,
      [dead, saleId],
    );
    await pool.query(`INSERT INTO processed_messages (consumer, message_id) VALUES ($1, $2)`, [
      RESERVATION_CONSUMER,
      conflict,
    ]);
    await redis.xadd(keys.reservations, '*', 'reservationId', conflict, 'userId', 'alice');

    const report = await rebuildRedis(pool, redis, saleId);
    expect(report).toMatchObject({ stock: totalStock, unpersisted: 0, buyers: 0 });

    const store = new RedisReservationStore(redis, logger);
    expect((await store.reserve(saleId, 'alice', randomUUID())).status).toBe('RESERVED');
  });

  it('reports a handled reservation that still holds a unit as drift', async () => {
    const conflict = randomUUID();
    const keys = saleRedisKeys(saleId);

    await pool.query(`INSERT INTO processed_messages (consumer, message_id) VALUES ($1, $2)`, [
      RESERVATION_CONSUMER,
      conflict,
    ]);
    await redis.xadd(keys.reservations, '*', 'reservationId', conflict, 'userId', 'alice');
    await redis.hset(keys.holders, 'alice', conflict);
    await redis.sadd(keys.buyers, 'alice');
    await redis.decr(keys.stock);

    expect(await reconcileSale(pool, redis, saleId)).toMatchObject({ unpersisted: 0, ok: false });
  });

  it('gives a newer reservation the unit over a dead order awaiting release', async () => {
    const dead = randomUUID();
    const rebuy = randomUUID();
    const keys = saleRedisKeys(saleId);

    // The dead order's unit went back and the user bought again, but the release
    // was never recorded and the new reservation is not persisted yet.
    await pool.query(
      `INSERT INTO orders (id, sale_id, user_id, status) VALUES ($1, $2, 'bob', 'CANCELLED')`,
      [dead, saleId],
    );
    await redis.xadd(keys.reservations, '*', 'reservationId', rebuy, 'userId', 'bob');

    const report = await rebuildRedis(pool, redis, saleId);
    expect(report).toMatchObject({ stock: totalStock - 1, unpersisted: 1, pendingReleases: 0 });
    expect(await redis.hget(keys.holders, 'bob')).toBe(rebuy);

    // The workers persist the new reservation and sweep the old order, and the
    // sweep must not hand back the unit the new order holds.
    await startWorkers();
    await waitForStatus('bob', 'COMPLETED');
    await waitUntil(async () => {
      const result = await pool.query('SELECT released_at FROM orders WHERE id = $1', [dead]);
      return result.rows[0]?.released_at !== null;
    }, 'the old order to be swept');

    expect(await redis.get(keys.stock)).toBe(String(totalStock - 1));
    expect(await redis.sismember(keys.buyers, 'bob')).toBe(1);
    expect(await reconcileSale(pool, redis, saleId)).toMatchObject({ drift: 0, ok: true });
  });

  it('keeps a sale with no stock key closed to reservations and releases', async () => {
    const store = new RedisReservationStore(redis, logger);
    const orderId = randomUUID();
    const keys = saleRedisKeys(saleId);
    expect((await store.reserve(saleId, 'fenced', orderId)).status).toBe('RESERVED');

    await redis.del(keys.stock);

    expect((await store.reserve(saleId, 'other', randomUUID())).status).toBe('UNAVAILABLE');
    await expect(store.release(saleId, 'fenced', orderId)).rejects.toBeInstanceOf(
      SaleStockMissingError,
    );
    // A release that reached INCR would have recreated the key and reopened the sale.
    expect(await redis.exists(keys.stock)).toBe(0);
    expect(await redis.hget(keys.holders, 'fenced')).toBe(orderId);
  });

  it('rebuilds the sale by itself when Redis comes back from a restart', async () => {
    const keys = saleRedisKeys(saleId);
    await startWorkers();
    const runId = await readRedisRunId(redis);
    await waitUntil(async () => (await redis.get(keys.redisRunId)) === runId, 'the run id record');

    // What a restart leaves: state missing the last writes, under a new run id.
    await redis.set(keys.stock, String(totalStock + 3));
    await redis.set(keys.redisRunId, 'previous-run');

    await waitUntil(
      async () => (await redis.get(keys.redisRunId)) === runId,
      'the rebuild after the restart',
    );
    expect(await redis.get(keys.stock)).toBe(String(totalStock));
  });

  it('never closes a sale that another replica has already rebuilt', async () => {
    const keys = saleRedisKeys(saleId);
    const runId = await readRedisRunId(redis);
    await redis.set(keys.redisRunId, 'previous-run');

    // Still the old run: this replica closes the sale.
    expect(await fenceRestartedSale(redis, saleId, runId)).toBe(true);
    expect(await redis.exists(keys.stock)).toBe(0);

    await rebuildRedis(pool, redis, saleId);

    // A second replica that read the old run id just before that rebuild finished.
    // Deleting stock now would strand the sale: run_id matches, so no check reruns.
    expect(await fenceRestartedSale(redis, saleId, runId)).toBe(false);
    expect(await redis.get(keys.stock)).toBe(String(totalStock));
  });

  it('reports a rebuild that lost the lock with a dedicated error', async () => {
    const keys = saleRedisKeys(saleId);
    await redis.set(keys.rebuildLock, '1', 'EX', 30);
    await expect(rebuildRedis(pool, redis, saleId)).rejects.toBeInstanceOf(RebuildInProgressError);
  });

  it('survives Postgres dropping an idle connection', async () => {
    const victim = createPgPool(postgresContainer.getConnectionUri());
    try {
      const { rows } = await victim.query<{ pid: number }>('SELECT pg_backend_pid() AS pid');
      await pool.query('SELECT pg_terminate_backend($1)', [rows[0]?.pid]);

      // Without a pool error listener this is an uncaught exception.
      await waitUntil(async () => victim.totalCount === 0, 'the dead client to be discarded');
      await expect(victim.query('SELECT 1')).resolves.toBeDefined();
    } finally {
      await victim.end();
    }
  });

  it('survives Postgres dropping a connection in the middle of a transaction', async () => {
    const victim = createPgPool(postgresContainer.getConnectionUri());
    const client = await victim.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query<{ pid: number }>('SELECT pg_backend_pid() AS pid');
      await pool.query('SELECT pg_terminate_backend($1)', [rows[0]?.pid]);

      // A checked-out client has no pool listener: without its own this is uncaught.
      await sleep(250);
      await expect(client.query('SELECT 1')).rejects.toThrow();
    } finally {
      client.release(true);
      await victim.end();
    }
  });

  it('fails a query on a stalled Postgres connection, then recovers', async () => {
    const proxy = new TcpProxy(postgresContainer.getHost(), postgresContainer.getMappedPort(5432));
    const port = await proxy.start();
    const stalling = createPgPool(`postgres://flash:flash@127.0.0.1:${port}/flash`);

    try {
      await stalling.query('SELECT 1');

      proxy.stall();
      await expect(stalling.query('SELECT 1')).rejects.toThrow(/timeout/i);
      proxy.resume();

      await expect(stalling.query('SELECT 1')).resolves.toBeDefined();
    } finally {
      await stalling.end();
      await proxy.stop();
    }
  });

  it('buffers reservations through a Postgres outage and finishes them after', async () => {
    let down = false;
    const unavailable = async () => {
      throw new Error('postgres is down');
    };
    const flakyPool = new Proxy(pool, {
      get(target, key) {
        if (down && (key === 'query' || key === 'connect')) return unavailable;
        const value = Reflect.get(target, key) as unknown;
        return typeof value === 'function' ? value.bind(target) : value;
      },
    }) as Pool;

    await startWorkers(flakyPool);
    down = true;

    const response = await purchase('pg-outage');
    expect(response.statusCode).toBe(202);
    const { orderId } = response.json();

    // Longer than one retry cycle plus the reclaim idle time.
    await sleep(4_000);
    const during = await pool.query('SELECT 1 FROM orders WHERE id = $1', [orderId]);
    expect(during.rowCount).toBe(0);

    down = false;
    await waitForStatus('pg-outage', 'COMPLETED');
    expect(await redis.get(saleRedisKeys(saleId).stock)).toBe(String(totalStock - 1));
  });

  it('records a saga message once when its publish is retried', async () => {
    const publish = bus.publish.bind(bus);
    let failures = 2;
    bus.publish = async (queue, body) => {
      if (queue === 'payment-requests' && failures > 0) {
        failures -= 1;
        throw new Error('broker is down');
      }
      await publish(queue, body);
    };

    await startWorkers();
    const { orderId } = (await purchase('broker-flaky')).json();
    await waitForStatus('broker-flaky', 'COMPLETED');

    expect(failures).toBe(0);
    const events = await pool.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM saga_events
        WHERE order_id = $1 AND type = 'payment.requested'`,
      [orderId],
    );
    expect(events.rows[0]?.count).toBe(1);
  });

  it('claims an idempotency key only briefly, then keeps the response', async () => {
    let claimTtl = -1;
    const store = new RedisReservationStore(redis, logger);
    const claimApp = buildApp({
      redis,
      pool,
      logger,
      saleId,
      saleCacheMs: 0,
      reservations: {
        reserve: async (...args) => {
          const [key] = await redis.keys('idempotency:*');
          claimTtl = await redis.ttl(key ?? '');
          return store.reserve(...args);
        },
        release: (...args) => store.release(...args),
        holder: (...args) => store.holder(...args),
        isBuyer: (...args) => store.isBuyer(...args),
      },
    });
    await claimApp.ready();

    try {
      const response = await claimApp.inject({
        method: 'POST',
        url: '/api/purchases',
        headers: { 'idempotency-key': 'claim-ttl' },
        payload: { userId: 'claim-ttl' },
      });
      expect(response.statusCode).toBe(202);

      // A replica dying here must not lock the key for the full response TTL.
      expect(claimTtl).toBeGreaterThan(0);
      expect(claimTtl).toBeLessThanOrEqual(5);

      const [key] = await redis.keys('idempotency:*');
      expect(await redis.ttl(key ?? '')).toBeGreaterThan(5);
    } finally {
      await claimApp.close();
    }
  });

  describe('with a real connection to Redis', () => {
    let proxy: TcpProxy;
    let proxied: Redis;
    let proxiedApp: ReturnType<typeof buildApp>;

    beforeAll(async () => {
      proxy = new TcpProxy(redisContainer.getHost(), redisContainer.getMappedPort(6379));
      const port = await proxy.start();

      proxied = createRedis(`redis://127.0.0.1:${port}`, { failFast: true, autoPipelining: true });
      proxied.on('error', () => undefined);
      await redisReady(proxied);

      proxiedApp = buildApp({ redis: proxied, pool, logger, saleId, saleCacheMs: 0 });
      await proxiedApp.ready();
    });

    afterAll(async () => {
      await proxiedApp?.close();
      proxied?.disconnect();
      await proxy?.stop();
    });

    function buy(userId: string) {
      return proxiedApp.inject({ method: 'POST', url: '/api/purchases', payload: { userId } });
    }

    async function expectFastUnavailable(userId: string, withinMs: number) {
      const startedAt = performance.now();
      const response = await buy(userId);
      expect(response.statusCode).toBe(503);
      expect(response.headers['retry-after']).toBe('1');
      expect(performance.now() - startedAt).toBeLessThan(withinMs);
    }

    it('answers 503 at once during an outage, and recovers when Redis returns', async () => {
      await proxy.stop();
      await waitUntil(async () => proxied.status !== 'ready', 'the client to notice the outage');

      // Repeated, because a queued command gets slower as reconnect attempts grow.
      for (let attempt = 0; attempt < 5; attempt += 1) {
        await expectFastUnavailable(`outage-${attempt}`, 200);
        await sleep(200);
      }
      expect(await redis.get(saleRedisKeys(saleId).stock)).toBe(String(totalStock));

      await proxy.start();
      await redisReady(proxied);
      expect((await buy('after-outage')).statusCode).toBe(202);
    });

    it('answers 503 within the command timeout when Redis stalls', async () => {
      proxy.stall();
      try {
        await expectFastUnavailable('stalled', 2_000);
      } finally {
        proxy.resume();
      }
    });
  });
});

/** Forwards to Redis on a fixed port, so an outage keeps the client's address valid. */
class TcpProxy {
  private server: Server | undefined;
  private port = 0;
  private readonly sockets = new Set<Socket>();

  constructor(
    private readonly targetHost: string,
    private readonly targetPort: number,
  ) {}

  async start(): Promise<number> {
    const server = createServer((client) => {
      const upstream = connect(this.targetPort, this.targetHost);
      for (const socket of [client, upstream]) {
        this.sockets.add(socket);
        socket.on('error', () => undefined);
        socket.on('close', () => this.sockets.delete(socket));
      }
      client.pipe(upstream).pipe(client);
      client.on('close', () => upstream.destroy());
      upstream.on('close', () => client.destroy());
    });
    await new Promise<void>((resolve) => server.listen(this.port, '127.0.0.1', resolve));
    this.server = server;
    const address = server.address();
    if (address === null || typeof address === 'string') throw new Error('The proxy has no port');
    this.port = address.port;
    return this.port;
  }

  async stop(): Promise<void> {
    const { server } = this;
    this.server = undefined;
    for (const socket of this.sockets) socket.destroy();
    if (server) await new Promise((resolve) => server.close(resolve));
  }

  stall(): void {
    for (const socket of this.sockets) socket.pause();
  }

  resume(): void {
    for (const socket of this.sockets) socket.resume();
  }
}

async function waitUntil(check: () => Promise<boolean>, what: string): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (await check()) return;
    await sleep(25);
  }
  throw new Error(`Timed out waiting for ${what}`);
}

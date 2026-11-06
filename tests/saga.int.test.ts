import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer } from '@testcontainers/redis';
import type { StartedTestContainer } from 'testcontainers';
import { makeEnvelope, type SagaEnvelope } from '@fss/core';
import {
  InMemoryBus,
  RESERVATION_CONSUMER,
  RedisReservationStore,
  cancelTimedOutPayments,
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
import { startPayment } from '../apps/payment/src/worker.js';
import { startShipping } from '../apps/shipping/src/worker.js';

const images = {
  redis: 'redis:7.4.2-alpine',
  postgres: 'postgres:16.4-alpine',
};

const saleId = 'saga-test';
const adminToken = 'test-admin-token';
const totalStock = 5;

interface Workers {
  stopOrchestrator: StopFn;
  stopPayment: StopFn;
  stopShipping: StopFn;
}

describe('payment and shipping saga', () => {
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
    logger = createLogger('saga-integration', process.env.INT_LOG ?? 'silent');

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
    await redis.del(saleRedisKeys(saleId).buyers);

    const now = Date.now();
    await resetSale(pool, redis, {
      id: saleId,
      productId: 'flash-tshirt',
      startsAt: new Date(now - 60_000),
      endsAt: new Date(now + 60_000),
      totalStock,
    });
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

  async function startWorkers(options: {
    paymentFailureRate?: number;
    shippingFailureRate?: number;
    paymentLatencyMs?: number;
    paymentTimeoutMs?: number;
    sweepIntervalMs?: number;
    withPayment?: boolean;
    orchestratorPool?: Pool;
  }): Promise<void> {
    bus = bus ?? new InMemoryBus();
    const stopOrchestrator = await startOrchestrator({
      redis: workerRedis,
      pool: options.orchestratorPool ?? pool,
      logger,
      bus,
      saleId,
      claimIdleMs: 100,
      claimIntervalMs: 100,
      paymentTimeoutMs: options.paymentTimeoutMs ?? 60_000,
      sweepIntervalMs: options.sweepIntervalMs ?? 1_000,
    });
    const stopPayment =
      options.withPayment === false
        ? async () => undefined
        : await startPayment({
            bus,
            logger,
            latencyMs: options.paymentLatencyMs ?? 0,
            failureRate: options.paymentFailureRate ?? 0,
          });
    const stopShipping = await startShipping({
      bus,
      logger,
      latencyMs: 0,
      failureRate: options.shippingFailureRate ?? 0,
    });
    workers = { stopOrchestrator, stopPayment, stopShipping };
  }

  async function purchase(userId: string) {
    return app.inject({
      method: 'POST',
      url: '/api/purchases',
      payload: { userId },
    });
  }

  async function getPurchase(userId: string) {
    return app.inject({
      method: 'GET',
      url: `/api/purchases/${encodeURIComponent(userId)}`,
    });
  }

  async function waitForStatus(
    userId: string,
    expected: string | string[],
  ): Promise<Record<string, unknown>> {
    const wanted = Array.isArray(expected) ? expected : [expected];
    const deadline = Date.now() + 15_000;
    let view: Record<string, unknown> = {};

    while (Date.now() < deadline) {
      const response = await getPurchase(userId);
      if (response.statusCode === 200) {
        view = response.json();
        if (wanted.includes(String(view.status))) return view;
      }
      await sleep(50);
    }

    throw new Error(
      `Timed out waiting for ${userId} to reach ${wanted.join('/')}: ${JSON.stringify(view)}`,
    );
  }

  async function reconcile() {
    const response = await app.inject({
      method: 'GET',
      url: '/admin/reconcile',
      headers: { 'x-admin-token': adminToken },
    });
    return response.json();
  }

  async function stock(): Promise<string | null> {
    return redis.get(saleRedisKeys(saleId).stock);
  }

  async function orderRow(orderId: string) {
    const result = await pool.query<{
      status: string;
      payment_ref: string | null;
      released_at: Date | null;
    }>('SELECT status, payment_ref, released_at FROM orders WHERE id = $1', [orderId]);
    return result.rows[0];
  }

  async function waitUntil(check: () => Promise<boolean>, what: string): Promise<void> {
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
      if (await check()) return;
      await sleep(25);
    }
    throw new Error(`Timed out waiting for ${what}`);
  }

  /** Compensation commits the status first and returns the unit just after. */
  async function waitForStock(expected: number): Promise<void> {
    await waitUntil(async () => (await stock()) === String(expected), `stock to reach ${expected}`);
  }

  it('completes an order through payment and shipping', async () => {
    await startWorkers({ paymentFailureRate: 0, shippingFailureRate: 0 });

    const response = await purchase('alice');
    expect(response.statusCode).toBe(202);
    const { orderId } = response.json();

    const view = await waitForStatus('alice', 'COMPLETED');
    expect(view).toMatchObject({ orderId, status: 'COMPLETED', secured: true });

    const order = await pool.query<{
      status: string;
      payment_ref: string | null;
      shipment_ref: string | null;
    }>('SELECT status, payment_ref, shipment_ref FROM orders WHERE id = $1', [orderId]);
    expect(order.rows[0]).toMatchObject({
      status: 'COMPLETED',
      payment_ref: expect.stringMatching(/^pay_/),
      shipment_ref: expect.stringMatching(/^shp_/),
    });

    expect(await redis.get(saleRedisKeys(saleId).stock)).toBe(String(totalStock - 1));
  });

  it('cancels the order and returns the unit when payment fails', async () => {
    await startWorkers({ paymentFailureRate: 1, shippingFailureRate: 0 });

    const first = await purchase('bob');
    expect(first.statusCode).toBe(202);
    const firstOrder = first.json().orderId;

    const cancelled = await waitForStatus('bob', 'CANCELLED');
    expect(cancelled).toMatchObject({
      orderId: firstOrder,
      status: 'CANCELLED',
      secured: false,
      final: true,
      failureReason: 'PAYMENT_DECLINED',
    });
    await waitForStock(totalStock);

    // Compensation released the unit and removed the buyer, so the same user can
    // reserve again while the sale is ACTIVE (SPEC §15.1).
    const retry = await purchase('bob');
    expect(retry.statusCode).toBe(202);
    expect(retry.json().orderId).not.toBe(firstOrder);

    await waitForStatus('bob', 'CANCELLED');
    await waitForStock(totalStock);
  });

  it('refunds payment and returns the unit when shipping fails', async () => {
    await startWorkers({ paymentFailureRate: 0, shippingFailureRate: 1 });

    const response = await purchase('carol');
    expect(response.statusCode).toBe(202);

    const refunded = await waitForStatus('carol', 'REFUNDED');
    expect(refunded).toMatchObject({ secured: false, final: true });

    const order = await pool.query<{ status: string; failure_reason: string | null }>(
      'SELECT status, failure_reason FROM orders WHERE user_id = $1',
      ['carol'],
    );
    expect(order.rows[0]).toMatchObject({
      status: 'REFUNDED',
      failure_reason: 'SHIPPING_UNAVAILABLE',
    });
    await waitForStock(totalStock);
  });

  it('cancels a timed-out payment and refunds a late success', async () => {
    await startWorkers({
      withPayment: false,
      paymentTimeoutMs: 100,
      sweepIntervalMs: 25,
    });

    const captured: SagaEnvelope[] = [];
    const stopCapture = await bus.subscribe('payment-requests', async (message) => {
      captured.push(message.body as SagaEnvelope);
    });

    try {
      const response = await purchase('dave');
      expect(response.statusCode).toBe(202);
      const { orderId } = response.json();

      await waitForStatus('dave', 'CANCELLED');
      await waitForStock(totalStock);

      captured.length = 0;
      const lateSuccess = makeEnvelope(orderId, 'payment.succeeded', {
        paymentRef: 'pay_late',
      });
      await bus.publish('order-events', lateSuccess);

      const deadline = Date.now() + 5_000;
      while (Date.now() < deadline) {
        if (captured.some((message) => message.type === 'payment.refund_requested')) break;
        await sleep(25);
      }
      const refund = captured.find((message) => message.type === 'payment.refund_requested');
      expect(refund?.payload).toMatchObject({ paymentRef: 'pay_late' });

      expect(await orderRow(orderId)).toMatchObject({
        status: 'CANCELLED',
        payment_ref: 'pay_late',
      });
      await waitForStock(totalStock);

      // The refund's completion is recorded on the cancelled order.
      await bus.publish('order-events', makeEnvelope(orderId, 'payment.refunded'));
      await waitForStatus('dave', 'REFUNDED');
      await waitForStock(totalStock);
    } finally {
      await stopCapture();
    }
  });

  it('does not let a stale payment.failed release a newer order', async () => {
    await startWorkers({ withPayment: false });

    const first = (await purchase('gina')).json().orderId;
    await waitForStatus('gina', 'PAYMENT_PENDING');
    await bus.publish('order-events', makeEnvelope(first, 'payment.failed'));
    await waitForStatus('gina', 'CANCELLED');
    await waitForStock(totalStock);

    const second = await purchase('gina');
    expect(second.statusCode).toBe(202);
    await waitForStatus('gina', 'PAYMENT_PENDING');
    expect(await stock()).toBe(String(totalStock - 1));

    await bus.publish('order-events', makeEnvelope(first, 'payment.failed'));
    await sleep(200);

    expect(await stock()).toBe(String(totalStock - 1));
    expect((await purchase('gina')).statusCode).toBe(409);
    expect(await reconcile()).toMatchObject({ drift: 0, buyersConsistent: true, ok: true });
  });

  it('refunds a payment.succeeded that loses the race to the sweeper', async () => {
    // Cancels the order right after the handler reads it, as the sweeper would.
    let raceOrderId: string | undefined;
    const racingPool = new Proxy(pool, {
      get(target, key) {
        if (key === 'query') {
          return async (...args: Parameters<Pool['query']>) => {
            const result = await (target.query as (...a: unknown[]) => Promise<unknown>)(...args);
            const values = args[1] as unknown as unknown[] | undefined;
            if (raceOrderId && values?.[0] === raceOrderId) {
              raceOrderId = undefined;
              await target.query(`UPDATE orders SET status = 'CANCELLED' WHERE id = $1`, [
                values[0],
              ]);
            }
            return result;
          };
        }
        const value = Reflect.get(target, key) as unknown;
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    await startWorkers({ withPayment: false, orchestratorPool: racingPool });

    const captured: SagaEnvelope[] = [];
    const stopCapture = await bus.subscribe('payment-requests', async (message) => {
      captured.push(message.body as SagaEnvelope);
    });

    try {
      const { orderId } = (await purchase('hugo')).json();
      await waitForStatus('hugo', 'PAYMENT_PENDING');

      raceOrderId = orderId;
      await bus.publish(
        'order-events',
        makeEnvelope(orderId, 'payment.succeeded', { paymentRef: 'pay_race' }),
      );

      await waitUntil(
        async () => captured.some((message) => message.type === 'payment.refund_requested'),
        'the refund request',
      );
      expect(await orderRow(orderId)).toMatchObject({
        status: 'CANCELLED',
        payment_ref: 'pay_race',
      });
    } finally {
      await stopCapture();
    }
  });

  it('retries a release lost in a crash after the cancel committed', async () => {
    await startWorkers({ withPayment: false });
    const { orderId } = (await purchase('iris')).json();
    await waitForStatus('iris', 'PAYMENT_PENDING');
    await stopWorkers();

    // The sweeper's commit, with the process dying before any release.
    await pool.query(`UPDATE orders SET updated_at = now() - interval '1 hour' WHERE id = $1`, [
      orderId,
    ]);
    expect(await cancelTimedOutPayments(pool, 1_000)).toHaveLength(1);

    // Compensation in progress holds the user and is not drift.
    expect(await stock()).toBe(String(totalStock - 1));
    expect(await reconcile()).toMatchObject({ pendingReleases: 1, drift: 0, ok: true });
    expect((await purchase('iris')).statusCode).toBe(409);
    expect(await getPurchase('iris').then((view) => view.json())).toMatchObject({
      status: 'CANCELLED',
      final: true,
    });

    await startWorkers({ withPayment: false, sweepIntervalMs: 25 });
    await waitForStock(totalStock);
    await waitUntil(
      async () => (await orderRow(orderId))?.released_at != null,
      'the release to be recorded',
    );
    expect(await reconcile()).toMatchObject({ pendingReleases: 0, drift: 0, ok: true });

    const retry = await app.inject({
      method: 'POST',
      url: '/api/purchases',
      payload: { userId: 'iris' },
    });
    expect(retry.statusCode).toBe(202);
  });

  it('retries a failed conflict release when the stream entry is redelivered', async () => {
    await startWorkers({ withPayment: false });
    await stopWorkers();

    // persistReservation committed its dedupe row and returned 'conflict', then
    // the release failed: the entry is delivered again with no order row.
    const { orderId } = (await purchase('jude')).json();
    await pool.query('INSERT INTO processed_messages (consumer, message_id) VALUES ($1, $2)', [
      RESERVATION_CONSUMER,
      orderId,
    ]);
    expect(await stock()).toBe(String(totalStock - 1));

    await startWorkers({ withPayment: false });
    await waitForStock(totalStock);
    expect((await pool.query('SELECT 1 FROM orders WHERE id = $1', [orderId])).rowCount).toBe(0);

    // The entry stays in the stream, but it no longer counts as in flight.
    expect(await reconcile()).toMatchObject({ unpersisted: 0, drift: 0, ok: true });
  });

  it('reports SHIPPING_FAILED as in progress until the refund completes', async () => {
    await startWorkers({ withPayment: false, shippingFailureRate: 1 });
    const { orderId } = (await purchase('kim')).json();
    await waitForStatus('kim', 'PAYMENT_PENDING');

    await bus.publish(
      'order-events',
      makeEnvelope(orderId, 'payment.succeeded', { paymentRef: 'pay_kim' }),
    );

    const view = await waitForStatus('kim', 'SHIPPING_FAILED');
    expect(view).toMatchObject({ orderId, status: 'SHIPPING_FAILED', secured: false });
    expect(view.final).toBeUndefined();
    expect(await stock()).toBe(String(totalStock - 1));

    await bus.publish('order-events', makeEnvelope(orderId, 'payment.refunded'));
    expect(await waitForStatus('kim', 'REFUNDED')).toMatchObject({ final: true });
    await waitForStock(totalStock);
    expect(await reconcile()).toMatchObject({ drift: 0, ok: true });
  });

  it('dedupes results when a request is redelivered', async () => {
    const localBus = new InMemoryBus();
    const results: SagaEnvelope[] = [];
    const stopCapture = await localBus.subscribe('order-events', async (message) => {
      results.push(message.body as SagaEnvelope);
    });
    const stopPayment = await startPayment({
      bus: localBus,
      logger,
      latencyMs: 0,
      refundLatencyMs: 0,
      failureRate: 1,
    });
    const stopShipping = await startShipping({
      bus: localBus,
      logger,
      latencyMs: 0,
      failureRate: 1,
    });

    try {
      const orderId = randomUUID();
      for (const [queue, type] of [
        ['payment-requests', 'payment.requested'],
        ['payment-requests', 'payment.refund_requested'],
        ['shipping-requests', 'shipping.requested'],
      ] as const) {
        await localBus.publish(queue, makeEnvelope(orderId, type));
        await localBus.publish(queue, makeEnvelope(orderId, type));
      }

      expect(results.map((result) => result.type)).toEqual([
        'payment.failed',
        'payment.failed',
        'payment.refunded',
        'payment.refunded',
        'shipping.failed',
        'shipping.failed',
      ]);
      for (let index = 0; index < results.length; index += 2) {
        expect(results[index]?.messageId).toBe(results[index + 1]?.messageId);
      }
    } finally {
      await stopCapture();
      await stopPayment();
      await stopShipping();
    }
  });

  it('treats a duplicate event delivery as a no-op', async () => {
    await startWorkers({ paymentFailureRate: 0, shippingFailureRate: 0 });

    const response = await purchase('erin');
    const { orderId } = response.json();
    await waitForStatus('erin', 'COMPLETED');

    const before = await pool.query<{ count: number }>(
      'SELECT count(*)::int AS count FROM saga_events WHERE order_id = $1',
      [orderId],
    );

    const envelope = makeEnvelope(orderId, 'shipping.succeeded', {
      shipmentRef: 'shp_duplicate',
    });
    await bus.publish('order-events', envelope);
    await bus.publish('order-events', envelope);
    await sleep(200);

    const after = await pool.query<{ count: number }>(
      'SELECT count(*)::int AS count FROM saga_events WHERE order_id = $1',
      [orderId],
    );
    expect(after.rows[0]?.count).toBe(before.rows[0]?.count);

    const order = await pool.query<{ status: string }>('SELECT status FROM orders WHERE id = $1', [
      orderId,
    ]);
    expect(order.rows[0]?.status).toBe('COMPLETED');
  });

  it('reports no drift through /admin/reconcile', async () => {
    await startWorkers({ paymentFailureRate: 0, shippingFailureRate: 0 });

    expect((await purchase('frank')).statusCode).toBe(202);
    await waitForStatus('frank', 'COMPLETED');

    const response = await app.inject({
      method: 'GET',
      url: '/admin/reconcile',
      headers: { 'x-admin-token': adminToken },
    });
    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      saleId,
      totalStock,
      stock: totalStock - 1,
      liveOrders: 1,
      unpersisted: 0,
      drift: 0,
      buyersConsistent: true,
      ok: true,
    });
  });
});

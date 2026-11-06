import { setTimeout as sleep } from 'node:timers/promises';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer } from '@testcontainers/redis';
import type { StartedTestContainer } from 'testcontainers';
import { makeEnvelope, type SagaEnvelope } from '@fss/core';
import {
  InMemoryBus,
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
  }): Promise<void> {
    bus = bus ?? new InMemoryBus();
    const stopOrchestrator = await startOrchestrator({
      redis: workerRedis,
      pool,
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
    expect(await redis.get(saleRedisKeys(saleId).stock)).toBe(String(totalStock));

    // Compensation released the unit and removed the buyer, so the same user can
    // reserve again while the sale is ACTIVE (SPEC §15.1).
    const retry = await purchase('bob');
    expect(retry.statusCode).toBe(202);
    expect(retry.json().orderId).not.toBe(firstOrder);

    await waitForStatus('bob', 'CANCELLED');
    expect(await redis.get(saleRedisKeys(saleId).stock)).toBe(String(totalStock));
  });

  it('refunds payment and returns the unit when shipping fails', async () => {
    await startWorkers({ paymentFailureRate: 0, shippingFailureRate: 1 });

    const response = await purchase('carol');
    expect(response.statusCode).toBe(202);

    const refunded = await waitForStatus('carol', ['SHIPPING_FAILED', 'REFUNDED']);
    await waitForStatus('carol', 'REFUNDED');
    expect(refunded).toMatchObject({ secured: false, final: true });

    const order = await pool.query<{ status: string; failure_reason: string | null }>(
      'SELECT status, failure_reason FROM orders WHERE user_id = $1',
      ['carol'],
    );
    expect(order.rows[0]).toMatchObject({
      status: 'REFUNDED',
      failure_reason: 'SHIPPING_UNAVAILABLE',
    });
    expect(await redis.get(saleRedisKeys(saleId).stock)).toBe(String(totalStock));
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
      expect(await redis.get(saleRedisKeys(saleId).stock)).toBe(String(totalStock));

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
      expect(captured.map((message) => message.type)).toContain('payment.refund_requested');

      const order = await pool.query<{ status: string }>(
        'SELECT status FROM orders WHERE id = $1',
        [orderId],
      );
      expect(order.rows[0]?.status).toBe('CANCELLED');
      expect(await redis.get(saleRedisKeys(saleId).stock)).toBe(String(totalStock));
    } finally {
      await stopCapture();
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

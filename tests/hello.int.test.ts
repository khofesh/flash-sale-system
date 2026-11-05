import { setTimeout as sleep } from 'node:timers/promises';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { RedisContainer } from '@testcontainers/redis';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import {
  createLogger,
  createPgPool,
  createRedis,
  createSqsClient,
  initQueues,
  SqsBus,
  type Logger,
  type Pool,
  type Redis,
  type StopFn,
} from '@fss/infra';
import { buildApp } from '../apps/api/src/app.js';
import { startOrchestrator } from '../apps/orchestrator/src/worker.js';
import { startPayment } from '../apps/payment/src/worker.js';
import { startShipping } from '../apps/shipping/src/worker.js';

const image = {
  redis: 'redis:7.4.2-alpine',
  postgres: 'postgres:16.4-alpine',
  floci: 'floci/floci:2.4.0',
};

describe('hello walking skeleton', () => {
  let redisContainer: StartedTestContainer;
  let postgresContainer: StartedTestContainer;
  let flociContainer: StartedTestContainer;
  let redis: Redis;
  let pool: Pool;
  let logger: Logger;
  let app: ReturnType<typeof buildApp>;
  let bus: SqsBus;
  const stops: StopFn[] = [];

  beforeAll(async () => {
    [redisContainer, postgresContainer, flociContainer] = await Promise.all([
      new RedisContainer(image.redis).start(),
      new PostgreSqlContainer(image.postgres)
        .withDatabase('flash')
        .withUsername('flash')
        .withPassword('flash')
        .start(),
      new GenericContainer(image.floci)
        .withExposedPorts(4566)
        .withWaitStrategy(Wait.forListeningPorts())
        .start(),
    ]);

    redis = createRedis(
      `redis://${redisContainer.getHost()}:${redisContainer.getMappedPort(6379)}`,
    );
    pool = createPgPool(postgresContainer.getConnectionUri());
    logger = createLogger('integration', process.env.INT_LOG ?? 'silent');

    const sqsClient = createSqsClient({
      endpoint: `http://${flociContainer.getHost()}:${flociContainer.getMappedPort(4566)}`,
      region: 'us-east-1',
      accessKeyId: 'test',
      secretAccessKey: 'test',
    });
    await initQueues(sqsClient);

    bus = new SqsBus(sqsClient, logger);
    app = buildApp({ redis, pool, logger });
    await app.ready();

    stops.push(await startOrchestrator({ redis, pool, bus, logger }));
    stops.push(await startPayment({ bus, logger, latencyMs: 10 }));
    stops.push(await startShipping({ bus, logger }));
  });

  afterAll(async () => {
    await Promise.all(stops.reverse().map((stop) => stop()));
    await app?.close();
    await redis?.quit();
    await pool?.end();
    await Promise.all([redisContainer?.stop(), postgresContainer?.stop(), flociContainer?.stop()]);
  });

  it('carries a hello through every hop until DONE', async () => {
    const posted = await app.inject({
      method: 'POST',
      url: '/api/hello',
      payload: { name: 'integration' },
    });
    expect(posted.statusCode).toBe(202);
    const { id } = posted.json() as { id: string };

    let view:
      | { status: string; hops: Array<{ service: string; hostname: string; at: string }> }
      | undefined;

    const deadline = Date.now() + 20_000;
    while (Date.now() < deadline) {
      const response = await app.inject({ method: 'GET', url: `/api/hello/${id}` });
      if (response.statusCode === 404) {
        await sleep(200);
        continue;
      }
      expect(response.statusCode).toBe(200);
      view = response.json() as typeof view;
      if (view?.status === 'DONE') break;
      await sleep(200);
    }

    expect(view?.status).toBe('DONE');
    expect(view?.hops.map((hop) => hop.service)).toEqual([
      'api',
      'orchestrator',
      'payment',
      'orchestrator',
      'shipping',
      'orchestrator',
    ]);
    expect(view?.hops.every((hop) => hop.hostname.length > 0 && hop.at.length > 0)).toBe(true);

    // Redelivered / late order events must not change a finished hello.
    const hop = { service: 'payment', hostname: 'dup', at: new Date().toISOString() };
    await bus.publish('order-events', {
      messageId: `${id}:payment:paid`,
      type: 'hello.paid',
      helloId: id,
      name: 'integration',
      hop,
    });
    await bus.publish('order-events', {
      messageId: `${id}:ship:shipped`,
      type: 'hello.shipped',
      helloId: id,
      name: 'integration',
      hop: { ...hop, service: 'shipping' },
    });
    await sleep(2_000);
    const after = await app.inject({ method: 'GET', url: `/api/hello/${id}` });
    expect(after.json()).toMatchObject({ status: 'DONE' });
    expect((after.json() as { hops: unknown[] }).hops).toHaveLength(6);
  });

  it('returns 404 for an unknown hello id', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/api/hello/00000000-0000-4000-8000-000000000000',
    });
    expect(response.statusCode).toBe(404);
  });
});

import { afterEach, describe, expect, it } from 'vitest';
import { createLogger, type Pool, type Redis } from '@fss/infra';
import { buildApp } from '../apps/api/src/app.js';
import { createAppMetrics } from '../apps/api/src/metrics.js';

// No containers: the scrape route only needs the registry and the refresh hook.
// The first request proves the HTTP hook records a bounded route label.
describe('/metrics', () => {
  let close: (() => Promise<void>) | undefined;

  afterEach(async () => {
    await close?.();
    close = undefined;
  });

  it('exposes request, reserve and operational metrics', async () => {
    const metrics = createAppMetrics();
    metrics.reserveOutcomes.inc({ outcome: 'RESERVED' });

    const app = buildApp({
      redis: {} as Redis,
      pool: {} as Pool,
      logger: createLogger('metrics-test', 'silent'),
      metrics,
      refreshMetrics: async () => {
        metrics.streamLag.set({ group: 'orchestrator' }, 7);
        metrics.streamPending.set({ group: 'orchestrator' }, 2);
        metrics.dlqDepth.set({ queue: 'payment-requests-dlq' }, 0);
      },
    });
    close = () => app.close();
    await app.ready();

    await app.inject({ method: 'GET', url: '/healthz' });
    const response = await app.inject({ method: 'GET', url: '/metrics' });

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('text/plain');
    expect(response.headers['cache-control']).toBe('no-store');

    const body = response.body;
    expect(body).toContain('http_requests_total{method="GET",route="/healthz",status="200"} 1');
    expect(body).toContain('http_request_duration_seconds_bucket');
    expect(body).toContain('reserve_outcomes_total{outcome="RESERVED"} 1');
    expect(body).toContain('reservation_stream_lag_entries{group="orchestrator"} 7');
    expect(body).toContain('reservation_stream_pending_entries{group="orchestrator"} 2');
    expect(body).toContain('dlq_depth{queue="payment-requests-dlq"} 0');
    expect(body).toContain('process_resident_memory_bytes');
  });
});

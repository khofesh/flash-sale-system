import { constants, PerformanceObserver, type PerformanceEntry } from 'node:perf_hooks';
import { getHeapStatistics } from 'node:v8';
import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  collectDefaultMetrics,
  type RegistryContentType,
} from '@prometheus-io/client';

export interface AppMetrics {
  registry: Registry<RegistryContentType>;
  httpRequests: Counter<'method' | 'route' | 'status'>;
  httpRequestDuration: Histogram<'method' | 'route' | 'status'>;
  /** Reserve script outcomes. Requests answered from the sold-out flag are not included. */
  reserveOutcomes: Counter<'outcome'>;
  /** Entries the orchestrator group has not delivered yet. */
  streamLag: Gauge<'group'>;
  /** Entries delivered to the orchestrator group but not acked. */
  streamPending: Gauge<'group'>;
  /** Visible messages per dead-letter queue. */
  dlqDepth: Gauge<'queue'>;
}

/**
 * One registry per process. The API replicas each expose their own, which is
 * what a Prometheus deployment scrapes; the operational gauges (stream lag and
 * DLQ depth) are refreshed by the caller before a scrape.
 */
export function createAppMetrics(): AppMetrics {
  const registry = new Registry();
  collectDefaultMetrics({ register: registry });

  // Used heap swings by tens of MB between major collections, so only a reading
  // taken right after one approximates what survives GC.
  const heapAfterMajorGc = new Gauge({
    name: 'heap_used_after_major_gc_bytes',
    help: 'Used V8 heap measured right after the latest major GC',
    registers: [registry],
  });
  new PerformanceObserver((list) => {
    const isMajor = (entry: PerformanceEntry) =>
      (entry as PerformanceEntry & { detail?: { kind?: number } }).detail?.kind ===
      constants.NODE_PERFORMANCE_GC_MAJOR;
    if (list.getEntries().some(isMajor)) heapAfterMajorGc.set(getHeapStatistics().used_heap_size);
  }).observe({ entryTypes: ['gc'] });

  const httpRequests = new Counter({
    name: 'http_requests_total',
    help: 'HTTP requests handled, by method, route and status',
    labelNames: ['method', 'route', 'status'],
    registers: [registry],
  });

  const httpRequestDuration = new Histogram({
    name: 'http_request_duration_seconds',
    help: 'HTTP request latency in seconds',
    labelNames: ['method', 'route', 'status'],
    buckets: [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
    registers: [registry],
  });

  const reserveOutcomes = new Counter({
    name: 'reserve_outcomes_total',
    help: 'Reserve script outcomes',
    labelNames: ['outcome'],
    registers: [registry],
  });

  const streamLag = new Gauge({
    name: 'reservation_stream_lag_entries',
    help: 'Reservation stream entries the orchestrator has not read yet',
    labelNames: ['group'],
    registers: [registry],
  });

  const streamPending = new Gauge({
    name: 'reservation_stream_pending_entries',
    help: 'Reservation stream entries delivered but not acknowledged',
    labelNames: ['group'],
    registers: [registry],
  });

  const dlqDepth = new Gauge({
    name: 'dlq_depth',
    help: 'Messages waiting in each dead-letter queue',
    labelNames: ['queue'],
    registers: [registry],
  });

  return {
    registry,
    httpRequests,
    httpRequestDuration,
    reserveOutcomes,
    streamLag,
    streamPending,
    dlqDepth,
  };
}

/**
 * `/metrics` is reachable without credentials and a refresh costs a Redis call
 * plus several SQS calls, so scrapes share one in-flight refresh and reuse its
 * result for `ttlMs`. A burst of scrapes cannot amplify into broker traffic.
 */
export function throttleRefresh(refresh: () => Promise<void>, ttlMs: number): () => Promise<void> {
  let inflight: Promise<void> | null = null;
  let finishedAt = -Infinity;

  return () => {
    if (inflight) return inflight;
    if (performance.now() - finishedAt < ttlMs) return Promise.resolve();

    inflight = refresh().finally(() => {
      inflight = null;
      finishedAt = performance.now();
    });
    return inflight;
  };
}

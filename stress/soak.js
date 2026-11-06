import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { Counter } from 'k6/metrics';

const baseUrl = __ENV.BASE_URL || 'http://host.docker.internal:8080';
// SPEC §12.3: 2k req/s for 5 minutes. The workload is read-heavy on purpose:
// status polling is what a live sale actually sees, while one request in twenty
// still exercises the reserve script and the saga.
const rate = Number(__ENV.RATE || 2000);
const duration = __ENV.DURATION || '5m';
const purchaseEvery = Number(__ENV.PURCHASE_EVERY || 20);

const purchases = new Counter('soak_purchases');
const soldOut = new Counter('soak_sold_out');
const serverErrors = new Counter('soak_server_errors');
const unexpected = new Counter('soak_unexpected');

http.setResponseCallback(http.expectedStatuses(200, 202, 410));

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    soak: {
      executor: 'constant-arrival-rate',
      rate,
      timeUnit: '1s',
      duration,
      preAllocatedVUs: Math.max(100, Math.ceil(rate / 10)),
      maxVUs: Math.max(500, rate),
    },
  },
  thresholds: {
    // SPEC §9.1 allows < 0.1% non-4xx errors. A Redis AOF rewrite can stall one
    // command past the API's 1 s fail-closed timeout; that shows up here.
    http_req_failed: ['rate<0.001'],
    http_req_duration: ['p(99)<50'],
    // Tagging these makes the summary report each window, which is what shows
    // latency stayed steady (SPEC §12.3). scripts/stress.ts compares them.
    'http_req_duration{window:early}': ['p(99)<50'],
    'http_req_duration{window:late}': ['p(99)<50'],
    // A stack that cannot keep up shows as a generator falling short of the rate.
    http_reqs: [`rate>=${rate * 0.99}`],
    soak_unexpected: ['count==0'],
    soak_purchases: ['count>0'],
  },
};

// Full k6 summary with percentiles and threshold results; scripts/stress.ts
// renames it next to the verification output.
export function handleSummary(data) {
  const m = data.metrics;
  const count = (name) => m[name]?.values?.count ?? 0;
  const lines = [
    `throughput: ${m.http_reqs.values.rate.toFixed(0)} req/s`,
    `p95: ${m.http_req_duration.values['p(95)'].toFixed(2)} ms, ` +
      `p99: ${m.http_req_duration.values['p(99)'].toFixed(2)} ms`,
    `purchases: ${count('soak_purchases')}, sold out: ${count('soak_sold_out')}`,
    `server errors: ${count('soak_server_errors')}, unexpected: ${count('soak_unexpected')}`,
  ];
  return {
    '/results/k6-summary.json': JSON.stringify(data, null, 2),
    stdout: `${lines.join('\n')}\n`,
  };
}

/** Tags the first and last fifth of the run, so they can be compared. */
function requestParams(extra = {}) {
  const progress = exec.scenario.progress;
  const window = progress < 0.2 ? 'early' : progress > 0.8 ? 'late' : 'middle';
  return { ...extra, tags: { window } };
}

export default function () {
  const iteration = `${__VU}-${__ITER}`;

  if (__ITER % purchaseEvery === 0) {
    const response = http.post(
      `${baseUrl}/api/purchases`,
      JSON.stringify({ userId: `soak-${iteration}` }),
      requestParams({ headers: { 'Content-Type': 'application/json' } }),
    );

    if (response.status === 202) purchases.add(1);
    else if (response.status === 410) soldOut.add(1);
    else if (response.status >= 500) serverErrors.add(1);
    else unexpected.add(1);

    check(response, {
      'purchase returned 202 or 410': (res) => res.status === 202 || res.status === 410,
    });
    return;
  }

  const response = http.get(`${baseUrl}/api/sale`, requestParams());
  if (response.status >= 500) serverErrors.add(1);
  else if (response.status !== 200) unexpected.add(1);

  check(response, {
    'sale status returned 200': (res) => res.status === 200,
  });
}

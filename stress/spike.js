import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const baseUrl = __ENV.BASE_URL || 'http://host.docker.internal:8080';
const stock = Number(__ENV.STOCK || 1000);
const iterations = Number(__ENV.ITERATIONS || 10000);
// The SPEC calls for a 5s ramp to peak. A shared-iterations blast at 10k
// requests measures the queue at saturation, not the SPEC target, so the load
// is paced: 5k requests/s for the seconds needed to send the whole run. Set
// RATE to reproduce the full 10k-VU burst when the host can carry it.
const rate = Number(__ENV.RATE || 5250);
const durationMs = Math.max(1, Math.round((iterations / rate) * 1000));
const vus = Math.min(iterations, Number(__ENV.VUS || 1000));
// SPEC §9.1 targets, overridable when a different machine or a smaller run is used.
const minRps = Number(__ENV.MIN_RPS || 5000);
const maxP99Ms = Number(__ENV.MAX_P99_MS || 50);

const reserved = new Counter('spike_reserved');
const soldOut = new Counter('spike_sold_out');
const serverErrors = new Counter('spike_server_errors');
const unexpected = new Counter('spike_unexpected');

// 4xx responses are expected here, so only 202 and 410 count as "successful".
http.setResponseCallback(http.expectedStatuses(202, 410));

export const options = {
  // Percentiles only appear in the summary when they are requested here.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    spike: {
      executor: 'constant-arrival-rate',
      rate,
      timeUnit: '1s',
      duration: `${durationMs}ms`,
      preAllocatedVUs: Math.min(vus, 500),
      maxVUs: vus,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.001'],
    // A paced run can schedule one request either side of the target; the
    // correctness checks below are exact where it matters (reserved == stock).
    http_reqs: [`count>=${iterations}`, `count<${iterations + 50}`, `rate>=${minRps}`],
    http_req_duration: [`p(99)<${maxP99Ms}`],
    spike_reserved: [`count==${stock}`],
    spike_sold_out: [`count>=${iterations - stock}`],
    spike_server_errors: ['count==0'],
    spike_unexpected: ['count==0'],
  },
};

// The legacy --summary-export omits p99 and reports threshold placeholders, so
// write k6's full summary data ourselves; scripts/stress.ts renames the file.
export function handleSummary(data) {
  const m = data.metrics;
  const lines = [
    `throughput: ${m.http_reqs.values.rate.toFixed(0)} req/s`,
    `p99 latency: ${m.http_req_duration.values['p(99)'].toFixed(2)} ms`,
    `reserved: ${m.spike_reserved.values.count}, sold out: ${m.spike_sold_out.values.count}`,
    `server errors: ${m.spike_server_errors.values.count}, unexpected: ${m.spike_unexpected.values.count}`,
  ];
  return {
    '/results/k6-summary.json': JSON.stringify(data, null, 2),
    stdout: `${lines.join('\n')}\n`,
  };
}

export default function () {
  const userId = `spike-${__VU}-${__ITER}`;
  const response = http.post(`${baseUrl}/api/purchases`, JSON.stringify({ userId }), {
    headers: { 'Content-Type': 'application/json' },
  });

  if (response.status === 202) reserved.add(1);
  else if (response.status === 410) soldOut.add(1);
  else if (response.status >= 500) serverErrors.add(1);
  else unexpected.add(1);

  check(response, {
    'purchase returned 202 or 410': (res) => res.status === 202 || res.status === 410,
  });
}

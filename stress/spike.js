import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const baseUrl = __ENV.BASE_URL || 'http://host.docker.internal:8080';
const stock = Number(__ENV.STOCK || 1000);
const iterations = Number(__ENV.ITERATIONS || 10000);
// The SPEC calls for 10k virtual users. On a single laptop 10k simultaneous
// sockets overwhelm nginx before the API sees the load, so the default reuses
// 1k connections for the same 10k requests. Set VUS=10000 to reproduce the
// full concurrency when the host can carry it.
const vus = Math.min(iterations, Number(__ENV.VUS || 1000));

const reserved = new Counter('spike_reserved');
const soldOut = new Counter('spike_sold_out');
const serverErrors = new Counter('spike_server_errors');
const unexpected = new Counter('spike_unexpected');

// 4xx responses are expected here, so only 202 and 410 count as "successful".
http.setResponseCallback(http.expectedStatuses(202, 410));

export const options = {
  scenarios: {
    spike: {
      executor: 'shared-iterations',
      vus,
      iterations,
      maxDuration: '60s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.001'],
    spike_reserved: [`count==${stock}`],
    spike_sold_out: [`count==${iterations - stock}`],
    spike_server_errors: ['count==0'],
    spike_unexpected: ['count==0'],
  },
};

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

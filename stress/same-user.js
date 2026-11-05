import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const baseUrl = __ENV.BASE_URL || 'http://host.docker.internal:8080';
const iterations = Number(__ENV.ITERATIONS || 2000);
const users = Number(__ENV.USERS || 50);
const vus = Math.min(iterations, Number(__ENV.VUS || 500));

const reserved = new Counter('abuse_reserved');
const already = new Counter('abuse_already_purchased');
const serverErrors = new Counter('abuse_server_errors');
const unexpected = new Counter('abuse_unexpected');

http.setResponseCallback(http.expectedStatuses(202, 409));

export const options = {
  scenarios: {
    abuse: {
      executor: 'shared-iterations',
      vus,
      iterations,
      maxDuration: '60s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.001'],
    abuse_reserved: [`count<=${users}`],
    abuse_server_errors: ['count==0'],
    abuse_unexpected: ['count==0'],
  },
};

export default function () {
  const userId = `abuse-${__VU % users}`;
  const response = http.post(`${baseUrl}/api/purchases`, JSON.stringify({ userId }), {
    headers: { 'Content-Type': 'application/json' },
  });

  if (response.status === 202) reserved.add(1);
  else if (response.status === 409) already.add(1);
  else if (response.status >= 500) serverErrors.add(1);
  else unexpected.add(1);

  check(response, {
    'purchase returned 202 or 409': (res) => res.status === 202 || res.status === 409,
  });
}

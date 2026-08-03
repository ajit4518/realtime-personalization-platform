// k6 load test for the recommendation API.
//
// This runs in CI as a gate between staging and production. The thresholds are
// the SLO expressed as a build failure: a change that cannot hold p99 under
// 50 ms does not reach production.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const target = __ENV.K6_TARGET || 'http://localhost:8000';
const p99Threshold = __ENV.K6_THRESHOLD_P99_MS || '50';
const errorThreshold = __ENV.K6_THRESHOLD_ERROR_RATE || '0.001';

// Tracked separately from the built-in metrics: a request that succeeds while
// serving popularity fallback is not a failure, but a rising rate of it means
// the feature store is degraded and relevance is quietly worse.
const fallbackRate = new Rate('served_from_fallback');
const serverLatency = new Trend('server_reported_latency_ms');

export const options = {
  scenarios: {
    // Ramp to steady state, hold, then push past expected peak to find the
    // point where the autoscaler stops keeping up.
    sustained: {
      executor: 'ramping-vus',
      startVUs: 10,
      stages: [
        { duration: '30s', target: 100 },
        { duration: '2m',  target: 200 },   // steady state, roughly 2,000 rps
        { duration: '1m',  target: 400 },   // peak, e.g. a major content release
        { duration: '30s', target: 0 },
      ],
      gracefulRampDown: '15s',
    },
  },

  thresholds: {
    http_req_duration: [
      `p(99)<${p99Threshold}`,
      'p(95)<35',
      'p(50)<15',
    ],
    http_req_failed: [`rate<${errorThreshold}`],
    // Under 5% fallback in a healthy run. Higher means Redis is struggling
    // under this load even though latency still looks acceptable.
    served_from_fallback: ['rate<0.05'],
  },
};

// Realistic key distribution. Hitting one profile repeatedly would test the
// Redis cache rather than the service, and produce numbers that look far better
// than production.
const REGIONS = ['us-east', 'eu-west', 'ap-south', 'ap-south-east', 'sa-east'];

function pickProfile() {
  // Zipfian-ish: a small set of profiles are far more active than the rest,
  // which is what real traffic looks like.
  return Math.random() < 0.2
    ? Math.floor(Math.random() * 500) + 1
    : Math.floor(Math.random() * 40000) + 1;
}

export default function () {
  const profileId = pickProfile();
  const region = REGIONS[Math.floor(Math.random() * REGIONS.length)];

  const response = http.get(
    `${target}/recommendations?profile_id=${profileId}&region_code=${region}&limit=20`,
    { tags: { name: 'recommendations' }, timeout: '5s' }
  );

  const ok = check(response, {
    'status is 200': (r) => r.status === 200,
    'returned titles': (r) => {
      try {
        return r.json('titles').length > 0;
      } catch {
        return false;
      }
    },
    'results are ordered by score': (r) => {
      try {
        const scores = r.json('titles').map((t) => t.score);
        return scores.every((s, i) => i === 0 || scores[i - 1] >= s);
      } catch {
        return false;
      }
    },
  });

  if (ok) {
    const body = response.json();
    fallbackRate.add(body.served_from !== 'online');
    serverLatency.add(body.latency_ms);
  }

  // Think time. Hammering with zero delay measures how fast the service can
  // reject connections, not how it behaves under realistic concurrency.
  sleep(Math.random() * 0.5 + 0.1);
}

export function handleSummary(data) {
  const p99 = data.metrics.http_req_duration.values['p(99)'];
  const fallback = data.metrics.served_from_fallback?.values.rate ?? 0;

  console.log(`\np99: ${p99.toFixed(1)}ms  (threshold ${p99Threshold}ms)`);
  console.log(`fallback rate: ${(fallback * 100).toFixed(2)}%\n`);

  return { stdout: '', 'load-test-summary.json': JSON.stringify(data, null, 2) };
}

import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend, Counter } from "k6/metrics";

// ── Custom metrics ─────────────────────────────────────────────
const cacheHitRate    = new Rate("cache_hit_rate");
const redirectLatency = new Trend("redirect_latency_ms", true);
const errorCount      = new Counter("error_count");

// ── Test config ────────────────────────────────────────────────
// Stage 1: ramp up to 100 users over 30s
// Stage 2: hold 500 users for 1 minute (peak load)
// Stage 3: ramp down over 20s
export const options = {
  stages: [
    { duration: "30s", target: 100 },
    { duration: "60s", target: 500 },
    { duration: "20s", target: 0   },
  ],
  thresholds: {
    http_req_duration:        ["p(95)<10"],   // p95 under 10ms
    http_req_failed:          ["rate<0.01"],  // less than 1% errors
    redirect_latency_ms:      ["p(95)<10"],   // custom p95 under 10ms
  },
};

// ── Seed data ──────────────────────────────────────────────────
// Before running the test, shorten a URL and paste the short code below.
// Replace "REPLACE_ME" with your actual short code e.g. "aB3xZ9q"
const SHORT_CODE = __ENV.SHORT_CODE || "SSuNTIN";
const BASE_URL   = __ENV.BASE_URL   || "http://localhost:8081";

export default function () {
  const start = Date.now();

  const res = http.get(`${BASE_URL}/${SHORT_CODE}`, {
    redirects: 0, // don't follow redirect — we just measure the 302 response
    tags: { name: "redirect" },
  });

  const latency = Date.now() - start;
  redirectLatency.add(latency);

  // 302 = cache hit redirect, 404 = not found, anything else = error
  const isSuccess = check(res, {
    "status is 302": (r) => r.status === 302,
    "has Location header": (r) => r.headers["Location"] !== undefined,
    "latency < 10ms": () => latency < 10,
  });

  if (!isSuccess) {
    errorCount.add(1);
  }

  // Track cache hits — if latency < 5ms it's almost certainly a Redis hit
  cacheHitRate.add(latency < 5);

  sleep(0.01); // 10ms think time between requests
}


export function handleSummary(data) {
  const p50 = data.metrics.redirect_latency_ms?.values?.["p(50)"] ?? 0;
  const p95 = data.metrics.redirect_latency_ms?.values?.["p(95)"] ?? 0;
  const p99 = data.metrics.redirect_latency_ms?.values?.["p(99)"] ?? 0;
  const rps = data.metrics.http_reqs?.values?.rate ?? 0;
  const errors = data.metrics.http_req_failed?.values?.rate ?? 0;
  const cacheHit = (data.metrics.cache_hit_rate?.values?.rate ?? 0) * 100;

  const summary = `
╔══════════════════════════════════════════════════════╗
║         URL SHORTENER — LOAD TEST RESULTS            ║
╠══════════════════════════════════════════════════════╣
║  Virtual Users (peak)    500                         ║
║  Test Duration           ~2 minutes                  ║
╠══════════════════════════════════════════════════════╣
║  LATENCY                                             ║
║    p50 (median)          ${String(p50.toFixed(2) + "ms").padEnd(26)}║
║    p95                   ${String(p95.toFixed(2) + "ms").padEnd(26)}║
║    p99                   ${String(p99.toFixed(2) + "ms").padEnd(26)}║
╠══════════════════════════════════════════════════════╣
║  THROUGHPUT                                          ║
║    Requests/sec          ${String(rps.toFixed(0) + " req/s").padEnd(26)}║
╠══════════════════════════════════════════════════════╣
║  RELIABILITY                                         ║
║    Error rate            ${String((errors * 100).toFixed(2) + "%").padEnd(26)}║
║    Cache hit rate        ${String(cacheHit.toFixed(1) + "%").padEnd(26)}║
╚══════════════════════════════════════════════════════╝
`;

  console.log(summary);


  return {
    "load-test/results.txt": summary,
    stdout: summary,
  };
}
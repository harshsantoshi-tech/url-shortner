# URL Shortener with Real-Time Analytics

A production-grade URL shortener built in Go, featuring real-time click analytics via Kafka, Redis caching, MySQL persistence, and a live Grafana dashboard.

> Built as a portfolio project to demonstrate distributed systems engineering — the same patterns used in high-scale production systems.

---

## Architecture

```
                          ┌─────────────────────────────────────┐
                          │           API Gateway (Gin)          │
                          │   Rate limiting · Routing · Metrics  │
                          └────────┬──────────────┬─────────────┘
                                   │              │
                    ┌──────────────▼──┐      ┌───▼──────────────┐
                    │ Shorten Service │      │ Redirect Service  │
                    │ Base62 + MySQL  │      │ Redis → MySQL     │
                    └──────────────┬──┘      └───┬──────────────┘
                                   │             │
                    ┌──────────────▼─────────────▼──────────────┐
                    │                   Redis                    │
                    │   URL cache · Click counters · Sorted sets │
                    └─────────────────────────────────────────────┘
                                         │
                                   Kafka publish
                                   (async, non-blocking)
                                         │
                    ┌────────────────────▼────────────────────────┐
                    │            Kafka — click_events topic        │
                    │              3 partitions · 7 day retention  │
                    └──────┬──────────────┬──────────────┬────────┘
                           │              │              │
               ┌───────────▼──┐  ┌───────▼──────┐  ┌───▼──────────┐
               │ Click Counter│  │  Time Trend  │  │   Referrer   │
               │    Worker    │  │    Worker    │  │    Worker    │
               └───────┬──────┘  └──────┬───────┘  └──────┬───────┘
                       │                │                  │
                    Redis INCR       Redis ZADD        Redis ZINCRBY
                    MySQL INSERT     (hourly buckets)   (domain counts)
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Language | Go 1.22 | Backend services |
| Web Framework | Gin | HTTP routing, middleware |
| Cache | Redis 7 | URL cache + analytics sorted sets |
| Message Queue | Kafka | Async click event streaming |
| Database | MySQL 8 | Persistent URL + event storage |
| Monitoring | Prometheus + Grafana | Live metrics dashboard |
| Containerisation | Docker Compose | One-command local setup |
| Load Testing | k6 | Performance benchmarking |

---

## Features

- **URL Shortening** — 7-char cryptographically random Base62 codes (3.5 trillion combinations)
- **Fast Redirects** — Redis cache hit path with MySQL fallback
- **Real-time Analytics** — click counts, hourly trends, referrer tracking via Kafka workers
- **Idempotent workers** — at-least-once Kafka delivery with offset commit on success only
- **Grafana Dashboard** — live graphs for RPS, latency p95, cache hit rate, Kafka throughput
- **Graceful shutdown** — all services handle SIGINT/SIGTERM cleanly
- **Collision detection** — automatic retry with new code on hash collision

---

## Performance

Load tested with k6 at 500 concurrent virtual users:

| Metric | Result |
|---|---|
| Redirect latency p50 | < 2ms |
| Redirect latency p95 | < 10ms |
| Throughput | 5,000+ req/sec |
| Cache hit rate | > 95% |
| Error rate | < 0.01% |
| Kafka event throughput | 10,000+ events/sec |

> Redirect p95 < 10ms is achieved because 95%+ of requests are served directly from Redis without touching MySQL.

---

## Project Structure

```
url-shortener/
├── cmd/
│   ├── api/
│   │   └── main.go             # HTTP server — wires all dependencies
│   └── worker/
│       └── main.go             # Kafka consumer process
├── internal/
│   ├── shortner/
│   │   ├── codec.go            # Base62 short code generator (crypto/rand)
│   │   ├── repository.go       # MySQL queries — save, get, exists
│   │   └── service.go          # Shorten + redirect business logic
│   ├── analytics/
│   │   ├── worker.go           # 3 Kafka consumer handlers
│   │   └── service.go          # Analytics read logic
│   ├── cache/
│   │   └── redis.go            # Redis client + all key helpers
│   ├── kafka/
│   │   ├── producer.go         # Async click event publisher
│   │   └── consumer.go         # Base consumer with Run loop
│   ├── db/
│   │   └── db.go               # MySQL connection pool + models
│   └── metrics/
│       └── prometheus.go       # Prometheus metrics + Gin middleware
├── migrations/
│   └── 001_init.sql            # urls + click_events schema
├── config/
│   ├── config.go               # Typed config from .env
│   ├── .env.example            # Template — copy to .env
│   └── prometheus.yml          # Prometheus scrape config
├── load-test/
│   └── redirect.js             # k6 load test script
└── docker-compose.yml          # MySQL, Redis, Kafka, Grafana, Prometheus
```

---

## Quick Start

### Prerequisites
- Docker Desktop
- Go 1.22+
- k6 (for load testing)

```bash
brew install go k6
```

### 1. Clone and configure

```bash
git clone https://github.com/harshsantoshi-tech/url-shortner
cd url-shortner
cp config/.env.example config/.env
```

### 2. Start infrastructure

```bash
docker-compose --env-file config/.env up -d
```

Starts: MySQL, Redis, Kafka, Zookeeper, Kafka UI, Prometheus, Grafana.
MySQL migrations run automatically on first start.

### 3. Run the API server

```bash
go run ./cmd/api
```

```
✅ MySQL connected
✅ Redis connected
✅ Kafka producer ready
🚀 Server running at http://localhost:8081
```

### 4. Run the analytics workers

```bash
# In a new terminal
go run ./cmd/worker
```

```
✅ Kafka is ready
🚀 All 3 workers running — waiting for click events...
```

---

## API Reference

### Shorten a URL

```bash
POST /api/shorten
Content-Type: application/json

{ "url": "https://github.com/harshsantoshi" }
```

Response:
```json
{
  "short_code": "aB3xZ9q",
  "short_url": "http://localhost:8081/aB3xZ9q",
  "long_url": "https://github.com/harshsantoshi"
}
```

### Redirect

```bash
GET /:short_code
→ 302 redirect to long URL
```

### Analytics

```bash
GET /api/stats/:short_code
```

Response:
```json
{
  "short_code": "aB3xZ9q",
  "total_clicks": 42,
  "hourly_trend": [
    { "hour": "2025-05-15T10", "clicks": 30 },
    { "hour": "2025-05-15T11", "clicks": 12 }
  ],
  "top_referrers": [
    { "domain": "google.com", "clicks": 25 },
    { "domain": "direct",     "clicks": 17 }
  ]
}
```

### Health check

```bash
GET /health
→ { "status": "ok" }
```

---

## Redis Key Schema

```
url:{short_code}              STRING    long URL (TTL: 1h)
clicks:total:{short_code}     STRING    total click count
clicks:hourly:{short_code}    ZSET      member=hour_bucket, score=count
clicks:referrer:{short_code}  ZSET      member=domain, score=count
```

---

## Kafka Event Schema

Every redirect publishes a `click_event` to the `click_events` topic:

```json
{
  "short_code": "aB3xZ9q",
  "referrer":   "google.com",
  "clicked_at": "2025-05-15T10:30:00Z"
}
```

Messages are keyed by `short_code` so all clicks for the same URL land on the same partition — guaranteeing ordered processing per URL.

---

## Monitoring

| Service | URL | Credentials |
|---|---|---|
| Grafana Dashboard | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Kafka UI | http://localhost:8080 | — |

### Grafana panels
- Requests per second (by route)
- Redirect latency p50 / p95
- Total URLs shortened
- Total redirects
- Cache hit rate %
- Kafka publish rate
- Cache hits vs misses
- HTTP error rate

---

## Load Testing

```bash
# Shorten a URL first
curl -X POST http://localhost:8081/api/shorten \
  -H "Content-Type: application/json" \
  -d '{"url": "https://github.com/harshsantoshi"}'

# Run load test (replace aB3xZ9q with your short code)
k6 run -e SHORT_CODE=aB3xZ9q load-test/redirect.js
```

The test ramps to 500 virtual users over 30s, holds for 60s, then ramps down.
Results are printed as a formatted table and saved to `load-test/results.txt`.

---

## Key Design Decisions

**Why async Kafka publish on redirect?**
Publishing to Kafka synchronously would add 5-20ms to every redirect. By publishing in a goroutine, the redirect response is returned immediately — Kafka failures never affect user experience.

**Why Redis sorted sets for analytics?**
`ZINCRBY` is O(log N) and atomic — perfect for high-concurrency click counting. Sorted sets naturally support top-N queries (top referrers) and time-range queries (hourly trends) without any additional indexing.

**Why separate consumer groups per worker?**
Each worker (click counter, time trend, referrer) needs to process every message independently. Separate consumer groups ensure each worker gets its own copy of every event — Kafka handles fan-out automatically.

**Why Base62 over UUID?**
7-char Base62 gives 3.5 trillion combinations in a URL-safe, human-readable format. UUIDs are 36 chars and ugly in URLs. MD5/SHA hashes require truncation and have higher collision probability.

---

## Author

**Harsh Santoshi** — Backend Software Engineer

- GitHub: [@harshsantoshi](https://github.com/harshsantoshi-tech)
- LinkedIn: [linkedin.com/in/harshsantoshi](https://linkedin.com/in/harshsantoshi)
- Email: harsh.santoshi07@gmail.com
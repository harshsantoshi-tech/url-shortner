# URL Shortener with Analytics

A production-grade URL shortener built in Go, featuring real-time analytics via Kafka, Redis caching, and a Grafana dashboard.

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Go 1.22 |
| Web Framework | Gin |
| Cache | Redis 7 (strings + sorted sets) |
| Message Queue | Kafka (Confluent) |
| Database | MySQL 8 |
| Monitoring | Prometheus + Grafana |
| Containerisation | Docker Compose |
| Load Testing | k6 |

## Architecture

```
Client
  └─► API Gateway (rate limiting, routing)
        ├─► Shorten Service   → MySQL + Redis
        ├─► Redirect Service  → Redis (cache hit) → MySQL (miss) → Kafka (publish click)
        └─► Analytics API     → Redis sorted sets

Kafka (click_events topic)
  ├─► Click Counter Worker   → Redis INCR + MySQL INSERT
  ├─► Time Trend Worker      → Redis ZADD (hourly buckets)
  └─► Referrer Worker        → Redis ZINCRBY + MySQL INSERT
```

## Quick Start

### Prerequisites
- Docker + Docker Compose
- Go 1.22+

### 1. Start infrastructure

```bash
docker-compose up -d
```

This starts: MySQL, Redis, Kafka, Zookeeper, Kafka UI, Prometheus, Grafana.

MySQL migrations run automatically on first start.

### 2. Verify everything is healthy

```bash
docker-compose ps
# All services should show "healthy" or "running"

# Test Redis
redis-cli ping   # → PONG

# Test MySQL
mysql -h 127.0.0.1 -u appuser -papppass urlshortener -e "SHOW TABLES;"
```

### 3. Run the API (Phase 2)

```bash
go run ./cmd/api
```

API available at `http://localhost:8081`

## Services & Ports

| Service | URL |
|---|---|
| API Server | http://localhost:8081 |
| Kafka UI | http://localhost:8080 |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |
| MySQL | localhost:3306 |
| Redis | localhost:6379 |

## API Endpoints (Phase 2+)

```
POST   /api/shorten          → { short_url: "http://localhost:8081/aB3xZ9q" }
GET    /:short_code           → 302 redirect to long URL
GET    /api/stats/:short_code → click analytics
GET    /metrics               → Prometheus metrics
GET    /health                → health check
```

## Redis Key Schema

```
url:{short_code}              STRING   long URL (TTL: 1h)
clicks:total:{short_code}     STRING   total click count (INCR)
clicks:hourly:{short_code}    ZSET     member=hour_bucket, score=count
clicks:referrer:{short_code}  ZSET     member=domain, score=count
```

## Performance Targets

| Metric | Target |
|---|---|
| Redirect latency (p95) | < 10ms (Redis cache hit) |
| Redirect throughput | 5,000+ req/sec |
| Kafka event throughput | 10,000+ events/sec |
| Cache hit rate | > 95% for hot URLs |

## Load Testing (Phase 6)

```bash
k6 run load-test/redirect.js
```

## Project Structure

```
url-shortener/
├── cmd/
│   ├── api/          API server entrypoint
│   └── worker/       Kafka consumer entrypoint
├── internal/
│   ├── shortener/    shorten + redirect business logic
│   ├── analytics/    analytics query logic
│   ├── cache/        Redis client + key helpers
│   ├── kafka/        producer + consumer wrappers
│   └── db/           MySQL models + queries
├── migrations/       SQL schema (auto-applied by Docker)
├── config/           .env + Prometheus + Grafana config
└── docker-compose.yml
```
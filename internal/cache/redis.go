package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/harshsantoshi-tech/url-shortner/config"
	"github.com/redis/go-redis/v9"
)

// Client wraps redis.Client and exposes domain-specific methods.
// Keeping Redis key construction here means no other package ever
// hard-codes a key pattern — the same mistake that causes silent
// key mismatches in production.
type Client struct {
	rdb *redis.Client
	ttl time.Duration // default TTL for url cache entries
}

// New creates and returns a connected Redis client.
func New(cfg *config.Config) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
		PoolSize: cfg.RedisPoolSize,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis: ping failed (is Redis running?): %w", err)
	}

	return &Client{rdb: rdb, ttl: cfg.RedisURLTTL}, nil
}

// ── URL cache ─────────────────────────────────────────────────────────────────

// urlKey returns the Redis key for caching short_code → long_url.
func urlKey(shortCode string) string {
	return "url:" + shortCode
}

// SetURL caches the mapping short_code → longURL with the default TTL.
func (c *Client) SetURL(ctx context.Context, shortCode, longURL string) error {
	return c.rdb.Set(ctx, urlKey(shortCode), longURL, c.ttl).Err()
}

// GetURL retrieves the long URL for a short code.
// Returns ("", redis.Nil) on a cache miss — callers must handle that case
// by falling back to MySQL.
func (c *Client) GetURL(ctx context.Context, shortCode string) (string, error) {
	return c.rdb.Get(ctx, urlKey(shortCode)).Result()
}

// DeleteURL removes a URL from cache (e.g. on expiry or deletion).
func (c *Client) DeleteURL(ctx context.Context, shortCode string) error {
	return c.rdb.Del(ctx, urlKey(shortCode)).Err()
}

// ── Analytics — click counter ─────────────────────────────────────────────────

// clickTotalKey returns the key for total click count of a short code.
func clickTotalKey(shortCode string) string {
	return "clicks:total:" + shortCode
}

// IncrClickTotal atomically increments the total click counter.
func (c *Client) IncrClickTotal(ctx context.Context, shortCode string) error {
	return c.rdb.Incr(ctx, clickTotalKey(shortCode)).Err()
}

// GetClickTotal returns the total click count for a short code.
func (c *Client) GetClickTotal(ctx context.Context, shortCode string) (int64, error) {
	return c.rdb.Get(ctx, clickTotalKey(shortCode)).Int64()
}

// ── Analytics — time buckets ─────────────────────────────────────────────────
// Uses a sorted set where score = unix timestamp and member = hour bucket string.
// e.g. ZADD clicks:hourly:abc1234 1700000000 "2024-11-14T15"

func clickHourlyKey(shortCode string) string {
	return "clicks:hourly:" + shortCode
}

// RecordHourlyClick adds a click into the hourly sorted set.
// member is an hour-bucket string like "2024-11-14T15".
// score is the current unix timestamp (enables ZRANGEBYSCORE time-range queries).
func (c *Client) RecordHourlyClick(ctx context.Context, shortCode, hourBucket string, ts time.Time) error {
	return c.rdb.ZIncrBy(ctx, clickHourlyKey(shortCode), 1, hourBucket).Err()
}

// GetHourlyClicks returns click counts per hour bucket within a time range.
func (c *Client) GetHourlyClicks(ctx context.Context, shortCode string) ([]redis.Z, error) {
	return c.rdb.ZRangeWithScores(ctx, clickHourlyKey(shortCode), 0, -1).Result()
}

// ── Analytics — referrer tracking ─────────────────────────────────────────────
// Uses a sorted set where member = referrer domain, score = click count.

func clickReferrerKey(shortCode string) string {
	return "clicks:referrer:" + shortCode
}

// RecordReferrer increments the click count for a referrer domain.
func (c *Client) RecordReferrer(ctx context.Context, shortCode, referrerDomain string) error {
	if referrerDomain == "" {
		referrerDomain = "direct"
	}
	return c.rdb.ZIncrBy(ctx, clickReferrerKey(shortCode), 1, referrerDomain).Err()
}

// GetTopReferrers returns the top N referrers by click count (highest first).
func (c *Client) GetTopReferrers(ctx context.Context, shortCode string, topN int) ([]redis.Z, error) {
	return c.rdb.ZRevRangeWithScores(ctx, clickReferrerKey(shortCode), 0, int64(topN-1)).Result()
}

// ── Misc ──────────────────────────────────────────────────────────────────────

// Close cleanly shuts down the Redis connection pool.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Raw exposes the underlying redis.Client for any advanced use cases.
func (c *Client) Raw() *redis.Client {
	return c.rdb
}
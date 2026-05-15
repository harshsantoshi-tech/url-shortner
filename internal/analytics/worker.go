package analytics

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/harshsantoshi-tech/url-shortner/internal/cache"
	"github.com/harshsantoshi-tech/url-shortner/internal/kafka"
	"github.com/jmoiron/sqlx"
)

// Workers holds dependencies shared across all 3 worker handlers.
type Workers struct {
	cache *cache.Client
	db    *sqlx.DB
}

// NewWorkers creates a Workers instance.
func NewWorkers(cache *cache.Client, db *sqlx.DB) *Workers {
	return &Workers{cache: cache, db: db}
}

// ── Worker 1 — Click Counter ──────────────────────────────────────────────────
// Increments total click count in Redis AND persists to MySQL.
// Redis → fast reads for analytics API
// MySQL → durable source of truth

func (w *Workers) ClickCounterHandler(ctx context.Context, event kafka.ClickEvent) error {
	// 1. Increment Redis counter (atomic INCR)
	if err := w.cache.IncrClickTotal(ctx, event.ShortCode); err != nil {
		return fmt.Errorf("click counter: redis incr failed: %w", err)
	}

	// 2. Persist raw event to MySQL click_events table
	if err := w.insertClickEvent(ctx, event); err != nil {
		return fmt.Errorf("click counter: mysql insert failed: %w", err)
	}

	log.Printf("[worker:click-counter] processed click for %s", event.ShortCode)
	return nil
}

// ── Worker 2 — Time Trend ─────────────────────────────────────────────────────
// Buckets clicks by hour using Redis sorted sets.
// member = "2024-11-14T15" (hour bucket)
// score  = click count (ZINCRBY increments it)
//
// This lets the analytics API answer:
// "How many clicks did this URL get per hour over the last 7 days?"

func (w *Workers) TimeTrendHandler(ctx context.Context, event kafka.ClickEvent) error {
	// Build hour bucket string e.g. "2024-11-14T15"
	hourBucket := event.ClickedAt.UTC().Format("2006-01-02T15")

	if err := w.cache.RecordHourlyClick(ctx, event.ShortCode, hourBucket, event.ClickedAt); err != nil {
		return fmt.Errorf("time trend: redis zadd failed: %w", err)
	}

	log.Printf("[worker:time-trend] recorded click for %s in bucket %s", event.ShortCode, hourBucket)
	return nil
}

// ── Worker 3 — Referrer ───────────────────────────────────────────────────────
// Tracks which domains are sending traffic to each short URL.
// Uses Redis sorted set: member = domain, score = click count.
//
// This lets the analytics API answer:
// "Top 10 referrers for this URL?"

func (w *Workers) ReferrerHandler(ctx context.Context, event kafka.ClickEvent) error {
	referrer := event.Referrer
	if referrer == "" {
		referrer = "direct"
	}

	if err := w.cache.RecordReferrer(ctx, event.ShortCode, referrer); err != nil {
		return fmt.Errorf("referrer: redis zincrby failed: %w", err)
	}

	log.Printf("[worker:referrer] recorded referrer %s for %s", referrer, event.ShortCode)
	return nil
}

// ── Shared helpers ────────────────────────────────────────────────────────────

// insertClickEvent writes a raw click event row to MySQL.
// Called by ClickCounterHandler to maintain a durable audit log.
func (w *Workers) insertClickEvent(ctx context.Context, event kafka.ClickEvent) error {
	query := `
		INSERT INTO click_events (short_code, referrer, clicked_at)
		VALUES (?, ?, ?)
	`
	clickedAt := event.ClickedAt
	if clickedAt.IsZero() {
		clickedAt = time.Now()
	}

	_, err := w.db.ExecContext(ctx, query, event.ShortCode, event.Referrer, clickedAt)
	return err
}
package analytics

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"

	"github.com/harshsantoshi-tech/url-shortner/internal/cache"
)

// Service reads analytics data from Redis and builds API responses.
type Service struct {
	cache *cache.Client
}

// NewService creates a new analytics Service.
func NewService(cache *cache.Client) *Service {
	return &Service{cache: cache}
}

// HourlyBucket represents clicks in a single hour window.
type HourlyBucket struct {
	Hour   string `json:"hour"`   // e.g. "2025-05-15T10"
	Clicks int64  `json:"clicks"`
}

// ReferrerStat represents click count from a single referrer domain.
type ReferrerStat struct {
	Domain string `json:"domain"`
	Clicks int64  `json:"clicks"`
}

// StatsResponse is the full analytics payload returned by the API.
type StatsResponse struct {
	ShortCode    string         `json:"short_code"`
	TotalClicks  int64          `json:"total_clicks"`
	HourlyTrend  []HourlyBucket `json:"hourly_trend"`
	TopReferrers []ReferrerStat `json:"top_referrers"`
}

// GetStats fetches all analytics for a given short code from Redis.
func (s *Service) GetStats(ctx context.Context, shortCode string) (*StatsResponse, error) {
	// ── 1. Total clicks ───────────────────────────────────────────
	totalClicks, err := s.cache.GetClickTotal(ctx, shortCode)
	if err != nil {
		// Key doesn't exist = zero clicks, not an error
		log.Printf("[analytics] no click total found for %s, defaulting to 0", shortCode)
		totalClicks = 0
	}

	// ── 2. Hourly trend ───────────────────────────────────────────
	hourlyRaw, err := s.cache.GetHourlyClicks(ctx, shortCode)
	if err != nil {
		log.Printf("[analytics] hourly fetch failed for %s: %v", shortCode, err)
	}

	hourlyTrend := make([]HourlyBucket, 0, len(hourlyRaw))
	for _, z := range hourlyRaw {
		member, ok := z.Member.(string)
		if !ok {
			continue
		}
		hourlyTrend = append(hourlyTrend, HourlyBucket{
			Hour:   member,
			Clicks: int64(z.Score),
		})
	}

	// Sort by hour ascending so the chart renders left → right
	sort.Slice(hourlyTrend, func(i, j int) bool {
		return hourlyTrend[i].Hour < hourlyTrend[j].Hour
	})

	// ── 3. Top referrers (top 10) ─────────────────────────────────
	referrersRaw, err := s.cache.GetTopReferrers(ctx, shortCode, 10)
	if err != nil {
		log.Printf("[analytics] referrer fetch failed for %s: %v", shortCode, err)
	}

	topReferrers := make([]ReferrerStat, 0, len(referrersRaw))
	for _, z := range referrersRaw {
		domain, ok := z.Member.(string)
		if !ok {
			continue
		}
		// Score is stored as float64 by Redis — convert to int64
		clicks, _ := strconv.ParseInt(fmt.Sprintf("%.0f", z.Score), 10, 64)
		topReferrers = append(topReferrers, ReferrerStat{
			Domain: domain,
			Clicks: clicks,
		})
	}

	return &StatsResponse{
		ShortCode:    shortCode,
		TotalClicks:  totalClicks,
		HourlyTrend:  hourlyTrend,
		TopReferrers: topReferrers,
	}, nil
}
package shortner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/harshsantoshi-tech/url-shortner/internal/cache"
	"github.com/harshsantoshi-tech/url-shortner/internal/kafka"
	"github.com/redis/go-redis/v9"
)

const maxCollisionRetries = 5

// Service contains all business logic for shortening and redirecting URLs.
type Service struct {
	repo       *Repository
	cache      *cache.Client
	producer   *kafka.Producer
	codeLength int
	baseURL    string
}

// NewService creates a new shortener Service.
func NewService(repo *Repository, cache *cache.Client, producer *kafka.Producer, codeLength int, baseURL string) *Service {
	return &Service{
		repo:       repo,
		cache:      cache,
		producer:   producer,
		codeLength: codeLength,
		baseURL:    baseURL,
	}
}

// ShortenRequest is the input to Shorten.
type ShortenRequest struct {
	LongURL string
}

// ShortenResponse is returned after successfully shortening a URL.
type ShortenResponse struct {
	ShortCode string
	ShortURL  string
}

// Shorten generates a unique short code, saves to MySQL, caches in Redis.
func (s *Service) Shorten(ctx context.Context, req ShortenRequest) (*ShortenResponse, error) {
	if req.LongURL == "" {
		return nil, errors.New("service: long_url cannot be empty")
	}

	for attempt := 0; attempt < maxCollisionRetries; attempt++ {
		code, err := Generate(s.codeLength)
		if err != nil {
			return nil, fmt.Errorf("service: code generation failed: %w", err)
		}

		exists, err := s.repo.Exists(ctx, code)
		if err != nil {
			return nil, err
		}
		if exists {
			log.Printf("[shortener] collision on code %s, retrying (%d/%d)", code, attempt+1, maxCollisionRetries)
			continue
		}

		if err := s.repo.Save(ctx, code, req.LongURL); err != nil {
			return nil, err
		}

		if err := s.cache.SetURL(ctx, code, req.LongURL); err != nil {
			log.Printf("[shortener] redis cache set failed for %s: %v", code, err)
		}

		return &ShortenResponse{
			ShortCode: code,
			ShortURL:  fmt.Sprintf("%s/%s", s.baseURL, code),
		}, nil
	}

	return nil, fmt.Errorf("service: failed to generate unique code after %d attempts", maxCollisionRetries)
}

// RedirectRequest carries the short code and click metadata.
type RedirectRequest struct {
	ShortCode string
	Referrer  string // raw Referer header value
}

// Redirect resolves a short code → long URL and publishes a click event.
//
// Hot path — optimized for speed:
//  1. Redis cache lookup  (~1ms)
//  2. MySQL fallback      (cache miss only)
//  3. Kafka publish       (async, never blocks the response)
func (s *Service) Redirect(ctx context.Context, req RedirectRequest) (string, error) {
	// 1. Redis cache hit (fast path)
	longURL, err := s.cache.GetURL(ctx, req.ShortCode)
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			log.Printf("[shortener] redis get error for %s: %v", req.ShortCode, err)
		}

		// 2. MySQL fallback
		u, dbErr := s.repo.GetByShortCode(ctx, req.ShortCode)
		if dbErr != nil {
			return "", dbErr // ErrNotFound → 404
		}
		longURL = u.LongURL

		// Re-populate cache
		if cacheErr := s.cache.SetURL(ctx, req.ShortCode, longURL); cacheErr != nil {
			log.Printf("[shortener] redis re-cache failed for %s: %v", req.ShortCode, cacheErr)
		}
	}

	// 3. Publish click event to Kafka — async, never blocks redirect
	go func() {
		referrerDomain := kafka.ExtractReferrerDomain(req.Referrer)
		event := kafka.ClickEvent{
			ShortCode: req.ShortCode,
			Referrer:  referrerDomain,
			ClickedAt: time.Now(),
		}
		if err := s.producer.PublishClickEvent(context.Background(), event); err != nil {
			log.Printf("[shortener] kafka publish failed for %s: %v", req.ShortCode, err)
		}
	}()

	return longURL, nil
}
package shortner

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/harshsantoshi-tech/url-shortner/internal/cache"
	"github.com/redis/go-redis/v9"
)

// maxCollisionRetries is how many times we retry if a generated
// short code already exists in the DB. Collision probability at
// 3.5 trillion combinations is extremely low, but we handle it properly.
const maxCollisionRetries = 5

// Service contains all business logic for shortening and redirecting URLs.
// It orchestrates: codec → repository → redis cache.
type Service struct {
	repo        *Repository
	cache       *cache.Client
	codeLength  int
	baseURL     string
}

// NewService creates a new shortener Service.
func NewService(repo *Repository, cache *cache.Client, codeLength int, baseURL string) *Service {
	return &Service{
		repo:       repo,
		cache:      cache,
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
	ShortURL  string // full URL e.g. http://localhost:8081/aB3xZ9q
}

// Shorten generates a unique short code, saves it to MySQL,
// caches it in Redis, and returns the full short URL.
func (s *Service) Shorten(ctx context.Context, req ShortenRequest) (*ShortenResponse, error) {
	if req.LongURL == "" {
		return nil, errors.New("service: long_url cannot be empty")
	}

	// Retry loop handles the (very rare) collision case
	for attempt := 0; attempt < maxCollisionRetries; attempt++ {
		code, err := Generate(s.codeLength)
		if err != nil {
			return nil, fmt.Errorf("service: code generation failed: %w", err)
		}

		// Check collision
		exists, err := s.repo.Exists(ctx, code)
		if err != nil {
			return nil, err
		}
		if exists {
			log.Printf("[shortener] collision on code %s, retrying (%d/%d)", code, attempt+1, maxCollisionRetries)
			continue
		}

		// Save to MySQL
		if err := s.repo.Save(ctx, code, req.LongURL); err != nil {
			return nil, err
		}

		// Cache in Redis — non-fatal if this fails
		// The redirect flow has a MySQL fallback anyway
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

// Redirect resolves a short code to its long URL.
//
// Hot path — optimized for speed:
//   1. Redis cache lookup  (cache hit → return immediately, ~1ms)
//   2. MySQL fallback      (cache miss → query DB, re-populate cache)
//
// This is the path that will handle 99% of traffic.
func (s *Service) Redirect(ctx context.Context, shortCode string) (string, error) {
	// 1. Redis cache hit (fast path)
	longURL, err := s.cache.GetURL(ctx, shortCode)
	if err == nil {
		return longURL, nil
	}

	// Ignore redis.Nil (cache miss) but surface real Redis errors
	if !errors.Is(err, redis.Nil) {
		log.Printf("[shortener] redis get error for %s: %v", shortCode, err)
	}

	// 2. MySQL fallback (cache miss)
	u, err := s.repo.GetByShortCode(ctx, shortCode)
	if err != nil {
		return "", err // ErrNotFound propagates up to HTTP handler → 404
	}

	// Re-populate cache for next request
	if err := s.cache.SetURL(ctx, shortCode, u.LongURL); err != nil {
		log.Printf("[shortener] redis re-cache failed for %s: %v", shortCode, err)
	}

	return u.LongURL, nil
}
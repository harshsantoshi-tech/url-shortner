package shortner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/harshsantoshi-tech/url-shortner/internal/cache"
	"github.com/harshsantoshi-tech/url-shortner/internal/kafka"
	"github.com/harshsantoshi-tech/url-shortner/internal/metrics"
	"github.com/redis/go-redis/v9"
)

const maxCollisionRetries = 5

type Service struct {
	repo       *Repository
	cache      *cache.Client
	producer   *kafka.Producer
	codeLength int
	baseURL    string
}

func NewService(repo *Repository, cache *cache.Client, producer *kafka.Producer, codeLength int, baseURL string) *Service {
	return &Service{repo: repo, cache: cache, producer: producer, codeLength: codeLength, baseURL: baseURL}
}

type ShortenRequest struct{ LongURL string }
type ShortenResponse struct {
	ShortCode string
	ShortURL  string
}

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
		metrics.URLsShortenedTotal.Inc()
		return &ShortenResponse{
			ShortCode: code,
			ShortURL:  fmt.Sprintf("%s/%s", s.baseURL, code),
		}, nil
	}
	return nil, fmt.Errorf("service: failed to generate unique code after %d attempts", maxCollisionRetries)
}

type RedirectRequest struct {
	ShortCode string
	Referrer  string
}

func (s *Service) Redirect(ctx context.Context, req RedirectRequest) (string, error) {
	// 1. Redis cache hit (fast path)
	longURL, err := s.cache.GetURL(ctx, req.ShortCode)
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			log.Printf("[shortener] redis get error for %s: %v", req.ShortCode, err)
		}
		metrics.CacheMissesTotal.Inc()

		// 2. MySQL fallback
		u, dbErr := s.repo.GetByShortCode(ctx, req.ShortCode)
		if dbErr != nil {
			return "", dbErr
		}
		longURL = u.LongURL

		if cacheErr := s.cache.SetURL(ctx, req.ShortCode, longURL); cacheErr != nil {
			log.Printf("[shortener] redis re-cache failed for %s: %v", req.ShortCode, cacheErr)
		}
	} else {
		metrics.CacheHitsTotal.Inc()
	}

	metrics.RedirectsTotal.Inc()

	// 3. Publish to Kafka async
	go func() {
		referrerDomain := kafka.ExtractReferrerDomain(req.Referrer)
		event := kafka.ClickEvent{
			ShortCode: req.ShortCode,
			Referrer:  referrerDomain,
			ClickedAt: time.Now(),
		}
		if err := s.producer.PublishClickEvent(context.Background(), event); err != nil {
			log.Printf("[shortener] kafka publish failed for %s: %v", req.ShortCode, err)
			metrics.KafkaPublishTotal.WithLabelValues("error").Inc()
		} else {
			metrics.KafkaPublishTotal.WithLabelValues("success").Inc()
		}
	}()

	return longURL, nil
}

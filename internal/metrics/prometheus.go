package metrics

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// All metrics are registered globally via promauto (auto-registers on init).
var (
	// HTTP request counter — labels: method, path, status
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "urlshortener_http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	// HTTP request duration histogram — labels: method, path
	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "urlshortener_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		},
		[]string{"method", "path"},
	)

	// URL shortening counter
	URLsShortenedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "urlshortener_urls_shortened_total",
		Help: "Total number of URLs shortened",
	})

	// Redirect counter
	RedirectsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "urlshortener_redirects_total",
		Help: "Total number of redirects served",
	})

	// Cache hit/miss counters
	CacheHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "urlshortener_cache_hits_total",
		Help: "Total number of Redis cache hits on redirect",
	})

	CacheMissesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "urlshortener_cache_misses_total",
		Help: "Total number of Redis cache misses on redirect",
	})

	// Kafka publish counter
	KafkaPublishTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "urlshortener_kafka_publish_total",
			Help: "Total Kafka click events published",
		},
		[]string{"status"}, // "success" or "error"
	)

	// Active goroutines gauge (useful for spotting goroutine leaks)
	ActiveGoroutines = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "urlshortener_active_goroutines",
		Help: "Number of active goroutines",
	})
)

// PrometheusMiddleware records request count and duration for every HTTP call.
// Attach this to the Gin router with r.Use(metrics.PrometheusMiddleware()).
func PrometheusMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.FullPath() // uses route pattern e.g. "/:short_code" not "/aB3xZ9q"
		if path == "" {
			path = "unknown"
		}

		c.Next() // process request

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())

		HTTPRequestsTotal.WithLabelValues(c.Request.Method, path, status).Inc()
		HTTPRequestDuration.WithLabelValues(c.Request.Method, path).Observe(duration)
	}
}
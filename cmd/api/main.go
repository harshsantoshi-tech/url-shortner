package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/harshsantoshi-tech/url-shortner/config"
	"github.com/harshsantoshi-tech/url-shortner/internal/analytics"
	"github.com/harshsantoshi-tech/url-shortner/internal/cache"
	"github.com/harshsantoshi-tech/url-shortner/internal/db"
	"github.com/harshsantoshi-tech/url-shortner/internal/kafka"
	"github.com/harshsantoshi-tech/url-shortner/internal/metrics"
	"github.com/harshsantoshi-tech/url-shortner/internal/shortner"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// ── 1. Load config ────────────────────────────────────────────
	cfg, err := config.Load("config/.env")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// ── 2. Connect MySQL ──────────────────────────────────────────
	database := db.MustConnect(cfg)
	defer database.Close()
	log.Println("✅ MySQL connected")

	// ── 3. Connect Redis ──────────────────────────────────────────
	redisClient, err := cache.New(cfg)
	if err != nil {
		log.Fatalf("failed to connect Redis: %v", err)
	}
	defer redisClient.Close()
	log.Println("✅ Redis connected")

	// ── 4. Connect Kafka producer ─────────────────────────────────
	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopicClickEvents)
	defer producer.Close()
	log.Println("✅ Kafka producer ready")

	// ── 5. Wire up services ───────────────────────────────────────
	repo         := shortner.NewRepository(database)
	shortnerSvc := shortner.NewService(repo, redisClient, producer, cfg.ShortCodeLength, cfg.BaseURL)
	analyticsSvc := analytics.NewService(redisClient)

	// ── 6. Setup Gin router ───────────────────────────────────────
	if cfg.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.Default()

	// Attach Prometheus middleware to all routes
	r.Use(metrics.PrometheusMiddleware())

	// ── Prometheus metrics endpoint ───────────────────────────────
	// Scraped by Prometheus every 15s, read by Grafana
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// ── Health check ──────────────────────────────────────────────
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "time": time.Now()})
	})

	// ── POST /api/shorten ─────────────────────────────────────────
	r.POST("/api/shorten", func(c *gin.Context) {
		var req struct {
			URL string `json:"url" binding:"required"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "url is required"})
			return
		}

		resp, err := shortnerSvc.Shorten(c.Request.Context(), shortner.ShortenRequest{
			LongURL: req.URL,
		})
		if err != nil {
			log.Printf("[api] shorten error: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to shorten URL"})
			return
		}

		c.JSON(http.StatusCreated, gin.H{
			"short_code": resp.ShortCode,
			"short_url":  resp.ShortURL,
			"long_url":   req.URL,
		})
	})

	// ── GET /:short_code — redirect ───────────────────────────────
	r.GET("/:short_code", func(c *gin.Context) {
		code := c.Param("short_code")

		if code == "/favicon.ico" {
			c.Status(http.StatusNoContent)
			return
		}

		longURL, err := shortnerSvc.Redirect(c.Request.Context(), shortner.RedirectRequest{
			ShortCode: code,
			Referrer:  c.GetHeader("Referer"),
		})
		if err != nil {
			if errors.Is(err, shortner.ErrNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"error": "short URL not found"})
				return
			}
			log.Printf("[api] redirect error for %s: %v", code, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}

		c.Redirect(http.StatusFound, longURL)
	})

	// ── GET /api/stats/:short_code ────────────────────────────────
	r.GET("/api/stats/:short_code", func(c *gin.Context) {
		code := c.Param("short_code")

		stats, err := analyticsSvc.GetStats(c.Request.Context(), code)
		if err != nil {
			log.Printf("[api] stats error for %s: %v", code, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch stats"})
			return
		}

		c.JSON(http.StatusOK, stats)
	})

	// ── 7. Start server with graceful shutdown ────────────────────
	srv := &http.Server{
		Addr:         ":" + cfg.APIPort,
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("🚀 Server running at http://localhost:%s", cfg.APIPort)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("⏳ Shutting down gracefully...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("forced shutdown: %v", err)
	}
	log.Println("✅ Server stopped cleanly")
}
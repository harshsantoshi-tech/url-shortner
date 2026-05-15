package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/harshsantoshi-tech/url-shortner/config"
	"github.com/harshsantoshi-tech/url-shortner/internal/analytics"
	"github.com/harshsantoshi-tech/url-shortner/internal/cache"
	"github.com/harshsantoshi-tech/url-shortner/internal/db"
	"github.com/harshsantoshi-tech/url-shortner/internal/kafka"
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

	// ── 4. Create worker handlers ─────────────────────────────────
	workers := analytics.NewWorkers(redisClient, database)

	// ── 5. Create 3 Kafka consumers ───────────────────────────────
	// Each consumer is in the SAME consumer group → Kafka distributes
	// partitions between them automatically.
	// All 3 process EVERY message independently (different logic).
	brokers := cfg.KafkaBrokers
	topic   := cfg.KafkaTopicClickEvents
	group   := cfg.KafkaConsumerGroup

	consumers := []*kafka.Consumer{
		kafka.NewConsumer(brokers, topic, group+"-click-counter", "click-counter", workers.ClickCounterHandler),
		kafka.NewConsumer(brokers, topic, group+"-time-trend",    "time-trend",    workers.TimeTrendHandler),
		kafka.NewConsumer(brokers, topic, group+"-referrer",      "referrer",      workers.ReferrerHandler),
	}

	// ── 6. Start all consumers in goroutines ──────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	for _, c := range consumers {
		wg.Add(1)
		c := c // capture loop variable
		go func() {
			defer wg.Done()
			if err := c.Run(ctx); err != nil {
				log.Printf("consumer error: %v", err)
			}
		}()
	}

	log.Println("🚀 All 3 workers running — waiting for click events...")

	// ── 7. Graceful shutdown on CTRL+C ────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("⏳ Shutting down workers...")
	cancel() // signal all consumers to stop

	// Wait for all goroutines to finish cleanly
	wg.Wait()

	// Close all consumers
	for _, c := range consumers {
		if err := c.Close(); err != nil {
			log.Printf("consumer close error: %v", err)
		}
	}

	log.Println("✅ All workers stopped cleanly")
}
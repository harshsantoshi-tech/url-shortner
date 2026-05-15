package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// HandlerFunc is the function signature every worker must implement.
// It receives a decoded ClickEvent and returns an error if processing fails.
type HandlerFunc func(ctx context.Context, event ClickEvent) error

// Consumer wraps kafka-go reader and provides a clean Run loop.
// Each worker (click counter, time trend, referrer) gets its own Consumer
// instance — they all share the same consumer group so Kafka distributes
// partitions between them automatically.
type Consumer struct {
	reader  *kafka.Reader
	handler HandlerFunc
	name    string // for logging
}

// NewConsumer creates a Kafka consumer for the given topic + group.
// name is just a label for logs e.g. "click-counter".
func NewConsumer(brokers []string, topic, groupID, name string, handler HandlerFunc) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,                // fetch as soon as 1 byte is available
		MaxBytes:       10 << 20,         // 10MB max per fetch
		MaxWait:        500 * time.Millisecond,
		CommitInterval: time.Second,      // auto-commit offsets every 1s
		StartOffset:    kafka.LastOffset, // only process new messages
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			log.Printf("[kafka:consumer:%s] error: %s", name, fmt.Sprintf(msg, args...))
		}),
	})

	return &Consumer{
		reader:  reader,
		handler: handler,
		name:    name,
	}
}

// Run starts the consumer loop. It blocks until ctx is cancelled.
// Call this in a goroutine for each worker.
//
// Flow per message:
//  1. Fetch message from Kafka
//  2. Decode JSON → ClickEvent
//  3. Call handler (worker-specific logic)
//  4. Commit offset only on success (at-least-once delivery)
func (c *Consumer) Run(ctx context.Context) error {
	log.Printf("[kafka:consumer:%s] starting", c.name)

	for {
		// Blocks until a message is available or ctx is cancelled
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			// ctx cancelled = clean shutdown, not an error
			if ctx.Err() != nil {
				log.Printf("[kafka:consumer:%s] shutting down", c.name)
				return nil
			}
			log.Printf("[kafka:consumer:%s] fetch error: %v", c.name, err)
			continue
		}

		// Decode the message payload
		var event ClickEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("[kafka:consumer:%s] decode error: %v — skipping message", c.name, err)
			// Commit anyway so we don't get stuck on a malformed message
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		// Call the worker handler
		if err := c.handler(ctx, event); err != nil {
			log.Printf("[kafka:consumer:%s] handler error: %v — will retry", c.name, err)
			// Don't commit — Kafka will redeliver this message
			// This gives us at-least-once processing guarantee
			continue
		}

		// Commit offset — message successfully processed
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("[kafka:consumer:%s] commit error: %v", c.name, err)
		}
	}
}

// Close cleanly shuts down the Kafka reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// Stats returns current consumer lag and other reader stats.
// Useful for exposing to Prometheus.
func (c *Consumer) Stats() kafka.ReaderStats {
	return c.reader.Stats()
}

// extractReferrerDomain pulls the domain from a full referrer URL.
// e.g. "https://www.google.com/search?q=foo" → "google.com"
// Returns "direct" if referrer is empty.
func ExtractReferrerDomain(referrer string) string {
	if referrer == "" {
		return "direct"
	}

	// Simple domain extraction without importing net/url
	// Strip protocol
	domain := referrer
	for _, prefix := range []string{"https://", "http://"} {
		if len(domain) > len(prefix) && domain[:len(prefix)] == prefix {
			domain = domain[len(prefix):]
			break
		}
	}

	// Strip path and query
	for i, ch := range domain {
		if ch == '/' || ch == '?' || ch == '#' {
			domain = domain[:i]
			break
		}
	}

	// Strip www.
	if len(domain) > 4 && domain[:4] == "www." {
		domain = domain[4:]
	}

	if domain == "" {
		return "direct"
	}

	return fmt.Sprintf("%s", domain)
}
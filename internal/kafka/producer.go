package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// ClickEvent is the message published to Kafka on every redirect.
// Keep it small — this is a high-throughput topic.
type ClickEvent struct {
	ShortCode string    `json:"short_code"`
	Referrer  string    `json:"referrer"`   // domain e.g. "google.com", empty = direct
	ClickedAt time.Time `json:"clicked_at"`
}

// Producer wraps kafka-go writer and exposes domain-specific publish methods.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer creates a Kafka producer for the click_events topic.
// It uses async writes (Async: true) so publishing never blocks the
// redirect hot path — a failed Kafka write should never cause a 500.
func NewProducer(brokers []string, topic string) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{}, // spread load across partitions
		Async:        true,                // non-blocking — fire and forget
		MaxAttempts:  3,
		BatchTimeout: 10 * time.Millisecond,
		ErrorLogger:  kafka.LoggerFunc(func(msg string, args ...interface{}) {
			log.Printf("[kafka:producer] error: %s", fmt.Sprintf(msg, args...))
		}),
	}

	return &Producer{writer: writer}
}

// PublishClickEvent sends a click event to Kafka asynchronously.
// Because the writer is async, this returns immediately — Kafka
// handles delivery in the background.
func (p *Producer) PublishClickEvent(ctx context.Context, event ClickEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("kafka: failed to marshal click event: %w", err)
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		// Key = short_code ensures all clicks for the same URL
		// go to the same partition → ordered processing per URL
		Key:   []byte(event.ShortCode),
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("kafka: failed to publish click event: %w", err)
	}

	return nil
}

// Close flushes any buffered messages and closes the writer.
// Always call this on shutdown.
func (p *Producer) Close() error {
	return p.writer.Close()
}
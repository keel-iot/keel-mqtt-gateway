package redpanda

import (
	"context"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// ConsumerConfig holds configuration for the Redpanda consumer.
type ConsumerConfig struct {
	Brokers  []string
	GroupID  string
	Topics   []string
	// ClientID identifies this consumer instance in Redpanda logs.
	ClientID string
	// SASLUsername and SASLPassword enable SCRAM-SHA-512 authentication.
	// Leave empty to connect without authentication.
	SASLUsername string
	SASLPassword string
}

// Message is a consumed record with decoded metadata.
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Partition int32
	Offset    int64
}

// Handler is the callback invoked for each consumed message.
// Return a non-nil error to signal processing failure (message will not be committed).
type Handler func(ctx context.Context, msg Message) error

// Consumer wraps a franz-go client for consuming messages from Redpanda.
type Consumer struct {
	client *kgo.Client
}

// NewConsumer creates a new Consumer that joins the given consumer group.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelWarn, nil)),
		kgo.DisableAutoCommit(),
	}
	if cfg.SASLUsername != "" {
		auth := scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}
		opts = append(opts, kgo.SASL(auth.AsSha512Mechanism()))
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create redpanda consumer: %w", err)
	}
	return &Consumer{client: client}, nil
}

// Run starts the consume loop. It blocks until ctx is cancelled.
// For each fetched record, handler is called. On success the offset is committed.
// If handler returns an error, the error is logged but the loop continues (at-least-once semantics).
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if fetches.IsClientClosed() {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			// Log fetch errors but keep running
			for _, e := range errs {
				fmt.Fprintf(os.Stderr, "redpanda fetch error topic=%s partition=%d: %v\n", e.Topic, e.Partition, e.Err)
			}
			continue
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			msg := Message{
				Topic:     rec.Topic,
				Key:       rec.Key,
				Value:     rec.Value,
				Partition: rec.Partition,
				Offset:    rec.Offset,
			}
			if err := handler(ctx, msg); err != nil {
				fmt.Fprintf(os.Stderr, "event handler error topic=%s offset=%d: %v\n", rec.Topic, rec.Offset, err)
			}
		})

		if err := c.client.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			return fmt.Errorf("commit offsets: %w", err)
		}
	}
}

// Close shuts down the consumer and leaves the consumer group.
func (c *Consumer) Close() {
	c.client.Close()
}

package redpanda

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// SASL SCRAM mechanism identifiers for ProducerConfig.SASLMechanism.
const (
	SASLMechanismScramSHA256 = "SCRAM-SHA-256"
	SASLMechanismScramSHA512 = "SCRAM-SHA-512"
)

// ProducerConfig holds configuration for the Redpanda producer.
type ProducerConfig struct {
	Brokers []string
	// ClientID identifies this producer instance in Redpanda logs.
	ClientID string
	// SASLUsername and SASLPassword enable SCRAM authentication.
	// Leave empty to connect without authentication.
	SASLUsername string
	SASLPassword string
	// SASLMechanism selects the SCRAM mechanism (SASLMechanismScramSHA256 or
	// SASLMechanismScramSHA512). Defaults to SCRAM-SHA-512 when empty.
	SASLMechanism string
}

// scramMechanism builds the SASL mechanism for cfg's SASLMechanism setting,
// defaulting to SCRAM-SHA-512 for backward compatibility when unset.
func scramMechanism(cfg ProducerConfig) sasl.Mechanism {
	auth := scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}
	if cfg.SASLMechanism == SASLMechanismScramSHA256 {
		return auth.AsSha256Mechanism()
	}
	return auth.AsSha512Mechanism()
}

// Producer wraps a franz-go client for producing messages to Redpanda.
type Producer struct {
	client *kgo.Client
}

// NewProducer creates a new Producer connected to the given brokers.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelWarn, nil)),
	}
	if cfg.SASLUsername != "" {
		opts = append(opts, kgo.SASL(scramMechanism(cfg)))
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create redpanda producer: %w", err)
	}
	return &Producer{client: client}, nil
}

// Publish sends a JSON-encoded message to the given topic.
// key may be empty; if provided it is used for partition routing.
func (p *Producer) Publish(ctx context.Context, topic, key string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	record := &kgo.Record{
		Topic:     topic,
		Value:     b,
		Timestamp: time.Now(),
	}
	if key != "" {
		record.Key = []byte(key)
	}

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}
	return nil
}

// PublishRaw sends a raw byte payload to the given topic.
func (p *Producer) PublishRaw(ctx context.Context, topic, key string, value []byte) error {
	record := &kgo.Record{
		Topic:     topic,
		Key:       []byte(key),
		Value:     value,
		Timestamp: time.Now(),
	}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}
	return nil
}

// PublishRawWithHeaders sends a raw byte payload with native Kafka record headers.
func (p *Producer) PublishRawWithHeaders(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	record := &kgo.Record{
		Topic:     topic,
		Key:       []byte(key),
		Value:     value,
		Timestamp: time.Now(),
	}
	for k, v := range headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}
	return nil
}

// Close flushes pending messages and closes the underlying client.
func (p *Producer) Close() {
	p.client.Close()
}

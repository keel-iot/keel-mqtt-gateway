package connector

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/keel/pkg/redpanda"
)

// KafkaHonoConfig holds configuration for KafkaHonoConnector.
type KafkaHonoConfig struct {
	// Brokers is the list of Kafka broker addresses.
	Brokers []string

	// SASLUsername for Kafka authentication.
	SASLUsername string

	// SASLPassword for Kafka authentication.
	SASLPassword string

	// TopicPrefix is the prefix for Hono topics. Default is "hono".
	// Production uses "hono.telemetry.${tenant_id}" and "hono.event.${tenant_id}".
	TopicPrefix string

	// ClientID identifies this producer in Kafka logs.
	ClientID string

	// Enabled determines if the connector is active.
	// If false, Forward no-ops and HealthCheck returns nil.
	Enabled bool
}

// honoProducer is satisfied by *redpanda.Producer; narrowed for testability.
type honoProducer interface {
	PublishRawWithHeaders(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
	Close()
}

// KafkaHonoConnector forwards device messages to Eclipse Hono-compatible
// Kafka topics (hono.telemetry.${tenant_id}, hono.event.${tenant_id}),
// with device_id/tenant_id as native Kafka headers rather than encoded in
// the key, so an existing Ditto connection (SASL SCRAM-SHA-256) can
// consume them with no reconfiguration.
type KafkaHonoConnector struct {
	mu       sync.RWMutex
	producer honoProducer
	config   KafkaHonoConfig
	log      *slog.Logger

	// Category mapping from MQTT topic to Hono topic suffix.
	// "telemetry" → "telemetry", "event" → "event"
}

// Init initializes the Kafka producer.
func (c *KafkaHonoConnector) Init(ctx context.Context, config map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.config = KafkaHonoConfig{
		SASLUsername: config["sasl_username"],
		SASLPassword: config["sasl_password"],
		TopicPrefix:  config["topic_prefix"],
		ClientID:     config["client_id"],
		Enabled:      config["enabled"] == "true",
	}

	if c.config.TopicPrefix == "" {
		c.config.TopicPrefix = "hono"
	}
	if c.config.ClientID == "" {
		c.config.ClientID = "keel-mqtt-gateway-kafka-hono"
	}

	// A disabled connector must be able to initialize as a no-op with no
	// broker configuration at all — brokers are only required to actually
	// create a producer, below.
	if !c.config.Enabled {
		c.log.Info("kafka-hono: connector disabled via config")
		return nil
	}

	brokersStr := config["brokers"]
	if brokersStr == "" {
		return fmt.Errorf("kafka-hono: missing required config: brokers")
	}
	var brokers []string
	for _, b := range strings.Split(brokersStr, ",") {
		b = strings.TrimSpace(b)
		if b != "" {
			brokers = append(brokers, b)
		}
	}
	c.config.Brokers = brokers

	producer, err := redpanda.NewProducer(redpanda.ProducerConfig{
		Brokers:       c.config.Brokers,
		ClientID:      c.config.ClientID,
		SASLUsername:  c.config.SASLUsername,
		SASLPassword:  c.config.SASLPassword,
		SASLMechanism: redpanda.SASLMechanismScramSHA256,
	})
	if err != nil {
		return fmt.Errorf("kafka-hono: create producer: %w", err)
	}

	c.producer = producer
	c.log.Info("kafka-hono: connector initialized",
		"brokers", c.config.Brokers,
		"topic_prefix", c.config.TopicPrefix)
	return nil
}

// Forward forwards a message to the appropriate Hono Kafka topic.
func (c *KafkaHonoConnector) Forward(ctx context.Context, req *ForwardRequest) (*ForwardResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.config.Enabled || c.producer == nil {
		return &ForwardResponse{Success: true}, nil
	}

	// Build Hono topic: {prefix}.{category}.{tenant_id}
	// category is extracted from the Ditto topic or mapped from the original MQTT topic
	category := c.categoryFromTopic(req.Topic)
	if category == "" {
		return &ForwardResponse{
			Success: false,
			Error:   fmt.Sprintf("unknown topic category: %s", req.Topic),
		}, nil
	}

	honoTopic := fmt.Sprintf("%s.%s.%s", c.config.TopicPrefix, category, req.TenantId)

	// Build Hono headers (required by Ditto's hono-to-ditto mappingScript)
	headers := map[string]string{
		"device_id": req.DeviceId,
		"tenant_id": req.TenantId,
	}

	// Add content-type if provided in the request headers
	if ct, ok := req.Headers["content-type"]; ok {
		headers["content-type"] = ct
	}

	// Key is the device_id alone: ditto-connection-hono.json doesn't assign it a
	// mapping role, so it's kept for Kafka partition routing only.
	if err := c.producer.PublishRawWithHeaders(ctx, honoTopic, req.DeviceId, req.Payload, headers); err != nil {
		c.log.Error("kafka-hono: publish failed",
			"topic", honoTopic,
			"device_id", req.DeviceId,
			"error", err)
		return &ForwardResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	c.log.Debug("kafka-hono: forwarded",
		"topic", honoTopic,
		"device_id", req.DeviceId,
		"tenant_id", req.TenantId,
		"bytes", len(req.Payload))
	return &ForwardResponse{Success: true}, nil
}

// HealthCheck checks if the Kafka producer is healthy.
func (c *KafkaHonoConnector) HealthCheck(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.config.Enabled {
		return nil
	}
	if c.producer == nil {
		return fmt.Errorf("kafka-hono: producer not initialized")
	}
	return nil
}

// Shutdown closes the Kafka producer.
func (c *KafkaHonoConnector) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.producer != nil {
		c.producer.Close()
		c.producer = nil
	}
	c.log.Info("kafka-hono: connector shut down")
	return nil
}

// categoryFromTopic extracts the Hono category (telemetry/event) from the raw
// device MQTT topic (req.Topic is pk.TopicName, see broker/hooks.go
// forwardToOutputConnector — not the Ditto-envelope or Kafka-output topic).
// Matches the device-side topic taxonomy enforced by isAllowedPublish in
// internal/broker/hooks.go: "telemetry"/"t"[/type] and "event"/"e"[/subject],
// optionally wrapped in the "via/<uuid>/..." gateway delegation pattern.
func (c *KafkaHonoConnector) categoryFromTopic(topic string) string {
	topic = stripViaPrefix(topic)
	parts := strings.SplitN(topic, "/", 2)
	switch parts[0] {
	case "telemetry", "t":
		return "telemetry"
	case "event", "e":
		return "event"
	}
	return ""
}

// stripViaPrefix strips a leading "via/<uuid>/" gateway delegation prefix so
// the remainder can be categorized normally. Returns topic unchanged if it
// doesn't match the pattern (invalid or missing UUID).
func stripViaPrefix(topic string) string {
	const prefix = "via/"
	if !strings.HasPrefix(topic, prefix) {
		return topic
	}
	rest := topic[len(prefix):]
	slashIdx := strings.IndexByte(rest, '/')
	if slashIdx < 0 {
		return topic
	}
	if _, err := uuid.Parse(rest[:slashIdx]); err != nil {
		return topic
	}
	return rest[slashIdx+1:]
}

// init registers the connector.
func init() {
	Register("kafka-hono", func(config map[string]string) (OutputConnector, error) {
		return &KafkaHonoConnector{
			log: slog.Default(),
		}, nil
	})
}

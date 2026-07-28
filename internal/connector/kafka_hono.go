package connector

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

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

// KafkaHonoConnector forwards device messages to Eclipse Hono-compatible Kafka topics.
// Produces messages with Hono-standard headers (device_id, tenant_id, content-type)
// so an existing Eclipse Ditto connection can consume them without reconfiguration.
//
// **SCHEMA REFERENCE** (from INWIT_KIMERA production ditto-connection-hono.json):
//   Topics:
//     - hono.telemetry.${tenant_id}
//     - hono.event.${tenant_id}
//   SASL mechanism: SCRAM-SHA-256
//   Headers (required by Ditto's hono-to-ditto mappingScript, read as native
//   Kafka record headers, not encoded in the key):
//     - device_id: device UUID
//     - tenant_id: tenant identifier
//   Payload: JSON text (raw device payload)
//
// This implementation matches the production schema validated against the actual
// INWIT_KIMERA Ditto connection configuration.
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
		c.config.ClientID = "keel-mqtt-cluster-kafka-hono"
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

// categoryFromTopic extracts the Hono category (telemetry/event) from a topic string.
// Supports both:
//   - Keel telemetry topics: "keel.tenant.device.telemetry.type"
//   - Ditto topics: "tenant.device.things.twin.commands.modify"
func (c *KafkaHonoConnector) categoryFromTopic(topic string) string {
	// Check if it's a Ditto protocol topic (contains "things/twin/commands")
	if strings.Contains(topic, "things/twin/commands") {
		return "telemetry"
	}
	// Check if it's a Ditto live message (things/live/messages)
	if strings.Contains(topic, "things/live/messages") {
		return "event"
	}
	// Check if it's a keel telemetry topic (contains ".telemetry.")
	if strings.Contains(topic, ".telemetry") {
		return "telemetry"
	}
	// Check if it's a keel event topic (contains ".event")
	if strings.Contains(topic, ".events") || strings.Contains(topic, ".event.") {
		return "event"
	}
	return ""
}

// init registers the connector.
func init() {
	Register("kafka-hono", func(config map[string]string) (OutputConnector, error) {
		return &KafkaHonoConnector{
			log: slog.Default(),
		}, nil
	})
}

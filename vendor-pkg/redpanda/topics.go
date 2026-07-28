package redpanda

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// TopicManager creates Redpanda topics using the Kafka Admin API.
// It is intended to be called by the provisioning-service at device
// registration time so topics exist before the device publishes.
type TopicManager struct {
	admin *kadm.Client
}

// DeviceTopics returns the full set of standard topic names for a device.
// These are all topics the device or the platform may write to.
func DeviceTopics(tenantSlug, fleetID, deviceID string) []string {
	return []string{
		Topic(tenantSlug, fleetID, deviceID, CategoryTelemetry, TypeMetrics),
		Topic(tenantSlug, fleetID, deviceID, CategoryTelemetry, TypeEvents),
		Topic(tenantSlug, fleetID, deviceID, CategoryStatus, TypeHeartbeat),
		Topic(tenantSlug, fleetID, deviceID, CategoryStatus, TypeOTA),
		Topic(tenantSlug, fleetID, deviceID, CategoryCommands, TypeConfig),
		Topic(tenantSlug, fleetID, deviceID, CategoryCommands, TypeOTACmd),
	}
}

// NewTopicManager creates a TopicManager using the given broker config.
func NewTopicManager(cfg ProducerConfig) (*TopicManager, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
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
		return nil, fmt.Errorf("create kadm client: %w", err)
	}
	return &TopicManager{admin: kadm.NewClient(client)}, nil
}

// EnsureDeviceTopics creates the standard set of topics for a device.
// It is idempotent: existing topics are silently ignored.
// partitions and replicationFactor control the new topics; pass 0 to use
// the broker's default.num.partitions and default.replication.factor.
func (m *TopicManager) EnsureDeviceTopics(ctx context.Context, tenantSlug, fleetID, deviceID string, partitions int32, replicationFactor int16) error {
	topics := DeviceTopics(tenantSlug, fleetID, deviceID)

	resp, err := m.admin.CreateTopics(ctx, partitions, replicationFactor, nil, topics...)
	if err != nil {
		return fmt.Errorf("create device topics: %w", err)
	}

	for _, t := range resp.Sorted() {
		if t.Err != nil && !isTopicExistsError(t.Err) {
			return fmt.Errorf("create topic %q: %w", t.Topic, t.Err)
		}
	}
	return nil
}

// DeleteDeviceTopics removes all standard topics for a device.
// Used during decommissioning. Errors for non-existent topics are ignored.
func (m *TopicManager) DeleteDeviceTopics(ctx context.Context, tenantSlug, fleetID, deviceID string) error {
	topics := DeviceTopics(tenantSlug, fleetID, deviceID)
	resp, err := m.admin.DeleteTopics(ctx, topics...)
	if err != nil {
		return fmt.Errorf("delete device topics: %w", err)
	}
	for _, t := range resp.Sorted() {
		if t.Err != nil && !isTopicNotExistsError(t.Err) {
			slog.Warn("redpanda: delete topic", "topic", t.Topic, "error", t.Err)
		}
	}
	return nil
}

// Close releases the underlying admin client.
func (m *TopicManager) Close() {
	m.admin.Close()
}

func isTopicExistsError(err error) bool {
	return err != nil && err.Error() == "TOPIC_ALREADY_EXISTS: Topic already exists."
}

func isTopicNotExistsError(err error) bool {
	return err != nil && err.Error() == "UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition."
}

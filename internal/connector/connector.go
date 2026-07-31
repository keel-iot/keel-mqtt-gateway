// Package connector provides the OutputConnector interface and implementations.
// Designed for future out-of-process plugin support (hashicorp/go-plugin pattern) —
// all exchanged types remain proto-mappable (no live Go references across boundary).
package connector

import (
	"context"
)

// OutputConnector is the interface for forwarding device messages to external systems.
// All methods accept only serializable data — no live connections, pointers to internal
// state, or callbacks. This makes the interface 1:1 mappable to gRPC/proto for
// future out-of-process plugin support (e.g., hashicorp/go-plugin).
//
// Implementations:
//   - KafkaHonoConnector: forwards to Eclipse Hono-compatible Kafka topics for Ditto
type OutputConnector interface {
	// Init initializes the connector with configuration.
	// Called once at startup; config is connector-specific (e.g., Kafka brokers).
	Init(ctx context.Context, config map[string]string) error

	// Forward forwards a message to the external system.
	// Returns ForwardResponse with success/error status.
	// ForwardRequest contains topic, payload, headers, device_id, tenant_id.
	Forward(ctx context.Context, req *ForwardRequest) (*ForwardResponse, error)

	// HealthCheck checks if the connector is healthy.
	// Returns nil if healthy, error otherwise.
	HealthCheck(ctx context.Context) error

	// Shutdown gracefully shuts down the connector.
	// Called at process exit; must flush pending data.
	Shutdown(ctx context.Context) error
}

// ConnectionEventTopic is the reserved ForwardRequest.Topic value used for
// device connect/disconnect events (as opposed to a real MQTT publish).
// Payload is a JSON object {"state": "online"|"offline"}. Reserved because
// it can never collide with a real device-published MQTT topic (MQTT topic
// levels can't start with "$" except broker-owned "$SYS" topics, which
// devices never publish to). A connector that doesn't care about
// connection events (e.g. kafka-hono) simply ignores this topic.
const ConnectionEventTopic = "$connection"

// ConnectorFactory creates an OutputConnector from configuration.
type ConnectorFactory func(config map[string]string) (OutputConnector, error)

// Registry holds available connector types.
var Registry = map[string]ConnectorFactory{}

// Register registers a connector type by name.
func Register(name string, factory ConnectorFactory) {
	Registry[name] = factory
}

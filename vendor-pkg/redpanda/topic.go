// Package redpanda provides producer/consumer wrappers for Redpanda (Kafka-compatible).
// Topic taxonomy follows the platform convention:
//
//	keel.{tenant_slug}.{fleet_id}.{device_id}.{category}.{type}
//
// The static "keel." prefix allows a single PREFIXED ACL to cover all tenant topics.
// Dots are used as separators because Kafka/Redpanda topic names forbid slashes.
package redpanda

import "fmt"

// Topic categories
const (
	CategoryTelemetry = "telemetry"
	CategoryStatus    = "status"
	CategoryCommands  = "commands"
)

// Topic types per category
const (
	// telemetry/
	TypeMetrics = "metrics"
	TypeEvents  = "events"

	// status/
	TypeHeartbeat = "heartbeat"
	TypeOTA       = "ota"

	// commands/
	TypeConfig = "config"
	TypeOTACmd = "ota"
)

// DeviceTopicPrefix is the static prefix applied to all device-scoped topics.
// It allows a single Redpanda ACL (PREFIXED) to cover all tenant topics
// without knowing tenant slugs in advance.
const DeviceTopicPrefix = "keel."

// Topic builds a platform topic name from its components.
//
//	tenant_slug: human-readable slug (e.g. "acme")
//	fleetID:     platform UUID of the fleet
//	deviceID:    platform UUID of the device
//	category:    CategoryTelemetry | CategoryStatus | CategoryCommands
//	typeName:    type within the category (e.g. TypeMetrics)
func Topic(tenantSlug, fleetID, deviceID, category, typeName string) string {
	return fmt.Sprintf("%s%s.%s.%s.%s.%s", DeviceTopicPrefix, tenantSlug, fleetID, deviceID, category, typeName)
}

// MetricsTopic returns the telemetry.metrics topic for a device.
func MetricsTopic(tenantSlug, fleetID, deviceID string) string {
	return Topic(tenantSlug, fleetID, deviceID, CategoryTelemetry, TypeMetrics)
}

// EventsTopic returns the telemetry.events topic for a device.
func EventsTopic(tenantSlug, fleetID, deviceID string) string {
	return Topic(tenantSlug, fleetID, deviceID, CategoryTelemetry, TypeEvents)
}

// HeartbeatTopic returns the status.heartbeat topic for a device.
func HeartbeatTopic(tenantSlug, fleetID, deviceID string) string {
	return Topic(tenantSlug, fleetID, deviceID, CategoryStatus, TypeHeartbeat)
}

// OTAStatusTopic returns the status.ota topic for a device.
func OTAStatusTopic(tenantSlug, fleetID, deviceID string) string {
	return Topic(tenantSlug, fleetID, deviceID, CategoryStatus, TypeOTA)
}

// ConfigCommandTopic returns the commands.config topic for a device.
func ConfigCommandTopic(tenantSlug, fleetID, deviceID string) string {
	return Topic(tenantSlug, fleetID, deviceID, CategoryCommands, TypeConfig)
}

// OTACommandTopic returns the commands.ota topic for a device.
func OTACommandTopic(tenantSlug, fleetID, deviceID string) string {
	return Topic(tenantSlug, fleetID, deviceID, CategoryCommands, TypeOTACmd)
}

// TenantPattern returns a prefixed pattern matching all topics for a tenant.
// Used for ACL configuration (resource-pattern-type prefixed).
func TenantPattern(tenantSlug string) string {
	return DeviceTopicPrefix + tenantSlug + "."
}

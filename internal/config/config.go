package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for the mqtt-gateway.
type Config struct {
	// MQTT plain-text listener
	MQTTPort int
	// MQTT TLS listener (0 = disabled). TLSCertDir/TLSClientAuth/TLSEnabled
	// are node-level infra flags (see cmd/server's parseClusterFlags), not
	// env-based config — kept out of this struct.
	MQTTTLSPort int

	// HTTP adapter port (Hono-compatible REST endpoints)
	HTTPPort int

	// PostgreSQL database URL — must point to the keel_devices database.
	DatabaseURL string

	// Redpanda (Kafka-compatible) connection details.
	RedpandaBrokers  []string
	RedpandaSASLUser string
	RedpandaSASLPass string

	// TwinInboundTopic is the keel-native Redpanda topic the gateway publishes
	// device state to; keel's twin-service consumes it. The envelope is keel's
	// own format (no Ditto/Hono references). Set empty to disable emission.
	// Defaults to "keel.twin.inbound".
	TwinInboundTopic string

	// OTAStatusTopic is the flat Redpanda topic that device OTA status messages
	// (status/ota) are mirrored to for ota-service. Empty disables the mirror.
	// Defaults to "platform.ota.status".
	OTAStatusTopic string

	// CAStatusTopic is the flat Redpanda topic that device SSH CA anchor acks
	// (status/ca) are mirrored to for provisioning-service. Empty disables the
	// mirror. Defaults to "platform.ca.status".
	CAStatusTopic string

	// DittoCompat enables optional Eclipse Ditto Protocol interop: when true the
	// gateway ALSO emits Ditto Protocol envelopes to DittoInboundTopic, so an
	// external Eclipse Ditto can consume them — letting mqtt-gateway act as a
	// drop-in Eclipse Hono replacement in front of a customer's Ditto.
	// Defaults to false.
	DittoCompat bool

	// DittoInboundTopic is the Redpanda topic used for Ditto Protocol interop.
	// Only used when DittoCompat is true. Defaults to "ditto.inbound".
	DittoInboundTopic string

	// HonoCompat enables optional Eclipse Hono inbound topic compatibility:
	// when true the gateway accepts Hono-style device topics (routing infix
	// "<tenant>/<device>", "via/<sub>/…" gateway pattern, property bags).
	// When false only keel-native short topics are accepted. Defaults to false.
	HonoCompat bool

	// CommandsTopic is the Redpanda topic that carries platform→device commands.
	// The commander consumes from this topic and pushes commands to connected devices.
	// Defaults to "platform.commands".
	CommandsTopic string

	// DeviceConnectionTopic is the Redpanda topic for device connect/disconnect
	// events consumed by twin-service. Defaults to "keel.device.connection".
	DeviceConnectionTopic string

	// LogLevel controls the slog output level.
	LogLevel string

	// OTLPEndpoint is the OpenTelemetry collector gRPC endpoint.
	// Empty string disables tracing.
	OTLPEndpoint string

	// MetricsAddr is the address for the Prometheus metrics HTTP server.
	// Defaults to ":9090".
	MetricsAddr string

	// AutoProvisioningURL is the device-service base URL used to register
	// devices that authenticate for the first time via X.509 certificate.
	// Empty string disables auto-provisioning.
	AutoProvisioningURL string

	// TenantCacheTTL controls how long per-tenant gateway config is cached.
	// Defaults to 5 minutes.
	TenantCacheTTL time.Duration

	// CredentialCacheTTL controls how long successful password validations are cached
	// to reduce bcrypt load during reconnect storms. Defaults to 30 seconds.
	// Only affects the file auth provider (AuthBackend == "file").
	CredentialCacheTTL time.Duration

	// RedisAddr is the address of the Redis server used for session persistence
	// and data-volume rate limiting.  Empty string disables Redis entirely.
	RedisAddr string
	// RedisPassword is the optional authentication password for Redis.
	RedisPassword string

	// AuthBackend selects the credential-validation backend.
	// "postgres" (default) — PostgreSQL devices.device_credentials
	// "file"               — static YAML credential file (dev/air-gapped)
	// "grpc"               — keel-core gRPC (future)
	AuthBackend string
	// CredentialFile is the path to the YAML static credential file.
	// Only used when AuthBackend == "file".
	CredentialFile string
	// KeelCoreGRPCAddr is the gRPC address of keel-core.
	// Only used when AuthBackend == "grpc".
	KeelCoreGRPCAddr string

	// OutputConnector enables the OutputConnector for external forwarding.
	// When empty, no connector is active. When "kafka-hono", forwards to Hono-compatible
	// Kafka topics for Ditto integration. Opt-in only — never active by default.
	OutputConnector string

	// KafkaHonoBrokers is the Kafka broker list for the kafka-hono connector.
	// Only used when OutputConnector == "kafka-hono".
	KafkaHonoBrokers string

	// KafkaHonoSASLUser is the SASL username for Kafka authentication.
	// Only used when OutputConnector == "kafka-hono".
	KafkaHonoSASLUser string

	// KafkaHonoSASLPass is the SASL password for Kafka authentication.
	// Only used when OutputConnector == "kafka-hono".
	KafkaHonoSASLPass string

	// KafkaHonoTopicPrefix is the topic prefix for Hono topics.
	// Default is "hono". Production uses "hono.telemetry.${tenant_id}".
	// Only used when OutputConnector == "kafka-hono".
	KafkaHonoTopicPrefix string
}

// Load reads configuration from environment variables with sensible defaults for
// local development.
func Load() (*Config, error) {
	mqttPort := 1883
	if v := os.Getenv("MQTT_PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid MQTT_PORT: %w", err)
		}
		mqttPort = p
	}

	mqttTLSPort := 0
	if v := os.Getenv("MQTT_TLS_PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid MQTT_TLS_PORT: %w", err)
		}
		mqttTLSPort = p
	}

	httpPort := 8085
	if v := os.Getenv("HTTP_PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid HTTP_PORT: %w", err)
		}
		httpPort = p
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/keel_devices?sslmode=disable"
	}

	var brokers []string
	if v := os.Getenv("REDPANDA_BROKERS"); v != "" {
		for _, b := range strings.Split(v, ",") {
			if b = strings.TrimSpace(b); b != "" {
				brokers = append(brokers, b)
			}
		}
	}

	twinTopic := os.Getenv("TWIN_INBOUND_TOPIC")
	if twinTopic == "" {
		twinTopic = "keel.twin.inbound"
	}

	otaStatusTopic := os.Getenv("OTA_STATUS_TOPIC")
	if otaStatusTopic == "" {
		otaStatusTopic = "platform.ota.status"
	}

	caStatusTopic := os.Getenv("CA_STATUS_TOPIC")
	if caStatusTopic == "" {
		caStatusTopic = "platform.ca.status"
	}

	dittoTopic := os.Getenv("DITTO_INBOUND_TOPIC")
	if dittoTopic == "" {
		dittoTopic = "ditto.inbound"
	}

	cmdTopic := os.Getenv("COMMANDS_TOPIC")
	if cmdTopic == "" {
		cmdTopic = "platform.commands"
	}

	connTopic := os.Getenv("DEVICE_CONNECTION_TOPIC")
	if connTopic == "" {
		connTopic = "keel.device.connection"
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}

	tenantCacheTTL := 5 * time.Minute
	if v := os.Getenv("TENANT_CACHE_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			tenantCacheTTL = d
		}
	}

	credCacheTTL := 30 * time.Second
	if v := os.Getenv("CREDENTIAL_CACHE_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			credCacheTTL = d
		}
	}

	return &Config{
		MQTTPort:              mqttPort,
		MQTTTLSPort:           mqttTLSPort,
		HTTPPort:              httpPort,
		DatabaseURL:           dbURL,
		RedpandaBrokers:       brokers,
		RedpandaSASLUser:      os.Getenv("REDPANDA_SASL_USER"),
		RedpandaSASLPass:      os.Getenv("REDPANDA_SASL_PASS"),
		TwinInboundTopic:      twinTopic,
		OTAStatusTopic:        otaStatusTopic,
		CAStatusTopic:         caStatusTopic,
		DittoCompat:           os.Getenv("DITTO_COMPAT") == "true",
		DittoInboundTopic:     dittoTopic,
		HonoCompat:            os.Getenv("HONO_COMPAT") == "true",
		CommandsTopic:         cmdTopic,
		DeviceConnectionTopic: connTopic,
		LogLevel:              logLevel,
		OTLPEndpoint:          os.Getenv("OTLP_ENDPOINT"),
		MetricsAddr:           metricsAddr,
		AutoProvisioningURL:   os.Getenv("AUTO_PROV_URL"),
		TenantCacheTTL:        tenantCacheTTL,
		CredentialCacheTTL:    credCacheTTL,
		RedisAddr:             os.Getenv("REDIS_ADDR"),
		RedisPassword:         os.Getenv("REDIS_PASSWORD"),
		AuthBackend:           os.Getenv("AUTH_BACKEND"),
		CredentialFile:        os.Getenv("CREDENTIAL_FILE"),
		KeelCoreGRPCAddr:      os.Getenv("KEEL_CORE_GRPC_ADDR"),
		OutputConnector:       os.Getenv("OUTPUT_CONNECTOR"),
		KafkaHonoBrokers:      os.Getenv("KAFKA_HONO_BROKERS"),
		KafkaHonoSASLUser:     os.Getenv("KAFKA_HONO_SASL_USER"),
		KafkaHonoSASLPass:     os.Getenv("KAFKA_HONO_SASL_PASS"),
		KafkaHonoTopicPrefix:  os.Getenv("KAFKA_HONO_TOPIC_PREFIX"),
	}, nil
}

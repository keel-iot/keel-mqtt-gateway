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

	// CommandsTopic is the Redpanda topic that carries platform→device commands.
	// The commander consumes from this topic and pushes commands to connected devices.
	// Defaults to "platform.commands".
	CommandsTopic string

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

	// DefaultTenantID is the fallback tenant used for password-auth
	// CONNECTs whose username has no "<deviceID>@<tenantID>" separator —
	// see broker.Config.DefaultTenantID's doc. Empty (default) preserves
	// fail-closed behaviour: such connects are rejected. Only set this on
	// genuinely single-tenant deployments.
	DefaultTenantID string

	// TenantCacheTTL controls how long per-tenant gateway config is cached.
	// Defaults to 5 minutes.
	TenantCacheTTL time.Duration

	// JWKSCacheTTL controls how long a tenant's fetched JWKS is cached
	// before a refresh is attempted (kid misses still trigger an immediate
	// refresh regardless of TTL). Defaults to 5 minutes.
	JWKSCacheTTL time.Duration

	// CredentialCacheTTL controls how long successful password validations are cached
	// to reduce bcrypt load during reconnect storms. Defaults to 30 seconds.
	// Only affects the file auth provider (AuthBackend == "file").
	CredentialCacheTTL time.Duration

	// SessionExpiryInterval bounds how long a persistent (clean_session=false)
	// MQTT session's offline QoS1/2 queue, ACL identity, and cluster routing
	// entry are kept alive after a client disconnects without reconnecting.
	// Passed to mochi-mqtt as Options.Capabilities.MaximumSessionExpiryInterval
	// — see internal/broker.New and keelHook.OnClientExpired. Defaults to 24h.
	SessionExpiryInterval time.Duration

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

	// OutputConnectorPlugins is a list of "network:addr" entries (e.g.
	// "tcp:127.0.0.1:7300", "unix:/var/run/keel/plugin.sock"), one per
	// attached OutputConnector plugin sidecar (see
	// internal/connector/pluginhost). Empty = no plugins attached
	// (default). Each entry runs alongside cfg.OutputConnector, not
	// instead of it — see design doc "N plugin = N sidecar".
	OutputConnectorPlugins []string
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

	cmdTopic := os.Getenv("COMMANDS_TOPIC")
	if cmdTopic == "" {
		cmdTopic = "platform.commands"
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

	jwksCacheTTL := 5 * time.Minute
	if v := os.Getenv("JWKS_CACHE_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			jwksCacheTTL = d
		}
	}

	credCacheTTL := 30 * time.Second
	if v := os.Getenv("CREDENTIAL_CACHE_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			credCacheTTL = d
		}
	}

	sessionExpiryInterval := 24 * time.Hour
	if v := os.Getenv("SESSION_EXPIRY_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			sessionExpiryInterval = d
		}
	}

	var outputConnectorPlugins []string
	if v := os.Getenv("OUTPUT_CONNECTOR_PLUGINS"); v != "" {
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				outputConnectorPlugins = append(outputConnectorPlugins, p)
			}
		}
	}

	return &Config{
		MQTTPort:               mqttPort,
		MQTTTLSPort:            mqttTLSPort,
		HTTPPort:               httpPort,
		DatabaseURL:            dbURL,
		RedpandaBrokers:        brokers,
		RedpandaSASLUser:       os.Getenv("REDPANDA_SASL_USER"),
		RedpandaSASLPass:       os.Getenv("REDPANDA_SASL_PASS"),
		CommandsTopic:          cmdTopic,
		LogLevel:               logLevel,
		OTLPEndpoint:           os.Getenv("OTLP_ENDPOINT"),
		MetricsAddr:            metricsAddr,
		AutoProvisioningURL:    os.Getenv("AUTO_PROV_URL"),
		DefaultTenantID:        os.Getenv("DEFAULT_TENANT_ID"),
		TenantCacheTTL:         tenantCacheTTL,
		JWKSCacheTTL:           jwksCacheTTL,
		CredentialCacheTTL:     credCacheTTL,
		SessionExpiryInterval:  sessionExpiryInterval,
		RedisAddr:              os.Getenv("REDIS_ADDR"),
		RedisPassword:          os.Getenv("REDIS_PASSWORD"),
		AuthBackend:            os.Getenv("AUTH_BACKEND"),
		CredentialFile:         os.Getenv("CREDENTIAL_FILE"),
		KeelCoreGRPCAddr:       os.Getenv("KEEL_CORE_GRPC_ADDR"),
		OutputConnector:        os.Getenv("OUTPUT_CONNECTOR"),
		KafkaHonoBrokers:       os.Getenv("KAFKA_HONO_BROKERS"),
		KafkaHonoSASLUser:      os.Getenv("KAFKA_HONO_SASL_USER"),
		KafkaHonoSASLPass:      os.Getenv("KAFKA_HONO_SASL_PASS"),
		KafkaHonoTopicPrefix:   os.Getenv("KAFKA_HONO_TOPIC_PREFIX"),
		OutputConnectorPlugins: outputConnectorPlugins,
	}, nil
}

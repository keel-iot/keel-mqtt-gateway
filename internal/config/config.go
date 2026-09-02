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

	// MQTT WebSocket listeners (0 = disabled, independently of each
	// other and of MQTTPort/MQTTTLSPort). MQTTWSSPort shares
	// TLSCertDir/TLSClientAuth with MQTTTLSPort — see
	// broker.Config.MQTTWSSPort's doc.
	MQTTWSPort  int
	MQTTWSSPort int

	// ProxyProtocol/ProxyProtocolTrustedCIDRs — see broker.Config's doc.
	// TrustedCIDRs is comma-separated in its env var (PROXY_PROTOCOL_TRUSTED_CIDRS).
	ProxyProtocol             bool
	ProxyProtocolTrustedCIDRs []string

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

	// ClavexWebhookSecret verifies POST /api/cluster/revocations —
	// see management.API.ClavexWebhookSecret's doc. Empty (default) makes
	// that endpoint reject every request (fail-closed, not permissive).
	ClavexWebhookSecret string

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

	// DeviceCACacheTTL controls how long a tenant's fetched device CA
	// (TenantGatewayConfig.ClavexCAURL) is cached before a refresh is
	// attempted. Defaults to 5 minutes.
	DeviceCACacheTTL time.Duration

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

	// MaxKeepAlive caps the MQTT Keep Alive interval a client may request.
	// Zero (default) disables the cap entirely — no behaviour change from
	// before this field existed. Only enforced for MQTT5 connections (see
	// internal/broker.MaxKeepAliveHook's doc for why MQTT 3.1.1 is
	// deliberately left untouched: it has no protocol mechanism to inform
	// the client of a server-side override, so silently enforcing a
	// shorter timeout there would risk disconnecting already-working
	// 3.1.1 devices with no way for them to know why).
	MaxKeepAlive time.Duration

	// ConnectRateLimitPerSec/ConnectRateLimitBurst and
	// PublishRateLimitPerSec/PublishRateLimitBurst — see
	// broker.Config's doc on each pair. Each pair must be either both
	// zero (disabled, default) or both positive; Load rejects any other
	// combination rather than let golang.org/x/time/rate's own zero-value
	// semantics (Limit(0) allows nothing at all, burst 0 blocks everything
	// except an Inf limit) leak into what "0" means in Keel's own config.
	ConnectRateLimitPerSec float64
	ConnectRateLimitBurst  int
	PublishRateLimitPerSec float64
	PublishRateLimitBurst  int

	// RedisAddr is the address of the Redis server used for session persistence
	// and data-volume rate limiting.  Empty string disables Redis entirely.
	RedisAddr string
	// RedisPassword is the optional authentication password for Redis.
	RedisPassword string

	// RedisConnectRetryInterval and RedisConnectMaxAttempts bound how long
	// the initial Redis connection at startup retries before giving up
	// fatally — a co-located Redis's own StatefulSet pod DNS name may not
	// be resolvable for a few seconds right after scheduling in a real K8s
	// rollout (same class of timing issue as core.olric's join-retry
	// budget). A single failed dial used to be immediately fatal.
	RedisConnectRetryInterval time.Duration
	RedisConnectMaxAttempts   int

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

	// InfluxTopicFilters limits Influx Line Protocol timestamp parsing to
	// matching MQTT topics. Empty disables payload parsing entirely.
	InfluxTopicFilters []string
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

	mqttWSPort := 0
	if v := os.Getenv("MQTT_WS_PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid MQTT_WS_PORT: %w", err)
		}
		mqttWSPort = p
	}

	mqttWSSPort := 0
	if v := os.Getenv("MQTT_WSS_PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid MQTT_WSS_PORT: %w", err)
		}
		mqttWSSPort = p
	}

	proxyProtocol := os.Getenv("PROXY_PROTOCOL") == "true"
	var proxyProtocolTrustedCIDRs []string
	if v := os.Getenv("PROXY_PROTOCOL_TRUSTED_CIDRS"); v != "" {
		for _, c := range strings.Split(v, ",") {
			if c = strings.TrimSpace(c); c != "" {
				proxyProtocolTrustedCIDRs = append(proxyProtocolTrustedCIDRs, c)
			}
		}
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

	redisConnectRetryInterval := 2 * time.Second
	if v := os.Getenv("REDIS_CONNECT_RETRY_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			redisConnectRetryInterval = d
		}
	}
	redisConnectMaxAttempts := 30 // ~60s budget at the default interval
	if v := os.Getenv("REDIS_CONNECT_MAX_ATTEMPTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			redisConnectMaxAttempts = n
		}
	}

	jwksCacheTTL := 5 * time.Minute
	if v := os.Getenv("JWKS_CACHE_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			jwksCacheTTL = d
		}
	}

	deviceCACacheTTL := 5 * time.Minute
	if v := os.Getenv("DEVICE_CA_CACHE_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			deviceCACacheTTL = d
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

	// MaxKeepAlive gets real validation (unlike the silently-ignored-on-error
	// durations above) because it has a hard MQTT wire-format ceiling: Keep
	// Alive is a uint16 of seconds (max 65535s, ~18.2h). A value that
	// doesn't fit isn't a matter of taste, it's a value the protocol
	// literally cannot represent — silently ignoring or truncating it could
	// mean a config intended to raise the cap actually enforces a much
	// smaller one.
	var maxKeepAlive time.Duration
	if v := os.Getenv("MAX_KEEPALIVE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid MAX_KEEPALIVE %q: %w", v, err)
		}
		if d < 0 {
			return nil, fmt.Errorf("invalid MAX_KEEPALIVE %q: must not be negative", v)
		}
		if d.Seconds() > 65535 {
			return nil, fmt.Errorf("invalid MAX_KEEPALIVE %q: exceeds MQTT's 65535s (uint16 seconds) limit", v)
		}
		maxKeepAlive = d
	}

	connectRateLimitPerSec, connectRateLimitBurst, err := parseRateLimitPair(
		"CONNECT_RATE_LIMIT_PER_SEC", "CONNECT_RATE_LIMIT_BURST")
	if err != nil {
		return nil, err
	}
	publishRateLimitPerSec, publishRateLimitBurst, err := parseRateLimitPair(
		"PUBLISH_RATE_LIMIT_PER_SEC", "PUBLISH_RATE_LIMIT_BURST")
	if err != nil {
		return nil, err
	}

	var outputConnectorPlugins []string
	if v := os.Getenv("OUTPUT_CONNECTOR_PLUGINS"); v != "" {
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				outputConnectorPlugins = append(outputConnectorPlugins, p)
			}
		}
	}

	var influxTopicFilters []string
	if v := os.Getenv("INFLUX_TOPIC_FILTERS"); v != "" {
		for _, filter := range strings.Split(v, ",") {
			if filter = strings.TrimSpace(filter); filter != "" {
				influxTopicFilters = append(influxTopicFilters, filter)
			}
		}
	}

	return &Config{
		MQTTPort:                  mqttPort,
		MQTTTLSPort:               mqttTLSPort,
		MQTTWSPort:                mqttWSPort,
		ProxyProtocol:             proxyProtocol,
		ProxyProtocolTrustedCIDRs: proxyProtocolTrustedCIDRs,
		MQTTWSSPort:               mqttWSSPort,
		HTTPPort:                  httpPort,
		DatabaseURL:               dbURL,
		RedpandaBrokers:           brokers,
		RedpandaSASLUser:          os.Getenv("REDPANDA_SASL_USER"),
		RedpandaSASLPass:          os.Getenv("REDPANDA_SASL_PASS"),
		CommandsTopic:             cmdTopic,
		LogLevel:                  logLevel,
		OTLPEndpoint:              os.Getenv("OTLP_ENDPOINT"),
		MetricsAddr:               metricsAddr,
		AutoProvisioningURL:       os.Getenv("AUTO_PROV_URL"),
		ClavexWebhookSecret:       os.Getenv("CLAVEX_WEBHOOK_SECRET"),
		DefaultTenantID:           os.Getenv("DEFAULT_TENANT_ID"),
		TenantCacheTTL:            tenantCacheTTL,
		JWKSCacheTTL:              jwksCacheTTL,
		DeviceCACacheTTL:          deviceCACacheTTL,
		CredentialCacheTTL:        credCacheTTL,
		SessionExpiryInterval:     sessionExpiryInterval,
		MaxKeepAlive:              maxKeepAlive,
		RedisAddr:                 os.Getenv("REDIS_ADDR"),
		RedisPassword:             os.Getenv("REDIS_PASSWORD"),
		RedisConnectRetryInterval: redisConnectRetryInterval,
		RedisConnectMaxAttempts:   redisConnectMaxAttempts,
		AuthBackend:               os.Getenv("AUTH_BACKEND"),
		CredentialFile:            os.Getenv("CREDENTIAL_FILE"),
		KeelCoreGRPCAddr:          os.Getenv("KEEL_CORE_GRPC_ADDR"),
		OutputConnector:           os.Getenv("OUTPUT_CONNECTOR"),
		KafkaHonoBrokers:          os.Getenv("KAFKA_HONO_BROKERS"),
		KafkaHonoSASLUser:         os.Getenv("KAFKA_HONO_SASL_USER"),
		KafkaHonoSASLPass:         os.Getenv("KAFKA_HONO_SASL_PASS"),
		KafkaHonoTopicPrefix:      os.Getenv("KAFKA_HONO_TOPIC_PREFIX"),
		OutputConnectorPlugins:    outputConnectorPlugins,
		InfluxTopicFilters:        influxTopicFilters,
		ConnectRateLimitPerSec:    connectRateLimitPerSec,
		ConnectRateLimitBurst:     connectRateLimitBurst,
		PublishRateLimitPerSec:    publishRateLimitPerSec,
		PublishRateLimitBurst:     publishRateLimitBurst,
	}, nil
}

// parseRateLimitPair reads a "<per-sec float>"/"<burst int>" env var pair
// and enforces that the pair is either both zero (disabled) or both
// positive — never a mix. A mix would silently fall into
// golang.org/x/time/rate's own zero-value semantics (Limit(0) allows
// nothing, burst 0 with a positive limit still blocks everything except
// an Inf limit), which isn't what "disabled" or "enabled" should mean in
// Keel's own config surface.
func parseRateLimitPair(perSecEnv, burstEnv string) (perSec float64, burst int, err error) {
	perSecStr := os.Getenv(perSecEnv)
	burstStr := os.Getenv(burstEnv)

	if perSecStr != "" {
		perSec, err = strconv.ParseFloat(perSecStr, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid %s %q: %w", perSecEnv, perSecStr, err)
		}
	}
	if burstStr != "" {
		burst, err = strconv.Atoi(burstStr)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid %s %q: %w", burstEnv, burstStr, err)
		}
	}

	if perSec < 0 {
		return 0, 0, fmt.Errorf("invalid %s %q: must not be negative", perSecEnv, perSecStr)
	}
	if burst < 0 {
		return 0, 0, fmt.Errorf("invalid %s %q: must not be negative", burstEnv, burstStr)
	}
	if (perSec == 0) != (burst == 0) {
		return 0, 0, fmt.Errorf("%s and %s must either both be zero (disabled) or both positive, got %s=%q %s=%q",
			perSecEnv, burstEnv, perSecEnv, perSecStr, burstEnv, burstStr)
	}

	return perSec, burst, nil
}

package connector

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaHonoConnector_CategoryFromTopic(t *testing.T) {
	conn := &KafkaHonoConnector{}

	tests := []struct {
		name     string
		topic    string
		expected string
	}{
		{
			name:     "bare telemetry topic",
			topic:    "telemetry",
			expected: "telemetry",
		},
		{
			name:     "short telemetry alias",
			topic:    "t",
			expected: "telemetry",
		},
		{
			name:     "telemetry topic with type suffix",
			topic:    "telemetry/metrics",
			expected: "telemetry",
		},
		{
			name:     "bare event topic",
			topic:    "event",
			expected: "event",
		},
		{
			name:     "short event alias",
			topic:    "e",
			expected: "event",
		},
		{
			name:     "event topic with subject suffix",
			topic:    "event/alarm",
			expected: "event",
		},
		{
			name:     "via gateway delegation, telemetry",
			topic:    "via/f47ac10b-58cc-4372-a567-0e02b2c3d479/telemetry",
			expected: "telemetry",
		},
		{
			name:     "via gateway delegation, event with subject",
			topic:    "via/f47ac10b-58cc-4372-a567-0e02b2c3d479/event/alarm",
			expected: "event",
		},
		{
			name:     "via prefix with invalid uuid is not stripped",
			topic:    "via/not-a-uuid/telemetry",
			expected: "",
		},
		{
			name:     "unknown topic",
			topic:    "status/heartbeat",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := conn.categoryFromTopic(tt.topic)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestKafkaHonoConnector_Init(t *testing.T) {
	ctx := context.Background()
	conn := &KafkaHonoConnector{log: testLogger()}

	tests := []struct {
		name    string
		config  map[string]string
		wantErr bool
	}{
		{
			name: "valid config",
			config: map[string]string{
				"enabled":       "true",
				"brokers":       "localhost:9092",
				"sasl_username": "user",
				"sasl_password": "pass",
			},
			wantErr: false,
		},
		{
			name: "disabled",
			config: map[string]string{
				"enabled": "false",
			},
			wantErr: false,
		},
		{
			name:    "missing brokers while enabled",
			config:  map[string]string{"enabled": "true"},
			wantErr: true,
		},
		{
			name:    "missing brokers while disabled is not an error",
			config:  map[string]string{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := conn.Init(ctx, tt.config)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestKafkaHonoConnector_Forward_Disabled(t *testing.T) {
	ctx := context.Background()
	conn := &KafkaHonoConnector{log: testLogger()}

	err := conn.Init(ctx, map[string]string{"enabled": "false"})
	require.NoError(t, err)

	req := &ForwardRequest{
		Topic:    "test.topic",
		Payload:  []byte(`{"test": "data"}`),
		DeviceId: "device-123",
		TenantId: "tenant-456",
	}

	resp, err := conn.Forward(ctx, req)
	assert.NoError(t, err)
	assert.True(t, resp.Success)
}

// fakeHonoProducer captures the last PublishRawWithHeaders call for assertions,
// standing in for a real *redpanda.Producer (which requires a live broker).
type fakeHonoProducer struct {
	topic   string
	key     string
	value   []byte
	headers map[string]string
}

func (f *fakeHonoProducer) PublishRawWithHeaders(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	f.topic = topic
	f.key = key
	f.value = value
	f.headers = headers
	return nil
}

func (f *fakeHonoProducer) Close() {}

func TestKafkaHonoConnector_Forward_UsesRealHeaders(t *testing.T) {
	fp := &fakeHonoProducer{}
	conn := &KafkaHonoConnector{
		producer: fp,
		config:   KafkaHonoConfig{Enabled: true, TopicPrefix: "hono"},
		log:      testLogger(),
	}

	req := &ForwardRequest{
		Topic:    "telemetry",
		Payload:  []byte(`{"temp": 21}`),
		DeviceId: "device-123",
		TenantId: "tenant-456",
		Headers:  map[string]string{"content-type": "application/json"},
	}

	resp, err := conn.Forward(context.Background(), req)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	assert.Equal(t, "hono.telemetry.tenant-456", fp.topic)
	assert.Equal(t, "device-123", fp.key, "key carries device_id alone, not encoded headers")
	assert.Equal(t, []byte(`{"temp": 21}`), fp.value)
	assert.Equal(t, map[string]string{
		"device_id":    "device-123",
		"tenant_id":    "tenant-456",
		"content-type": "application/json",
	}, fp.headers)
}

func TestKafkaHonoConnector_HealthCheck(t *testing.T) {
	ctx := context.Background()
	conn := &KafkaHonoConnector{log: testLogger()}

	// Disabled connector should be healthy
	err := conn.Init(ctx, map[string]string{"enabled": "false"})
	require.NoError(t, err)

	err = conn.HealthCheck(ctx)
	assert.NoError(t, err)
}

func TestKafkaHonoConnector_Shutdown(t *testing.T) {
	ctx := context.Background()
	conn := &KafkaHonoConnector{log: testLogger()}

	err := conn.Init(ctx, map[string]string{"enabled": "false"})
	require.NoError(t, err)

	err = conn.Shutdown(ctx)
	assert.NoError(t, err)
}

func TestRegistry(t *testing.T) {
	// Verify kafka-hono is registered
	factory, ok := Registry["kafka-hono"]
	assert.True(t, ok, "kafka-hono should be registered")

	conn, err := factory(map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, conn)

	_, ok = conn.(*KafkaHonoConnector)
	assert.True(t, ok, "factory should return KafkaHonoConnector")
}

func testLogger() *slog.Logger {
	return slog.Default()
}

// Package commander consumes platform→device commands from Redpanda and
// delivers them to connected devices via the mochi-mqtt server.
package commander

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/keel/pkg/redpanda"
	mqtt "github.com/mochi-mqtt/server/v2"
)

// Command is the structure placed on the Redpanda "platform.commands" topic
// by the fleet-service or api-gateway when a platform operator sends a command
// to a specific device.
type Command struct {
	// DeviceID is the target device UUID (must match the MQTT client ID).
	DeviceID string `json:"device_id"`
	// TenantID is the tenant UUID of the target device.
	// When set, the command is delivered using the Hono topic format:
	//   command/<tenantID>//<deviceID>/req/<reqID>/<subject>
	// Required for Kanto suite-connector compatibility.
	TenantID string `json:"tenant_id,omitempty"`
	// Type is the command type / subject (e.g. "modify", "config", "ota").
	Type string `json:"type"`
	// Payload is the raw command payload forwarded as-is to the device.
	Payload json.RawMessage `json:"payload"`
}

// Commander consumes from the "platform.commands" Redpanda topic and
// publishes each command to the MQTT topic "command/{device_id}" so the target
// device receives it on its subscription "command/{own_device_id}".
type Commander struct {
	consumer *redpanda.Consumer
	server   *mqtt.Server
	log      *slog.Logger
}

// New creates a Commander backed by the given Redpanda consumer group.
// commandsTopic is the Redpanda topic to consume (e.g. "platform.commands").
func New(
	brokers []string,
	saslUser, saslPass string,
	commandsTopic string,
	server *mqtt.Server,
	log *slog.Logger,
) (*Commander, error) {
	consumer, err := redpanda.NewConsumer(redpanda.ConsumerConfig{
		Brokers:      brokers,
		GroupID:      "keel-mqtt-gateway-commander",
		Topics:       []string{commandsTopic},
		ClientID:     "mqtt-gateway-commander",
		SASLUsername: saslUser,
		SASLPassword: saslPass,
	})
	if err != nil {
		return nil, fmt.Errorf("create commander consumer: %w", err)
	}

	return &Commander{
		consumer: consumer,
		server:   server,
		log:      log,
	}, nil
}

// Run starts consuming commands. It blocks until ctx is cancelled.
func (c *Commander) Run(ctx context.Context) error {
	c.log.Info("mqtt-gateway: commander started")
	return c.consumer.Run(ctx, func(ctx context.Context, msg redpanda.Message) error {
		return c.handle(ctx, msg)
	})
}

// handle processes a single command message from Redpanda.
func (c *Commander) handle(_ context.Context, msg redpanda.Message) error {
	var cmd Command
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		c.log.Warn("mqtt-gateway: malformed command message, skipping", "error", err)
		return nil // do not re-queue
	}
	if cmd.DeviceID == "" {
		c.log.Warn("mqtt-gateway: command missing device_id, skipping")
		return nil
	}

	subject := cmd.Type
	if subject == "" {
		subject = "req"
	}

	var (
		mqttTopic string
		payload   []byte
		err       error
	)

	if cmd.TenantID != "" {
		// Hono command topic format for Kanto suite-connector:
		// command/<tenantID>//<deviceID>/req/<reqID>/<subject>
		//
		// suite-connector translates this to local MQTT:
		// command///<reqID>/<subject>
		// which Kanto services (software-update, container-management) subscribe to.
		reqID := uuid.New().String()
		mqttTopic = fmt.Sprintf("command/%s//%s/req/%s/%s", cmd.TenantID, cmd.DeviceID, reqID, subject)
		// For Hono commands the payload IS the Ditto protocol message (or raw JSON).
		payload = cmd.Payload
	} else {
		// Legacy format: command/<deviceID> — used by custom device agents.
		mqttTopic = "command/" + cmd.DeviceID
		payload, err = json.Marshal(map[string]any{
			"type":    cmd.Type,
			"payload": cmd.Payload,
		})
		if err != nil {
			return fmt.Errorf("marshal command envelope: %w", err)
		}
	}

	if err := c.server.Publish(mqttTopic, payload, false, 1); err != nil {
		c.log.Error("mqtt-gateway: deliver command via MQTT",
			"device_id", cmd.DeviceID,
			"type", cmd.Type,
			"error", err,
		)
		return err
	}

	c.log.Info("mqtt-gateway: command delivered",
		"device_id", cmd.DeviceID,
		"type", cmd.Type,
		"mqtt_topic", mqttTopic,
	)
	return nil
}

// Close shuts down the underlying Redpanda consumer.
func (c *Commander) Close() {
	c.consumer.Close()
}

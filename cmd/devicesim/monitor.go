package main

import (
	"encoding/json"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// monitor is a "test-consumer" role MQTT client (see
// internal/broker/hooks.go's testConsumerUsername) subscribed to
// "telemetry/#" on one broker. It never publishes; its only job is to
// timestamp deliveries for the shared metrics collector so publish→delivery
// latency and loss can be measured, mirroring test/e2e/cross_node_test.go's
// consumer pattern but running continuously instead of once per assertion.
type monitor struct {
	broker string
	client mqtt.Client
	mx     *metrics
}

const (
	testConsumerUsername = "test-consumer"
	testConsumerPassword = "consumer-e2e-testpass"
)

func newMonitor(broker string, mx *metrics) *monitor {
	return &monitor{broker: broker, mx: mx}
}

func (m *monitor) start() error {
	opts := mqtt.NewClientOptions().
		AddBroker(m.broker).
		SetClientID("devicesim-monitor-" + randID()).
		SetUsername(testConsumerUsername).
		SetPassword(testConsumerPassword).
		SetConnectTimeout(10 * time.Second).
		SetAutoReconnect(true)
	m.client = mqtt.NewClient(opts)
	token := m.client.Connect()
	if !token.WaitTimeout(15 * time.Second) {
		return fmt.Errorf("monitor connect to %s timed out", m.broker)
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("monitor connect to %s: %w", m.broker, err)
	}

	subTok := m.client.Subscribe("telemetry/#", 0, func(_ mqtt.Client, msg mqtt.Message) {
		recvAt := time.Now()
		var envelope message
		if err := json.Unmarshal(msg.Payload(), &envelope); err != nil {
			return
		}
		m.mx.recordDelivery(envelope.MsgID, recvAt)
	})
	if !subTok.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("monitor subscribe on %s timed out", m.broker)
	}
	return subTok.Error()
}

func (m *monitor) stop() {
	if m.client != nil && m.client.IsConnected() {
		m.client.Disconnect(200)
	}
}

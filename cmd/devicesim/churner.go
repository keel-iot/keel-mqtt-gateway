package main

import (
	"context"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// churner drives subscription churn against Olric's routing state using a
// dedicated "test-consumer" role client (see internal/broker/hooks.go).
//
// Regular simulated devices cannot be used for this: OnACLCheck only allows
// ordinary devices to subscribe to "command/<deviceID>" patterns, while the
// test-consumer role is the only one permitted to subscribe under
// "telemetry/" — and only to "telemetry/#" or a *literal* "telemetry/<x>"
// topic (no partial wildcards). So churn topics here are literal
// "telemetry/<base>/<random>" strings, which satisfy that ACL and still
// exercise exactly the routing-table churn (subscribe/unsubscribe rewriting
// Olric's topic->node index) that motivated moving routing out of Raft.
type churner struct {
	id     string
	broker string
	client mqtt.Client
}

func newChurner(broker string) *churner {
	return &churner{id: "devicesim-churn-" + randID(), broker: broker}
}

func (c *churner) connect() error {
	opts := mqtt.NewClientOptions().
		AddBroker(c.broker).
		SetClientID(c.id).
		SetUsername(testConsumerUsername).
		SetPassword(testConsumerPassword).
		SetConnectTimeout(10 * time.Second).
		SetAutoReconnect(true)
	c.client = mqtt.NewClient(opts)
	token := c.client.Connect()
	if !token.WaitTimeout(15 * time.Second) {
		return fmt.Errorf("churner connect to %s timed out", c.broker)
	}
	return token.Error()
}

func (c *churner) disconnect() {
	if c.client != nil && c.client.IsConnected() {
		c.client.Disconnect(200)
	}
}

// loop repeatedly subscribes to a fresh literal topic and unsubscribes from
// it at roughly rateHz cycles/sec, for the given duration. Every 10th cycle
// additionally probes routing convergence: how long it takes for the new
// subscription to become visible on every other core node's management API
// (GET /api/cluster/routes), which is the piece of state this scenario
// specifically means to stress.
func (c *churner) loop(rateHz float64, duration time.Duration, topicBase string, mx *metrics, mgmtClients []*mgmtClient, selfMgmtIdx int) {
	if rateHz <= 0 || c.client == nil {
		return
	}
	period := time.Duration(float64(time.Second) / rateHz)
	deadline := time.Now().Add(duration)
	cycle := 0
	for time.Now().Before(deadline) {
		topic := fmt.Sprintf("telemetry/%s/%s", topicBase, randID())
		start := time.Now()
		subTok := c.client.Subscribe(topic, 0, nil)
		subOK := subTok.WaitTimeout(3 * time.Second)
		var err error
		if !subOK {
			err = fmt.Errorf("subscribe timed out")
		} else {
			err = subTok.Error()
		}

		if err == nil && cycle%10 == 0 && len(mgmtClients) > 1 {
			measureConvergence(topic, mgmtClients, selfMgmtIdx, mx)
		}

		unsubTok := c.client.Unsubscribe(topic)
		unsubTok.WaitTimeout(3 * time.Second)
		mx.recordChurnCycle(err)
		cycle++

		elapsed := time.Since(start)
		if elapsed < period {
			time.Sleep(period - elapsed)
		}
	}
}

// measureConvergence polls every management API *other* than selfMgmtIdx
// until the topic shows up in that node's routes snapshot (or times out),
// and records the slowest of those latencies — the time from the client's
// perspective for the subscribe to become fully cluster-visible, not just
// locally visible on the node it subscribed through.
func measureConvergence(topic string, mgmtClients []*mgmtClient, selfMgmtIdx int, mx *metrics) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var worst time.Duration
	sawAny := false
	for i, mc := range mgmtClients {
		if i == selfMgmtIdx {
			continue
		}
		lat, ok := mc.waitRouteVisible(ctx, topic, "", 2*time.Second)
		if ok {
			sawAny = true
			if lat > worst {
				worst = lat
			}
		}
	}
	if sawAny {
		mx.recordConvergence(worst)
	}
}

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// simDevice wraps one simulated MQTT device: connect, optional shared
// wildcard subscribe, periodic publish, and reconnect-on-demand for the
// storm scenario. It deliberately mirrors test/e2e/cross_node_test.go's
// connectClient (SetAutoReconnect(false): the simulator drives reconnects
// explicitly so it can measure reconnect latency/success itself instead of
// relying on paho's internal backoff, which would make the storm scenario's
// numbers describe paho's backoff curve instead of the cluster's behavior).
type simDevice struct {
	id       string
	username string
	password string
	broker   string
	topic    string // telemetry/<tenant>/<device-id>, this device's own publish topic

	cfg *config
	mx  *metrics

	mu     sync.Mutex
	client mqtt.Client
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func newSimDevice(id, tenantID, broker string, cfg *config, mx *metrics) *simDevice {
	return &simDevice{
		id:       id,
		username: id + "@" + tenantID,
		password: cfg.devicePassword,
		broker:   broker,
		topic:    fmt.Sprintf("telemetry/%s/%s", tenantID, id),
		cfg:      cfg,
		mx:       mx,
		stopCh:   make(chan struct{}),
	}
}

func (d *simDevice) opts() *mqtt.ClientOptions {
	return mqtt.NewClientOptions().
		AddBroker(d.broker).
		SetClientID(d.id).
		SetUsername(d.username).
		SetPassword(d.password).
		SetConnectTimeout(10 * time.Second).
		SetAutoReconnect(false).
		SetCleanSession(true)
}

// connect performs a single blocking connect attempt and records the
// outcome/latency into recordFn (either metrics.recordConnect or
// metrics.recordStormReconnect, depending on the caller).
func (d *simDevice) connect(recordFn func(time.Duration, error)) error {
	d.mu.Lock()
	d.client = mqtt.NewClient(d.opts())
	client := d.client
	d.mu.Unlock()

	start := time.Now()
	token := client.Connect()
	ok := token.WaitTimeout(15 * time.Second)
	var err error
	if !ok {
		err = fmt.Errorf("connect timed out")
	} else if e := token.Error(); e != nil {
		err = e
	}
	recordFn(time.Since(start), err)
	return err
}

// disconnect tears down the underlying MQTT connection without stopping the
// device's background loops — used by the reconnect storm scenario, which
// then calls connect again to bring the device back.
func (d *simDevice) disconnect() {
	d.mu.Lock()
	client := d.client
	d.mu.Unlock()
	if client != nil && client.IsConnected() {
		client.Disconnect(200)
	}
	d.mx.recordStormDisconnect()
}

// runPublishLoop publishes at cfg.publishRateHz until stopCh is closed. Each
// device jitters its own ticker phase by up to one period so N devices
// starting simultaneously don't all publish in lockstep (which would create
// an artificial thundering-herd pattern not present in real fleets).
func (d *simDevice) runPublishLoop() {
	if d.cfg.publishRateHz <= 0 {
		return
	}
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		period := time.Duration(float64(time.Second) / d.cfg.publishRateHz)
		jitter := time.Duration(rand.Int63n(int64(period)))
		select {
		case <-time.After(jitter):
		case <-d.stopCh:
			return
		}
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			select {
			case <-d.stopCh:
				return
			case <-ticker.C:
				d.publishOne()
			}
		}
	}()
}

func (d *simDevice) publishOne() {
	d.mu.Lock()
	client := d.client
	d.mu.Unlock()
	if client == nil || !client.IsConnected() {
		return
	}
	msg := newMessage(d.id, d.cfg.payloadBytes)
	payload, err := marshalMessage(msg)
	if err != nil {
		return
	}
	sentAt := time.Now()
	token := client.Publish(d.topic, 0, false, payload)
	go func() {
		token.WaitTimeout(5 * time.Second)
		d.mx.recordPublish(msg.MsgID, sentAt, token.Error())
	}()
}

func (d *simDevice) stop() {
	close(d.stopCh)
	d.wg.Wait()
	d.mu.Lock()
	client := d.client
	d.mu.Unlock()
	if client != nil && client.IsConnected() {
		client.Disconnect(200)
	}
}

func marshalMessage(m message) ([]byte, error) {
	return json.Marshal(m)
}

func init() {
	// Silence paho's default logger chatter (connection retries etc.) —
	// the simulator reports its own connect/publish outcomes via metrics,
	// and thousands of devices logging individually would drown stdout.
	mqtt.ERROR = log.New(nopWriter{}, "", 0)
	mqtt.CRITICAL = log.New(nopWriter{}, "", 0)
	mqtt.WARN = log.New(nopWriter{}, "", 0)
	mqtt.DEBUG = log.New(nopWriter{}, "", 0)
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

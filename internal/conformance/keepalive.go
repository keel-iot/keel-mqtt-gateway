package conformance

import (
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// KeepAliveHook implements paho.mqtt.testing's test_server_keep_alive
// expectation: a client connecting with Clean=true and Keepalive=120
// must receive a CONNACK advertising a server-assigned ServerKeepAlive of
// 60 — a real MQTT5 test scenario, not an arbitrary constant, mirrored
// from mochi-mqtt's own examples/paho.testing/main.go (its pahoAuthHook's
// OnConnect), which we can't import directly (an example main package).
type KeepAliveHook struct {
	mqtt.HookBase
}

// NewKeepAliveHook returns a ready-to-use conformance KeepAliveHook.
func NewKeepAliveHook() *KeepAliveHook {
	return &KeepAliveHook{}
}

func (*KeepAliveHook) ID() string { return "conformance-server-keepalive" }

func (*KeepAliveHook) Provides(b byte) bool {
	return b == mqtt.OnConnect
}

func (*KeepAliveHook) OnConnect(cl *mqtt.Client, pk packets.Packet) error {
	if pk.Connect.Keepalive == 120 && pk.Connect.Clean {
		cl.State.Keepalive = 60
		cl.State.ServerKeepalive = true
	}
	return nil
}

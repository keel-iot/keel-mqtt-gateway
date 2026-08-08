package broker

import (
	"testing"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

func TestMaxKeepAliveHook_Provides(t *testing.T) {
	h := NewMaxKeepAliveHook(300)
	if !h.Provides(mqtt.OnConnect) {
		t.Error("expected Provides(OnConnect) to be true")
	}
	if h.Provides(mqtt.OnACLCheck) {
		t.Error("expected Provides(OnACLCheck) to be false — this hook only handles OnConnect")
	}
}

// TestMaxKeepAliveHook_MQTT311Untouched pins the deliberate decision:
// MQTT 3.1.1 has no Server Keep Alive property, so this hook must never
// mutate cl.State for a non-MQTT5 client — see the type's doc for why.
func TestMaxKeepAliveHook_MQTT311Untouched(t *testing.T) {
	h := NewMaxKeepAliveHook(60)
	cl := &mqtt.Client{Properties: mqtt.ClientProperties{ProtocolVersion: 4}}
	cl.State.Keepalive = 9999 // pre-set to a value the hook must not touch

	if err := h.OnConnect(cl, packets.Packet{Connect: packets.ConnectParams{Keepalive: 9999}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cl.State.Keepalive != 9999 {
		t.Errorf("expected MQTT 3.1.1 client's Keepalive to be left untouched, got %d", cl.State.Keepalive)
	}
	if cl.State.ServerKeepalive {
		t.Error("expected ServerKeepalive to remain false for MQTT 3.1.1")
	}
}

// TestMaxKeepAliveHook_Boundaries covers the boundary matrix requested for
// this feature: below max, exactly at max, above max, zero (unlimited),
// and an extreme (uint16 max) requested value.
func TestMaxKeepAliveHook_Boundaries(t *testing.T) {
	const max = 300

	cases := []struct {
		name         string
		requested    uint16
		wantOverride bool
	}{
		{"below max", 60, false},
		{"exactly at max", 300, false},
		{"one above max", 301, true},
		{"zero (unlimited requested)", 0, true},
		{"extreme uint16 max", 65535, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := NewMaxKeepAliveHook(max)
			cl := &mqtt.Client{Properties: mqtt.ClientProperties{ProtocolVersion: 5}}

			if err := h.OnConnect(cl, packets.Packet{Connect: packets.ConnectParams{Keepalive: c.requested}}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if c.wantOverride {
				if !cl.State.ServerKeepalive {
					t.Error("expected an override (ServerKeepalive=true)")
				}
				if cl.State.Keepalive != max {
					t.Errorf("expected overridden Keepalive=%d, got %d", max, cl.State.Keepalive)
				}
			} else {
				if cl.State.ServerKeepalive {
					t.Error("expected no override (ServerKeepalive=false)")
				}
				if cl.State.Keepalive != 0 {
					t.Errorf("expected Keepalive left at its zero value (untouched), got %d", cl.State.Keepalive)
				}
			}
		})
	}
}

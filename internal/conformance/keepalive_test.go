package conformance

import (
	"testing"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

func TestKeepAliveHook_OverridesOnMatchingConnect(t *testing.T) {
	h := NewKeepAliveHook()
	cl := &mqtt.Client{}
	pk := packets.Packet{Connect: packets.ConnectParams{Keepalive: 120, Clean: true}}

	if err := h.OnConnect(cl, pk); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cl.State.Keepalive != 60 {
		t.Errorf("expected Keepalive=60, got %d", cl.State.Keepalive)
	}
	if !cl.State.ServerKeepalive {
		t.Error("expected ServerKeepalive=true")
	}
}

func TestKeepAliveHook_NoOverrideOnOtherConnects(t *testing.T) {
	h := NewKeepAliveHook()

	cases := []packets.ConnectParams{
		{Keepalive: 120, Clean: false}, // not a clean session
		{Keepalive: 60, Clean: true},   // not the specific 120 the suite tests
	}
	for _, cp := range cases {
		cl := &mqtt.Client{}
		if err := h.OnConnect(cl, packets.Packet{Connect: cp}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cl.State.ServerKeepalive {
			t.Errorf("expected no override for %+v", cp)
		}
	}
}

func TestKeepAliveHook_Provides(t *testing.T) {
	h := NewKeepAliveHook()
	if !h.Provides(mqtt.OnConnect) {
		t.Error("expected Provides(OnConnect) to be true")
	}
	if h.Provides(mqtt.OnACLCheck) {
		t.Error("expected Provides(OnACLCheck) to be false — this hook only handles OnConnect")
	}
}

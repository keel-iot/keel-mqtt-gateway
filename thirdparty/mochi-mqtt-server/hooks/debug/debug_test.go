package debug

import (
	"testing"

	"github.com/mochi-mqtt/server/v2/packets"
)

func TestPacketMetaShowPacketDataRedactsConnectPassword(t *testing.T) {
	h := &Hook{config: &Options{ShowPacketData: true}}
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Connect},
		Connect: packets.ConnectParams{
			ClientIdentifier: "device-1",
			Password:         []byte("secret"),
		},
	}

	meta := h.packetMeta(pk)
	dumped, ok := meta["packet"].(packets.Packet)
	if !ok {
		t.Fatalf("packet metadata type = %T, want packets.Packet", meta["packet"])
	}
	if got := string(dumped.Connect.Password); got != "" {
		t.Fatalf("packet metadata contains CONNECT password %q", got)
	}
	if _, ok := meta["password"]; ok {
		t.Fatal("packet metadata unexpectedly contains the top-level password field")
	}
}

func TestPacketMetaShowPasswordsRemainsExplicit(t *testing.T) {
	h := &Hook{config: &Options{ShowPasswords: true}}
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Connect},
		Connect:     packets.ConnectParams{Password: []byte("secret")},
	}

	meta := h.packetMeta(pk)
	if got := meta["password"]; got != "secret" {
		t.Fatalf("explicit password debug value = %v, want secret", got)
	}
}

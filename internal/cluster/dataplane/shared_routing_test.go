package dataplane

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestSharedRoutingMetadataRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name    string
		filters []string
	}{
		{"legacy", nil},
		{"ordinary-only", []string{}},
		{"selected-group", []string{"$share/a/telemetry/#"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, received := startTestServer(t)
			client := NewGRPCForwarder(func(string) (string, bool) { return addr, true }, nil)
			t.Cleanup(func() {
				client.mu.Lock()
				defer client.mu.Unlock()
				for _, conn := range client.conns {
					_ = conn.Close()
				}
			})
			if err := client.Forward(context.Background(), "target", &Message{Topic: "telemetry/a", SharedFilters: tc.filters}); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-received:
				if !reflect.DeepEqual(got.SharedFilters, tc.filters) {
					t.Fatalf("shared filters=%#v, want %#v", got.SharedFilters, tc.filters)
				}
			case <-time.After(time.Second):
				t.Fatal("forward did not arrive")
			}
		})
	}
}

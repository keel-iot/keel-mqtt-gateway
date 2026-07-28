package forwarder_test

import (
	"testing"

	"github.com/keel-iot/keel-mqtt-gateway/internal/forwarder"
)

func TestParsePropertyBag(t *testing.T) {
	tests := []struct {
		name            string
		topic           string
		wantClean       string
		wantContentType string
		wantTTL         int
		wantRawLen      int
	}{
		{
			name:      "no property bag",
			topic:     "telemetry",
			wantClean: "telemetry",
		},
		{
			name:            "content-type only",
			topic:           "telemetry/?content-type=application%2Fjson",
			wantClean:       "telemetry",
			wantContentType: "application/json",
			wantRawLen:      1,
		},
		{
			name:            "content-type and hono-ttl",
			topic:           "telemetry/t1/d1/?content-type=text%2Fplain&hono-ttl=30",
			wantClean:       "telemetry/t1/d1",
			wantContentType: "text/plain",
			wantTTL:         30,
			wantRawLen:      2,
		},
		{
			name:       "hono-ttd alias",
			topic:      "event/?hono-ttd=60",
			wantClean:  "event",
			wantTTL:    60,
			wantRawLen: 1,
		},
		{
			name:            "deep topic with property bag",
			topic:           "telemetry/myTenant/myDevice/metrics/?content-type=application%2Fjson&hono-ttl=10",
			wantClean:       "telemetry/myTenant/myDevice/metrics",
			wantContentType: "application/json",
			wantTTL:         10,
			wantRawLen:      2,
		},
		{
			name:      "malformed query does not panic",
			topic:     "telemetry/?%zz",
			wantClean: "telemetry",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clean, bag := forwarder.ParsePropertyBag(tc.topic)

			if clean != tc.wantClean {
				t.Errorf("cleanTopic = %q, want %q", clean, tc.wantClean)
			}
			if bag.ContentType != tc.wantContentType {
				t.Errorf("ContentType = %q, want %q", bag.ContentType, tc.wantContentType)
			}
			if bag.TTL != tc.wantTTL {
				t.Errorf("TTL = %d, want %d", bag.TTL, tc.wantTTL)
			}
			if tc.wantRawLen > 0 && len(bag.Raw) != tc.wantRawLen {
				t.Errorf("len(Raw) = %d, want %d", len(bag.Raw), tc.wantRawLen)
			}
		})
	}
}

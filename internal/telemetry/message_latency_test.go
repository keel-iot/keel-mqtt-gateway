package telemetry

import (
	"testing"
	"time"
)

func TestInfluxLineTimestampPrecision(t *testing.T) {
	want := time.Unix(1_700_000_000, 123456789)
	cases := []struct {
		name string
		line string
		want time.Time
	}{
		{"seconds", "temperature,device=d1 value=1 1700000000", time.Unix(1700000000, 0)},
		{"milliseconds", "temperature,device=d1 value=1 1700000000123", time.Unix(1700000000, int64(123*time.Millisecond))},
		{"microseconds", "temperature,device=d1 value=1 1700000000123456", time.Unix(1700000000, int64(123456*time.Microsecond))},
		{"nanoseconds", "temperature,device=d1 value=1 1700000000123456789", want},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := influxLineTimestamp(tc.line)
			if !ok {
				t.Fatal("expected timestamp to be parsed")
			}
			if !got.Equal(tc.want) {
				t.Fatalf("timestamp = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestInfluxLineTimestampMissingOrInvalid(t *testing.T) {
	for _, line := range []string{
		"temperature,device=d1 value=1",
		"temperature,device=d1 value=1 not-a-timestamp",
		"# comment",
		"{\"timestamp\":1700000000}",
	} {
		if _, ok := influxLineTimestamp(line); ok {
			t.Errorf("influxLineTimestamp(%q) unexpectedly parsed", line)
		}
	}
}

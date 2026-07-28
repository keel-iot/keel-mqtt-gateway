package connector

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
)

// testutilCounterTotal reads the current value of the ForwarderDropped
// counter for a given connector/reason label pair, so tests can assert on
// the delta caused by their own actions regardless of what earlier tests
// in the same process already recorded (the metric is a package-level
// global, shared across the test binary).
func testutilCounterTotal(t *testing.T, connector, reason string) float64 {
	t.Helper()
	return testutil.ToFloat64(telemetry.ForwarderDropped.WithLabelValues(connector, reason))
}

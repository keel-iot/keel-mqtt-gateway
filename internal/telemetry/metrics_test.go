package telemetry

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestQosDropped_IncrementsByQosLabel(t *testing.T) {
	before1 := testutil.ToFloat64(QosDropped.WithLabelValues("1"))
	before2 := testutil.ToFloat64(QosDropped.WithLabelValues("2"))

	QosDropped.WithLabelValues("1").Inc()
	QosDropped.WithLabelValues("1").Inc()
	QosDropped.WithLabelValues("2").Inc()

	if got, want := testutil.ToFloat64(QosDropped.WithLabelValues("1")), before1+2; got != want {
		t.Fatalf("qos=1 count = %v, want %v", got, want)
	}
	if got, want := testutil.ToFloat64(QosDropped.WithLabelValues("2")), before2+1; got != want {
		t.Fatalf("qos=2 count = %v, want %v", got, want)
	}
}

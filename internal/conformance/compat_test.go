package conformance

import (
	"testing"

	mqtt "github.com/mochi-mqtt/server/v2"
)

func TestApplyCompatibilities_SetsExpectedFlags(t *testing.T) {
	caps := mqtt.NewDefaultServerCapabilities()
	ApplyCompatibilities(caps)

	if !caps.Compatibilities.ObscureNotAuthorized {
		t.Error("expected ObscureNotAuthorized to be enabled")
	}
}

// TestApplyCompatibilities_DefaultIsOff pins mochi-mqtt's own default for
// this flag — if a future mochi-mqtt release flips the default, this
// fails loudly instead of silently changing production behavior
// (ApplyCompatibilities must only ever be called for --conformance-test).
func TestApplyCompatibilities_DefaultIsOff(t *testing.T) {
	caps := mqtt.NewDefaultServerCapabilities()
	if caps.Compatibilities.ObscureNotAuthorized {
		t.Error("expected ObscureNotAuthorized to default to false")
	}
}

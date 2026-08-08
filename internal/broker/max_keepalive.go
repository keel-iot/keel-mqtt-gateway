package broker

import (
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
)

// MaxKeepAliveHook caps the effective MQTT Keep Alive interval a client
// may request, enforced server-side via mochi-mqtt's own OnConnect
// extension point (see its server.go's SendConnack: "You can set this
// dynamically using the OnConnect hook" — this is the mechanism the
// library itself documents for exactly this, not a workaround).
//
// Deliberately MQTT5-only. MQTT 3.1.1 has no Server Keep Alive property —
// no protocol mechanism exists to tell a 3.1.1 client the server intends
// to enforce a shorter timeout than requested. Enforcing the cap there
// anyway (mutating cl.State.Keepalive still works mechanically — mochi-mqtt
// uses it for both versions to set the real idle-disconnect deadline via
// cl.refreshDeadline right after OnConnect returns) would silently make
// already-working 3.1.1 clients — e.g. devices migrated as-is from
// another broker, still keeping their own original interval — get
// disconnected for a reason they were never told about and have no way
// to adapt to. Left untouched, not as an oversight but as the
// spec-driven, predictable choice: only override what the protocol lets
// the client learn about.
type MaxKeepAliveHook struct {
	mqtt.HookBase
	// MaxSeconds is the enforced ceiling, already validated to fit MQTT's
	// uint16-seconds Keep Alive wire format (see config.Load's
	// MAX_KEEPALIVE validation) — this hook trusts its caller, it does not
	// re-validate.
	MaxSeconds uint16
}

// NewMaxKeepAliveHook returns a hook enforcing maxSeconds as the effective
// upper bound on any MQTT5 client's requested Keep Alive.
func NewMaxKeepAliveHook(maxSeconds uint16) *MaxKeepAliveHook {
	return &MaxKeepAliveHook{MaxSeconds: maxSeconds}
}

func (*MaxKeepAliveHook) ID() string { return "keel-max-keepalive" }

func (*MaxKeepAliveHook) Provides(b byte) bool {
	return b == mqtt.OnConnect
}

// OnConnect overrides cl.State.Keepalive (and announces it via
// cl.State.ServerKeepalive) whenever the client's requested value is
// either above the configured maximum, or zero. Zero is deliberately
// treated as "exceeds the maximum", not "leave alone": MQTT5 defines
// Keep Alive=0 as disabling the keep-alive mechanism entirely, which is
// the opposite of what a configured maximum is for — an operator setting
// MaxKeepAlive wants every connection bounded, including ones that asked
// for no bound at all. Within bounds (1..MaxSeconds inclusive), the
// client's own requested value is left completely untouched — no
// property is added to the CONNACK, matching today's behaviour exactly.
func (h *MaxKeepAliveHook) OnConnect(cl *mqtt.Client, pk packets.Packet) error {
	if cl.Properties.ProtocolVersion != 5 {
		return nil
	}
	requested := pk.Connect.Keepalive
	if requested != 0 && requested <= h.MaxSeconds {
		return nil
	}
	cl.State.Keepalive = h.MaxSeconds
	cl.State.ServerKeepalive = true
	telemetry.MaxKeepAliveOverridesTotal.Inc()
	return nil
}

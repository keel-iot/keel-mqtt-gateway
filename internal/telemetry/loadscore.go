package telemetry

import (
	"context"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// EdgeLoadScore is the composite HPA load metric for an edge pod:
//
//	connection_fraction * 0.6 + cpu_fraction * 0.4 + overload_fraction * 0.2
//
// where connection_fraction is clamped to [0, 1] at the soft connection
// threshold, and overload_fraction is a quadratic ramp from 0 to 1 between
// the soft and hard connection thresholds. The result is computed
// application-side (see design doc,
// "HPA sui nodi edge" and risk #5) so the custom-metrics adapter only ever
// has to relay a single already-aggregated number, not compute a formula
// itself. Only meaningful on a node actually running the broker
// (edge/combined/standalone) — a pure core node never starts the sampler.
var EdgeLoadScore = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: "keel_gateway",
	Name:      "edge_load_score",
	Help:      "Composite HPA load score: connection fraction*0.6 + CPU fraction*0.4 + quadratic connection-overload fraction*0.2.",
})

// ActiveBrokerConnections is the authoritative number of currently open
// MQTT broker connections for this process. It is sampled directly from
// mochi-mqtt's Server.Info.ClientsConnected, unlike the per-tenant
// active_connections gauge which is maintained by authentication hooks.
var ActiveBrokerConnections = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: "keel_gateway",
	Name:      "active_broker_connections",
	Help:      "Number of currently open MQTT broker connections for this pod.",
})

// EdgeConnectionsFraction, EdgeConnectionsOverloadFraction and
// EdgeCPUFraction are the terms behind EdgeLoadScore, exposed individually
// for dashboards/debugging — the HPA itself should only ever reference the
// composite.
var (
	EdgeConnectionsFraction = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "keel_gateway",
		Name:      "edge_load_connections_fraction",
		Help:      "active_connections / soft_connections_limit for this pod, clamped to [0,1] — the base connections term of edge_load_score.",
	})
	EdgeConnectionsOverloadFraction = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "keel_gateway",
		Name:      "edge_load_connections_overload_fraction",
		Help:      "Quadratic connection-overload ramp between the soft and hard per-pod connection thresholds, clamped to [0,1].",
	})
	EdgeCPUFraction = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "keel_gateway",
		Name:      "edge_load_cpu_fraction",
		Help:      "Process CPU time consumed since the last sample, as a fraction of cpu_limit cores, clamped to [0,1] — the CPU term of edge_load_score.",
	})
)

// cpuSampler tracks process CPU time (user+system, via getrusage) between
// successive samples to derive a utilisation fraction, the same way
// `docker stats`/cgroup CPU accounting does, without needing to read
// cgroup files directly (container-runtime-agnostic: works identically on
// K8s, plain Docker, or a bare VM per the project's "same binary
// everywhere" constraint).
type cpuSampler struct {
	lastCPU  time.Duration
	lastWall time.Time
}

// sample returns the CPU-time fraction of cpuLimit cores consumed since the
// previous call (0 on the very first call, when there is no prior delta).
func (s *cpuSampler) sample(cpuLimit float64) float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	cpuTime := time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond +
		time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
	now := time.Now()

	var frac float64
	if !s.lastWall.IsZero() && cpuLimit > 0 {
		wallDelta := now.Sub(s.lastWall).Seconds()
		cpuDelta := (cpuTime - s.lastCPU).Seconds()
		if wallDelta > 0 {
			frac = cpuDelta / wallDelta / cpuLimit
		}
	}
	s.lastCPU = cpuTime
	s.lastWall = now
	return clamp01(frac)
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// connectionPressure returns the base connection fraction and the additional
// overload ramp. The soft limit remains the denominator of the original
// connections term, so values up to that threshold behave exactly as before.
// Above it, the quadratic ramp avoids triggering an HPA scale-up immediately
// for every connection above the soft threshold while still reaching 1 at the
// hard threshold. Invalid/non-increasing limits disable the extra ramp.
func connectionPressure(connections, softLimit, hardLimit int) (base, overload float64) {
	if softLimit <= 0 {
		return 0, 0
	}

	base = clamp01(float64(connections) / float64(softLimit))
	if hardLimit <= softLimit || connections <= softLimit {
		return base, 0
	}

	ramp := clamp01(float64(connections-softLimit) / float64(hardLimit-softLimit))
	return base, ramp * ramp
}

// RunEdgeLoadScoreSampler periodically recomputes EdgeLoadScore (and its three
// component gauges) until ctx is done. connCount is polled fresh on every
// tick — for the real broker this is mqtt.Server.Info.ClientsConnected
// (already tracked natively by mochi-mqtt, see server.go). softConnectionsLimit
// and hardConnectionsLimit are the per-pod connection thresholds; the former
// is the base-term denominator, while the latter is the ceiling of the
// progressive overload ramp. cpuLimit is the CPU request/limit denominator.
func RunEdgeLoadScoreSampler(ctx context.Context, connCount func() int, softConnectionsLimit, hardConnectionsLimit int, cpuLimit float64, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	sampler := &cpuSampler{}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	tick := func() {
		connections := connCount()
		ActiveBrokerConnections.Set(float64(connections))
		connFrac, overloadFrac := connectionPressure(connections, softConnectionsLimit, hardConnectionsLimit)
		cpuFrac := sampler.sample(cpuLimit)
		score := clamp01(connFrac*0.6 + cpuFrac*0.4 + overloadFrac*0.2)

		EdgeConnectionsFraction.Set(connFrac)
		EdgeConnectionsOverloadFraction.Set(overloadFrac)
		EdgeCPUFraction.Set(cpuFrac)
		EdgeLoadScore.Set(score)
	}

	tick() // first sample establishes the CPU baseline; connections term is meaningful immediately
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick()
		}
	}
}

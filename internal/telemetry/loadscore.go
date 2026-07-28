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
//	(active_connections / connections_limit) * 0.6 + cpu_fraction * 0.4
//
// both terms clamped to [0, 1], computed application-side (see design doc,
// "HPA sui nodi edge" and risk #5) so the custom-metrics adapter only ever
// has to relay a single already-aggregated number, not compute a formula
// itself. Only meaningful on a node actually running the broker
// (edge/combined/standalone) — a pure core node never starts the sampler.
var EdgeLoadScore = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: "keel_gateway",
	Name:      "edge_load_score",
	Help:      "Composite HPA load score for this edge pod: (active_connections/limit)*0.6 + cpu_fraction*0.4, both terms clamped to [0,1].",
})

// EdgeConnectionsFraction and EdgeCPUFraction are the two terms behind
// EdgeLoadScore, exposed individually for dashboards/debugging — the HPA
// itself should only ever reference the composite.
var (
	EdgeConnectionsFraction = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "keel_gateway",
		Name:      "edge_load_connections_fraction",
		Help:      "active_connections / connections_limit for this pod, clamped to [0,1] — the connections term of edge_load_score.",
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

// RunEdgeLoadScoreSampler periodically recomputes EdgeLoadScore (and its two
// component gauges) until ctx is done. connCount is polled fresh on every
// tick — for the real broker this is mqtt.Server.Info.ClientsConnected
// (already tracked natively by mochi-mqtt, see server.go). connectionsLimit
// and cpuLimit are the per-pod capacity figures (matching the K8s
// Deployment's expected connections-per-pod threshold and CPU
// request/limit, respectively) that the two terms are normalised against.
func RunEdgeLoadScoreSampler(ctx context.Context, connCount func() int, connectionsLimit int, cpuLimit float64, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	sampler := &cpuSampler{}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	tick := func() {
		connFrac := 0.0
		if connectionsLimit > 0 {
			connFrac = clamp01(float64(connCount()) / float64(connectionsLimit))
		}
		cpuFrac := sampler.sample(cpuLimit)
		score := connFrac*0.6 + cpuFrac*0.4

		EdgeConnectionsFraction.Set(connFrac)
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

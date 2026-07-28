package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// report is the structured output written to stdout and optionally to
// -report-json / -report-csv. It intentionally captures percentiles, not
// just averages, per the task's requirement.
type report struct {
	Scenario  string    `json:"scenario"`
	StartedAt time.Time `json:"started_at"`
	Duration  string    `json:"duration"`

	DeviceCount int `json:"device_count"`
	BrokerCount int `json:"broker_count"`

	ConnectSuccesses int     `json:"connect_successes"`
	ConnectFailures  int     `json:"connect_failures"`
	ConnectP50Ms     float64 `json:"connect_p50_ms"`
	ConnectP95Ms     float64 `json:"connect_p95_ms"`
	ConnectP99Ms     float64 `json:"connect_p99_ms"`

	Published     int     `json:"published"`
	PublishErrors int     `json:"publish_errors"`
	Delivered     int     `json:"delivered"`
	Lost          int     `json:"lost"`
	LossPercent   float64 `json:"loss_percent"`

	LatencyP50Ms  float64 `json:"latency_p50_ms"`
	LatencyP95Ms  float64 `json:"latency_p95_ms"`
	LatencyP99Ms  float64 `json:"latency_p99_ms"`
	LatencyMeanMs float64 `json:"latency_mean_ms"`

	// Reconnect storm fields (zero when the scenario wasn't run).
	StormDisconnected      int     `json:"storm_disconnected,omitempty"`
	StormReconnected       int     `json:"storm_reconnected,omitempty"`
	StormReconnectFailed   int     `json:"storm_reconnect_failed,omitempty"`
	StormSuccessPercent    float64 `json:"storm_success_percent,omitempty"`
	StormReconnectP50Ms    float64 `json:"storm_reconnect_p50_ms,omitempty"`
	StormReconnectP95Ms    float64 `json:"storm_reconnect_p95_ms,omitempty"`
	StormReconnectP99Ms    float64 `json:"storm_reconnect_p99_ms,omitempty"`
	StormReconnectWindowMs float64 `json:"storm_reconnect_window_ms,omitempty"`

	// Subscription churn fields (zero when the scenario wasn't run).
	ChurnCycles            int     `json:"churn_cycles,omitempty"`
	ChurnErrors            int     `json:"churn_errors,omitempty"`
	ConvergenceP50Ms       float64 `json:"convergence_p50_ms,omitempty"`
	ConvergenceP95Ms       float64 `json:"convergence_p95_ms,omitempty"`
	ConvergenceP99Ms       float64 `json:"convergence_p99_ms,omitempty"`
	ConvergenceSampleCount int     `json:"convergence_sample_count,omitempty"`

	DockerStats                  []statsSummary `json:"docker_stats,omitempty"`
	DockerStatsUnavailableReason string         `json:"docker_stats_unavailable_reason,omitempty"`

	Notes []string `json:"notes,omitempty"`
}

func buildReport(cfg *config, mx *metrics, started time.Time, dockerSummaries []statsSummary, dockerOK bool, dockerErr string, notes []string) *report {
	published, publishErrors, delivered, lost, latencies := mx.messageSnapshot()

	mx.mu.Lock()
	defer mx.mu.Unlock()

	p50, p95, p99 := percentiles(latencies)
	cp50, cp95, cp99 := percentiles(mx.connectLatencies)
	sp50, sp95, sp99 := percentiles(mx.stormReconnectLatencies)
	convp50, convp95, convp99 := percentiles(mx.convergenceLatencies)

	totalOutcomes := delivered + lost
	lossPct := 0.0
	if totalOutcomes > 0 {
		lossPct = 100 * float64(lost) / float64(totalOutcomes)
	}

	stormTotal := mx.stormReconnected + mx.stormReconnectFailed
	stormSuccessPct := 0.0
	if stormTotal > 0 {
		stormSuccessPct = 100 * float64(mx.stormReconnected) / float64(stormTotal)
	}

	r := &report{
		Scenario:    cfg.scenario,
		StartedAt:   started,
		Duration:    time.Since(started).Round(time.Millisecond).String(),
		DeviceCount: cfg.deviceCount,
		BrokerCount: len(cfg.brokers),

		ConnectSuccesses: mx.connectSuccesses,
		ConnectFailures:  mx.connectFailures,
		ConnectP50Ms:     ms(cp50),
		ConnectP95Ms:     ms(cp95),
		ConnectP99Ms:     ms(cp99),

		Published:     published,
		PublishErrors: publishErrors,
		Delivered:     delivered,
		Lost:          lost,
		LossPercent:   lossPct,

		LatencyP50Ms:  ms(p50),
		LatencyP95Ms:  ms(p95),
		LatencyP99Ms:  ms(p99),
		LatencyMeanMs: ms(mean(latencies)),

		DockerStats: dockerSummaries,
		Notes:       notes,
	}
	if !dockerOK {
		r.DockerStatsUnavailableReason = dockerErr
	}

	if cfg.stormEnabled {
		r.StormDisconnected = mx.stormDisconnected
		r.StormReconnected = mx.stormReconnected
		r.StormReconnectFailed = mx.stormReconnectFailed
		r.StormSuccessPercent = stormSuccessPct
		r.StormReconnectP50Ms = ms(sp50)
		r.StormReconnectP95Ms = ms(sp95)
		r.StormReconnectP99Ms = ms(sp99)
		r.StormReconnectWindowMs = ms(cfg.stormReconnectWin)
	}
	if cfg.churnEnabled {
		r.ChurnCycles = mx.churnCycles
		r.ChurnErrors = mx.churnErrors
		r.ConvergenceP50Ms = ms(convp50)
		r.ConvergenceP95Ms = ms(convp95)
		r.ConvergenceP99Ms = ms(convp99)
		r.ConvergenceSampleCount = len(mx.convergenceLatencies)
	}
	return r
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func (r *report) printText() {
	fmt.Printf("\n==================== devicesim report: %s ====================\n", r.Scenario)
	fmt.Printf("started:        %s\n", r.StartedAt.Format(time.RFC3339))
	fmt.Printf("duration:       %s\n", r.Duration)
	fmt.Printf("devices:        %d across %d broker(s)\n", r.DeviceCount, r.BrokerCount)
	fmt.Printf("\n-- connections --\n")
	fmt.Printf("success/fail:   %d / %d\n", r.ConnectSuccesses, r.ConnectFailures)
	fmt.Printf("connect p50/p95/p99: %.1f / %.1f / %.1f ms\n", r.ConnectP50Ms, r.ConnectP95Ms, r.ConnectP99Ms)
	fmt.Printf("\n-- publish / delivery --\n")
	fmt.Printf("published:      %d (errors: %d)\n", r.Published, r.PublishErrors)
	fmt.Printf("delivered/lost: %d / %d (loss %.2f%%)\n", r.Delivered, r.Lost, r.LossPercent)
	fmt.Printf("e2e latency p50/p95/p99/mean: %.1f / %.1f / %.1f / %.1f ms\n", r.LatencyP50Ms, r.LatencyP95Ms, r.LatencyP99Ms, r.LatencyMeanMs)

	if r.Scenario == "storm" || r.StormDisconnected > 0 {
		fmt.Printf("\n-- reconnect storm --\n")
		fmt.Printf("disconnected:   %d\n", r.StormDisconnected)
		fmt.Printf("reconnected/failed within %.0fms window: %d / %d (success %.2f%%)\n",
			r.StormReconnectWindowMs, r.StormReconnected, r.StormReconnectFailed, r.StormSuccessPercent)
		fmt.Printf("reconnect p50/p95/p99: %.1f / %.1f / %.1f ms\n", r.StormReconnectP50Ms, r.StormReconnectP95Ms, r.StormReconnectP99Ms)
	}

	if r.Scenario == "churn" || r.ChurnCycles > 0 {
		fmt.Printf("\n-- subscription churn --\n")
		fmt.Printf("cycles:         %d (errors: %d)\n", r.ChurnCycles, r.ChurnErrors)
		fmt.Printf("routing convergence p50/p95/p99 (n=%d): %.1f / %.1f / %.1f ms\n",
			r.ConvergenceSampleCount, r.ConvergenceP50Ms, r.ConvergenceP95Ms, r.ConvergenceP99Ms)
	}

	if len(r.DockerStats) > 0 {
		fmt.Printf("\n-- docker stats (cpu%%, mem MB) --\n")
		for _, s := range r.DockerStats {
			fmt.Printf("%-40s cpu[min/max]=%.1f/%.1f%%  mem[start/end/max]=%.1f/%.1f/%.1f MB (n=%d)\n",
				s.Container, s.CPUMinPct, s.CPUMaxPct, s.MemStartMB, s.MemEndMB, s.MemMaxMB, s.SampleCount)
		}
	} else if r.DockerStatsUnavailableReason != "" {
		fmt.Printf("\ndocker stats unavailable: %s\n", r.DockerStatsUnavailableReason)
	}

	if len(r.Notes) > 0 {
		fmt.Printf("\n-- notes --\n")
		for _, n := range r.Notes {
			fmt.Printf("- %s\n", n)
		}
	}
	fmt.Println("=====================================================================")
}

func (r *report) writeJSON(path string) error {
	if path == "" {
		return nil
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func (r *report) appendCSV(path string) error {
	if path == "" {
		return nil
	}
	header := []string{
		"scenario", "started_at", "duration", "device_count",
		"connect_successes", "connect_failures", "connect_p50_ms", "connect_p95_ms", "connect_p99_ms",
		"published", "publish_errors", "delivered", "lost", "loss_percent",
		"latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "latency_mean_ms",
		"storm_success_percent", "storm_reconnect_p95_ms",
		"churn_cycles", "convergence_p95_ms",
	}
	row := []string{
		r.Scenario, r.StartedAt.Format(time.RFC3339), r.Duration, itoa(r.DeviceCount),
		itoa(r.ConnectSuccesses), itoa(r.ConnectFailures), f2(r.ConnectP50Ms), f2(r.ConnectP95Ms), f2(r.ConnectP99Ms),
		itoa(r.Published), itoa(r.PublishErrors), itoa(r.Delivered), itoa(r.Lost), f2(r.LossPercent),
		f2(r.LatencyP50Ms), f2(r.LatencyP95Ms), f2(r.LatencyP99Ms), f2(r.LatencyMeanMs),
		f2(r.StormSuccessPercent), f2(r.StormReconnectP95Ms),
		itoa(r.ChurnCycles), f2(r.ConvergenceP95Ms),
	}

	writeHeader := false
	if _, err := os.Stat(path); err != nil {
		writeHeader = true
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if writeHeader {
		if err := w.Write(header); err != nil {
			return err
		}
	}
	return w.Write(row)
}

func itoa(i int) string   { return fmt.Sprintf("%d", i) }
func f2(f float64) string { return fmt.Sprintf("%.2f", f) }

package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

// runScenario is the entry point for "devicesim run". It builds the device
// population, connects it (optionally rate-limited), runs steady-state
// publishing, then runs whichever scenario is enabled (storm xor churn —
// running both in one invocation would conflate their effects on the
// report, so pick one at a time), and finally prints/writes the report.
func runScenario(args []string) {
	cfg := parseConfig(args)

	deviceIDs, err := loadOrGenerateDeviceIDs(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		os.Exit(1)
	}
	cfg.deviceCount = len(deviceIDs)

	mx := newMetrics()
	var notes []string

	fmt.Printf("devicesim: scenario=%s devices=%d brokers=%v\n", cfg.scenario, cfg.deviceCount, cfg.brokers)

	// Sanity check: confirm the cluster is reachable before spending time
	// connecting thousands of devices against a cluster that isn't up.
	if len(cfg.mgmtAddrs) > 0 {
		mc := newMgmtClient(cfg.mgmtAddrs[0])
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		nodes, err := mc.nodes(ctx)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "run: cluster not reachable at %s: %v\n", cfg.mgmtAddrs[0], err)
			os.Exit(1)
		}
		fmt.Printf("devicesim: cluster reachable, %d node(s) reported by %s\n", len(nodes), cfg.mgmtAddrs[0])
	}

	// Monitors: one wildcard subscriber per broker, used to measure
	// end-to-end latency/loss regardless of which node a device published
	// through (this is exactly the cross-node path test/e2e validates).
	var monitors []*monitor
	if cfg.monitorPerNode {
		for _, b := range cfg.brokers {
			m := newMonitor(b, mx)
			if err := m.start(); err != nil {
				fmt.Fprintf(os.Stderr, "run: monitor on %s failed to start: %v\n", b, err)
				os.Exit(1)
			}
			monitors = append(monitors, m)
		}
		// Give monitor SUBACKs a moment to land before devices start
		// publishing, mirroring the e2e test's post-subscribe margin.
		time.Sleep(500 * time.Millisecond)
	}
	defer func() {
		for _, m := range monitors {
			m.stop()
		}
	}()

	// Loss sweeper: runs for the whole scenario so "lost" is a live number
	// in intermediate output, not just a final cleanup step.
	sweepStop := make(chan struct{})
	var sweepWG sync.WaitGroup
	sweepWG.Add(1)
	go func() {
		defer sweepWG.Done()
		t := time.NewTicker(cfg.deliveryTimeout / 2)
		defer t.Stop()
		for {
			select {
			case <-sweepStop:
				return
			case <-t.C:
				mx.sweepLost(cfg.deliveryTimeout)
			}
		}
	}()

	dockerSampler := newDockerStatsSampler(cfg.dockerContainers, cfg.dockerStatsInterval)
	if cfg.dockerStats {
		dockerSampler.start()
	}

	started := time.Now()

	devices := buildDevices(deviceIDs, cfg, mx)
	connectDevices(devices, cfg, mx)

	fmt.Printf("devicesim: connected, starting steady-state publish for %s\n", cfg.steadyDuration)
	for _, d := range devices {
		d.runPublishLoop()
	}

	switch {
	case cfg.stormEnabled:
		time.Sleep(cfg.stormAfter)
		runStormScenario(devices, cfg, mx, &notes)
		remaining := cfg.steadyDuration - cfg.stormAfter - cfg.stormReconnectWin
		if remaining > 0 {
			time.Sleep(remaining)
		}
	case cfg.churnEnabled:
		time.Sleep(cfg.churnAfter)
		runChurnScenario(cfg, mx, &notes)
		remaining := cfg.steadyDuration - cfg.churnAfter - cfg.churnDuration
		if remaining > 0 {
			time.Sleep(remaining)
		}
	default:
		time.Sleep(cfg.steadyDuration)
	}

	fmt.Println("devicesim: stopping devices...")
	for _, d := range devices {
		d.stop()
	}

	// Drain window: give in-flight publishes up to deliveryTimeout to
	// arrive before the final loss sweep, then sweep once more.
	time.Sleep(cfg.deliveryTimeout)
	mx.sweepLost(cfg.deliveryTimeout)
	close(sweepStop)
	sweepWG.Wait()

	if cfg.dockerStats {
		dockerSampler.stop()
	}
	dockerSummaries, dockerOK, dockerErr := dockerSampler.summaries()
	if cfg.dockerStatsCSV != "" {
		if err := dockerSampler.writeSeriesCSV(cfg.dockerStatsCSV); err != nil {
			fmt.Fprintf(os.Stderr, "run: write docker stats CSV: %v\n", err)
		} else {
			fmt.Printf("devicesim: docker stats time series written to %s\n", cfg.dockerStatsCSV)
		}
	}

	r := buildReport(cfg, mx, started, dockerSummaries, dockerOK, dockerErr, notes)
	r.printText()
	if err := r.writeJSON(cfg.reportJSON); err != nil {
		fmt.Fprintf(os.Stderr, "run: write JSON report: %v\n", err)
	}
	if err := r.appendCSV(cfg.reportCSV); err != nil {
		fmt.Fprintf(os.Stderr, "run: append CSV report: %v\n", err)
	}
}

func loadOrGenerateDeviceIDs(cfg *config) ([]string, error) {
	if cfg.deviceIDsFile != "" {
		data, err := os.ReadFile(cfg.deviceIDsFile)
		if err != nil {
			return nil, fmt.Errorf("read -device-ids-file: %w", err)
		}
		var ids []string
		for _, line := range splitLines(string(data)) {
			if line != "" {
				ids = append(ids, line)
			}
		}
		if len(ids) == 0 {
			return nil, fmt.Errorf("-device-ids-file %s contained no device IDs", cfg.deviceIDsFile)
		}
		return ids, nil
	}
	return nil, fmt.Errorf("-device-ids-file is required (generate one with 'devicesim gen-credentials -ids-out <file>' and load the matching credentials.yaml into the cluster)")
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			line := s[start:i]
			line = trimCR(line)
			out = append(out, line)
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, trimCR(s[start:]))
	}
	return out
}

func trimCR(s string) string {
	if len(s) > 0 && s[len(s)-1] == '\r' {
		return s[:len(s)-1]
	}
	return s
}

func buildDevices(ids []string, cfg *config, mx *metrics) []*simDevice {
	devices := make([]*simDevice, len(ids))
	for i, id := range ids {
		broker := cfg.brokers[i%len(cfg.brokers)] // round-robin distribution across brokers
		devices[i] = newSimDevice(id, cfg.tenantID, broker, cfg, mx)
	}
	return devices
}

// connectDevices connects every device, either all at once (connectRatePerS
// == 0) or staggered at the configured rate. Connections happen
// concurrently within each "wave" — real device fleets don't serialize
// TCP handshakes, and the whole point of a rate is to shape arrival load
// on the cluster, not to also throttle the simulator's own concurrency.
func connectDevices(devices []*simDevice, cfg *config, mx *metrics) {
	if cfg.connectRatePerS <= 0 {
		var wg sync.WaitGroup
		for _, d := range devices {
			wg.Add(1)
			go func(d *simDevice) {
				defer wg.Done()
				_ = d.connect(mx.recordConnect)
			}(d)
		}
		wg.Wait()
		return
	}

	waveSize := cfg.connectRatePerS
	for start := 0; start < len(devices); start += waveSize {
		end := start + waveSize
		if end > len(devices) {
			end = len(devices)
		}
		var wg sync.WaitGroup
		for _, d := range devices[start:end] {
			wg.Add(1)
			go func(d *simDevice) {
				defer wg.Done()
				_ = d.connect(mx.recordConnect)
			}(d)
		}
		wg.Wait()
		if end < len(devices) {
			time.Sleep(time.Second)
		}
	}
}

// runStormScenario disconnects stormPercent of devices simultaneously, then
// reconnects them all concurrently and measures how many succeed within
// stormReconnectWin.
func runStormScenario(devices []*simDevice, cfg *config, mx *metrics, notes *[]string) {
	n := int(cfg.stormPercent * float64(len(devices)))
	if n <= 0 {
		*notes = append(*notes, "storm-percent resulted in 0 devices selected; scenario skipped")
		return
	}
	perm := rand.Perm(len(devices))
	selected := make([]*simDevice, n)
	for i := 0; i < n; i++ {
		selected[i] = devices[perm[i]]
	}

	fmt.Printf("devicesim: reconnect storm — disconnecting %d/%d devices\n", n, len(devices))
	var wg sync.WaitGroup
	for _, d := range selected {
		wg.Add(1)
		go func(d *simDevice) {
			defer wg.Done()
			d.disconnect()
		}(d)
	}
	wg.Wait()

	fmt.Printf("devicesim: reconnect storm — reconnecting within %s\n", cfg.stormReconnectWin)
	deadline := time.Now().Add(cfg.stormReconnectWin)
	var rwg sync.WaitGroup
	for _, d := range selected {
		rwg.Add(1)
		go func(d *simDevice) {
			defer rwg.Done()
			remaining := time.Until(deadline)
			if remaining <= 0 {
				mx.recordStormReconnect(0, fmt.Errorf("reconnect window already elapsed"))
				return
			}
			done := make(chan error, 1)
			start := time.Now()
			go func() { done <- d.connect(func(time.Duration, error) {}) }()
			select {
			case err := <-done:
				mx.recordStormReconnect(time.Since(start), err)
			case <-time.After(remaining):
				mx.recordStormReconnect(time.Since(start), fmt.Errorf("did not reconnect within window"))
			}
		}(d)
	}
	rwg.Wait()
}

// runChurnScenario spins up cfg.churnConcurrency dedicated "test-consumer"
// role clients (see churner.go) and has each run a subscribe/unsubscribe
// loop for churnDuration, splitting the aggregate churnRateHz evenly across
// them. This deliberately does NOT reuse device clients: ordinary devices'
// ACL only permits subscribing to "command/<deviceID>" topics, so they
// cannot generate the telemetry-routing churn this scenario needs.
func runChurnScenario(cfg *config, mx *metrics, notes *[]string) {
	n := cfg.churnConcurrency
	if n <= 0 {
		*notes = append(*notes, "churn-concurrency is 0; scenario skipped")
		return
	}
	perClientRate := cfg.churnRateHz / float64(n)

	fmt.Printf("devicesim: subscription churn — %d churner client(s), %.2f cycles/sec each, for %s\n",
		n, perClientRate, cfg.churnDuration)

	var mgmtClients []*mgmtClient
	for _, addr := range cfg.mgmtAddrs {
		mgmtClients = append(mgmtClients, newMgmtClient(addr))
	}

	churners := make([]*churner, n)
	for i := 0; i < n; i++ {
		broker := cfg.brokers[i%len(cfg.brokers)]
		churners[i] = newChurner(broker)
		if err := churners[i].connect(); err != nil {
			*notes = append(*notes, fmt.Sprintf("churner %d failed to connect: %v", i, err))
			churners[i] = nil
		}
	}
	defer func() {
		for _, ch := range churners {
			if ch != nil {
				ch.disconnect()
			}
		}
	}()

	var wg sync.WaitGroup
	for i, ch := range churners {
		if ch == nil {
			continue
		}
		selfMgmtIdx := i % max(len(mgmtClients), 1)
		wg.Add(1)
		go func(ch *churner, selfMgmtIdx int) {
			defer wg.Done()
			ch.loop(perClientRate, cfg.churnDuration, cfg.churnTopicBase, mx, mgmtClients, selfMgmtIdx)
		}(ch, selfMgmtIdx)
	}
	wg.Wait()
}

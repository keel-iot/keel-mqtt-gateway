package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// dockerStatsSampler periodically shells out to `docker stats --no-stream`
// for a fixed set of container names and keeps a time series per
// container. This is the simplest possible way to get CPU/mem numbers
// without adding a Docker SDK dependency to a throwaway load-test tool —
// acceptable per the task's "simple and correct over optimized" guidance.
// If `docker` isn't on PATH or the containers aren't running, sampling is
// silently skipped (recorded once as a warning) so the rest of the run
// still produces a report.
type dockerStatsSampler struct {
	containers []string
	interval   time.Duration

	mu                sync.Mutex
	series            map[string][]statSample // container -> samples over time
	unavailable       bool
	unavailableReason string

	stopCh chan struct{}
	wg     sync.WaitGroup
}

type statSample struct {
	at        time.Time
	cpuPct    float64
	memUsedMB float64
}

func newDockerStatsSampler(containers []string, interval time.Duration) *dockerStatsSampler {
	return &dockerStatsSampler{
		containers: containers,
		interval:   interval,
		series:     make(map[string][]statSample),
		stopCh:     make(chan struct{}),
	}
}

func (s *dockerStatsSampler) start() {
	if len(s.containers) == 0 {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		s.sampleOnce() // sample immediately for a baseline point
		for {
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				s.sampleOnce()
			}
		}
	}()
}

func (s *dockerStatsSampler) stop() {
	close(s.stopCh)
	s.wg.Wait()
}

func (s *dockerStatsSampler) sampleOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	args := append([]string{"stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"}, s.containers...)
	cmd := exec.CommandContext(ctx, "docker", args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		s.mu.Lock()
		s.unavailable = true
		s.unavailableReason = err.Error()
		s.mu.Unlock()
		return
	}

	now := time.Now()
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 3 {
			continue
		}
		name := fields[0]
		cpuPct := parsePercent(fields[1])
		memMB := parseMemMB(fields[2])
		s.mu.Lock()
		s.series[name] = append(s.series[name], statSample{at: now, cpuPct: cpuPct, memUsedMB: memMB})
		s.mu.Unlock()
	}
}

func parsePercent(s string) float64 {
	s = strings.TrimSuffix(strings.TrimSpace(s), "%")
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

// parseMemMB parses docker's "12.34MiB / 1.9GiB" MemUsage format and
// returns the used side in MB (decimal, for readability — precision to a
// fraction of a MB doesn't matter for this purpose).
func parseMemMB(s string) float64 {
	usedPart := strings.SplitN(s, "/", 2)[0]
	usedPart = strings.TrimSpace(usedPart)
	var numStr, unit string
	for i, r := range usedPart {
		if !(r >= '0' && r <= '9' || r == '.') {
			numStr = usedPart[:i]
			unit = usedPart[i:]
			break
		}
	}
	if numStr == "" {
		numStr = usedPart
	}
	v, _ := strconv.ParseFloat(numStr, 64)
	switch strings.ToLower(unit) {
	case "gib":
		return v * 1024
	case "kib":
		return v / 1024
	default: // MiB or unrecognized
		return v
	}
}

// summary returns, per container, the min/max/last CPU% and memory MB seen
// — enough to answer "did memory grow non-linearly during the run" without
// dumping the entire time series into the report.
type statsSummary struct {
	Container   string  `json:"container"`
	SampleCount int     `json:"sample_count"`
	CPUMinPct   float64 `json:"cpu_min_pct"`
	CPUMaxPct   float64 `json:"cpu_max_pct"`
	MemStartMB  float64 `json:"mem_start_mb"`
	MemEndMB    float64 `json:"mem_end_mb"`
	MemMaxMB    float64 `json:"mem_max_mb"`
}

// writeSeriesCSV writes every sample taken during the run (not just the
// start/end/max aggregate) so the memory curve can be plotted or inspected
// point-by-point — this is what distinguishes "plateau after N minutes"
// from "still climbing when the run ended" more reliably than any single
// aggregate number can.
func (s *dockerStatsSampler) writeSeriesCSV(path string) error {
	if path == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{"container", "elapsed_s", "cpu_pct", "mem_mb"}); err != nil {
		return err
	}

	// Find the earliest sample across all containers to use as t=0, so
	// series from containers sampled in the same pass line up.
	var t0 time.Time
	for _, samples := range s.series {
		if len(samples) == 0 {
			continue
		}
		if t0.IsZero() || samples[0].at.Before(t0) {
			t0 = samples[0].at
		}
	}

	for _, name := range s.containers {
		for _, sm := range s.series[name] {
			elapsed := sm.at.Sub(t0).Seconds()
			row := []string{
				name,
				strconv.FormatFloat(elapsed, 'f', 2, 64),
				strconv.FormatFloat(sm.cpuPct, 'f', 2, 64),
				strconv.FormatFloat(sm.memUsedMB, 'f', 2, 64),
			}
			if err := w.Write(row); err != nil {
				return err
			}
		}
	}
	return w.Error()
}

func (s *dockerStatsSampler) summaries() ([]statsSummary, bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unavailable && len(s.series) == 0 {
		return nil, false, s.unavailableReason
	}
	var out []statsSummary
	for _, name := range s.containers {
		samples := s.series[name]
		if len(samples) == 0 {
			continue
		}
		sum := statsSummary{
			Container:   name,
			SampleCount: len(samples),
			CPUMinPct:   samples[0].cpuPct,
			CPUMaxPct:   samples[0].cpuPct,
			MemStartMB:  samples[0].memUsedMB,
			MemEndMB:    samples[len(samples)-1].memUsedMB,
			MemMaxMB:    samples[0].memUsedMB,
		}
		for _, sm := range samples {
			if sm.cpuPct < sum.CPUMinPct {
				sum.CPUMinPct = sm.cpuPct
			}
			if sm.cpuPct > sum.CPUMaxPct {
				sum.CPUMaxPct = sm.cpuPct
			}
			if sm.memUsedMB > sum.MemMaxMB {
				sum.MemMaxMB = sm.memUsedMB
			}
		}
		out = append(out, sum)
	}
	return out, true, ""
}

package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// config holds every flag for "devicesim run". Kept as one flat struct
// (rather than per-scenario structs) because most fields are shared across
// scenarios and operators will want to vary one or two flags at a time
// against the docker-compose defaults.
type config struct {
	// Target cluster.
	brokers    []string // MQTT broker URLs, one per edge/core node to round-robin devices over
	mgmtAddrs  []string // management HTTP API base URLs, one per core node (for routing snapshot + node list)
	tenantID   string
	tenantSlug string

	// Device population.
	deviceCount     int
	deviceIDsFile   string // optional: file with pre-generated device UUIDs (one per line), from gen-credentials -ids-out
	devicePassword  string
	connectRatePerS int  // 0 = connect all devices simultaneously
	monitorPerNode  bool // one wildcard "test-consumer" subscriber per mgmt/broker pair, for latency/loss measurement

	// Publish load.
	publishRateHz  float64 // messages/sec per device, 0 disables steady publishing
	payloadBytes   int
	steadyDuration time.Duration

	// Reconnect storm scenario.
	stormEnabled      bool
	stormAfter        time.Duration // how long to run steady-state before triggering the storm
	stormPercent      float64       // fraction of devices to disconnect, 0..1
	stormReconnectWin time.Duration // window within which disconnected devices must reconnect

	// Subscription churn scenario. Churn is driven by dedicated
	// "test-consumer" role clients (see churner.go), not by device
	// clients: ordinary devices' ACL only permits subscribing to
	// "command/<deviceID>" patterns, so they cannot generate the
	// telemetry-routing churn this scenario needs to stress Olric.
	churnEnabled     bool
	churnAfter       time.Duration
	churnDuration    time.Duration
	churnConcurrency int     // number of concurrent churner clients
	churnRateHz      float64 // aggregate subscribe+unsubscribe cycles/sec across all churner clients
	churnTopicBase   string  // literal topic segment: "telemetry/<base>/<random>"

	// Reporting.
	scenario            string        // "steady", "storm", "churn" — label for the report only
	reportJSON          string        // optional path to write a JSON report
	reportCSV           string        // optional path to write per-scenario summary CSV row (appended)
	deliveryTimeout     time.Duration // how long to wait for a published message before counting it lost
	dockerStats         bool
	dockerContainers    []string
	dockerStatsInterval time.Duration // how often to sample `docker stats`; shorter = finer-grained memory curve for plateau-vs-leak analysis
	dockerStatsCSV      string        // optional path to write the raw per-sample memory/CPU time series (not just start/end/max)
}

func parseConfig(args []string) *config {
	fs := flag.NewFlagSet("run", flag.ExitOnError)

	var brokers, mgmts, containers string
	c := &config{}

	fs.StringVar(&brokers, "brokers", "tcp://localhost:11883,tcp://localhost:21883,tcp://localhost:31883", "comma-separated MQTT broker URLs; devices round-robin across them")
	fs.StringVar(&mgmts, "mgmt-addrs", "http://localhost:18090,http://localhost:28090,http://localhost:38090", "comma-separated management API base URLs, one per core node")
	fs.StringVar(&c.tenantID, "tenant-id", "22222222-2222-2222-2222-222222222222", "tenant UUID shared by simulated devices (must match gen-credentials)")
	fs.StringVar(&c.tenantSlug, "tenant-slug", "devicesim", "tenant slug (informational, not sent over the wire)")

	fs.IntVar(&c.deviceCount, "devices", 200, "number of simulated devices (ignored if -device-ids-file is set; count derived from file)")
	fs.StringVar(&c.deviceIDsFile, "device-ids-file", "", "file with one device UUID per line (from gen-credentials -ids-out); if unset, UUIDs are generated in-memory and will NOT authenticate unless -device-count matches a credentials.yaml generated with the same seed — normally you should always set this")
	fs.StringVar(&c.devicePassword, "device-password", fixedPassword, "shared device password (must match the credentials file's bcrypt hash)")
	fs.IntVar(&c.connectRatePerS, "connect-rate", 0, "devices connected per second at startup; 0 = all at once")
	fs.BoolVar(&c.monitorPerNode, "monitor-per-node", true, "run one wildcard test-consumer subscriber per broker to measure end-to-end latency/loss")

	fs.Float64Var(&c.publishRateHz, "publish-rate", 0.2, "publish rate per device, messages/sec")
	fs.IntVar(&c.payloadBytes, "payload-bytes", 128, "publish payload size in bytes (a timestamp header is embedded regardless of size)")
	fs.DurationVar(&c.steadyDuration, "steady-duration", 60*time.Second, "how long to run steady-state publishing before scenario-specific actions (or total run length if no scenario is enabled)")

	fs.BoolVar(&c.stormEnabled, "storm", false, "enable the reconnect-storm scenario")
	fs.DurationVar(&c.stormAfter, "storm-after", 30*time.Second, "delay after steady-state starts before triggering the storm")
	fs.Float64Var(&c.stormPercent, "storm-percent", 1.0, "fraction of devices to disconnect simultaneously, 0..1")
	fs.DurationVar(&c.stormReconnectWin, "storm-reconnect-window", 10*time.Second, "window within which disconnected devices must reconnect")

	fs.BoolVar(&c.churnEnabled, "churn", false, "enable the subscription churn scenario")
	fs.DurationVar(&c.churnAfter, "churn-after", 10*time.Second, "delay after steady-state starts before beginning churn")
	fs.DurationVar(&c.churnDuration, "churn-duration", 30*time.Second, "how long to run the churn pattern")
	fs.IntVar(&c.churnConcurrency, "churn-concurrency", 20, "number of dedicated test-consumer-role clients driving subscribe/unsubscribe churn")
	fs.Float64Var(&c.churnRateHz, "churn-rate", 50, "aggregate subscribe+unsubscribe cycles/sec across all churner clients")
	fs.StringVar(&c.churnTopicBase, "churn-topic-base", "churn", "literal topic segment for churned subscriptions: telemetry/<base>/<random>")

	fs.StringVar(&c.scenario, "scenario-name", "", "label for the report; defaults to steady/storm/churn based on enabled flags")
	fs.StringVar(&c.reportJSON, "report-json", "", "optional path to write the full JSON report")
	fs.StringVar(&c.reportCSV, "report-csv", "", "optional path to append a one-line CSV summary (creates header if file doesn't exist)")
	fs.DurationVar(&c.deliveryTimeout, "delivery-timeout", 5*time.Second, "max wait for a published message to be observed by a monitor before it counts as lost")
	fs.BoolVar(&c.dockerStats, "docker-stats", true, "sample `docker stats` for the containers in -docker-containers during the run")
	fs.StringVar(&containers, "docker-containers", "keel-mqtt-cluster-poc-core-1-1,keel-mqtt-cluster-poc-core-2-1,keel-mqtt-cluster-poc-core-3-1", "comma-separated docker container names to sample stats from")
	fs.DurationVar(&c.dockerStatsInterval, "docker-stats-interval", 5*time.Second, "how often to sample docker stats; use a shorter interval (e.g. 2s) for a finer memory curve when distinguishing a plateau from unbounded growth")
	fs.StringVar(&c.dockerStatsCSV, "docker-stats-csv", "", "optional path to write the full per-sample docker stats time series as CSV (container,elapsed_s,cpu_pct,mem_mb), not just the aggregate summary")

	_ = fs.Parse(args)

	c.brokers = splitCSV(brokers)
	c.mgmtAddrs = splitCSV(mgmts)
	c.dockerContainers = splitCSV(containers)

	if c.scenario == "" {
		switch {
		case c.stormEnabled:
			c.scenario = "storm"
		case c.churnEnabled:
			c.scenario = "churn"
		default:
			c.scenario = "steady"
		}
	}

	if len(c.brokers) == 0 {
		fmt.Fprintln(os.Stderr, "run: -brokers must not be empty")
		os.Exit(2)
	}
	return c
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

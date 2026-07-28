// Command devicesim is a standalone load-generation tool for the
// keel-mqtt-cluster docker-compose deployment. It is NOT part of the
// keel-gateway binary and does not import any of its internal packages —
// it only talks to the cluster the same way a real device or the e2e test
// would: real MQTT connections (github.com/eclipse/paho.mqtt.golang) against
// the brokers exposed by docker-compose, plus the read-only management HTTP
// API (internal/cluster/management) for routing-convergence and node-list
// polling.
//
// See test/e2e/cross_node_test.go for the pattern this reuses: password auth
// as "<device-uuid>@<tenant-uuid>", AUTH_BACKEND=file credentials, and the
// "test-consumer" role for wildcard subscription across nodes.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "gen-credentials":
		runGenCredentials(os.Args[2:])
	case "run":
		runScenario(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `devicesim - MQTT device simulator / load generator for keel-mqtt-cluster

Usage:
  devicesim gen-credentials [flags]   Write a credentials.yaml with N simulated devices
  devicesim run [flags]               Run a load scenario against a running cluster

Run "devicesim run -h" or "devicesim gen-credentials -h" for flag details.
`)
}

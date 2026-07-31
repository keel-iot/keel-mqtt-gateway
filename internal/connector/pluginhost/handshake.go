// Package pluginhost bridges connector.OutputConnector to hashicorp/go-plugin,
// so an OutputConnector implementation can run out-of-process as a sidecar
// container instead of in this binary. It never changes the OutputConnector
// contract itself (see internal/connector/proto/forward.proto) — a call site
// that already holds a connector.OutputConnector (e.g. connector.BufferedConnector,
// internal/broker/hooks.go) cannot tell whether it is talking to an in-process
// implementation (kafka-hono) or an attached plugin.
package pluginhost

import "github.com/hashicorp/go-plugin"

// PluginName is the Dispense/Plugins map key both sides of the handshake
// must agree on. One entry regardless of how many distinct plugins are
// deployed — each plugin gets its own sidecar/ReattachConfig (see
// Attach), not its own name here.
const PluginName = "output_connector"

// Handshake must match exactly between broker (host) and plugin binary,
// or go-plugin refuses the connection. ProtocolVersion bump is required
// for any breaking change to forward.proto.
var Handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "KEEL_OUTPUT_CONNECTOR_PLUGIN",
	MagicCookieValue: "6f9d2f6e-3f0a-4e7a-9b1a-1a9f0d2c7b31",
}

// pluginMap is passed to both plugin.NewClient (host side) and plugin.Serve
// (plugin side). Only one entry: today's design is one OutputConnector per
// plugin process (see design doc "N plugin = N sidecar"), not multiple
// named plugins multiplexed over one connection.
func pluginMap(p *GRPCConnectorPlugin) map[string]plugin.Plugin {
	return map[string]plugin.Plugin{
		PluginName: p,
	}
}

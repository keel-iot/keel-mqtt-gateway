package pluginhost

import (
	"fmt"
	"net"

	goplugin "github.com/hashicorp/go-plugin"

	"github.com/keel-iot/keel-mqtt-gateway/internal/connector"
)

// Attach connects to an OutputConnector plugin already running at addr
// (a sidecar container, reachable over TCP inside the pod network — e.g.
// "127.0.0.1:7300", or a unix socket shared via an emptyDir volume). It
// never spawns or owns the plugin process: in Kubernetes the sidecar's
// lifecycle belongs to the pod, not to this broker, so Pid is left unset —
// the returned closer only tears down this side's client connection.
//
// This is the "N plugin = N sidecar" model from the design doc: call
// Attach once per configured plugin endpoint, and add the resulting
// connector.OutputConnector to the OutputManager's connector list (not yet
// wired — see design doc "Meccanismo di plugin").
func Attach(network, addr string) (conn connector.OutputConnector, closer func(), err error) {
	tcpAddr, err := resolveAddr(network, addr)
	if err != nil {
		return nil, nil, fmt.Errorf("pluginhost: resolve %s %s: %w", network, addr, err)
	}

	client := goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig: Handshake,
		Plugins:         pluginMap(&GRPCConnectorPlugin{}),
		Reattach: &goplugin.ReattachConfig{
			Protocol:        goplugin.ProtocolGRPC,
			ProtocolVersion: int(Handshake.ProtocolVersion),
			Addr:            tcpAddr,
		},
		AllowedProtocols: []goplugin.Protocol{goplugin.ProtocolGRPC},
	})

	rpcClient, err := client.Client()
	if err != nil {
		client.Kill()
		return nil, nil, fmt.Errorf("pluginhost: connect to %s: %w", addr, err)
	}

	raw, err := rpcClient.Dispense(PluginName)
	if err != nil {
		client.Kill()
		return nil, nil, fmt.Errorf("pluginhost: dispense %s: %w", PluginName, err)
	}

	impl, ok := raw.(connector.OutputConnector)
	if !ok {
		client.Kill()
		return nil, nil, fmt.Errorf("pluginhost: %s does not implement OutputConnector", addr)
	}

	return impl, client.Kill, nil
}

// Serve runs impl as an OutputConnector plugin server, blocking forever.
// Called from a plugin binary's main() — never from this broker.
func Serve(impl connector.OutputConnector) {
	goplugin.Serve(&goplugin.ServeConfig{
		HandshakeConfig: Handshake,
		Plugins:         pluginMap(&GRPCConnectorPlugin{Impl: impl}),
		GRPCServer:      goplugin.DefaultGRPCServer,
	})
}

func resolveAddr(network, addr string) (net.Addr, error) {
	switch network {
	case "tcp":
		return net.ResolveTCPAddr("tcp", addr)
	case "unix":
		return net.ResolveUnixAddr("unix", addr)
	default:
		return nil, fmt.Errorf("unsupported network %q (want tcp or unix)", network)
	}
}

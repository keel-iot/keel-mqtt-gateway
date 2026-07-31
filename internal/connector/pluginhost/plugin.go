package pluginhost

import (
	"context"

	goplugin "github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"

	"github.com/keel-iot/keel-mqtt-gateway/internal/connector"
)

// GRPCConnectorPlugin implements go-plugin's plugin.GRPCPlugin for a single
// connector.OutputConnector. It is used on both sides of the wire:
//   - plugin side (a standalone sidecar binary): set Impl to the real
//     OutputConnector implementation and call goplugin.Serve — GRPCServer
//     is invoked to register it.
//   - host side (this broker): leave Impl nil and use Attach — GRPCClient
//     is invoked to build a client-side connector.OutputConnector.
type GRPCConnectorPlugin struct {
	goplugin.NetRPCUnsupportedPlugin

	Impl connector.OutputConnector
}

func (p *GRPCConnectorPlugin) GRPCServer(_ *goplugin.GRPCBroker, s *grpc.Server) error {
	connector.RegisterOutputConnectorServer(s, &grpcServer{impl: p.Impl})
	return nil
}

func (p *GRPCConnectorPlugin) GRPCClient(_ context.Context, _ *goplugin.GRPCBroker, conn *grpc.ClientConn) (interface{}, error) {
	return &grpcClient{client: connector.NewOutputConnectorClient(conn)}, nil
}

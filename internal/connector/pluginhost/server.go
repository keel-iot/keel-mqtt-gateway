package pluginhost

import (
	"context"

	"github.com/keel-iot/keel-mqtt-gateway/internal/connector"
)

// grpcServer adapts a connector.OutputConnector (Go interface) to the
// generated connector.OutputConnectorServer (gRPC), so a plugin process
// can serve it over go-plugin. Runs inside the plugin binary, never
// inside this broker.
type grpcServer struct {
	connector.UnimplementedOutputConnectorServer

	impl connector.OutputConnector
}

func (s *grpcServer) Init(ctx context.Context, req *connector.InitRequest) (*connector.Empty, error) {
	if err := s.impl.Init(ctx, req.GetConfig()); err != nil {
		return nil, err
	}
	return &connector.Empty{}, nil
}

func (s *grpcServer) Forward(ctx context.Context, req *connector.ForwardRequest) (*connector.ForwardResponse, error) {
	return s.impl.Forward(ctx, req)
}

func (s *grpcServer) HealthCheck(ctx context.Context, _ *connector.Empty) (*connector.Empty, error) {
	if err := s.impl.HealthCheck(ctx); err != nil {
		return nil, err
	}
	return &connector.Empty{}, nil
}

func (s *grpcServer) Shutdown(ctx context.Context, _ *connector.Empty) (*connector.Empty, error) {
	if err := s.impl.Shutdown(ctx); err != nil {
		return nil, err
	}
	return &connector.Empty{}, nil
}

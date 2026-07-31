package pluginhost

import (
	"context"

	"github.com/keel-iot/keel-mqtt-gateway/internal/connector"
)

// grpcClient adapts the generated connector.OutputConnectorClient (gRPC) to
// connector.OutputConnector (Go interface), so the broker can use an
// attached plugin exactly like any in-process connector — hooks.go and
// connector.BufferedConnector never see the difference.
type grpcClient struct {
	client connector.OutputConnectorClient
}

func (c *grpcClient) Init(ctx context.Context, config map[string]string) error {
	_, err := c.client.Init(ctx, &connector.InitRequest{Config: config})
	return err
}

func (c *grpcClient) Forward(ctx context.Context, req *connector.ForwardRequest) (*connector.ForwardResponse, error) {
	return c.client.Forward(ctx, req)
}

func (c *grpcClient) HealthCheck(ctx context.Context) error {
	_, err := c.client.HealthCheck(ctx, &connector.Empty{})
	return err
}

func (c *grpcClient) Shutdown(ctx context.Context) error {
	_, err := c.client.Shutdown(ctx, &connector.Empty{})
	return err
}

var _ connector.OutputConnector = (*grpcClient)(nil)

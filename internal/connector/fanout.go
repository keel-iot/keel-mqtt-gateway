package connector

import (
	"context"
	"log/slog"
)

// FanOut sends req to every connector in conns — one goroutine per
// connector, so a slow or stuck connector never delays the others or this
// call's return, and a panicking connector (in particular a plugin-backed
// one, out of the caller's control) can never take the calling process
// down. Used by every call site that owns a []OutputConnector (MQTT
// hooks.go, the HTTP adapter) so the isolation guarantee is applied
// uniformly.
//
// Each connector is expected to be non-blocking on its own (see
// BufferedConnector) — the fan-out here is about isolating connectors from
// each other, not about making an individually blocking connector safe.
func FanOut(ctx context.Context, log *slog.Logger, deviceID string, conns []OutputConnector, req *ForwardRequest) {
	for _, conn := range conns {
		conn := conn
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error("output-connector: forward panicked", "device_id", deviceID, "panic", r)
				}
			}()

			resp, err := conn.Forward(ctx, req)
			if err != nil {
				log.Error("output-connector: forward error", "device_id", deviceID, "error", err)
				return
			}
			if !resp.Success {
				log.Warn("output-connector: forward failed", "device_id", deviceID, "error", resp.Error)
			}
		}()
	}
}

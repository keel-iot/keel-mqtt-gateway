package broker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
	"github.com/keel-iot/keel-mqtt-gateway/internal/connector"
	"github.com/keel-iot/keel-mqtt-gateway/internal/forwarder"
	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
)

type quotaCommands struct{ calls atomic.Int64 }

func (*quotaCommands) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *quotaCommands) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error { h.calls.Add(1); return next(ctx, cmd) }
}
func (h *quotaCommands) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.calls.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

func TestPublishWithoutOutputConnectorsSkipsQuotaAndPreservesMetrics(t *testing.T) {
	redisHook, _ := newTestRedisSessionHook(t)
	h := newTestHook(nil)
	h.rdb = redisHook.router
	// A lookup against this cache would touch a nil database pool. No connector
	// means no quota/config lookup should occur at all.
	h.tenantCache = auth.NewTenantConfigCache(nil, time.Minute)
	h.provider = noopAuthProvider{}
	h.clients["device"] = deviceState("device")
	commands := new(quotaCommands)
	h.rdb.Client().AddHook(commands)
	tenant := h.clients["device"].info.TenantID.String()
	bytesMetric := telemetry.BytesPublished.WithLabelValues(tenant)
	before := testutil.ToFloat64(bytesMetric)
	for qos := byte(0); qos <= 2; qos++ {
		pk := packets.Packet{TopicName: "test/telemetry", Payload: []byte("payload"), FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos}}
		out, err := h.OnPublish(&mqtt.Client{ID: "device"}, pk)
		if err != nil || string(out.Payload) != "payload" {
			t.Fatalf("publish failed: %+v, %v", out, err)
		}
	}
	if calls := commands.calls.Load(); calls != 0 {
		t.Fatalf("no-output publishing made %d Redis requests", calls)
	}
	if got := testutil.ToFloat64(bytesMetric) - before; got != 21 {
		t.Fatalf("published bytes metric delta %v, want 21", got)
	}
}

type quotaRecordingConnector struct {
	connector.OutputConnector
	requests chan *connector.ForwardRequest
}

func (c *quotaRecordingConnector) Forward(_ context.Context, req *connector.ForwardRequest) (*connector.ForwardResponse, error) {
	c.requests <- req
	return &connector.ForwardResponse{Success: true}, nil
}

func TestPublishWithOutputConnectorStillEnforcesByteQuota(t *testing.T) {
	redisHook, ctx := newTestRedisSessionHook(t)
	pool, cache := newTestPool(t)
	h := newTestHook(nil)
	h.rdb = redisHook.router
	h.tenantCache = cache
	h.provider = noopAuthProvider{}
	h.clients["device"] = deviceState("device")
	tenant := h.clients["device"].info.TenantID.String()
	_, err := pool.Exec(ctx, `INSERT INTO devices.tenant_gateway_config(tenant_id,max_bytes_per_day)
 VALUES ($1,5) ON CONFLICT (tenant_id) DO UPDATE SET max_bytes_per_day=EXCLUDED.max_bytes_per_day`, tenant)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM devices.tenant_gateway_config WHERE tenant_id=$1", tenant)
	})
	output := &quotaRecordingConnector{requests: make(chan *connector.ForwardRequest, 2)}
	h.outputConnectors = []connector.OutputConnector{output}
	pk := packets.Packet{TopicName: "test/topic", Payload: []byte("abc"), FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}}
	cl := &mqtt.Client{ID: "device"}
	if _, err := h.OnPublish(cl, pk); err != nil {
		t.Fatal(err)
	}
	select {
	case req := <-output.requests:
		if string(req.Payload) != "abc" {
			t.Fatalf("wrong forwarded payload: %q", req.Payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("allowed output was not forwarded")
	}
	// A second 3-byte payload exceeds the 5-byte quota, but MQTT still succeeds.
	if _, err := h.OnPublish(cl, pk); err != nil {
		t.Fatalf("output quota rejected MQTT publish: %v", err)
	}
	total, err := h.rdb.Client().Get(ctx, forwarder.DailyBytesKey(tenant, time.Now())).Int64()
	if err != nil || total != 3 {
		t.Fatalf("quota accounting changed: total=%d, err=%v", total, err)
	}
	select {
	case <-output.requests:
		t.Fatal("over-quota output was forwarded")
	case <-time.After(100 * time.Millisecond):
	}
}

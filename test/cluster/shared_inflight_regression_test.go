//go:build cluster

package cluster

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/redis/go-redis/v9"
)

func TestSharedInflightRegression_TwentyLiveMembersNoOfflineGrowth(t *testing.T) {
	h := NewHarness(t, 1, 3, []string{deviceAID})
	const filter = "$share/bridge-regression/telemetry/#"
	oldIDs := make([]string, 9)
	for i := range oldIDs {
		oldIDs[i] = fmt.Sprintf("old-bridge-%02d", i)
		c := connectSharedPersistentClient(t, h.MQTTAddr(i%3), oldIDs[i], consumerUsername, consumerPassword)
		if token := c.Subscribe(filter, 1, nil); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
			t.Fatalf("subscribe old bridge: %v", token.Error())
		}
		c.Disconnect(250)
	}
	waitForSharedOfflineOwners(t, h, oldIDs, filter, 25*time.Second)
	received := make(chan string, 1000)
	for i := 0; i < 20; i++ {
		c := connectClientWithHandler(t, h.MQTTAddr(i%2), fmt.Sprintf("live-bridge-%02d", i), func(_ mqtt.Client, msg mqtt.Message) {
			received <- string(msg.Payload())
		})
		if token := c.Subscribe(filter, 1, nil); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
			t.Fatalf("subscribe live bridge: %v", token.Error())
		}
	}
	for i := 0; i < 2; i++ {
		if !waitForLiveRoute(t, h, 0, filter, h.Edges[i].ID, 15*time.Second) {
			t.Fatal("live shared route did not converge")
		}
	}
	// edge-3 has no live consumer: its publishes also exercise forwarding.
	const total = 120
	for edge := 0; edge < 3; edge++ {
		publisher := connectClient(t, h.MQTTAddr(edge), fmt.Sprintf("qos2-publisher-%d", edge), deviceAID, DevicePwd)
		for i := 0; i < total/3; i++ {
			payload := fmt.Sprintf("edge-%d-message-%d", edge, i)
			if token := publisher.Publish(publishTopicFor(deviceAID), 2, false, payload); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
				t.Fatalf("QoS2 publish %s: %v", payload, token.Error())
			}
		}
	}
	seen := make(map[string]bool)
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for len(seen) < total {
		select {
		case payload := <-received:
			if seen[payload] {
				t.Fatalf("duplicate shared delivery: %s", payload)
			}
			seen[payload] = true
		case <-deadline.C:
			t.Fatalf("received %d/%d unique messages", len(seen), total)
		}
	}
	select {
	case payload := <-received:
		t.Fatalf("extra shared delivery after %d unique messages: %s", total, payload)
	case <-time.After(time.Second):
	}
	ctx := context.Background()
	host, err := h.Redis[0].Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := h.Redis[0].MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: net.JoinHostPort(host, port.Port())})
	defer rdb.Close()
	for _, clientID := range oldIDs {
		key := "keel:gw:IFM:" + base64.RawURLEncoding.EncodeToString([]byte(clientID))
		if n, err := rdb.HLen(ctx, key).Result(); err != nil || n != 0 {
			t.Errorf("offline bridge %s accumulated %d IFM, err=%v", clientID, n, err)
		}
	}
}

func TestSharedInflightRegression_SameEdgeResumeDeliversExactlyOnce(t *testing.T) {
	h := NewHarness(t, 1, 1, []string{deviceAID})
	const clientID = "same-edge-shared-bridge"
	const filter = "$share/same-edge-regression/telemetry/#"
	c := connectSharedPersistentClient(t, h.MQTTAddr(0), clientID, consumerUsername, consumerPassword)
	if token := c.Subscribe(filter, 1, nil); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		t.Fatalf("subscribe: %v", token.Error())
	}
	c.Disconnect(250)
	waitForSharedOfflineOwners(t, h, []string{clientID}, filter, 20*time.Second)
	publisher := connectClient(t, h.MQTTAddr(0), "same-edge-qos2-publisher", deviceAID, DevicePwd)
	if token := publisher.Publish(publishTopicFor(deviceAID), 2, false, "offline-shared-message"); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		t.Fatalf("publish: %v", token.Error())
	}
	received := make(chan string, 10)
	connectPersistentWithHandler(t, h.MQTTAddr(0), clientID, func(_ mqtt.Client, msg mqtt.Message) {
		received <- string(msg.Payload())
	})
	select {
	case payload := <-received:
		if payload != "offline-shared-message" {
			t.Fatalf("unexpected payload: %s", payload)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("same-edge resume did not recover the Redis offline queue")
	}
	select {
	case payload := <-received:
		t.Fatalf("offline shared delivery duplicated: %s", payload)
	case <-time.After(time.Second):
	}
}

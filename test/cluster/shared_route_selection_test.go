//go:build cluster

package cluster

import (
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestSharedRouting_SharedLiveAndOrdinaryRouteMustNotDuplicate(t *testing.T) {
	h := NewHarness(t, 1, 2, []string{deviceAID})
	const sharedFilter = "$share/review-group/telemetry/#"
	shared := make(chan string, 10)
	for edge := 0; edge < 2; edge++ {
		edge := edge
		id := []string{"shared-on-edge-1", "shared-on-edge-2"}[edge]
		c := connectClientWithHandler(t, h.MQTTAddr(edge), id, func(_ mqtt.Client, _ mqtt.Message) {
			shared <- id
		})
		if token := c.Subscribe(sharedFilter, 1, nil); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
			t.Fatalf("subscribe shared: %v", token.Error())
		}
	}
	ordinary := make(chan bool, 10)
	c := connectClientWithHandler(t, h.MQTTAddr(1), "ordinary-subscriber", func(_ mqtt.Client, _ mqtt.Message) {
		ordinary <- true
	})
	if token := c.Subscribe("telemetry/#", 1, nil); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		t.Fatalf("subscribe ordinary: %v", token.Error())
	}
	for edge := 0; edge < 2; edge++ {
		if !waitForLiveRoute(t, h, 0, sharedFilter, h.Edges[edge].ID, 15*time.Second) {
			t.Fatal("shared route not visible")
		}
	}
	if !waitForLiveRoute(t, h, 0, "telemetry/#", h.Edges[1].ID, 15*time.Second) {
		t.Fatal("ordinary route not visible")
	}
	publisher := connectClient(t, h.MQTTAddr(0), "review-qos2-publisher", deviceAID, DevicePwd)
	if token := publisher.Publish(publishTopicFor(deviceAID), 2, false, "overlapping-live-route"); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		t.Fatalf("QoS2 publish: %v", token.Error())
	}
	select {
	case <-ordinary:
	case <-time.After(10 * time.Second):
		t.Fatal("ordinary subscriber did not receive the message")
	}
	var deliveries []string
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case id := <-shared:
			deliveries = append(deliveries, id)
		case <-timer.C:
			t.Logf("one publish to one shared group: deliveries=%v", deliveries)
			if len(deliveries) != 1 {
				t.Fatalf("shared group received %d copies, want 1: %v", len(deliveries), deliveries)
			}
			return
		}
	}
}

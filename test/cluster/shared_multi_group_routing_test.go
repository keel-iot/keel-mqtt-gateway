//go:build cluster

package cluster

import (
	"fmt"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestSharedRouting_MultipleGroupsAndOrdinaryFanout(t *testing.T) {
	h := NewHarness(t, 1, 3, []string{deviceAID})
	type receipt struct{ observer, payload string }
	received := make(chan receipt, 3000)
	subscribe := func(edge int, clientID, observer, filter string) {
		t.Helper()
		c := connectClientWithHandler(t, h.MQTTAddr(edge), clientID, func(_ mqtt.Client, msg mqtt.Message) {
			received <- receipt{observer, string(msg.Payload())}
		})
		if token := c.Subscribe(filter, 1, nil); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
			t.Fatalf("subscribe %s: %v", clientID, token.Error())
		}
		if !waitForLiveRoute(t, h, 0, filter, h.Edges[edge].ID, 15*time.Second) {
			t.Fatalf("route %s -> %s did not converge", filter, h.Edges[edge].ID)
		}
	}
	for edge := 0; edge < 3; edge++ {
		id := fmt.Sprintf("ordinary-%d", edge)
		subscribe(edge, id, id, "telemetry/#")
	}
	for _, edge := range []int{0, 1} {
		subscribe(edge, fmt.Sprintf("group-a-%d", edge), "group-a", "$share/a/telemetry/#")
	}
	for _, edge := range []int{1, 2} {
		subscribe(edge, fmt.Sprintf("group-b-%d", edge), "group-b", "$share/b/telemetry/#")
	}
	const perSourceQoS = 10
	const total = 3 * 3 * perSourceQoS
	for edge := 0; edge < 3; edge++ {
		publisher := connectClient(t, h.MQTTAddr(edge), fmt.Sprintf("multi-publisher-%d", edge), deviceAID, DevicePwd)
		for qos := byte(0); qos <= 2; qos++ {
			for i := 0; i < perSourceQoS; i++ {
				payload := fmt.Sprintf("edge-%d-qos-%d-message-%d", edge, qos, i)
				if token := publisher.Publish(publishTopicFor(deviceAID), qos, false, payload); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
					t.Fatalf("publish %s: %v", payload, token.Error())
				}
			}
		}
	}
	seen := make(map[receipt]bool)
	counts := make(map[string]int)
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	for len(seen) < total*5 { // two shared groups and three ordinary clients
		select {
		case got := <-received:
			if seen[got] {
				t.Fatalf("duplicate for %s: %s", got.observer, got.payload)
			}
			seen[got] = true
			counts[got.observer]++
		case <-timer.C:
			t.Fatalf("incomplete delivery: counts=%v; expected %d per observer", counts, total)
		}
	}
	for _, observer := range []string{"group-a", "group-b", "ordinary-0", "ordinary-1", "ordinary-2"} {
		if counts[observer] != total {
			t.Fatalf("observer %s: %d messages, want %d", observer, counts[observer], total)
		}
	}
	select {
	case got := <-received:
		t.Fatalf("extra delivery after complete fanout: %+v", got)
	case <-time.After(time.Second):
	}
	t.Logf("%d publishes across three edges, QoS0/1/2: %v", total, counts)
}

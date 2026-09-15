//go:build cluster

package cluster

import (
	"fmt"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// TestQoS2EdgeFailure_HandshakeContinuesAndLiveForwardingSurvives validates
// the production-relevant QoS2 path where the publishing Edge remains alive,
// one routed subscriber Edge is killed brutally, and another routed
// subscriber Edge remains live. Paho exposes the completed publish token, not
// individual PUBREC/PUBREL/PUBCOMP packets; completion of a QoS2 Publish token
// is therefore used as the observable terminal handshake.
func TestQoS2EdgeFailure_HandshakeContinuesAndLiveForwardingSurvives(t *testing.T) {
	h := NewHarness(t, 3, 3, []string{deviceAID})
	const filter = "telemetry/#"

	deadSubscriber := connectClient(t, h.MQTTAddr(0), "qos2-dead-edge-subscriber", consumerUsername, consumerPassword)
	liveSubscriber := connectClient(t, h.MQTTAddr(1), "qos2-live-edge-subscriber", consumerUsername, consumerPassword)
	publisher := connectClient(t, h.MQTTAddr(2), "qos2-edge-failure-publisher", deviceAID, DevicePwd)

	var mu sync.Mutex
	received := make(map[string]int)
	liveSub := liveSubscriber.Subscribe(filter, 2, func(_ mqtt.Client, msg mqtt.Message) {
		mu.Lock()
		received[string(msg.Payload())]++
		mu.Unlock()
	})
	if !liveSub.WaitTimeout(10*time.Second) || liveSub.Error() != nil {
		t.Fatalf("live QoS2 subscriber subscribe: %v", liveSub.Error())
	}
	deadSub := deadSubscriber.Subscribe(filter, 2, nil)
	if !deadSub.WaitTimeout(10*time.Second) || deadSub.Error() != nil {
		t.Fatalf("dead-target QoS2 subscriber subscribe: %v", deadSub.Error())
	}

	deadID := h.Edges[0].ID
	liveID := h.Edges[1].ID
	if !waitForLiveRoute(t, h, 0, filter, deadID, 15*time.Second) || !waitForLiveRoute(t, h, 0, filter, liveID, 15*time.Second) {
		t.Fatalf("routing did not contain both QoS2 targets: dead=%s live=%s", deadID, liveID)
	}
	routes := fetchRoutes(t, h, 0)
	t.Logf("routing baseline: %q -> %v; publisher=%s subscriber_edges=[%s,%s]", filter, routes[filter], h.Edges[2].ID, deadID, liveID)

	baseline := publishQoS2Messages(t, publisher, received, "qos2-baseline", 3)
	t.Logf("QoS2 baseline: messages=%d max_publish_token=%s", len(baseline), maxDuration(baseline))

	faultAt := time.Now()
	h.KillEdge(0)
	t.Logf("brutal Edge failure: node=%s at %s", deadID, faultAt.Format(time.RFC3339Nano))

	failureDurations := publishQoS2Messages(t, publisher, received, "qos2-during-edge-failure", 5)
	t.Logf("QoS2 during failure: messages=%d max_publish_token=%s", len(failureDurations), maxDuration(failureDurations))

	if !waitForRouteAbsent(t, h, 0, filter, deadID, 20*time.Second) {
		t.Fatalf("route to dead Edge %s did not converge away", deadID)
	}
	t.Logf("route converged: node=%s delta=%s", deadID, time.Since(faultAt))

	after := publishQoS2Messages(t, publisher, received, "qos2-after-convergence", 3)
	t.Logf("QoS2 after convergence: messages=%d max_publish_token=%s", len(after), maxDuration(after))

	// The live subscriber must receive all messages published during and after
	// the failure. Each payload is a logical message and should be delivered
	// once by the live Edge in this non-reconnect scenario.
	for prefix, count := range map[string]int{
		"qos2-baseline":            3,
		"qos2-during-edge-failure": 5,
		"qos2-after-convergence":   3,
	} {
		for i := 0; i < count; i++ {
			payload := fmt.Sprintf("%s-%d", prefix, i)
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				mu.Lock()
				got := received[payload]
				mu.Unlock()
				if got >= 1 {
					if got > 1 {
						t.Errorf("logical QoS2 message %q delivered %d times on live subscriber", payload, got)
					}
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			mu.Lock()
			got := received[payload]
			mu.Unlock()
			if got == 0 {
				t.Errorf("logical QoS2 message %q was not delivered to live subscriber", payload)
			}
		}
	}
}

func publishQoS2Messages(t *testing.T, publisher mqtt.Client, received map[string]int, prefix string, count int) []time.Duration {
	t.Helper()
	durations := make([]time.Duration, 0, count)
	for i := 0; i < count; i++ {
		payload := fmt.Sprintf("%s-%d", prefix, i)
		start := time.Now()
		token := publisher.Publish(publishTopicFor(deviceAID), 2, false, payload)
		if !token.WaitTimeout(10*time.Second) || token.Error() != nil {
			t.Fatalf("QoS2 publish %q: %v", payload, token.Error())
		}
		durations = append(durations, time.Since(start))
	}
	return durations
}

func maxDuration(values []time.Duration) time.Duration {
	var max time.Duration
	for _, value := range values {
		if value > max {
			max = value
		}
	}
	return max
}

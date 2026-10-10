//go:build cluster

package cluster

import (
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// TestCrossNodeUnsubscribeStopsRemoteDelivery is the C2 scenario from issue
// #12: a live subscription is removed on Edge 1, its distributed route
// disappears, and a matching publish through Edge 2 must not be forwarded to
// the unsubscribed client.
func TestCrossNodeUnsubscribeStopsRemoteDelivery(t *testing.T) {
	h := NewHarness(t, 3, 2, []string{deviceAID})

	const filter = "telemetry/#"
	consumer := connectClient(t, h.MQTTAddr(0), "unsubscribe-remote-consumer", consumerUsername, consumerPassword)
	defer consumer.Disconnect(250)

	received := make(chan string, 1)
	subToken := consumer.Subscribe(filter, 1, func(_ mqtt.Client, msg mqtt.Message) {
		received <- string(msg.Payload())
	})
	if !subToken.WaitTimeout(10*time.Second) || subToken.Error() != nil {
		t.Fatalf("subscribe: %v", subToken.Error())
	}

	if !waitForLiveRoute(t, h, 0, filter, h.Edges[0].ID, 15*time.Second) {
		t.Fatalf("live route %q -> %s never became observable before unsubscribe", filter, h.Edges[0].ID)
	}

	unsubToken := consumer.Unsubscribe(filter)
	if !unsubToken.WaitTimeout(10*time.Second) || unsubToken.Error() != nil {
		t.Fatalf("unsubscribe: %v", unsubToken.Error())
	}
	if !waitForRouteAbsent(t, h, 0, filter, h.Edges[0].ID, 15*time.Second) {
		t.Fatalf("route %q -> %s remained after unsubscribe", filter, h.Edges[0].ID)
	}

	publisher := connectClient(t, h.MQTTAddr(1), "unsubscribe-remote-publisher", deviceAID, DevicePwd)
	defer publisher.Disconnect(250)
	pubToken := publisher.Publish(publishTopicFor(deviceAID), 1, false, "must-not-reach-unsubscribed-client")
	if !pubToken.WaitTimeout(10*time.Second) || pubToken.Error() != nil {
		t.Fatalf("publish after unsubscribe: %v", pubToken.Error())
	}

	select {
	case got := <-received:
		t.Fatalf("unsubscribed client received a remote publish: %q", got)
	case <-time.After(5 * time.Second):
	}
}

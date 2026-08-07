package conformance

import "testing"

func TestConformanceACL_AllowsArbitraryTopics(t *testing.T) {
	h := NewACLHook()
	cases := []struct {
		topic string
		write bool
	}{
		{"TopicA", true},
		{"TopicA/B", false},
		{"+/C", false},
		{"#", false},
		{"/TopicA", true},
		{nosubscribeTopic, true}, // publish to it must remain allowed
	}
	for _, c := range cases {
		if !h.OnACLCheck(nil, c.topic, c.write) {
			t.Errorf("expected allow for topic=%q write=%v, got deny", c.topic, c.write)
		}
	}
}

func TestConformanceACL_PahoNoSubscribeSemantics(t *testing.T) {
	h := NewACLHook()

	if h.OnACLCheck(nil, nosubscribeTopic, false) {
		t.Error("expected subscribe to test/nosubscribe to be denied")
	}
	if !h.OnACLCheck(nil, nosubscribeTopic, true) {
		t.Error("expected publish to test/nosubscribe to remain allowed — paho.mqtt.testing's own -n flag help text describes it as subscribe-only")
	}
}

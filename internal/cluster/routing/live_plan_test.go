package routing

import "testing"

func TestLivePlanRetainsGroupDecisionWithOverlappingOrdinaryTargets(t *testing.T) {
	r := newTestRouter(t)
	const a = "$share/a/telemetry/#"
	const b = "$share/b/telemetry/#"
	r.mu.Lock()
	for _, node := range []string{"edge-a", "edge-b", "edge-c"} {
		r.addLocalLocked("telemetry/#", node)
	}
	r.addLocalLocked(a, "edge-a")
	r.addLocalLocked(a, "edge-b")
	r.addLocalLocked(b, "edge-b")
	r.addLocalLocked(b, "edge-c")
	r.mu.Unlock()
	for _, source := range []string{"edge-a", "edge-b", "edge-c"} {
		plan := r.LiveDeliveryPlan("telemetry/device", source)
		if len(plan.Nodes) != 3 || len(plan.SharedNodes) != 2 {
			t.Fatalf("source %s: incomplete plan %+v", source, plan)
		}
		if node := plan.SharedNodes[a]; node != "edge-a" && node != "edge-b" {
			t.Fatalf("group a chose a non-member: %s", node)
		}
		if node := plan.SharedNodes[b]; node != "edge-b" && node != "edge-c" {
			t.Fatalf("group b chose a non-member: %s", node)
		}
		if source != "edge-c" && plan.SharedNodes[a] != source {
			t.Fatalf("group a lost local preference: source=%s plan=%+v", source, plan)
		}
		if source != "edge-a" && plan.SharedNodes[b] != source {
			t.Fatalf("group b lost local preference: source=%s plan=%+v", source, plan)
		}
	}
}

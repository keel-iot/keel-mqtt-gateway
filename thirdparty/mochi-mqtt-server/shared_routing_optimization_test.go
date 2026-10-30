// SPDX-License-Identifier: MIT
package mqtt

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

func TestSubscriberIDsPreserveRoutingSemantics(t *testing.T) {
	x := NewTopicsIndex()
	for _, v := range []struct{ id, filter string }{
		{"ordinary-a", "a/#"}, {"ordinary-a", "a/+"}, {"ordinary-b", "a/+"},
		{"one", "$share/g/a/#"}, {"two", "$share/g/a/#"},
		{"other-filter", "$share/g/a/+"}, {"system", "$SYS/#"},
	} {
		x.Subscribe(v.id, packets.Subscription{Filter: v.filter, Qos: 2})
	}
	for i := 0; i < 100; i++ {
		ids := x.SubscriberIDs("a/b", "")
		require.Len(t, ids, 4)
		require.Contains(t, ids, "ordinary-a")
		require.Contains(t, ids, "ordinary-b")
		require.Contains(t, ids, "other-filter")
		require.True(t, containsID(ids, "one") != containsID(ids, "two"))
		require.ElementsMatch(t, []string{"ordinary-a", "ordinary-b", "other-filter"}, x.SubscriberIDs("a/b", "one"))
	}
	require.ElementsMatch(t, []string{"system"}, x.SubscriberIDs("$SYS/status", ""))
	require.Empty(t, x.SubscriberIDs("", ""))
	// Local shared preference must not remove an ordinary delivery to that node.
	x.Subscribe("one", packets.Subscription{Filter: "a/b"})
	require.ElementsMatch(t, []string{"ordinary-a", "ordinary-b", "other-filter", "one"}, x.SubscriberIDs("a/b", "one"))
	x.Unsubscribe("$share/g/a/#", "one")
	require.ElementsMatch(t, []string{"ordinary-a", "ordinary-b", "other-filter", "one", "two"}, x.SubscriberIDs("a/b", "one"))
}

func containsID(ids []string, id string) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

func TestSubscriberIDsConcurrentMembershipChanges(t *testing.T) {
	x := NewTopicsIndex()
	filter := "$share/g/a/#"
	x.Subscribe("stable", packets.Subscription{Filter: filter})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			x.Subscribe("changing", packets.Subscription{Filter: filter})
			x.Unsubscribe(filter, "changing")
		}
	}()
	for i := 0; i < 2000; i++ {
		ids := x.SubscriberIDs("a/b", "")
		require.Len(t, ids, 1)
		require.True(t, ids[0] == "stable" || ids[0] == "changing")
		require.Empty(t, x.SubscriberIDs("a/b", "stable"))
	}
	wg.Wait()
	require.ElementsMatch(t, []string{"stable"}, x.SubscriberIDs("a/b", ""))
}

func BenchmarkSharedRoutingSnapshot12(b *testing.B) {
	x := NewTopicsIndex()
	for i := 0; i < 12; i++ {
		x.Subscribe(fmt.Sprint(i), packets.Subscription{Filter: "$share/g/a/#", Qos: 2})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		subs := x.Subscribers("a/b")
		subs.SelectShared()
		subs.MergeSharedSelected()
	}
}

func BenchmarkSharedRoutingIDs12(b *testing.B) {
	x := NewTopicsIndex()
	for i := 0; i < 12; i++ {
		x.Subscribe(fmt.Sprint(i), packets.Subscription{Filter: "$share/g/a/#", Qos: 2})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.SubscriberIDs("a/b", "")
	}
}

// A deterministic queue fixture: unlike newTestClient it has no WriteLoop
// competing with the benchmark/test for the queued packet.
func newSharedQueuedClient(s *Server) (cl *Client, r net.Conn, w net.Conn) {
	r, w = net.Pipe()
	cl = newClient(w, &ops{info: s.Info, hooks: s.hooks, log: s.Log, options: s.Options})
	cl.State.Inflight.maximumSendQuota = 5
	cl.State.Inflight.sendQuota = 5
	return
}

func BenchmarkSharedDelivery12Members(b *testing.B) {
	s := newServer()
	cl, r, w := newSharedQueuedClient(s)
	cl.ID = "selected"
	s.Clients.Add(cl)
	b.Cleanup(func() { _ = r.Close(); _ = w.Close() })
	filter := "$share/g/a/#"
	sub := packets.Subscription{Filter: filter, Qos: 0}
	members := map[string]packets.Subscription{"selected": sub}
	for i := 0; i < 11; i++ {
		members[fmt.Sprint(i)] = sub
	}
	subs := &Subscribers{
		Shared:               map[string]map[string]packets.Subscription{filter: members},
		SharedSelected:       map[string]packets.Subscription{"selected": sub},
		SharedSelectedGroups: map[string]string{filter: "selected"},
		Subscriptions:        map[string]packets.Subscription{},
	}
	subs.MergeSharedSelected()
	pk := packets.Packet{TopicName: "a/b", Payload: make([]byte, 256), FixedHeader: packets.FixedHeader{Type: packets.Publish}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.publishSharedToSubscribers(subs, pk)
		if cl.State.outbound.len() != 1 {
			b.Fatal("delivery not queued")
		}
		if _, ok := cl.State.outbound.pop(nil); !ok {
			b.Fatal("no accepted delivery")
		}
		atomic.AddInt32(&cl.State.outboundQty, -1)
	}
}

func TestSharedSingleSelectedMemberMergesAllGroupProperties(t *testing.T) {
	s := newServer()
	cl, r, w := newSharedQueuedClient(s)
	cl.ID = "selected"
	s.Clients.Add(cl)
	t.Cleanup(func() { _ = r.Close(); _ = w.Close() })
	first := packets.Subscription{Filter: "$share/one/a/#", Qos: 0, Identifier: 3}
	second := packets.Subscription{Filter: "$share/two/a/+", Qos: 2, Identifier: 7}
	// A hook can supply its own initial selection. The delivery must still
	// merge the raw matching subscriptions just like the full fallback planner.
	subs := &Subscribers{
		Shared: map[string]map[string]packets.Subscription{
			first.Filter: {"selected": first}, second.Filter: {"selected": second},
		},
		SharedSelected:       map[string]packets.Subscription{"selected": first},
		SharedSelectedGroups: map[string]string{first.Filter: "selected", second.Filter: "selected"},
		Subscriptions:        map[string]packets.Subscription{"selected": first},
	}
	pk := packets.Packet{TopicName: "a/b", Payload: []byte("payload"), FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2}}
	clients := s.publishSharedToSubscribers(subs, pk)
	require.Equal(t, map[string]bool{"selected": true}, clients)
	require.Equal(t, 1, cl.State.outbound.len())
	out, ok := cl.State.outbound.pop(nil)
	require.True(t, ok)
	require.Equal(t, byte(2), out.FixedHeader.Qos)
	require.Contains(t, out.Properties.SubscriptionIdentifier, 3)
	require.Contains(t, out.Properties.SubscriptionIdentifier, 7)
	require.Equal(t, 1, cl.State.Inflight.LenOutbound())
}

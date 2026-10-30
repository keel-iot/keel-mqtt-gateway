// SPDX-License-Identifier: MIT

package mqtt

import (
	"sync"
	"testing"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

func TestSharedGatherSnapshotIsIndependent(t *testing.T) {
	shared := NewSharedSubscriptions()
	first := packets.Subscription{Filter: "$share/g/topic/#", Qos: 1}
	second := packets.Subscription{Filter: "$share/g/topic/+", Qos: 2}
	shared.Add("g", "first", first)
	shared.Add("g", "second", second)
	p := &particle{shared: shared}
	x := NewTopicsIndex()
	snapshot := &Subscribers{Shared: map[string]map[string]packets.Subscription{
		first.Filter: {"already-matched": first},
	}}
	x.gatherSharedSubscriptions(p, snapshot)
	require.Len(t, snapshot.Shared[first.Filter], 2)
	require.Equal(t, first, snapshot.Shared[first.Filter]["first"])
	require.Equal(t, second, snapshot.Shared[second.Filter]["second"])
	delete(snapshot.Shared[first.Filter], "first")
	snapshot.Shared[second.Filter]["second"] = first
	stored, ok := shared.Get("g", "first")
	require.True(t, ok)
	require.Equal(t, first, stored)
	stored, ok = shared.Get("g", "second")
	require.True(t, ok)
	require.Equal(t, second, stored)
	shared.Delete("g", "second")
	require.Contains(t, snapshot.Shared[second.Filter], "second", "later unsubscribe must not mutate a snapshot")
}

func TestSharedGatherDuringSubscriptionChanges(t *testing.T) {
	shared := NewSharedSubscriptions()
	sub := packets.Subscription{Filter: "$share/group/topic/#", Qos: 1}
	shared.Add("group", "stable", sub)
	p := &particle{shared: shared}
	x := NewTopicsIndex()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			shared.Add("group", "changing", sub)
			shared.Delete("group", "changing")
		}
	}()
	for i := 0; i < 2000; i++ {
		snapshot := &Subscribers{}
		x.gatherSharedSubscriptions(p, snapshot)
		require.Equal(t, sub, snapshot.Shared[sub.Filter]["stable"])
		// Consumers may mutate their snapshot after collection. This must not
		// race with a subscription writer or delete the stored stable member.
		delete(snapshot.Shared[sub.Filter], "stable")
	}
	wg.Wait()
	_, exists := shared.Get("group", "stable")
	require.True(t, exists)
}

func BenchmarkSharedGather12Members(b *testing.B) {
	shared := NewSharedSubscriptions()
	for i := 0; i < 12; i++ {
		shared.Add("group", string(rune('a'+i)), packets.Subscription{
			Filter: "$share/group/topic/#", Qos: 1,
		})
	}
	p := &particle{shared: shared}
	x := NewTopicsIndex()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.gatherSharedSubscriptions(p, &Subscribers{})
	}
}

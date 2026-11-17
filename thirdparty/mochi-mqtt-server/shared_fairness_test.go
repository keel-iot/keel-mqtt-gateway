// SPDX-License-Identifier: MIT
package mqtt

import (
	"fmt"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestSharedRoundRobinConcurrentAcrossTopics(t *testing.T) {
	x := NewTopicsIndex()
	filter := "$share/bridge/devices/#"
	for _, id := range []string{"c", "a", "b"} {
		x.Subscribe(id, packets.Subscription{Filter: filter, Qos: 1})
	}
	counts := map[string]int{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for worker := 0; worker < 12; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				topic := fmt.Sprintf("devices/%d/%d", worker, i)
				// Routing and inventory reads must not consume delivery turns.
				x.SubscriberIDs(topic, "a")
				x.Subscribers(topic)
				subs := x.Subscribers(topic)
				subs.SelectShared()
				mu.Lock()
				counts[subs.SharedSelectedGroups[filter]]++
				mu.Unlock()
			}
		}(worker)
	}
	wg.Wait()
	require.Equal(t, map[string]int{"a": 400, "b": 400, "c": 400}, counts)
}

func TestSharedRoundRobinMembershipAndPruning(t *testing.T) {
	x := NewTopicsIndex()
	filter := "$share/bridge/devices/#"
	other := "$share/other/devices/#"
	for _, f := range []string{filter, other} {
		for _, id := range []string{"a", "b", "c"} {
			x.Subscribe(id, packets.Subscription{Filter: f, Qos: 1})
		}
	}
	selectMember := func(prune bool) *Subscribers {
		subs := x.Subscribers("devices/one")
		if prune {
			delete(subs.Shared[filter], "a")
		}
		subs.SelectShared()
		return subs
	}
	require.Equal(t, "a", selectMember(false).SharedSelectedGroups[filter])
	subs := x.Subscribers("devices/two")
	delete(subs.Shared, other)
	subs.SelectShared()
	require.Equal(t, "b", subs.SharedSelectedGroups[filter])
	subs = selectMember(false)
	require.Equal(t, "c", subs.SharedSelectedGroups[filter])
	require.Equal(t, "b", subs.SharedSelectedGroups[other])
	counts := map[string]int{}
	for i := 0; i < 20; i++ {
		counts[selectMember(true).SharedSelectedGroups[filter]]++
	}
	require.Equal(t, map[string]int{"b": 10, "c": 10}, counts)
	for _, id := range []string{"a", "b", "c"} {
		x.Unsubscribe(filter, id)
	}
	x.Subscribe("new", packets.Subscription{Filter: filter})
	require.Equal(t, "new", selectMember(false).SharedSelectedGroups[filter])
	x.Subscribe("z", packets.Subscription{Filter: filter})
	require.Equal(t, "z", selectMember(false).SharedSelectedGroups[filter])
}

func TestSharedRoundRobinDeliversOnePacketPerPublish(t *testing.T) {
	s := newServer()
	filter := "$share/bridge/devices/#"
	clients := []*Client{}
	for _, id := range []string{"a", "b", "c"} {
		cl, r, w := newSharedQueuedClient(s)
		cl.ID = id
		s.Clients.Add(cl)
		t.Cleanup(func() { _ = r.Close(); _ = w.Close() })
		s.Topics.Subscribe(id, packets.Subscription{Filter: filter, Qos: 1})
		clients = append(clients, cl)
	}
	for i := 0; i < 30; i++ {
		s.publishToSubscribers(packets.Packet{TopicName: fmt.Sprintf("devices/%d", i), FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, Payload: []byte("data")})
	}
	for _, cl := range clients {
		require.Equal(t, 10, cl.State.Inflight.LenOutbound(), cl.ID)
	}
}

func TestSharedRoundRobinDuringMembershipChanges(t *testing.T) {
	x := NewTopicsIndex()
	filter := "$share/bridge/devices/#"
	x.Subscribe("stable", packets.Subscription{Filter: filter})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			x.Subscribe("changing", packets.Subscription{Filter: filter})
			x.Unsubscribe(filter, "changing")
		}
	}()
	for i := 0; i < 1000; i++ {
		subs := x.Subscribers("devices/one")
		subs.SelectShared()
		require.Len(t, subs.SharedSelected, 1)
		require.Contains(t, subs.Shared[filter], subs.SharedSelectedGroups[filter])
	}
	wg.Wait()
}

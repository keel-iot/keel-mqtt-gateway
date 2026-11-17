# Local shared delivery fairness

When several MQTT clients subscribe to the same full shared filter on one edge,
selecting the first Go map entry can concentrate deliveries on one client. In
Kimera, three consumers on the same edge received approximately 818, 260 and
258 messages/s, while the busiest consumer accumulated thousands of inflight
messages. This observation motivates fair selection; it does not establish the
cause of every outstanding inflight message.

The broker now rotates initial delivery selection in sorted client-ID order.
Each indexed shared group/filter owns an atomic cursor, shared by all matching
concrete topics and concurrent publishers. Merely querying subscriptions or
cluster routing does not advance it. Keel's selection hook removes disconnected
members before rotation. Removing the last subscription discards the cursor;
a new group starts a new rotation. Existing snapshots can safely finish using
the previous cursor during membership changes.

For a stable candidate set, initial selection counts differ by at most one.
This is fairness by message count, not byte volume, consumer speed, or CPU.
Membership changes do not guarantee uninterrupted strict rotation. Explicit
hook selections, ordinary/shared subscription merging, and the existing bounded
fallback for definite pre-acceptance failures retain their behavior. Ambiguous
persistence or transport outcomes must not trigger additional deliveries.

This change does not rebalance bridge TCP connections, alter the inflight quota,
or retry a saturated remote edge on another edge. An edge without local bridge
members can already forward through the existing cluster route. Retrying other
remote destinations safely requires an explicit rejected-before-acceptance
result and bounded retry handling; that remains separate work.

## Verification

`thirdparty/mochi-mqtt-server/shared_fairness_test.go` covers exact selection
counts with concurrent publishers and varying topics, independent shared groups,
snapshot pruning, subscription removal/recreation, concurrent membership
changes, and actual QoS1 inflight acceptance (one member per publication).
Existing shared fallback tests cover quota exhaustion, disconnected and No Local
members, overlapping subscriptions, QoS2, queue rollback, and ambiguous failures.

Run the main module and the nested fork independently:

```sh
go test ./...
(cd thirdparty/mochi-mqtt-server && go test ./...)
(cd thirdparty/mochi-mqtt-server && go test -race . -run 'TestShared|TestPublishShared|TestPublishToSubscribersShared' -count=1)
```

After deployment, compare input rate deltas among consumers sharing each edge
and full filter, plus their Redis inflight counts and quota warnings. Existing
backlogs need time to drain; uniform rates across different edges are not a
contract of local rotation. No Kimera deployment is included in this change.

A local `BenchmarkSharedRoutingSnapshot12` comparison (three samples, overlapping
other test activity) measured approximately 3.1–3.3 us/op and 3640 B / 13
allocations on v0.3.5, versus 4.2–5.2 us/op and 4088 B / 16 allocations with
rotation. This isolated selection cost includes sorting the caller-owned member
list and carrying cursor references; it is not an end-to-end throughput or
production CPU estimate. The cursor metadata is only allocated for snapshots
that contain shared subscriptions.

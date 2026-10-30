# Keel patch: correct ack type for a QoS 2 publish rejected via OnPublish

Base: `github.com/mochi-mqtt/server/v2 v2.7.9`.

## Bug

`Server.processPublish` (server.go), when an `OnPublish` hook returns a
`packets.Code` error for an MQTT5 publish, always builds the reply as a
**PUBACK** — even when the original publish was QoS 2, which must get a
**PUBREC** per the MQTT5 spec (PUBREC is the response to a QoS 2 PUBLISH;
0x97 Quota Exceeded is an explicitly valid PUBREC reason code). A PUBREC
with a reason code >= 0x80 ends that publish negatively — the sender
must not send PUBREL for it — so answering with a PUBACK instead is a
real wire-protocol violation, not a cosmetic mismatch: it can leave a
strictly compliant sender's own QoS 2 state machine confused about what
just happened to that packet ID.

The ACL-deny branch earlier in the same function already selects
PUBACK/PUBREC by QoS correctly — this is the same fix applied to the
`OnPublish`-error branch, which never had it.

## Fix

One-line-shaped: select `ackType` from `pk.FixedHeader.Qos` before
building the ack, exactly like the ACL-deny branch above it.

## Status

- Reported upstream: https://github.com/mochi-mqtt/server/issues/510
- PR upstream: https://github.com/mochi-mqtt/server/pull/511
- Reproduced and regression-tested in Keel:
  `internal/broker/ratelimit_integration_test.go`'s
  `TestPublishRateLimit_MQTT5_QoS2_PubrecQuotaExceeded`.

## Removal condition

This condition originally applied to the single QoS2 patch above. The fork
now carries the other changes documented below as well. Removing the local
`replace` and this directory requires parity for every retained patch and
Keel integration, including directional persistence, result-aware callbacks,
queue behavior and the internal flow hint used by Redis. An upstream release
containing only #511 is insufficient.

# Keel patch: separate inbound and outbound Packet Identifier state

## Bug

MQTT permits a client-to-server transaction and a server-to-client
transaction to use the same numeric Packet Identifier. Mochi's single
`Inflight` map keyed only by `uint16` allowed inbound QoS state (for example
`PUBREC[7]`) to replace an outbound `PUBLISH[7]`, losing the outbound message
and its associated send-quota state. This is the collision described by
upstream issue https://github.com/mochi-mqtt/server/issues/545.

## Fix

`Inflight` now keeps independent inbound and outbound maps. Protocol paths,
Packet ID allocation, resend, completion, and cleanup select the appropriate
domain explicitly. Equal numeric IDs therefore coexist without imposing a
non-MQTT global uniqueness restriction.

The Redis session hook mirrors the same identity with `in:<id>` and
`out:<id>` fields. Legacy numeric fields remain readable during migration and
are removed on completion; new writes are always directional.

## Tests

- equal inbound/outbound IDs coexist in memory;
- inbound QoS2 processing preserves an outbound message with the same ID;
- SUBSCRIBE/UNSUBSCRIBE may use an ID equal to an outbound PUBLISH ID;
- Redis persistence uses independent directional fields (live Redis test);
- Mochi and Keel broker tests pass with `go test -race`.

# Keel patch: fallback for saturated shared-subscription members

Local patch identity: `KMOCHI-PATCH-003`.

## Bug

Shared subscriptions were reduced to one selected client before delivery.
If that client's inflight quota or pending-write queue was full, the broker
discarded the other members and lost the shared delivery. This affected the
generic Mochi shared-subscription path and was reproduced with one saturated
member and one healthy member.

## Fix

`Subscribers.SelectShared` records the selected member per shared filter while
retaining the complete candidate set. `Server.publishSharedToSubscribers`
attempts each candidate at most once and advances only for failures known to
occur before acceptance (quota, disconnected client, ACL rejection, or a
pending-write queue rejection whose state is rolled back). A No Local
suppression is also treated as a pre-acceptance no-op. Once a packet is
accepted, later transport failures do not trigger another shared delivery.

The existing one-delivery merge behavior for a client selected by multiple
matching subscriptions is preserved. Non-shared subscriptions are unchanged.
Topic-alias reservations are made transactionally with enqueue and rolled back
when a pre-acceptance enqueue fails.

## Tests

- baseline reproducer: saturated selected member had no fallback;
- saturated, disconnected, no-local, and pending-queue members fall back;
- all candidates are attempted once and exhaustion is bounded;
- separate shared groups remain independent;
- QoS 1 and QoS 2 fallback;
- accepted delivery does not fall back after transport failure;
- full `publishToSubscribers` path and existing non-shared regressions.

## Status

This is a generic Mochi-only patch prepared for upstream review. No upstream
issue or pull request was opened by this change. The patch is removable once
an upstream release provides equivalent pre-acceptance fallback semantics and
the corresponding regression coverage.

# Keel integration: result-aware QoS persistence boundary

Local integration extension (not part of `KMOCHI-PATCH-003`): persistence
hooks may optionally implement `QosHookResult`. Mochi uses the result-aware
publish/drop callbacks for server-to-client delivery. A persistence error
causes the delivery attempt to stop as ambiguous, so shared-subscription
fallback cannot send the same logical message to another member. Existing
void-only hooks retain their historical behavior.

This is fail-closed, not a claim of transactional Redis durability. If Redis
returns an ambiguous error after a mutation, the persistent state may remain
until a later recovery/reconciliation operation. Completion cleanup remains
best-effort under the historical void hook paths. The extension can be
removed or replaced when the broker and persistence contract provide an
upstream-equivalent result/context API.

## Lazy outbound packet storage

`ClientState.outbound` uses a bounded FIFO whose packet array is allocated
only when data is queued. It starts small, grows up to the existing
`MaximumClientWritesPending` limit (still 8192 by default), clears consumed
packet references, and releases large backing arrays after draining. The
single writer is notified through a separate zero-payload channel. An
explicit zero-capacity configuration retains unbuffered rendezvous semantics.

This avoids reserving about 64 KiB of packet pointers for every connection,
including publish-only clients. It does not reduce the maximum backlog or
alter QoS persistence/overflow callbacks. Closing a client releases queued
transport copies while preserving its separate inflight state for recovery.
The pending-write counter is incremented before a packet becomes visible to
the writer, preventing a fast consumer from decrementing it first.

Regression coverage is in `packet_queue_test.go`: FIFO across wrap/growth,
all 8192 default queue slots, concurrent producers, empty-reader wakeup,
cancellation, unbuffered handoff, queue reclamation on close, and a blocked
socket writer. Existing queue-full QoS rollback/shared-fallback tests use the
new private queue API and retain their assertions. The default limit must
not be lowered as a substitute for this allocation change.

## Single-copy shared subscription snapshots

`TopicsIndex.gatherSharedSubscriptions` builds the caller-owned result while
holding the shared-subscription read lock, instead of calling `GetAll` to
make an intermediate copy of every group/member map and copying it again.
The result still contains all matching members grouped by the exact filter;
it never exposes the registry's group/member maps. Shared selection/fallback
and the public `GetAll` API retain their behavior.

`shared_snapshot_test.go` verifies snapshot isolation, exact-filter grouping,
preservation of earlier matches, and concurrent add/delete/read operations.
Its 12-member benchmark records 2456 B / 7 allocations per gather instead
of 4912 B / 14 allocations in the release baseline on the measured toolchain.
These figures describe that helper, not total broker throughput.

## Restore ordinary offline QoS storage after shared-fallback integration

Upstream Mochi v2.7.9 preserves inflight state when a persistent subscriber
has no connection. Our shared-fallback integration incorrectly rolled it
back for ordinary subscriptions too, breaking offline QoS1/2 delivery and
the publication-expiry conformance test.

`publishToClientMode` now scopes disconnected-candidate rollback to explicit
shared delivery attempts. Ordinary callers keep upstream's offline storage
behavior; shared attempts retain their existing rollback/fallback behavior.
No upstream version or external patch was imported.

`offline_delivery_regression_test.go` covers MQTT 3.1.1/5 and QoS1/2, including
disconnect during persistence, quota/accounting, and preserving older inflight
messages. Tests fail with unconditional rollback restored. See
`docs/testing/OFFLINE_DELIVERY_UPSTREAM_REVIEW.md` in the parent repository
for the upstream comparison and protocol-test evidence.

## Selected upstream release-candidate backports (2026-10-29)

The base remains v2.7.9. Selected changes from PR #543's
`3b7c3e698b46d38ab8e91d35f94c662047382d81` were backported, preserving the
local queue, shared fallback, directional maps, persistence schema and
offline-delivery correction:

- wrapped publish refusal uses the code extracted by `errors.As` (#543);
- successful QoS1 PUBACK uses the valid success reason (#497/#518);
- PUBLISH retains User Properties with Request Problem Information=0 (#512);
- unknown inbound topic aliases are refused rather than silently published (#521);
- explicit zero Receive Maximum, Maximum Packet Size and Subscription
  Identifier are rejected, preserving absent-property semantics (#527);
- retained particle paths are read under the writer's lock (#539);
- MQTT3 server disconnect closes without a MQTT5 DISCONNECT, including
  PassiveClientDisconnect (#536 and the release-candidate follow-up);
- inbound QoS1 PUBACKs are stateless (#519), with the `InboundFlow` hint from
  #548 adapted to Keel's Redis hook. Completion callbacks remain available,
  but a stateless inbound ACK cannot delete an outbound record or perform
  pointless Redis work. Normal outbound completion is unchanged.

Upstream contributors' changes are tracked by PR/commit in
`docs/testing/MOCHI_UPSTREAM_ADOPTION.md` in the parent repository. New
regression tests in `upstream_backports_test.go` and
`packets/upstream_backports_test.go` include wire checks, wrapped refusals,
negative/control cases and concurrent retained scans. The parent module's
`TestRedisStatelessInboundQoS1CannotDeleteOutboundDelivery` checks both Redis
schemas and zero command dispatch for the stateless path.

This is a selective backport, not adoption of the full release candidate.
In particular #513 is deferred until Keel's subscription callbacks handle
per-filter refusal consistently; #506/#552/#553 require their own performance
and integration review. The existing Go dependency versions are preserved.

### ID-only routing and lazy shared fallback (2026-10-30)

- Added `TopicsIndex.SubscriberIDs(topic, localSharedMember)` for Keel's
  inter-edge routing. It traverses the existing MQTT trie and collects only
  ordinary target IDs and one target per full shared filter, preserving local
  shared preference. It reads subscription maps under their locks, without
  copying subscription properties or building shared-selection maps.
- The normal `Subscribers` API retains its caller-owned mutable snapshots for
  hooks and local delivery. Its candidate filtering and fallback contract are
  unchanged.
- Shared delivery builds its full fallback plan lazily when a single selected
  client fails before acceptance. Successful single-client selection avoids
  copying all candidates. Safe failure reuses the first result rather than
  attempting that client twice; ambiguous persistence/transport errors stop
  fallback as before. Raw group properties are still merged for hook selections.
- Tests cover wildcard/system routing, shared-filter independence, local
  preference, concurrent membership changes and custom-hook property merging.
  Existing shared failure/rollback tests exercise the lazy fallback path.

### Ordinary offline enqueue ownership in Keel clusters (2026-10-30)

Run `OnSelectSubscribers` for ordinary-only matches as well as shared matches.
The hook already documents subscriber filtering before shared selection; it now
also runs when no shared filter matches. Keel uses this to exclude disconnected
ordinary local sessions when its Redis-backed distributed offline dispatcher is
enabled. This prevents the inline forwarded publish from persisting one Mochi
copy before Keel queues another copy for the same client.

No hook/default standalone behavior changes: Mochi still persists ordinary
offline QoS state when no external-cluster filtering hook removes that target.
Connected and inline clients remain eligible. Existing ordinary offline and
disconnect-during-persistence tests continue to cover the default behavior.

## Distributed shared-subscription selection (v0.2.46)

`packets.Packet.SharedFilters` and `Server.PublishWithSharedFilters` carry
internal per-publish routing metadata. Nil preserves ordinary broker
selection; an explicit empty slice suppresses shared delivery on a node
reached only for ordinary/offline subscriptions. The Keel subscriber hook
filters shared groups before Mochi selects a local member. This metadata is
not an MQTT property and is not transmitted to devices. Packet copies
preserve the nil/empty distinction.

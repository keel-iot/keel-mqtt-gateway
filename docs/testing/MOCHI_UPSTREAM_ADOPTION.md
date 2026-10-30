# Mochi proposal review and selective backports — 2026-10-29

## Scope and source snapshots

Reviewed the open-proposal catalogue and the release candidate
[PR #543](https://github.com/mochi-mqtt/server/pull/543), pinned to
`3b7c3e698b46d38ab8e91d35f94c662047382d81` (`release/2.8.0`). Upstream
`main`/v2.7.9 was `5b7f94bde4072edf6db62b69345f9454cae4936f` at review time.
PR status and file snapshots are preserved in the local investigation
artifacts. Open proposals are not treated as released fixes.

This fork remains **v2.7.9 plus selected backports and Keel-specific patches**.
It has not been replaced wholesale with the release candidate, and no Go
dependency, chart, application version, or release tag was changed.

## Included in this batch

| Source | Result in this fork | Regression evidence |
|---|---|---|
| #543, supplemental commit `3b7c3e6` | Use the `packets.Code` extracted by `errors.As`; a wrapped hook refusal no longer panics. Preserve the existing QoS1 PUBACK / QoS2 PUBREC selection | `TestUpstreamWrappedRefusalPreservesQoSAck` |
| #497 / #518, `91d251c` | Successful QoS1 PUBACK uses reason 0x00, not the SUBACK granted-QoS code | `TestUpstreamQoS1SuccessReasonIsValidPubackCode` forces the reason byte onto the wire using a property |
| #512, `602019e` | Request Problem Information=0 does not strip application User Properties from PUBLISH; ACK properties remain filtered | `TestUpstreamProblemInfoDoesNotStripPublishProperties` |
| #521, `e7887cd` | An unregistered topic alias with no topic causes a protocol-error disconnect instead of acknowledging an empty-topic publish; empty mappings are not registered | `TestUpstreamUnregisteredTopicAliasIsRefused`, existing registered-alias tests |
| #527, `09fb1f8` | Distinguish absent CONNECT limits from explicit zero Receive Maximum / Maximum Packet Size, and reject a zero Subscription Identifier | `packets.TestUpstreamConnectDistinguishesAbsentAndZeroLimits`, `packets.TestUpstreamSubscribeRejectsExplicitZeroIdentifier`, `TestUpstreamZeroLimitsAreRefusedOnWire` |
| #536 (`d46e405`) and #543 supplemental correction | MQTT 3.1/3.1.1 server disconnect closes TCP without sending the MQTT5 DISCONNECT; PassiveClientDisconnect cannot leave a v3 connection open | `TestUpstreamDisconnectRespectsProtocolAndPassiveMode`, updated takeover/wire fixtures |
| #539, `d31d54e` | Read retained particle paths under their particle locks, matching the writers | `TestUpstreamRetainMessageWhileScanningDoesNotRace`, adapted from ChristophNetsch's upstream regression |
| #519 (`1b35e0f`) and direction hint from #548 | A client's QoS1 PUBACK is stateless: no temporary inbound inflight or publish-storage callback. Preserve completion callbacks with `Packet.InboundFlow=true`; Keel's Redis hook ignores that stateless callback, protecting equal-numbered outbound state and avoiding useless Redis work | `TestUpstreamInboundQoS1DoesNotPersistStatelessAck`; live-Redis `TestRedisStatelessInboundQoS1CannotDeleteOutboundDelivery` |

The last item is adapted to our existing directional maps and `in:`/`out:`
Redis fields. It does not import PR #548's alternative storage migration or
all of its quota-accounting changes. Incoming PUBACKs acknowledging a server
delivery still execute outbound completion normally. The Redis regression
checks legacy schema 1 and schema 2, confirms **zero Redis commands** for the
stateless callbacks, preserves an outbound record with the same identifier,
and then proves an actual outbound completion deletes it.

The ACL-denial test also takes the synchronization fix from #530: it joins
the worker before asserting on the stopped client. MQTT3 takeover expects a
closed connection with no server DISCONNECT; the MQTT5 wire fixture now
explicitly uses protocol version 5. These test updates retain protocol checks.

## Already present or equivalent

- Fabio's [#511](https://github.com/mochi-mqtt/server/pull/511) QoS2 rejection
  fix remains intact and is covered again by the wrapped-error regression.
- The malformed MQTT5 SUBSCRIBE bounds check (#524 / duplicate #555) was
  already present. No second copy was applied.
- The client-list deadlock correction (#489 / #507 / #525) was already
  present, as were the corresponding listener CloseAll/accept changes from
  #509 and our additional local shutdown tests. Later lifecycle proposals
  still need separate integration review.
- Directional inflight maps and inbound SUBSCRIBE/UNSUBSCRIBE identifier
  checks already cover the central concern of #514 / #546 / #548.
- Local `GetAll` timestamp ordering and the rewritten `NextImmediate`
  already avoid the changes addressed by #529 and #520; no single-map
  implementation was copied over the directional one.
- The receive-packet oversize response path from #528 and gorilla/websocket
  1.5.3 from #533 were already present. Existing expiry fix #460 also remains.

## Deferred proposals and reasons

| Proposal | Assessment |
|---|---|
| [#506](https://github.com/mochi-mqtt/server/pull/506), pooled subscriber routing / Range | Promising for allocation reduction, but requires an ownership/lifetime audit of every `Subscribers` consumer and hook. Its pool reset does not know our additional `SharedSelectedGroups` state. Our single-copy shared gather remains in place. Requires a dedicated port and before/after profiles |
| [#552](https://github.com/mochi-mqtt/server/pull/552), ACK coalescing / fast encoder | Relevant to syscall and CPU costs, but rewrites Read/Write buffering and ACK flushing. Must be reconciled with our queue, packet-encoding hooks, QoS persistence and failure boundaries, then measured under burst, slow-hook and disconnect loads |
| [#553](https://github.com/mochi-mqtt/server/pull/553), ACK after hook settlement | Includes the batching changes and adds a new delivery/acknowledgement contract. Requires connector and persistence design review, not just a mechanical import |
| [#513](https://github.com/mochi-mqtt/server/pull/513), per-filter OnSubscribe refusals | Tested in isolation, then deliberately excluded. Keel's cluster/backfill callback currently handles all filters, and Redis persists each returned filter. Importing only the broker half could register a refused filter elsewhere. Needs coordinated callback filtering and integration tests |
| #534, report whether unsubscribe removed an entry | Changes method-value/interface signatures and callback/count semantics. Needs reconciliation with cluster and Redis cleanup, not a bulk copy |
| #490 / #549, capability announcement / retained subscription identifier | Candidate for a further protocol batch with configured-limit and retained-replay wire tests. Our directional SUBSCRIBE packet-ID check is already present; the remaining changes were not imported here |
| #496 and remaining #548 quota changes | Need a separate per-direction quota/idempotency review against the current fork; the release candidate explicitly excludes #496 |
| #531, bounded socket/WebSocket writes | Operationally useful, but introduces timeout configuration and changes when slow clients are disconnected. Requires transport and slow-reader validation before choosing defaults |
| #540 / #550 / #551 / #542, additional shutdown and WebSocket lifecycle changes | Overlap local lifecycle patches. Keep for a dedicated merge with direct EstablishConnection, late accept, silent TCP and WebSocket shutdown tests |
| #517 / #523 / #535 | Additional $SYS refusal behavior, broader subscriber-selection hook dispatch, and refused-CONNECT observability. Reviewed as further wire/hook integration work; not required by this batch |
| #476 / #508 | Alternative implementations overlapping narrower fixes already present; no duplicate import |
| #461 | Lowering the maximum pending queue to 64 is unnecessary for the allocation fix: our lazy queue retains the configured/default capacity of 8,192 |
| #442 | Expiry intent is already covered by merged #460 in v2.7.9 |
| #402 / #421 / #473 / #487 | Session-reload hooks, dynamic hook removal, Docker TLS configuration and QUIC add features outside this batch; Keel has its own lifecycle/configuration integration |
| #493 / #516 / #522 / #526 / #532 | Logging and test synchronization overlap local safety changes or unrelated tests; no blanket overwrite of local code |
| #468 / #505 | Upstream README spelling and dependency bump are outside this fork-behavior batch. Dependencies were not downgraded to the release candidate's versions |

## Validation and limits

Before backporting, the new protocol tests reproduced the missing behavior,
including the wrapped-error panic. The retained-message race reproduced under
`-race`. The separate Redis regression reproduced deletion of an equal-ID
outbound record in both schemas before the Redis guards were added.

Checks cover the complete fork and Keel unit suites, Keel's suite with a
dedicated real Redis, targeted race repetitions (including prior lazy-queue,
shared-fallback and offline-delivery tests), wire refusals, build/vet and the
complete standalone Paho run. The report runner retains its evidence-backed
HARNESS classifications; other failures remain failures.

This is not a new production cluster/failover campaign or a throughput claim.
The Redis command elimination is checked directly; its CPU/latency impact
still needs a representative production-style benchmark.

Snapshots, before/after failures, test logs and the task-only patch are kept
under `/home/fabio/keel-benchmark-lab/profiling/upstream-adoption-20261029/`.
The original pre-existing checkout changes are preserved. No commit, push,
release tag or production deployment is part of this work.

The final complete standalone report has MQTT 3.1.1 **10 PASS / 0 FAIL** and
MQTT 5 **25 PASS / 0 FAIL / 2 HARNESS** (flow-control and the documented
subscription-options barrier race). The offline-delivery and publication-expiry
regressions remain passing. The separate, previously unresolved session-expiry
flake in the feature matrix has not been reclassified or claimed fixed.

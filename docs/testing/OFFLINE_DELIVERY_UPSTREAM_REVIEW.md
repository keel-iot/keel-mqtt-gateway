# Offline delivery: upstream comparison and local regression

Reviewed on 2026-10-29 before changing delivery behavior. Mochi's upstream
`main` and latest release `v2.7.9` both resolve to
`5b7f94bde4072edf6db62b69345f9454cae4936f`.

## Findings

Upstream `publishToClient` stores QoS state before checking whether the
subscriber has a connection. On a disconnected subscriber it returns
`CodeDisconnect` but **keeps the inflight** for session recovery. Keel's
shared-fallback patch added rollback to this branch for every delivery,
including ordinary persistent subscriptions. That local change deleted the
messages that the offline-queueing tests expected to recover.

The local MQTT 5 publication-expiry failure also received zero messages;
it did not establish incorrect expiry arithmetic. Restoring the ordinary
offline inflight behavior makes that test pass too.

The correction uses an explicit shared-attempt mode only from
`publishSharedToSubscribers`. Ordinary delivery retains upstream's offline
storage behavior. Disconnected shared candidates still roll back tentative
QoS state before fallback, including when the disconnect occurs during the
persistence callback. Existing inflight messages remain intact.

No dependency version was changed. These upstream items were inspected:

- [Release v2.7.9](https://github.com/mochi-mqtt/server/releases/tag/v2.7.9)
  already contains [message-expiry PR #460](https://github.com/mochi-mqtt/server/pull/460).
  The relevant behavior was already present in this fork.
- [PR #461](https://github.com/mochi-mqtt/server/pull/461) proposes lowering
  the pending-write default to 64. It is open/unmerged; lowering capacity is
  different from our lazy storage with unchanged capacity.
- [PR #506](https://github.com/mochi-mqtt/server/pull/506) proposes pooled
  subscriber routing and iterators. It is open/unmerged and relevant to
  future allocation work, not a fix for this offline delivery regression.
- [PR #548](https://github.com/mochi-mqtt/server/pull/548) addresses independent
  inbound/outbound packet-identifier spaces. It is open/unmerged; the local
  fork already has a directional inflight patch. It was not imported here.

## Evidence

`TestOrdinaryOfflineDeliveryPreservesQoSState` exercises MQTT 3.1.1/5,
QoS1/2, and a stopped client or missing connection through subscriber routing.
It checks payload, expiry, inflight accounting, quota, persistence callbacks,
and absence of transport enqueue. All eight cases failed before the fix.
A test-only adaptation to the unmodified upstream implementation passes.

`TestDisconnectDuringPersistenceSeparatesOrdinaryAndSharedDelivery` checks
both QoS levels after a disconnect in the persistence callback. Ordinary
deliveries retain the new inflight; shared attempts undo only that new
inflight and restore quota. An older inflight must survive both paths.

Restoring the unconditional rollback in an isolated copy makes the regression
tests fail; restoring the fix makes them pass. The targeted race tests run
20 repetitions, including the existing shared-fallback tests.

Logs in `/tmp`: `keel-offline-before.log`, `keel-offline-after.log`,
`keel-offline-upstream-comparison.log`, `keel-offline-mutation-fails.log`,
`keel-offline-mutation-restored.log`, `keel-offline-race.log`, and
`keel-offline-conformance-after.log`. The original failing Paho reports are
preserved in `/home/fabio/keel-benchmark-lab/profiling/memory-optimized-conformance-artifacts`.

This investigation tests standalone protocol behavior and local shared
fallback. It does not certify the separate Core/Edge/Redis recovery path.

## Additional external-suite races

The first corrected run passes the three original failing cases but exposes
a request/response failure; the next run passes request/response and exposes
the initial No Local assertion in subscription-options testing. Both test
methods wait on A's already-received SUBACK after B subscribes. A controlled
TCP proxy reproduces both races on unmodified upstream Mochi and the local
fork. Waiting on B's actual SUBACK makes both complete tests pass on both
brokers, without changing any assertion or production code.

See `test/conformance/evidence/subscription_barrier_race.md`. The report runner
classifies only the documented empty-message assertion and pinned source
location as HARNESS, with evidence required; other failures in either method
remain FAIL. Its self-tests cover exact signatures, different assertions,
missing traces, and missing evidence. The existing unresolved session-expiry
flake remains unchanged and is not reclassified by this work.

## Final validation

The final complete standalone run exits zero: MQTT 3.1.1 has 10 PASS,
0 FAIL; MQTT 5 has 25 PASS, 0 FAIL, 2 HARNESS (flow-control and the precisely
matched request/response barrier race). Both offline-queueing tests and
publication expiry pass. The subscription-options test passes in this run;
its earlier observed failure is preserved and independently reproduced.

The full main-module and fork unit suites, build, vet, targeted race tests
(20 repetitions), mutation check, and 13 report-runner self-tests pass.
Database-dependent unit tests without `TEST_DATABASE_URL` and the separate
build-tagged multi-container cluster suite were not exercised here.

Final artifacts are preserved in
`/home/fabio/keel-benchmark-lab/profiling/offline-fix-20261029/`, including the
task-only code patch, upstream comparison, controlled proxy outcomes, complete
suite log, and `final-mqtt_3_1_1.json` / `final-mqtt_5.json`.

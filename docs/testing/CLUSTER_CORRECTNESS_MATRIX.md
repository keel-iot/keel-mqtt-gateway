# Cluster Correctness Matrix — Phase 3 (Distributed Correctness) specification

This document belongs to **Phase 3 — Distributed Correctness** in
`ROADMAP.md`'s own six-phase numbering (Business Readiness → Feature
Correctness → Distributed Correctness → Chaos → Performance →
Comparative Benchmarking) — GitHub milestone #4. An earlier pass at
this work incorrectly referred to it as "Phase 2"; that was a mistake
in the task that produced it, not a change to the repository's
numbering, and has been corrected throughout this document. The
repository's existing phase structure is the source of truth and is
not being renamed or renumbered to match that mistake.

**Status: `Phase 3 specification ready — execution unblocked`.**
No Phase 3 test has been implemented or executed yet. Do not read this
as "Phase 3 started" — Phase 2 ("Feature Correctness," milestone #3)
completed 2026-08-10 (baseline commit
`24df14063efa3cf04c268d0e9671ee676125d4a6`, tag
`phase2-feature-correctness-baseline`), which removes the dependency
that previously blocked Phase 3 execution, but implementing the first
scenario below is a separate, not-yet-started task.

Phase 2's job was different from the Paho conformance suite: "does
every MQTT capability Keel claims to support have a production-relevant
test," systematically closing gaps in `FEATURE_MATRIX.md` — SUPPORTED →
concrete test → verified production behavior → regression coverage, for
each. Testing "does a persistent session survive Edge loss" (Phase 3)
before establishing "does a persistent session work correctly under
normal conditions at all" (Phase 2) would have built Phase 3 scenarios
on an unverified foundation; that risk is now closed.

Phase 3 question: *are the distributed correctness properties Keel
claims actually true under deterministic cluster transitions and
failures?* This is deterministic correctness testing — no chaos, no
random fault injection, no performance measurement. Those are Phase 4
and Phase 5 (`ROADMAP.md`).

Everything below is derived from reading the current source
(`internal/cluster/**`, `internal/broker/hooks.go`, `internal/session/**`,
`cmd/server/main.go`), reviewed 2026-08-10 against baseline commit
`88bd2ac406402f0fe748224f941adcb98438338d`, **not** from any prior ADR
or design doc — several places below where the code has diverged from
what a design doc would lead you to expect are called out explicitly,
because that divergence is itself the point of this exercise.

## 1. Source-verified architecture summary

Condensed from a full source review; every claim below has a
file:function reference and was independently re-derived from the code,
not carried over from documentation.

### Control plane (Raft)

- **Bootstrap/join**: exactly one node self-elects via `--bootstrap`
  (`raft/node.go:Bootstrap`, no-ops if the on-disk log is non-empty).
  Every other core is added as a voter by whichever node is currently
  raft leader, reacting to gossip (`membership.go:maybeAddVoter`) with a
  periodic reconcile backstop every 2s (`reconcileVoters`). There is no
  separate "join RPC" in the raft package.
- **Leader loss**: no Keel-specific handling — hashicorp/raft's own
  election runs untouched. Writes (`registry.go:apply`, 2s timeout) fail
  with `ErrNotLeader`/`ErrLeadershipLost` during the window and fall
  back once to a gRPC call against the currently-known leader address;
  reads are pure FSM reads, unaffected.
- **Quorum size**: never Keel-hardcoded; it's whatever hashicorp/raft
  computes from its own configuration. The one voter-count Keel computes
  itself (`redis_failover.go:coreVoterCount`) is used only for the Redis
  failover "is this a real multi-core cluster" guard, not raft quorum.
- **Quorum loss**: **no automatic detection or recovery exists** —
  `lifecycle/monitor.go`'s own doc comment states this is deferred to a
  later phase. The only quorum-loss recovery path is the offline,
  manual `keel-gateway restore raft` CLI (`raft.RecoverCluster`).
- **Restart**: BoltDB log + file snapshot store reopen automatically
  (hashicorp/raft's own replay); no bespoke reload code, and no
  corrupted-state repair path — a bad file is a fatal `os.Exit(1)`.
- **FSM commit set** (`fsm.go:Apply`): session ownership, ACL
  roles/bindings, enabled rulesets, Redis-primary designation,
  certificate revocation. **Not** "cluster metadata only" as a casual
  read of some docs might suggest — ACL and cert-revocation state live
  here too, deliberately (fail-closed).
- **Rejoin**: no raft-specific rejoin logic; gossip has its own explicit
  `rejoinIfIsolated` loop (every 3s) that re-joins the seed list if no
  core peer is visible — raft voter membership is then re-derived from
  gossip once it recovers, not the other way around.
- **Graceful shutdown**: `lifecycle.Drain` — leadership transfer (if
  leader) + gossip `Leave` (5s timeout) — is invoked **only** via
  `POST /api/cluster/drain` / the `drain` CLI (wired as the Helm chart's
  `preStop` hook), **never** on a bare SIGTERM/SIGINT. It does not
  remove the node from the raft configuration.

### Edge membership, live/offline session ownership

- **Live ownership claim**: CONNECT → `OnConnectAuthenticate` →
  `claimClusterSession` → raft `Apply(OpClaimSession)`. **Strongly
  consistent** — this is a real raft-committed write, not gossip/AP.
- **CONNECT race arbitration**: real, race-safe. Both claims serialize
  through the single raft log; the FSM's own comment states "new
  connection always wins" — decided deterministically in `Apply`, not
  left to caller timing.
- **Eviction of the prior owner**: `Apply` returns the evicted node;
  `claimClusterSession` fires an async, **best-effort, no-retry** gRPC
  `Evict` call. If that call is lost, the stale connection is cleaned up
  only by its own MQTT keepalive eventually timing out — not by any
  follow-up kick.
- **Offline ownership**: a distinct mechanism from live ownership —
  rendezvous (highest-random-weight) hashing over live edge node IDs
  (`session/placement.go:Owner`), consensus-free (every node computes
  the same answer independently); only the resulting placement write
  is persisted (Olric, not raft).
- **Reassignment on node death**: deterministic **given** a converged
  membership view, but not instant — bounded by gossip detection time
  plus the reconciler's ~20s±25% jittered tick
  (`session/reconciler.go`).
- **Verified gap**: no code purges a dead **edge** node's stale
  live-subscriber routing entries — `RoutingSweep` explicitly
  logs-never-deletes; `lifecycle.Monitor`'s purge path is core-node-only.
  A route to a dead edge can persist until something else overwrites it.

### Routing / Olric

- Subscribe writes an Olric key (never read back, timestamp only) and
  publishes a `routeEvent` on a pub/sub channel every Router instance
  consumes into its own in-memory trie; a 10s full-scan reconcile is the
  convergence backstop if a pub/sub message is dropped.
- Unsubscribe is a real, synchronous Olric delete, not just eventual.
- Keel's own `routing.Router.index` is a **second, separate**
  `mochimqtt.TopicsIndex` instance from mochi-mqtt's own per-node
  `Server.Topics` — kept in sync only via `routing.Reconciler`'s
  periodic diff, not shared state.
- **No TTLs anywhere** in the routing store — removal is always
  explicit (`Delete`/`Unsubscribe`/`PurgeNode`), never auto-expiry.
- Live and offline routing are enforced as genuinely separate,
  non-colliding key namespaces (`$offlineroute/`, base64'd
  `$offline/` ownership keys) — real code, not a comment-only intent.
- Redis-primary failover: gossip-triggered (primary absent >30s),
  raft-voter-count-gated ("is this really multi-core"), candidate
  picked unilaterally by whichever core is raft leader, but the
  resulting fact is written via a real raft `Apply` (quorum-committed).

### Data plane / QoS / persistence

- Cluster forwarding (Edge→Edge) is always direct point-to-point gRPC,
  **never** through raft — only the routing-table *lookup* is
  raft-replicated data; the forward itself is a brand-new local publish
  on the receiving node, not a passthrough of the original packet.
- **Verified gap, real message loss**: if a forward to a live-only
  subscriber's node fails (dead/partitioned), it is logged and dropped
  — **no retry, no queue, no fallback.** (An offline-owned copy on the
  same dead node is unaffected, since `DeliverOffline` already ran
  locally on the *sender* before any forward attempt.)
- QoS1/2 inflight state is persisted to Redis synchronously, inline on
  the publish/ack path (`OnQosPublish`/`OnQosComplete` — blocking
  `HSet`/`HDel`), not backgrounded.
- Inflight recovery on reconnect (possibly to a different Edge) is
  real: a "ghost" `mqtt.Client` is seeded from Redis-scanned inflight
  state on `OnSessionEstablish`. This is **reconnect-triggered only** —
  no proactive background sweep exists.
- **Verified gap, real risk**: offline-queued messages get packet IDs
  from a Redis-global, cluster-wide `INCR` counter; mochi-mqtt's own
  live-connection packet-ID counter is separate, in-memory, and never
  seeded from the Redis value on reconnect. Nothing in the code
  prevents the two counters from allocating colliding/duplicate packet
  IDs across a reconnect-to-a-different-node with queued offline
  messages.
- Retained messages: Redis-backed, immediately cluster-wide visible on
  write (no propagation delay) — but retained-message *delivery* on
  subscribe is QoS0-only; full QoS1/2 retained redelivery is explicitly
  not implemented ("would require mochi-mqtt's private inflight
  machinery, not reachable from this package").
- **Verified gap**: `SessionExpiryInterval` elapsing deletes only
  local in-memory state and unsubscribes cluster routes — it never
  touches Redis. The subscription/inflight/packet-ID-counter Redis keys
  for an expired session are orphaned indefinitely.
- `DeliverOffline` is a genuine forward-to-rendezvous-owner mechanism
  (reuses the same gRPC forwarder as live delivery, not a separate
  path), with real, explicit deduplication (`MarkDelivered`, Redis
  `SetNX` with TTL) guarding the "ownership moved mid-delivery" race —
  except when `PublishID` is empty/unparseable (e.g. a peer mid
  rolling-upgrade), where dedup is skipped by explicit design, accepting
  at-least-once duplication in that one case.

## 2. Accepted invariants

Only invariants the source above actually supports. Each links to §4's
scenario(s).

**A. Live session ownership**
- **A1.** At any instant, the raft-committed live-ownership record for
  a given ClientID names at most one node. *(Strongly verified — single
  raft log, deterministic Apply.)*
- **A2.** A newly-committed CONNECT for a ClientID always supersedes
  the previous live owner's ownership record. *(Verified — FSM's own
  "new connection always wins" logic.)*
- **A3.** Eviction of a superseded live connection is best-effort and
  not guaranteed to complete promptly — the only guaranteed backstop is
  that node's own MQTT keepalive timeout. *(Deliberately a weak
  invariant — matches the verified no-retry design, not an aspiration.)*

**B. Offline ownership**
- **B1.** For a fixed, converged live-edge membership view, `Owner(clientID)`
  is deterministic and identical on every node. *(Verified — pure
  rendezvous hash, no shared state needed to agree.)*
- **B2.** Removing a non-owner node from the live-edge view does not
  change any other ClientID's resolved owner. *(Verified — rendezvous
  hashing's defining property.)*
- **B3.** Losing the current owner eventually (not instantly) causes
  reassignment to a new deterministic owner, bounded by gossip
  propagation + the reconciler's tick interval. *(Verified, with the
  timing bound stated explicitly rather than implied as instant.)*

**C. Routing correctness**
- **C1.** A live subscription eventually becomes routable from every
  node that later needs to forward to it, via either the pub/sub event
  or the 10s scan reconcile backstop. *(Verified.)*
- **C2.** An unsubscribe removes the corresponding route synchronously
  (not just eventually). *(Verified.)*
- **C3.** Live and offline routing entries never collide in the
  same key space. *(Verified — explicit prefixing/base64 encoding, real
  code.)*
- **C4 — accepted with an explicit caveat, not a clean invariant:**
  routes to a *dead core* node are eventually purged
  (`lifecycle.Monitor`); routes to a *dead edge* node are **not** —
  this asymmetry is real and must be tested as such, not smoothed over.

**D. QoS / session durability**
- **D1.** QoS1/2 inflight state for a message accepted by an Edge
  survives that Edge's death, recoverable on the client's next
  reconnect (to any Edge), via Redis-persisted inflight state.
  *(Verified — but recovery is reconnect-triggered only, no background
  sweep; state that never reconnects sits in Redis indefinitely, see D3.)*
- **D2.** Cluster-forwarded delivery to a live subscriber is
  **at-most-once and not guaranteed** under a partition/dead-node
  forward failure — there is no retry. This is stated as the current
  true behavior, not as a target to defend. *(Verified gap, kept as an
  invariant about actual behavior so a regression — e.g. someone adding
  a silent double-delivery — can also be caught.)*
- **D3 — cluster-only remainder.** Session expiry does not clean up
  the Redis packet-ID counter key. *(Verified gap. Its sibling —
  subscription/inflight key orphaning, which reproduces with zero
  clustering — has been reclassified to Phase 2/`FEATURE_MATRIX.md`,
  see §4 risk #4; this line is deliberately narrowed to the part that
  actually requires cluster topology to exist.)*

**E. Core/quorum safety**
- **E1.** Losing a single follower (with 3 cores) does not interrupt
  leader-held writes. *(Verified — this is hashicorp/raft's own
  documented behavior; Keel adds nothing on top and needs no
  Keel-specific test beyond confirming the library behaves as
  documented in this deployment's exact configuration.)*
- **E2.** No live-ownership or ACL write commits without quorum.
  *(Verified — every such write is a raft `Apply`, which hashicorp/raft
  itself refuses without quorum.)*
- **E3 — explicitly NOT an invariant Keel enforces:** automatic
  detection of or recovery from quorum loss. There is no code for this;
  recovery is a manual, offline `RecoverCluster` operation. Any Phase 3
  scenario here tests the *manual* recovery procedure, not automatic
  self-healing, because self-healing doesn't exist.

**F. Membership / reconciliation**
- **F1.** `session.Reconciler`'s offline-ownership correction pass is
  idempotent — re-running it against an already-converged state performs
  no writes. *(Verified with an existing test:
  `internal/session/reconciler_test.go`'s
  `TestReconciler_UnchangedOwner_DoesNotCallPlace`.)*
- **F2.** A non-owner edge node's departure does not trigger any
  ownership/routing write for sessions it didn't own. *(Verified — same
  rendezvous property as B2, restated for the membership-loss angle;
  also directly exercised at the reconciler level by
  `internal/session/reconciler_test.go`'s
  `TestReconciler_MembershipChange_MovesOnlyAffectedSessions`.)*

**G. Data-plane forwarding**
- **G1.** Forwarding to a resolved node is always a direct gRPC call,
  never requiring raft consensus. *(Verified.)*
- **G2.** Duplicate offline delivery from a mid-reconciliation
  ownership move is bounded by the dedup mechanism, except when
  `PublishID` is unparseable, where at-least-once duplication is
  accepted by explicit design. *(Verified — stated with its real
  exception, not as an absolute guarantee.)*

## 3. Rejected / modified candidate invariants

| Candidate (from the task prompt) | Verdict | Why |
|---|---|---|
| "Raft elects a leader" | Rejected as an invariant to test | Bad invariant by the task's own example — it's a hashicorp/raft library property, not a Keel product claim. What Keel-specific correctness properties *depend* on leader election are A1/A2/E2 above; those are tested instead. |
| "Olric contains the route" | Rejected as an invariant to test | Same reasoning — implementation detail. C1 (the product-level "a subscription eventually becomes routable") is the real invariant. |
| "Restoring quorum allows progress without violating previous committed decisions" | Modified → E3 | No Keel code implements automatic quorum-loss detection/recovery at all — there is nothing "automatic" to test. The real, honest invariant is about the *manual* `RecoverCluster` procedure, and about hashicorp/raft's own guarantee (never contradict a previously-committed log entry) — which is a library property, not something to write a Keel-specific test claiming credit for. |
| "Zero-loss QoS1/2" (README previously claimed "Zero-Loss QoS 1/2" unconditionally) | Rejected as stated, replaced by D1/D2 | Verified false as an unconditional claim: cluster-forwarded live delivery has a real, uncontested message-loss path on forward failure (D2). The true, narrower invariant that *is* verified is D1 (Redis-persisted inflight survives the *accepting* Edge's death) — cluster fan-out to *other* subscribers is a separate, weaker guarantee. **`README.md` corrected 2026-08-10** to state the narrower, actually-verified claim and link this gap directly, rather than waiting for Phase 3 execution — a known false public claim doesn't need to wait for the phase that discovered it. |
| "Dead nodes eventually disappear from routing decisions" | Split into C4 | True for core nodes, false for edge nodes. Stating it as one invariant would hide the asymmetry. |
| "No duplicate forwarding beyond what MQTT/QoS semantics allow" (task's own Part G candidate) | Modified → G2 | Mostly true, but the unparseable-`PublishID` exception is real and deliberate — the invariant must name the exception, not pretend it's absolute. |
| "Packet ID continuity is preserved as required" (task's own Part D candidate) | **Rejected outright** | Verified false: no code reconciles mochi-mqtt's live in-memory packet-ID counter with the Redis-global offline counter on reconnect-to-a-different-node. This is a real, open correctness risk (§5), not weakened into a passing invariant. |

## 4. Newly discovered correctness risks (not weakened into invariants)

These are genuine findings from this review, not from any injected
fault yet — recorded honestly per the task's explicit instruction not
to hide a discovered bug by softening the invariant around it.

1. **Packet ID collision risk across reconnect-to-a-different-node
   with queued offline messages** (§1, Data plane; rejected candidate
   in §3). **Scope check performed**: `DeliverOffline`'s own doc
   comment states it "no-ops when registry or store is nil (standalone
   mode...)" — verified from source, the Redis `PKID` counter this risk
   depends on is only ever written when `ClusterRegistry` is non-nil.
   It cannot manifest in a true single-node, non-clustered deployment
   at all, but it also does **not** require any node failure — a
   perfectly healthy cluster with ordinary reconnect-to-any-Edge
   traffic is enough. Stays **Phase 3** (already named explicitly in
   `ROADMAP.md`'s pre-existing Phase 3 scope, "Packet ID continuity" —
   this review confirms that scoping was correct, not just inherited).
   No test exists yet. Tracked: issue #14.
2. **Cluster-forwarded live delivery has no retry on a partitioned/dead
   target node** — silent, permanent loss for that one delivery
   attempt (D2). Requires cluster forwarding to exist at all — **Phase
   3**. `README.md`'s unconditional "Zero-Loss QoS 1/2" claim built on
   top of this has already been corrected (2026-08-10) to state the
   narrower, actually-verified guarantee rather than waiting for Phase 3
   execution to fix a known-false public claim. Tracked: issue #14.
3. **Dead edge nodes' stale routing entries are never purged** (C4).
   Requires a multi-node cluster to exist — **Phase 3**. Low urgency
   alone, but combined with #2 above, a dead edge can keep silently
   absorbing (and losing) forwarded messages indefinitely until an
   unrelated write happens to overwrite that specific route.
4. **`SessionExpiryInterval` leaves Redis-side session state orphaned**
   (D3) — **splits by scope, verified from source, not one risk**:
   - `keel:gw:SUB` (subscriptions) and `keel:gw:IFM` (inflight) keys are
     written by `RedisSessionHook`, which has **zero dependency on
     `ClusterRegistry`** (grepped: no reference to it anywhere in
     `internal/broker/redis_session.go`) — installed whenever
     `cfg.RedisClient` is configured, including a genuinely standalone,
     single-node deployment using Redis only for restart-survival. This
     part of the gap is real with **zero clustering involved** — a
     single-node MQTT/session-correctness bug, **Phase 2 scope**, not
     Phase 3. It belongs in `FEATURE_MATRIX.md`'s audit, not this
     document.
   - `keel:gw:PKID:<clientID>` orphaning is cluster-only (same gating as
     risk #1) — stays **Phase 3**, and compounds risk #1 if that packet
     ID space is ever reused after expiry.
5. **Concurrent per-Core `session.Reconciler` computation against
   possibly-diverging gossip views (B1/B2/B3)** — **risk hypothesis,
   not a FAIL**. Verified from source: `session.Reconciler` runs
   unconditionally on every Core (no `raft.IsLeader()` gate, unlike
   `redisFailoverLoop`'s single-arbiter pattern), each independently
   computing `Owner(clientID, liveEdges)` from its own gossip-derived
   `liveEdges` view and writing the result into the same shared
   Olric-backed store (`OfflineOwnership.Place`). If two Cores'
   gossip-derived membership views diverge even briefly (e.g. right
   after an Edge death, before gossip converges everywhere), they could
   independently compute different owners and race to write both into
   the same key. **Investigated, not reproduced**: the management API
   (`GET /api/cluster/routes`/`/api/cluster/sessions`) only exposes the
   post-write, Olric-replicated *result* — reading it from three
   different Core processes would observe the same shared value three
   times, not three independent computations, so it cannot prove or
   disprove agreement between the Reconcilers themselves without adding
   test-only instrumentation to production code, which was deliberately
   not done. Deferred to Phase 3's Session/QoS recovery rung: proof, if
   any, will come from an end-to-end MQTT-level scenario (offline
   publish → reconnect through a different Edge → correct, single
   delivery) rather than an artificial ownership-agreement probe.

None of these are fixed as part of this task. Each becomes its own
tracked issue (§6) with the invariant/test status left `FAIL` or
`MISSING TEST` — never silently marked resolved.

## 5. Result semantics for Phase 3

Same discipline as `CONTRIBUTING.md`'s MQTT conformance classification,
adapted:

- Public states: **PASS**, **FAIL**, **N/A**.
- **HARNESS** is available but expected to be rare — only for a
  genuine, evidenced defect in the *test harness* itself (e.g. the
  Testcontainers orchestration racing its own readiness check), never
  for an inconvenient result from the actual cluster. Same evidence bar
  as the MQTT conformance HARNESS rule: a written investigation, not an
  assertion.
- **UNRESOLVED/FLAKY** stays internal triage metadata only, never a
  public result. A flaky Phase 3 scenario is `FAIL` until proven
  reproducible one way or the other, exactly like `test_session_expiry`.
- A failed run is never silently promoted to PASS by a retry. Retries,
  if run at all for diagnosis, are documented separately from the
  authoritative result (same rule this task's own Part B applied to
  the MQTT conformance run).
- Per-scenario evidence uses the same structured template as
  `test/conformance/evidence/*.md`: **Requirement / Test / Environment /
  Expected / Observed / Evidence / Result.**

## 6. Deterministic scenarios

Each maps to §2's invariants. "Current test reference" is left blank
where none exists — no test is invented here, only specified.

### Core

| Scenario | Invariant(s) | Current test | Missing test |
|---|---|---|---|
| Start 3 Core nodes, confirm single leader, quorum-gated writes succeed | E1, E2 | none | **Missing** — needs Testcontainers harness (§7) |
| Kill a follower, confirm writes still commit | E1 | **`test/cluster/core_follower_death_test.go`'s `TestCoreFollowerDeath_ClusterStaysOperational`** (build-tag `cluster`, real 3-Core/2-Edge cluster, follower identified via a real `GET /api/cluster/nodes` read — never assumed from bootstrap order) — PASS, 3/3 clean runs. Verified: a brand-new CONNECT's ownership arbitration (real `raft.Apply`) still commits with 2/3 cores live; already-flowing cross-node MQTT traffic is undisturbed; an already-live session is not evicted by a Core-only event. Leader identity before/after is diagnostic only, never asserted. | — |
| Kill the leader, confirm a new leader is elected and writes resume | (library property, not Keel-specific — low priority) | none | Optional — confirms configuration, not Keel code |
| Lose quorum (kill 2 of 3), confirm writes fail closed, no split-brain commit | E2 | none | **Missing** |
| Restore quorum via `RecoverCluster`, confirm no committed decision is contradicted | E3 | `internal/cluster/raft/backup_restore_test.go`'s `TestRecoverCluster_RequiresVoters` and `TestBackupAndRestoreRoundTrip` cover input validation and the backup/restore data round-trip, but **not** the actual "recover after real quorum loss, confirm no committed decision contradicted" property | **Missing** — this is the real, honest version of the "restore quorum" scenario; existing tests are adjacent, not equivalent |
| Restart a Core with intact BoltDB state, confirm it resumes without re-election churn | (restart behavior, §1) | none | **Missing** |
| Restart a Core with deliberately corrupted BoltDB file, confirm fail-fast (`os.Exit`), not silent corruption | (verified gap: no repair path) | none | **Missing** — confirms the *documented absence* of recovery, not a new feature |
| Join/rejoin: kill all 3 Cores, restart, confirm gossip `rejoinIfIsolated` + `reconcileVoters` reconverge | (§1 rejoin) | **`internal/cluster/membership/rejoin_test.go`'s `TestRejoinIfIsolated_RepopulatesAfterRealDeathAndRestart`** (in-process gossip logic) plus, for the real container-restart path (one follower, narrowed from "all 3" per the ladder's atomic-scenario rule — see the Edge/session table's reuse note below): **`test/cluster/core_follower_rejoin_test.go`'s `TestCoreFollowerRejoin_ReconvergesAfterRestart`** — real `StopCore`/`StartCore` (container survives, real on-disk raft state), waits for observable convergence (management API back up, leader agreement with a never-stopped core, still-a-raft-voter), not sleeps. PASS, 3/3 clean runs. | A real "kill all 3, restart all 3" total-outage version is still missing — this proves single-follower rejoin, not simultaneous total loss |

### Edge / session ownership

| Scenario | Invariant(s) | Current test | Missing test |
|---|---|---|---|
| Simultaneous duplicate-ClientID CONNECT on two Edges, confirm exactly one committed owner and the loser gets evicted (eventually, via keepalive if the Evict RPC is dropped) | A1, A2, A3 | none | **Missing** — this is the single highest-value Phase 3 test given how strongly A1/A2 are already verified at the code level; a live test would catch a regression in the raft-Apply ordering itself |
| Owner Edge dies while session is live, client reconnects to a different Edge | A1, D1 | **`test/cluster/ownership_reconnect_test.go`'s `TestOwnerEdgeDies_ReconnectSucceedsOnAnotherEdge`** — PASS, 3/3 clean runs. **Verified:** owner-loss is detected (reconnect claim succeeds without waiting on dead-node detection); reconnect on a surviving Edge succeeds promptly; single-live-owner invariant preserved (functional post-reconnect session, real publish/deliver round trip). **Not verified by this scenario** (same reuse principle as the Core-follower-death row above — one scenario proves several invariants, not every consequence of an Edge dying): offline queued delivery, stale-route cleanup on the dead Edge, QoS inflight recovery — those belong to the routing/QoS sections below, not duplicated here. | — |
| Stale (evicted) owner's connection attempts to continue publishing after losing ownership | A3 | none | **Missing** — specifically exercises the "no retry, keepalive-only" backstop |
| Core leader changes mid-CONNECT arbitration (kill leader between the local `Apply` attempt and its retry) | A2, E2 | none | **Missing** — timing-sensitive, needs deterministic control over when leadership changes, not a race |

### Offline ownership / routing

| Scenario | Invariant(s) | Current test | Missing test |
|---|---|---|---|
| Fixed 3-Edge membership, confirm `Owner(clientID)` computed identically on all 3 | B1 | `internal/session/placement_test.go`'s `TestOwner_Deterministic` (single-process, pure-function level) | Cluster-level (real multi-process) version missing |
| Remove a non-owner Edge, confirm unrelated ClientIDs' ownership is unchanged | B2, F2 | `internal/session/placement_test.go`'s `TestOwner_MinimalDisruption` and `internal/session/reconciler_test.go`'s `TestReconciler_MembershipChange_MovesOnlyAffectedSessions` (both fake-store/single-process level) | Cluster-level (real multi-process) version missing |
| Kill the owning Edge, confirm reassignment happens within a bounded window (gossip + reconciler tick) | B3 | none | **Missing** |
| Subscribe on Edge A, publish through Edge B, confirm delivery | C1 | **`test/e2e/cross_node_test.go`'s `TestCrossNodeDelivery`** (build-tag `e2e`, real docker-compose 3-node cluster, real authenticated MQTT clients, wildcard subscribe across nodes) — already exists, already exercises this exact invariant. Independently reconfirmed via the Testcontainers harness (§7, issue #16): `test/cluster/happy_path_test.go`'s `TestMultiNodeHappyPath` (build-tag `cluster`, 3 Core + 2 Edge, self-contained per-run cluster, no pre-existing docker-compose stack) — PASS, 3/3 clean runs. | — |
| Unsubscribe, then publish from a remote Edge, confirm no delivery | C2 | none — `TestCrossNodeDelivery` has no unsubscribe path | **Missing** |
| Kill destination Edge mid-flight, publish from another Edge, confirm the *documented* loss (D2) is what actually happens — not silently swallowed as a pass | C4, D2 | none | **Missing — this is the test that turns §4 risk #2 from "discovered in review" into "continuously regression-tested"** |
| Add a new Edge, confirm it becomes a valid forward target only after routing convergence, not before | C1 | none | **Missing** |

### QoS / session durability

| Scenario | Invariant(s) | Current test | Missing test |
|---|---|---|---|
| QoS1 publish accepted by Edge A, kill Edge A before PUBACK reaches the original publisher, confirm Redis-persisted inflight recoverable on reconnect | D1 | none | **Missing** |
| QoS2 handshake interrupted at PUBREC/PUBREL/PUBCOMP boundaries, Edge death at each stage | D1 | none | **Missing**, 3 sub-scenarios (one per boundary) |
| Persistent session reconnects to a different Edge after accumulating both live and Redis-queued-offline packet IDs, confirm whether a collision occurs | (§4 risk #1 — expected to currently FAIL or reveal the gap) | none | **Missing — this is the test that proves or disproves risk #1; expect it to fail today** |
| SessionExpiryInterval elapses, confirm exactly what is and isn't deleted (local state: yes; Redis: no, per D3) | D3 | none | **Missing — written to assert the *actual*, documented-gap behavior, not an aspirational full cleanup** |

### Storage

| Scenario | Invariant(s) | Current test | Missing test |
|---|---|---|---|
| Redis temporarily unavailable during a publish, confirm behavior matches `OnQosDropped`'s documented fail path, not a crash | D1-adjacent | none | **Missing** |
| Redis restart, confirm reconnection and that in-flight-at-restart state isn't silently lost | D1 | none | **Missing** |
| Redis primary failover (kill primary, confirm gossip-triggered promotion + raft-committed `SetRedisPrimary`) | (§1 Redis failover) | **`internal/cluster/membership/redis_failover_test.go`'s `TestReconcileRedisPrimary_FailoverOnPrimaryMissingBeyondThreshold`, `_HealthyPrimaryReconfiguresReplicas`, `_SingleCoreGuard`, `_FailoverSkipsWhenPromoteFails`** — unit-level with a fake raft/registry, covers the decision logic thoroughly | Real multi-container version (actual Redis containers, actual kill) still missing |

Every "Missing test" row above is a candidate for implementation once
Phase 3 moves from specification to execution — not part of this task.

## 7. Test environment design

**Status: implemented, issue #16 closed.** `test/cluster/` (build-tag
`cluster`) brings up a real, multi-process Keel cluster via
Testcontainers-for-Go and tears it down per test — no pre-existing
Kubernetes or docker-compose stack required. First scenario proven
(§6's "Subscribe on Edge A, publish through Edge B" row): `go test
-tags cluster ./test/cluster/...`.

One real, production-relevant bug surfaced while building the harness
and is now fixed: `internal/cluster/membership`'s raft-driven Redis
failover reconciler issues a self-referential `SLAVEOF` when multiple
core nodes report the same co-located Redis address — the harness now
runs one Redis instance per core (matching
`docker-compose.core-edge-split.yml`'s real topology) to avoid it, and
the Dockerfile's `go mod download` ordering bug (missing
`thirdparty/mochi-mqtt-server/` before `go mod download`) was found and
fixed independently of any test harness — a real, currently-broken
build until this fix.

One matching-semantics gap was also found and filed as **issue #19**
(child of #12, Phase 3 milestone, non-blocking for #10):
`internal/cluster/routing.Router`'s Olric-backed subscription matching
does not match a bare parent-level topic (e.g. `"telemetry"`) against a
`"#"` filter the way mochi-mqtt's own local, single-node matching does
— a cross-node delivery miss for a topic shape `isAllowedPublish`
otherwise permits. Worked around in `TestMultiNodeHappyPath` by
publishing a Hono-shaped sub-path instead (same convention
`cross_node_test.go` already uses). Not fixed here — tracked in #19
for when the routing/data-plane rung of the ladder is reached.

Goal: a developer runs `go test -tags cluster ./test/cluster/...` and
gets a real, multi-process Keel cluster up, exercises one deterministic
scenario, and tears it down — no pre-existing Kubernetes cluster
required.

**Primary approach: Testcontainers for Go.**
- Containers: N Core containers (real `keel-mqtt-gateway --role=core`
  binary/image), M Edge containers (`--role=edge`), 1+ Redis
  container(s) (primary + optional replica to match production
  topology), a throwaway Postgres (already required unconditionally by
  `internal/db.Migrate`, per the existing conformance harness's own
  pattern in `test/conformance/run.sh`).
- Networking: Testcontainers' own Docker network per test run: deterministic,
  isolated, and already how `test/conformance/run.sh` isolates its own
  throwaway Postgres — same pattern extended to a multi-container topology.
- Deterministic process control: kill = `docker stop`/`container.Terminate`
  on a specific named container, not a random target — this is what
  keeps Phase 3 "deterministic," per the task's own boundary. Restart =
  `docker start` on the same container so its Docker volume (BoltDB/Redis
  data) survives, to test the intact-state-restart scenarios in §6.
- Clean storage between scenarios: fresh named volumes per test case,
  torn down in `t.Cleanup`, mirroring `test/conformance/run.sh`'s own
  `trap cleanup EXIT`.
- Observability: container logs collected via Testcontainers' own log
  consumer API into per-test artifact files (mirrors
  `test/conformance/artifacts/`'s existing convention); metrics scraped
  from each container's `/metrics` endpoint at test end for
  post-hoc inspection, not live assertions (Phase 5's job).
- Artifact collection: same `test/conformance/evidence/*.md` convention,
  one file per scenario, structured per §5.

**Explicit boundary — deferred to Phase 4, not designed here:**
Toxiproxy/network-partition simulation, packet loss/latency injection,
random kill timing. The only "network transition" Phase 3 needs is
container stop/start and Docker network connect/disconnect for the
*specific*, deterministic partition scenarios in §6 (e.g. "Edge is
unreachable from other Edges but reachable from Core" if a scenario
genuinely needs that distinction) — narrow, named, reproducible, never
randomized.

## 8. What this document is not

Not a chaos-testing plan (Phase 4). Not a performance benchmark plan
(Phase 5). As of the Testcontainers harness (§7) and the first §6
scenario going PASS, this is no longer a pure design document — but
most of §6 is still unimplemented, and nothing here claims an
invariant in §2 has been *disproven*: the risks in §4 not yet
reproduced by a running scenario remain read-from-source only (that
reproduction is exactly what the rest of Phase 3 execution is for).

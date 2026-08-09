# Keel MQTT Gateway — Roadmap

Six phases, in order. Not because later phases matter less, but because
each one needs the previous one's ground truth to mean anything: chaos
testing without known failover invariants just produces "we broke
things and it seemed fine"; a benchmark without correctness testing
first measures how fast a broker loses messages.

```
1. Business readiness      — close the functional no-gos
2. Feature correctness      — every claim has a test
3. Distributed correctness  — cluster, ownership, persistence, failover
4. Failure correctness      — reproducible chaos
5. Performance              — know the real limits and scaling curve
6. Competitive benchmarking — public, reproducible comparison
```

**1–2 say what Keel can do. 3–4 say whether Keel can be trusted when
things go wrong. 5 says how far it scales. 6 says how it compares.** For
an enterprise evaluator, 3 and 4 will matter more than a raw msg/s
number — that's specifically where the Core/Edge architecture has to
earn its complexity, not just claim it.

Same discipline throughout, already established for MQTT conformance
(see `FEATURE_MATRIX.md`, `CONTRIBUTING.md`): **no `PASS`/`SUPPORTED` row
without a referenceable test.** A matrix that can't be traced back to a
real test is a brochure, not evidence.

## Phase 1 — Business readiness: close the functional no-gos

Not feature parity with EMQX — the specific gaps whose absence makes an
evaluator say "interesting, but we can't even pilot this." Verified
against the current codebase 2026-08-08, not assumed:

### Protocol / transport
- [x] MQTT 3.1.1 and MQTT 5
- [x] TLS / mTLS (`internal/broker`'s `CertReloader`, X.509 CN auth)
- [x] **WebSocket / WSS — closed.** `internal/broker/broker.go` now wires
      `listeners.NewWebsocket` for both `MQTTWSPort` (plain WS) and
      `MQTTWSSPort` (WSS, sharing the TLS-TCP listener's cert/reload
      config). Covered by `TestWebSocket_EndToEnd_ConnectPublishSubscribe`
      (real CONNECT/SUBSCRIBE/server-push over a live WebSocket connection,
      production ACL enforced) and `TestNew_WSSRequiresTLSCertDir`. Opt-in
      via Helm `ws.enabled`/`tls.enabled`, disabled by default.
      Closed: [keel-iot/keel-mqtt-gateway#5](https://github.com/keel-iot/keel-mqtt-gateway/issues/5)
- [x] **Proxy Protocol — closed.** `internal/broker/proxyproto_listener.go`
      wraps the plain and TLS TCP listeners with PROXY protocol v1/v2
      parsing (`github.com/pires/go-proxyproto`). Requires an explicit
      trusted-CIDR allowlist — connections from outside it are rejected
      outright, never silently trusted or silently downgraded, since a
      PROXY header is otherwise an unauthenticated claim about the
      sender's own address. Covered by
      `TestProxyProtocol_TCP_TrustedSourceRealIP` and
      `TestProxyProtocol_TCP_UntrustedSourceRejected`. Opt-in via Helm
      `proxyProtocol.enabled`/`proxyProtocol.trustedCidrs`, disabled by
      default. Closed: [keel-iot/keel-mqtt-gateway#6](https://github.com/keel-iot/keel-mqtt-gateway/issues/6)
- [SUSPENDED] MQTT bridging (broker-to-broker) — README previously claimed
      an "MQTT Bridge" under Integration; code has no such thing, only
      Kafka/HTTP `OutputConnector`s (confirmed 2026-08-09). The false claim
      is removed from README. Not promoted to a Phase 1 gap: no concrete
      customer/PoC requirement for it exists yet, unlike WebSocket/WSS,
      Proxy Protocol, and rate limiting. Revisit if a real case emerges —
      does not block production readiness in the meantime.

### MQTT feature completeness
Tracked directly in `FEATURE_MATRIX.md`, not duplicated here — that file
*is* the backlog for this block. As of this baseline: QoS 0/1/2,
persistent sessions, retained, will/will delay (MQTT5), shared
subscriptions, Receive Maximum, Maximum Packet Size, Topic Alias,
Request/Response, Subscription Identifiers, Server Keep Alive are PASS
with a real test reference each. Open: `test_session_expiry`
(`UNRESOLVED`, keel-iot/keel-mqtt-gateway#3), 3.1.1 will-message
coverage, enhanced authentication (AUTH packet / SASL-style challenge).

### Security
- [x] TLS/mTLS, JWT/JWKS, ACL (RBAC + legacy topic-shape fallback)
- [x] Credential rotation (JWKS cache, device CA cache)
- [x] Certificate revocation with active eviction of already-connected
      sessions (see `keel-cert-manager` integration)
- [ ] **Publish-rate and connect-attempt rate limiting — confirmed gap.**
      `MaxConnections` per tenant exists (`TenantGatewayConfig.MaxConnections`,
      concurrent connections only). No connect-rate or publish-rate limiter
      exists anywhere in the codebase (confirmed 2026-08-09) — a single
      client/tenant can flood the broker or hammer it with reconnects with
      no protection beyond the connection-count ceiling. Deliberately not
      one global limiter: connection concurrency, connect-attempt rate, and
      publish rate are independent axes; subscription rate deferred unless
      a real requirement emerges. Local-to-Edge by design, not
      cluster-coordinated — a security/operational protection, not a
      billing-grade global quota, so no Raft/Redis coordination on the
      publish hot path. Tracked: [keel-iot/keel-mqtt-gateway#8](https://github.com/keel-iot/keel-mqtt-gateway/issues/8)
- [~] Audit/telemetry for "why was this client rejected" — `ConnectionsTotal{result}`
      exists per-tenant; a per-client structured audit trail (the
      `security-guide`/"audit structured" idea from
      `docs/alternatives-and-future-work.md`'s roadmap) does not.

### Operations
- [x] `/readyz`/`/healthz` (TLS, Redis, raft leader, local Olric
      reachability)
- [x] Graceful shutdown (`cl.Stop`, `server.Close`, raft leadership
      transfer + drain on core)
- [x] Rolling update — validated on real GKE clusters (see README's
      "Validated" list)
- [x] Prometheus metrics, structured (slog JSON) logs
- [x] Config via env vars + Helm values
- [x] Backup/restore CLI (`keel admin backup`/`restore`, see
      `deploy/helm/.../CONFIGURATION.md` §9)
- [ ] Documented upgrade path (schema/state versioning across releases)
      — **not yet verified**; `internal/db/migrate.go` handles Postgres
      schema migration, but a general "what happens on N→N+1 with
      incompatible raft/Olric/Redis state" story isn't written down yet.
- [x] Known behavior on Redis/Core/Edge loss — see the postmortems in
      `docs/postmortems/` and this baseline's own `test_flow_control2`/
      Receive Maximum investigation for the methodology, though this is
      really Phase 3/4's job to make systematic rather than incident-driven.

**Rule of thumb carried into triage**: if the gap blocks a realistic
PoC, it goes first. If it only closes a feature-parity table against
EMQX, it goes later. WebSocket/WSS, Proxy Protocol, and publish/connect
rate limiting are the three confirmed, concrete gaps from this pass.
MQTT bridging was a false README claim (removed), not promoted to a
Phase 1 gap — no concrete requirement for it exists yet.

## Phase 2 — Feature correctness: every claim has a test

**Definition of Done for any new protocol feature** (see `CONTRIBUTING.md`'s
"Definition of Done" section for the enforced version of this):

```
Implementation
  → Unit test
  → Integration/protocol test
  → Regression test
  → FEATURE_MATRIX.md row (with a real test reference)
  → Docs/config update
  → Metric/log, only if operationally meaningful
```

Minimum bar per feature, not just "it compiles": a below-limit case, an
at-limit boundary case, an above-limit case with the correct reason
code, the MQTT 3.1.1 vs MQTT 5 behavioral split where they differ, a
disabled/unconfigured case proving backward compatibility, and a
mutation-verified test wherever the test backs a public claim — the
exact pattern already used for `MaxKeepAlive`'s boundary matrix and
`NoInheritedPropertiesOnAck`'s regression test.

## Phase 3 — Distributed correctness (cluster / failover)

This is where testing stops being "a generic MQTT broker" and starts
being Keel specifically — the Core/Edge split, Raft control plane, and
Olric data plane exist to make specific guarantees, and this phase is
where those guarantees get checked, not assumed.

**A. Control plane** — initial leader election, leader loss, follower
loss, quorum loss/return, Core restart (intact PVC, corrupted/partial
state), rolling Core restart, join/rejoin, membership convergence.
Invariants to check, not just "cluster went green again": never two
valid owners at once, no illegitimate ownership change, no decision
made without quorum where CP semantics require one.

**B. Edge / data plane** — sudden Edge death, graceful shutdown,
scale N→N+1 and N→N-1, client reconnect, routing table rebuild,
gRPC forwarding to a dead node, stale membership, stale routes,
distributed duplicate client ID, offline-session reassignment.
Invariants: no duplicate delivery beyond what MQTT's own QoS semantics
allow, no double live owner, routing converges, offline sessions stay
recoverable.

**C. Storage / session state** — Redis primary loss, replica
promotion/failover, Redis restart, temporary storage loss, an Edge
dying mid-QoS-persist, inflight recovery, Packet ID continuity,
retained-message persistence, session expiry surviving a restart.

Output: a **Cluster Correctness Matrix**, same evidence discipline as
`FEATURE_MATRIX.md`, kept as its own document (distributed-systems
invariants aren't the same axis as MQTT protocol conformance and
shouldn't be flattened into one table).

## Phase 4 — Failure correctness (chaos)

Only meaningful once Phase 3 has stated the invariants — chaos testing
without a known-correct baseline just produces "we broke things and it
looked fine," which proves nothing.

Approach: Testcontainers/Toxiproxy-style reproducible fault injection,
not a requirement for a live Kubernetes cluster for every scenario.

- **Process failures**: `kill -9` Edge, `kill -9` Core leader, kill
  Redis, random restarts.
- **Network**: Edge↔Core partition, Core↔Core partition, Edge↔Edge
  partition, Edge↔Redis partition, latency injection, packet loss,
  asymmetric partition, temporary blackhole.
- **Resource**: CPU throttling, memory limits, slow disk, storage full
  (where simulable), bandwidth limits.
- **Timing**: failure mid-CONNECT, mid-SUBSCRIBE, mid-QoS2-handshake,
  mid-session-restore, during a reconnect storm.

Per scenario, record: precondition, failure injected, duration,
expected invariant, observed result, recovery time, message loss,
duplicates, evidence — mirroring `test/conformance/evidence/`'s format.
Distinguish explicitly: **survival** (system doesn't die), **correctness**
(semantics not violated), **recovery** (returns to a healthy state),
**recovery time** (how long it takes). A cluster that "doesn't crash"
but loses half its sessions has not passed the chaos test.

## Phase 5 — Performance characterization

Only once correctness and failure handling are stable enough to trust
the numbers.

- **Throughput**: msg/s by QoS (0/1/2 separately), payload size
  (64B/1KB/10KB/100KB), fan-out (1:1, 1:N), retained on/off,
  persistence on/off.
- **Latency**: never just the mean — p50/p95/p99/p99.9 where sample
  size allows. Separate publish-ingress, broker-forwarding, and
  end-to-end subscriber latency.
- **Scalability**: concurrent connections, reconnects/s, subscription
  count, topic cardinality, session count, inflight count. Specifically
  worth re-measuring here: **does Edge memory now scale with the load
  that Edge actually owns, not the whole fleet** — the exact property
  the offline-session-placement redesign (ADR-005, closed the Kimera OOM
  postmortem) was meant to deliver. That's a far more meaningful number
  for this architecture than a raw msg/s figure.
- **Resource efficiency**: CPU/message, RAM/connection, RAM/subscription,
  RAM/offline session, network overhead, storage IOPS.

Repeat a subset of these *during* the Phase 4 failure scenarios, not
only in steady state — "200k msg/s, p99 < Xms, 0 loss, during an Edge
loss" is a far stronger claim than "500k msg/s steady state" for this
product's actual positioning.

## Phase 6 — Competitive benchmarking

Deliberately last — only worth doing once confident the comparison
itself is fair.

A public `BENCHMARK_SPEC.md` first: hardware, CPU architecture, RAM,
kernel, Kubernetes/container runtime, broker version, topology,
persistence settings, TLS on/off, QoS, payload, client count,
subscription count, topic distribution, warm-up procedure, test
duration, measurement methodology — published, not just implied by a
graph.

Competitors, each for a different reason: **Mosquitto** (efficient
single-node baseline), **VerneMQ** (the historical OSS cluster
comparison this project's own README already positions against),
**EMQX** (the modern, widely-deployed reference), **HiveMQ** (if a
comparable config/license can be obtained).

No single "Keel is faster" ranking — profile-by-profile: throughput,
latency, memory, reconnect storm, Edge failure, leader failure, rolling
upgrade. A result like "EMQX has higher steady-state throughput, but
Keel degrades less during Edge churn" is more credible — and more
useful — than a single winner-take-all number. If Keel loses a
benchmark outright, publish that too: it says where to work, and it's
what makes the benchmarks Keel wins credible in the first place.

# mochi-mqtt upstream management

Keel embeds `github.com/mochi-mqtt/server/v2` as its MQTT protocol engine.
This document is the disciplined process for consuming, verifying, and
tracking upstream work — same evidence bar as `FEATURE_MATRIX.md`: a
claim without a verified source or a reproduced result is not a fact,
it's a lead.

Review baseline: 2026-08-10. Facts below are marked by how they were
established — **verified (source)**, **verified (live test/repro)**,
**upstream author claim** (not independently reproduced), or
**inference** (a judgment call, not a measurement).

## 1. Current dependency — verified facts

- `go.mod` requires `github.com/mochi-mqtt/server/v2 v2.7.9`.
- A `replace` directive points it at `./thirdparty/mochi-mqtt-server` — a
  full local copy of the v2.7.9 tree with one patch applied (see
  `thirdparty/mochi-mqtt-server/PATCH.md`). This is **MOCHI-PATCH-001**
  (§6). No other local modifications exist.
- `go.sum` has no `mochi-mqtt` entry — expected, a local filesystem
  `replace` target isn't checksummed.
- The patch is the QoS2 PUBACK→PUBREC fix from this session, upstreamed
  as [mochi-mqtt/server#511](https://github.com/mochi-mqtt/server/pull/511)
  (issue [#510](https://github.com/mochi-mqtt/server/issues/510)), open,
  unmerged as of this review.

### Upstream project health — verified facts

- Latest tag: `v2.7.9`, 2025-03-01.
- Latest commit to `main`: `5b7f94b`, 2025-03-01 ("Update server version").
- **No commits and no releases in the ~17 months since**, despite 19 open
  PRs and 25 open issues, several containing real correctness/concurrency
  fixes (below).
- Issue [#491](https://github.com/mochi-mqtt/server/issues/491) ("Is
  mochi-mqtt/server dead?", opened 2026-01-20): the original maintainer
  (`mochi-co`) confirmed reduced involvement, transferred org ownership,
  and lowered the required review-approval count to 2. A listed
  maintainer (`bkupidura`) then stated *"I dont have enough resources to
  work on this codebase, and will not be able to deliver time mochi-mqtt
  deserves. Is there any active developer interested?"* — as of this
  review, no confirmed new maintainer has taken that over (one contributor,
  `code-grey`, offered; no confirmation found that the offer was accepted).
- This is the single most important input to §7 (fork escalation) — not a
  reason to fork today, but a real, dated signal to track, not to ignore.

## 2. Critical API surface — verified from source

Grep-verified call sites (`grep -rl "mochi-mqtt/server/v2" internal/ cmd/`),
not assumed from docs:

| mochi API | Keel call sites | What breaks if upstream changes it |
|---|---|---|
| `mqtt.Server`, `mqtt.New`, `mqtt.Options`, `Options.Capabilities` | `internal/broker/broker.go` | Every listener, hook registration, session-expiry/keepalive capability wiring |
| `listeners.Listener` interface, `listeners.NewTCP`, `listeners.NewWebsocket`, `listeners.Config` | `internal/broker/broker.go`, `internal/broker/proxyproto_listener.go` | Plain/TLS/WS/WSS listeners, and Keel's own PROXY-protocol listener (a custom `listeners.Listener` implementation — depends on the interface's exact shape, not just behavior) |
| `mqtt.OnConnectAuthenticate`, `mqtt.OnConnect` | `internal/broker/hooks.go`, `internal/broker/max_keepalive.go` | Auth (password/JWT/X.509), connect-rate limiting, MaxKeepAlive |
| `mqtt.OnACLCheck` | `internal/broker/hooks.go` | Per-topic publish/subscribe authorization — a semantics change here is the highest-blast-radius category possible |
| `mqtt.OnPublish` | `internal/broker/hooks.go` | Cluster forwarding, output-connector fan-out, data-volume quota, **publish-rate limiting** (this session's work) |
| `mqtt.OnSubscribed`, `mqtt.OnUnsubscribed`, `mqtt.OnSessionEstablish`, `mqtt.OnDisconnect`, `mqtt.OnClientExpired` | `internal/broker/hooks.go`, `internal/broker/redis_session.go` | Cluster routing-table sync, offline-session ownership, Redis-backed session persistence |
| `mqtt.OnRetainMessage` | `internal/broker/hooks.go` | Retained-message hook path (separate from `internal/broker`'s own `RetainedStore`) |
| `packets.Code`, `packets.ErrQuotaExceeded`, `packets.ErrRejectPacket` | `internal/broker/hooks.go` (rate limiter) | See the dedicated risk note below — this is a real, live dependency on exact ack-building behavior, not just "some error type" |
| `mqtt.NewTopicsIndex`, `mqtt.TopicsIndex` | `internal/cluster/routing/router.go` | Keel keeps its own **separate** `TopicsIndex` instance for cluster routing decisions, entirely independent of the live broker's own subscriber index — see the PR #506 risk note below, this separation is exactly why that PR's claims don't automatically transfer |
| `mqtt.IsSharedFilter` | `internal/broker/hooks.go` | Shared-subscription topic-shape detection |
| `mqtt.testing` (Paho-adjacent conformance helpers) | `internal/conformance/*.go` | `--conformance-test`-only scaffolding, never production |

**Concrete, verified risk already in production code**: Keel's new
publish-rate limiter (`internal/broker/hooks.go`'s `OnPublish`) returns
`packets.ErrRejectPacket` for MQTT 3.1.1 and MQTT5 QoS0 over-quota
publishes, *relying on* `processPublish`'s current behavior of silently
dropping that error with no ack and no disconnect. Issue
[#465](https://github.com/mochi-mqtt/server/issues/465) and PR
[#476](https://github.com/mochi-mqtt/server/pull/476) both propose
changing exactly this: making `ErrRejectPacket` produce a real PUBACK
(reason 0x83) instead of silence. **If either is ever adopted, Keel's own
`TestPublishRateLimit_MQTT5_QoS0_DroppedConnectionSurvives` and
`TestPublishRateLimit_MQTT311_DroppedNotDisconnected` would need
re-verification against the new behavior** — this is not hypothetical,
it's a direct, named dependency between an open upstream proposal and a
Keel regression test that already exists.

## 3. Status and decision vocabulary

Two independent axes — a PR's *decision* (what Keel intends to do) and
its *status* (how far that intention has actually been carried out).
Never conflate them: a PR can be `Decision: REQUIRED` and
`Status: NOT REVIEWED` on day one.

**Decision:**
- `REQUIRED` — blocks something Keel needs; must be adopted (upstream or downstream) on a bounded timeline.
- `EVALUATE` — plausibly relevant; needs the verification work in §4 before a call is made.
- `WATCH` — relevant but not actionable yet (no PR, unresolved upstream discussion, or superseded by a competing PR).
- `REJECT` — reviewed and declined, with a stated reason.
- `ADOPTED` — carried downstream today (patched locally and/or already shipping in Keel).
- `UPSTREAMED` — Keel's own fix, submitted upstream (may or may not be merged yet).

**Status:**
- `NOT REVIEWED` — appears in the inventory, nothing else done yet.
- `UNDER REVIEW` — source has been read, no verification run yet.
- `DOWNSTREAM VALIDATED` — **all** of the following are true for the exact commit reviewed:
  - exact upstream commit SHA recorded (not just a PR number — PRs get force-pushed);
  - source review completed and documented;
  - relevant upstream unit/integration tests pass;
  - Keel's own regression suite passes (`go test ./...`, `-count=1`);
  - MQTT conformance suite passes where the change is protocol-facing;
  - `-race` passes on affected packages for concurrency-sensitive changes;
  - independent benchmark reproduced for performance/memory claims (§9) — an upstream author's number is never sufficient on its own;
  - no known regression introduced (checked explicitly, not assumed).
  A written note lives at `docs/upstream/validation/mochi-<PR>.md` (template in §4).
- `DOWNSTREAM REJECTED` — validated (or partially validated) and found wanting; the note explains why.
- `ADOPTED` — validated and actually in use (thirdparty patch and/or upstreamed and released).

**Only `DOWNSTREAM VALIDATED` or `ADOPTED` work is eligible to be
mentioned on the upstream PR** — see §8's engagement policy.

## 4. Inventory — open mochi-mqtt PRs and directly relevant issues

Reviewed 2026-08-10. `Keel relevance` and `Risk` are this review's
judgment (**inference**), not upstream claims. Every row's evidence is
either **verified (source)** — I read the diff/linked code — or
**upstream claim** where noted.

| # | Title | Category | Keel relevance | Risk | Verification required | Decision | Status |
|---|---|---|---|---|---|---|---|
| [#511](https://github.com/mochi-mqtt/server/pull/511) | QoS2 publish rejection → PUBREC not PUBACK | CORRECTNESS | High — Keel's own publish-rate limiter needs this exact byte-correct | Low (single-line, mutation-tested) | Done — regression + mutation test in Keel, standalone repro against pristine v2.7.9 | **ADOPTED** (downstream patch) / UPSTREAMED | **DOWNSTREAM VALIDATED** (this is Keel's own fix — see `docs/upstream/validation/mochi-511.md`) |
| [#476](https://github.com/mochi-mqtt/server/pull/476) | Broader QoS2 fix + removes the `ErrRejectPacket` no-op, disconnects MQTT<5 clients on hook error | CORRECTNESS | High — overlaps #511 *and* would change the exact `ErrRejectPacket` silence Keel's rate limiter relies on (§2) | Medium-high — behavior change beyond what Keel asked for, touches MQTT 3.x path too | Full regression + conformance + a targeted check of Keel's own rate-limit drop-path tests against the new behavior | EVALUATE | UNDER REVIEW |
| [#465](https://github.com/mochi-mqtt/server/issues/465) | Issue root of #476: `ErrRejectPacket` should produce a PUBACK reason, not silence | CORRECTNESS | High (same dependency as above) | — (issue, no code) | Track alongside #476 | WATCH | NOT REVIEWED beyond the issue text |
| [#474](https://github.com/mochi-mqtt/server/issues/474) / [#497](https://github.com/mochi-mqtt/server/pull/497) | Normal QoS1 PUBACK uses `CodeGrantedQos1` (0x01), not `CodeSuccess` (0x00) | CORRECTNESS | **High — verified (source): 0x01 is not a valid MQTT5 PUBACK reason code per spec table 3.4.2-1 at all (it's a SUBACK-only code); every non-error QoS1 publish on today's Keel binary is affected, unmodified by any Keel hook** | Low (one-line-shaped fix per the PR) | Independent packet-capture confirmation on a live Keel binary (not yet done — flagged, not assumed); Keel conformance re-run | **EVALUATE, leaning REQUIRED pending live confirmation** | UNDER REVIEW |
| [#500](https://github.com/mochi-mqtt/server/issues/500) | `publishToClient` uses the wrong packet's `TopicName` for outbound Topic Alias under some conditions | CORRECTNESS | High — verified (source, diff description) plausible interaction with Keel's own cluster-forwarded publishes (packet copies across nodes are exactly the "certain conditions" the issue alludes to) | Medium | No PR yet; needs a Keel-side reproduction attempt against cluster-forwarded Topic Alias before it can even be scored precisely | WATCH (no PR to evaluate yet) | NOT REVIEWED beyond the issue text |
| [#442](https://github.com/mochi-mqtt/server/pull/442) | Message Expiry Interval not honored for retained messages (timing of `pk.Expiry` assignment) | CORRECTNESS | Medium — Keel has its own `RetainedStore` and a PASS row for Message Expiry Interval | Low | Verify against Keel's retained-message + expiry conformance test, confirm the guarded double-assignment doesn't change non-retained-message timing | EVALUATE | UNDER REVIEW |
| [#490](https://github.com/mochi-mqtt/server/pull/490) | CONNACK doesn't send `MaximumPacketSize` | CORRECTNESS (spec completeness) | Medium — unclear if Keel's own `test_maximum_packet_size` PASS actually exercises the server→client property or only client→server enforcement | Low | Packet-capture the current CONNACK on Keel's binary to confirm the property is actually missing today | EVALUATE | UNDER REVIEW |
| [#507](https://github.com/mochi-mqtt/server/pull/507) | `GetByListener` recursive `RLock` deadlock fix + reproduction test | CONCURRENCY | High — a real deadlock, reachable in production (client connecting while a listener closes) | Low (one-line fix; PR ships its own deadline-based repro test) | `-race` + the PR's own repro test; confirm no Keel call site holds the same lock pattern elsewhere | EVALUATE, leaning REQUIRED | UNDER REVIEW |
| [#489](https://github.com/mochi-mqtt/server/pull/489) | Same `GetByListener` deadlock, same one-line fix, no test | CONCURRENCY | Same as #507 | Same as #507 | Superseded in practice by #507 (identical fix, #507 additionally adds a regression test) | **REJECT** in favor of #507 — not a criticism of #489, #507 is strictly more complete | NOT REVIEWED beyond the diff (superseded) |
| [#509](https://github.com/mochi-mqtt/server/pull/509) | `ClientsWg.Add`/`Wait` shutdown race — registration moved into `Listeners.Serve`, shutdown "latches" and drops late connections | CONCURRENCY / LIFECYCLE | High — affects graceful shutdown correctness, which Keel's own Edge drain (`preStop` hook, `keel-mqtt-gateway drain`) depends on | Medium — changes what happens to a connection arriving exactly at shutdown (dropped, not accounted) | `-race`, the PR's own `TestCloseWhileConnecting`, and a Keel-side drain/shutdown scenario test | EVALUATE | UNDER REVIEW |
| [#508](https://github.com/mochi-mqtt/server/pull/508) | Same underlying race as #509, different fix (`Add` moved to each listener's accept loop via a new `setClientsWg` interface) | CONCURRENCY / LIFECYCLE | Same as #509 — **these two PRs overlap and propose different fixes for the same bug**, same relationship as #511/#476 | Medium | Same as #509; if both remain open, a decision between them (not both) will be needed before adoption | WATCH pending which one upstream prefers | UNDER REVIEW — note: this PR's own body contains a Claude Code attribution/session link, a verified fact worth logging (not a judgment on the fix's quality — AI-authored patches get the exact same verification bar as any other) |
| [#496](https://github.com/mochi-mqtt/server/pull/496) | "fix inflight by mutex" — replaces an atomic TOCTOU check with a mutex | CONCURRENCY | Medium — inflight tracking is on Keel's QoS1/2 hot path | High — the PR's own stated reasoning (Windows vs Linux memory-model difference explains why it "doesn't deadlock on Windows") **does not hold up**: Go's memory model and atomic operations don't differ by OS/architecture in the way described; a real TOCTOU race may still exist independent of that explanation, but the diagnosis needs independent re-derivation, not acceptance as given | Full concurrency review from first principles (not from the PR's own explanation), `-race`, targeted stress test | EVALUATE, with an explicit note that the PR's *diagnosis* needs independent verification separate from whether its *fix* happens to be correct | UNDER REVIEW |
| [#493](https://github.com/mochi-mqtt/server/pull/493) | "fix log spam when EOF error on connection close" | API / MAINTAINABILITY (observability) | Low — cosmetic log noise, not protocol behavior | **Verified (source) defect in the PR itself**: the diff reads `if err != nil && errors.Is(err, io.EOF)` — this is inverted from the PR's own stated intent (silence EOF, keep logging real errors); as written it does the opposite: logs only on EOF, silently swallows every real error | None needed to reach a decision — the diff itself, read plainly, contradicts its own description | **REJECT as currently written** (not a criticism of the goal, the logic is backwards) | UNDER REVIEW |
| [#506](https://github.com/mochi-mqtt/server/pull/506) | `sync.Pool` + `Range` iterators for the topics/subscriber routing hot path, author claims 75% fewer allocations | PERFORMANCE / MEMORY | High — direct overlap with `mqtt.TopicsIndex`, the exact type Keel's `internal/cluster/routing/router.go` uses **on its own, separate `TopicsIndex` instance**, not through `Server.publishToSubscribers` | Medium, subtle: **verified (source)** — the new pooling model requires callers to call the new `Subscribers.Release()` to return objects to the pool; Keel's `router.go` calls `TopicsIndex.Subscribers(topic)` directly and would have no reason to know about or call `Release()`. This isn't a correctness bug (an un-released object just becomes normal garbage, sync.Pool tolerates that), but it means **the author's allocation-reduction claim would not transfer to Keel's own routing hot path** without also updating `router.go` — a concrete example of why upstream benchmarks aren't assumed to apply to Keel's workload | Reproduce the author's benchmark first (label it "upstream author result" until then), then a Keel-workload benchmark (high topic cardinality, high fan-out — the profile `router.go` actually runs under), `-race`, full conformance | EVALUATE | UNDER REVIEW |
| [#461](https://github.com/mochi-mqtt/server/pull/461) | Change `MaximumClientWritesPending` default from 8192 to 64 | PERFORMANCE / MEMORY | Medium — Keel doesn't currently override this value (verified: no `MaximumClientWritesPending` reference anywhere in `internal/`) | High — a 128x smaller default backpressure buffer changes drop/block behavior under bursty publish patterns; the PR's own claim of "no effect on core functionality" is an **upstream author claim**, not measured against any workload resembling Keel's | Before/after benchmark under a reconnect-storm and high-fan-out scenario (not the PR author's unstated workload), confirm no increase in `QosDropped`/backpressure-related metrics Keel already tracks | **REJECT as a blind default change** — if this value needs tuning, it should be a Keel-configured value verified against Keel's own load-test workloads, not an inherited upstream default change | UNDER REVIEW |
| [#487](https://github.com/mochi-mqtt/server/pull/487) | MQTT-over-QUIC listener (`quic-go`) | TRANSPORT | See §7 (dedicated review) | See §7 | See §7 | See §7 | UNDER REVIEW |
| [#473](https://github.com/mochi-mqtt/server/pull/473) | Docker example config: per-listener TLS cert options | NOT RELEVANT TO KEEL | Keel has its own `CertReloader`/Helm-driven TLS config, doesn't use mochi's example config loader | — | — | REJECT (not applicable) | NOT REVIEWED beyond the description |
| [#421](https://github.com/mochi-mqtt/server/pull/421) | Dynamic hook removal (early/incomplete, references issue #420) | API / MAINTAINABILITY | Low today — Keel doesn't currently need to remove hooks at runtime | Unknown — PR itself is described as an initial commit, not a finished design | Re-review once/if it matures | WATCH | NOT REVIEWED beyond the description |
| [#402](https://github.com/mochi-mqtt/server/pull/402) | New hook to reload an individual client session from storage, aimed at clustered deployments | API / MAINTAINABILITY | **High in principle** — this is conceptually adjacent to what Keel's own offline-session-ownership/Redis-session-hook machinery already does independently, worth understanding whether it could simplify or conflict with Keel's approach | Unknown — needs a real design read, not just the description | Full design review against `internal/broker/redis_session.go` and `internal/session` before any opinion is formed | EVALUATE | NOT REVIEWED beyond the description |
| [#468](https://github.com/mochi-mqtt/server/pull/468) | README spelling fix | NOT RELEVANT TO KEEL | None | None | None | REJECT (not applicable) | NOT REVIEWED beyond the title |
| [#505](https://github.com/mochi-mqtt/server/pull/505) | Dependabot: bump `golang.org/x/net` 0.33.0→0.55.0 | SECURITY / MAINTAINABILITY | Medium — Keel's own `go.mod` already carries a newer `golang.org/x/net` transitively; worth confirming this bump doesn't lag behind what Keel already effectively uses | Low | Diff Keel's own resolved `x/net` version against this bump | WATCH | NOT REVIEWED beyond the title |

Not itemized individually (reviewed, genuinely out of scope for Keel):
issues #452 (WebUI request), #431 (MongoDB persistence plugin request),
#469 (publish-excluding-clients feature request), #418/#423 (WS/WSS
usage questions, not bugs), #404 (feature request), #403 (feature
request), #440 (sysinfo cosmetic question), #437/#425 (`Ledger` hook,
Keel doesn't use it), #481/#477 (client-metadata feature requests),
#362 (usage question), #482 (`--config` flag usage question), #50
(discussions pointer).

## 5. Downstream patch policy

> Keel may carry downstream mochi-mqtt patches for protocol correctness,
> security, concurrency defects, or production blockers when an upstream
> release is not available. Every downstream patch must have an upstream
> reference where appropriate, targeted regression coverage, and a
> documented removal condition.

Preferred order, always attempted top-down:

**A. Fix upstream.** Open an issue and/or PR in `mochi-mqtt/server`,
keep the patch minimal and general-purpose (not Keel-shaped). This is
the default and the only outcome that removes the maintenance burden
entirely. Example: #510/#511.

**B. Temporary downstream patch.** Allowed only when Keel needs the fix
before an upstream release exists. Requirements, all mandatory:
- references the upstream issue/PR;
- has Keel regression coverage, mutation-tested where it backs a
  correctness claim (same bar as `CONTRIBUTING.md`'s existing rule);
- is independently removable — a single `go.mod` `replace` line plus one
  documented directory, never entangled with unrelated Keel code;
- lives in `thirdparty/<module-name>` with a `PATCH.md` recording the
  bug, the fix, upstream links, and the exact removal condition.

**C. Keel-specific workaround.** Only when the behavior genuinely
belongs to Keel, not the MQTT engine (e.g. a Keel hook compensating for
something that is legitimately Keel's own policy, not a protocol bug).
Not a substitute for A or B when the defect is actually in mochi-mqtt.

No open-ended feature development inside the mochi fork. The fork
exists to carry verified fixes, not to grow Keel-specific functionality
that has no upstream home.

## 6. Patch tracking

| Field | MOCHI-PATCH-001 |
|---|---|
| Upstream issue | [#510](https://github.com/mochi-mqtt/server/issues/510) |
| Upstream PR | [#511](https://github.com/mochi-mqtt/server/pull/511) |
| Category | CORRECTNESS |
| Reason | MQTT5 QoS2 publish rejected via `OnPublish` must get PUBREC, not PUBACK |
| Affected mochi version | v2.7.9 |
| Location | `thirdparty/mochi-mqtt-server/` (`replace` in `go.mod`) |
| Keel regression | `internal/broker/ratelimit_integration_test.go`'s `TestPublishRateLimit_MQTT5_QoS2_PubrecQuotaExceeded` |
| Date adopted | 2026-08-10 |
| Status | ADOPTED (downstream), UPSTREAMED (PR open, unmerged) |
| Removal condition | A released `github.com/mochi-mqtt/server/v2` version containing this fix; then delete the `replace` directive, delete `thirdparty/mochi-mqtt-server/`, bump the dependency, rerun the regression test against the real released module |

No other downstream patches exist. These IDs are for downstream patch
bookkeeping only — never used for public protocol test identifiers,
which stay real, greppable test names per `FEATURE_MATRIX.md`'s existing
rule.

## 7. Fork evaluation

**Not warranted today.** A permanent `keel-iot/mochi-mqtt` fork is not
justified by upstream being slow on its own — that's exactly the
mistake this policy exists to avoid. Escalation requires *multiple* of
the following becoming true, not one:

- [ ] Several `REQUIRED` correctness/security patches remain unmerged
      upstream for a prolonged period (a specific, dated threshold —
      e.g. 6+ months with no maintainer response — not a vibe).
- [ ] Keel routinely needs changes to mochi internals (not just
      config/behavior at the hook boundary).
- [ ] Upstream stops issuing releases entirely, for an extended period,
      with no maintenance path (§1's 17-month gap is a real, current
      data point toward this — not yet a confirmed "stopped forever",
      since #491's thread shows the org is aware and reorganizing).
- [ ] Essential Go toolchain or security updates can't be consumed
      because upstream's `go.mod`/CI hasn't moved.
- [ ] Keel is, in practice, already maintaining a significant fraction
      of the MQTT engine via accumulated downstream patches (currently:
      one, single-line).

**Recommendation**: monitor, don't act. Concretely: re-run §1's
"upstream project health" check (last commit/release dates, #491's
thread) at a fixed cadence (this doc should be revisited at least
quarterly, or immediately if a second `REQUIRED` patch needs to go
downstream) rather than reactively, so the escalation decision is made
from accumulated evidence, not a single frustrating week.

## 8. Upstream engagement policy

Only comment on an upstream PR/issue when Keel's review reached
`DOWNSTREAM VALIDATED` or produced genuinely new information (a
reproduction, a benchmark, a correctness finding) — not on every PR
reviewed here, and not as a rubber stamp.

Language rules:
- Never write "Approved" — Keel is not a maintainer or official
  reviewer of this project.
- Never write "This PR is safe to merge" unless the change is trivially
  so (e.g. a typo fix) — a downstream consumer's regression suite
  passing is evidence for that PR's fitness for Keel, not a general
  merge endorsement.
- Prefer: *"We independently validated this downstream and observed no
  regressions in the scenarios listed below."* or *"From a Keel
  downstream-consumer perspective, this change passes our current
  correctness and regression gates."*
- Link the full validation note (`docs/upstream/validation/mochi-<PR>.md`)
  rather than restating it inline — reproducible review, not a review
  someone has to trust.
- Never imply a competing PR (e.g. #476 vs #511, #489 vs #507, #508 vs
  #509) is wrong — note the overlap and Keel's narrower/different scope
  factually, as already done on #511 (see that PR's comment thread).

This is also a credibility investment, not just a process: a downstream
consumer that shows up with "we tested this against our conformance and
regression suites, here's what we found" reads very differently to
maintainers and other consumers than "please merge this." #511 is the
first instance of that; the goal is for it not to be a one-off.

## 9. Performance / memory PR review protocol

For every PERFORMANCE or MEMORY-categorized PR before it can move past
`EVALUATE`:

1. Reproduce the upstream author's own benchmark first, if reproducible
   from the PR alone. Until reproduced, its numbers are labeled
   **"upstream author result"**, never stated as fact.
2. Benchmark against a Keel-relevant workload — not the upstream
   author's workload. Candidates: high topic cardinality, large
   subscription set, high fan-out, reconnect storm, many concurrent
   clients, the publish hot path, retained messages, shared
   subscriptions where applicable. Pick the ones the specific PR
   actually touches (§4's per-PR notes already identify this for #506
   and #461).
3. Measure: throughput, p50/p95/p99 latency, heap usage, allocations/op,
   bytes/op, GC pressure, CPU.
4. Run the full correctness suite (conformance + Keel regression) before
   and after — a performance win with a correctness or concurrency
   regression is rejected outright, no exception, regardless of the
   performance number.
5. `-race` clean on any concurrency-adjacent change (most of these are).

## 10. QUIC — dedicated review

See PR [#487](https://github.com/mochi-mqtt/server/pull/487) and issue
[#484](https://github.com/mochi-mqtt/server/issues/484) (the feature
request that preceded it).

**Verified from source (diff read):**
- New `listeners/quic.go`, built on `github.com/quic-go/quic-go`, a new
  third-party dependency.
- Bumps upstream's own `go.mod` from `go 1.21` to `go 1.24.0` — a real
  toolchain-version cost to adopting this, independent of the feature
  itself.
- Implements `listeners.Listener` (the same interface Keel's own
  `proxyProtoListener` implements) — architecturally, this is "a new
  listener," consistent with how TCP/TLS/WS are already integrated, not
  a deeper protocol-engine change.
- TLS is mandatory (`Init` returns `ErrTLSRequired` if `TLSConfig` is
  nil) with ALPN defaulted to `"mqtt"` — consistent with QUIC's
  TLS-always design, not an added Keel burden beyond what Keel already
  does for its TLS listener.
- References "NanoSDK client format" compatibility in a doc comment —
  a specific, narrow client ecosystem, not general MQTT-over-QUIC
  client interoperability (there is no ratified OASIS MQTT-over-QUIC
  spec at all as of this review — this is a de facto, single-project
  convention, not a standard).
- No test evidence beyond the PR's own `listeners/quic_test.go` was
  independently run as part of this review.
- Author (`snac21`) is not among the small set of maintainers/frequent
  contributors seen elsewhere in this inventory — a first-time
  large-surface contribution to a project currently short on active
  reviewers (§1).

**Classification: later roadmap. Not a Phase 1 blocker, not even a
stretch goal.**

Reasoning:
- No confirmed Keel customer/PoC requirement for QUIC exists (mirrors
  the exact reasoning already applied to MQTT bridging in `ROADMAP.md`).
- WebSocket/WSS, Proxy Protocol, and connect/publish rate limiting were
  the three *evidence-confirmed* Phase 1 blockers this session closed;
  QUIC has no equivalent evidence of blocking a real evaluation today.
- Adopting it now would mean absorbing a first-time, large-diff,
  not-yet-widely-reviewed PR, a new third-party dependency, and a
  toolchain bump, into a project whose own upstream is already
  under-resourced (§1) — compounding risk for a transport with no
  ratified spec and a narrow client ecosystem.
- If a real customer requirement for QUIC materializes, this section is
  the starting point for re-evaluation, not a rejection that needs to be
  re-litigated from zero.

## 11. Deliverables checklist (this review)

1. [x] `docs/upstream/mochi-mqtt.md` (this document).
2. [x] Categorized inventory of relevant open PRs/issues (§4).
3. [x] Downstream patch policy (§5).
4. [x] Candidate patch priority list (§4's Decision column, ordered by
       §3's category priority: correctness → security → concurrency →
       lifecycle → memory → performance → transport).
5. [x] Explicit decision per reviewed PR (§4).
6. [x] Fork recommendation: not warranted today, explicit escalation
       criteria defined (§7).
7. [x] QUIC recommendation: later roadmap, not Phase 1 (§10).
8. [x] Tests/benchmarks needed before adopting each EVALUATE candidate
       (§4's "Verification required" column; general protocol in §9).

No production code was modified as part of this review beyond the
already-existing, already-validated MOCHI-PATCH-001. No PR was merged,
cherry-picked, or commented on as a result of this review (§8's bar
wasn't met for anything new here).

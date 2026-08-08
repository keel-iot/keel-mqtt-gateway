# Contributing to keel-mqtt-gateway

## Running tests locally

```sh
go build ./...
go vet ./...
go test ./...
```

Unit tests require no external services. For cluster/e2e scenarios that do
(Redpanda, TLS certs, a multi-node compose topology), see
`test/e2e/*.sh` and `deploy/docker-compose/README.md` — these are run
manually, not yet part of CI (see below).

A few tests in `internal/broker` require `TEST_DATABASE_URL` (a real
Postgres) and are skipped otherwise — see `internal/db/migrate_test.go`'s
doc for how to point one at the docker-compose Postgres.

## Definition of Done for a new protocol feature

See `ROADMAP.md` for the full phased plan this belongs to. For any new
MQTT protocol feature, all of the following before it's done — not just
"it compiles":

```
Implementation
  → Unit test
  → Integration/protocol test
  → Regression test
  → FEATURE_MATRIX.md row (real test reference, not a placeholder)
  → Docs/config update
  → Metric/log, only if operationally meaningful (never a decorative metric)
```

Minimum test coverage per feature — not exhaustive for every feature,
but the default to deviate from deliberately, not by omission:

- a below-limit / normal case;
- a boundary case exactly at the limit;
- an above-limit case, with the correct MQTT reason code;
- the MQTT 3.1.1 vs. MQTT 5 split, wherever the spec actually differs
  (never uniform behavior imposed for its own sake — see
  `MaxKeepAlive`'s MQTT5-only design for the precedent);
- the disabled/unconfigured case, proving backward compatibility;
- mutation verification (revert the fix, confirm the test fails, restore
  it) for any test backing a public claim — see the section below.

**No `PASS`/`SUPPORTED` row in `FEATURE_MATRIX.md` without a
referenceable test.** A matrix entry that can't be traced to a real,
greppable test name is a brochure claim, not evidence — exactly the
failure mode this whole methodology exists to prevent.

## Protocol regression suite

Every real MQTT protocol behavior discovered wrong or missing via an
external suite (`test/conformance`'s Eclipse Paho run — see
`docs/alternatives-and-future-work.md`'s 2026-08-07 entries) gets a small,
fast, local Go test instead of depending on that suite to catch a
regression later. Two categories, never mixed in the same package:

**Mutation verification, for any test backing a claim worth trusting
later**: temporarily revert the fix, confirm the test fails, restore
the fix, confirm it passes again, leave no mutation behind. Not
mandatory for every test in the repo, but expected for anything cited
as evidence a specific bug is fixed and stays fixed — a test that never
demonstrably fails is not proof of anything. Every test listed below
has been through this at least once.

**Production protocol regression** (`internal/broker`) — tests real
Keel behavior, no dependency on `internal/conformance`:
- `TestNew_NoInheritedPropertiesOnAckAlwaysEnabled` (`broker_test.go`) —
  PUBACK/PUBREC must never echo the original PUBLISH's Properties.
- `TestFlowControl_ReceiveMaximumExceeded_DisconnectsWithReasonBeforeClose`
  (`flow_control_test.go`) — exceeding Receive Maximum gets a DISCONNECT
  (reason 0x93) written to the wire *before* the connection closes.
- `TestMaxKeepAliveHook_*` (`max_keepalive_test.go`) and
  `TestMaxKeepAlive_EndToEnd_ConnackServerKeepAliveProperty`
  (`flow_control_test.go`) — the `MaxKeepAlive` feature's MQTT5-only
  Server Keep Alive override, including the MQTT 3.1.1-untouched
  guarantee and the below/at/above/zero/extreme boundary matrix.
- `TestProductionACL_Unchanged` (`hooks_test.go`) — production ACL still
  denies the same Paho-suite-shaped topics `--conformance-test` allows;
  the two modes must never converge by accident.

**Conformance harness regression** (`internal/conformance`) — tests
behavior needed *only* to keep `--conformance-test` passing the Paho
suite; changing these never affects production:
- `TestConformanceMode_*` (`mode_test.go`) — standalone-only, fail-fast
  on core/edge/combined.
- `TestConformanceAuth_*` (`auth_test.go`) — allow-all provider.
- `TestConformanceACL_*` (`acl_test.go`) — allow-arbitrary-topics ACL,
  including the exact (subscribe-only) `test/nosubscribe` semantics.
- `TestApplyCompatibilities_*` (`compat_test.go`) — `ObscureNotAuthorized`
  is on for conformance and confirmed off by mochi-mqtt's own default
  otherwise; deliberately does *not* cover `NoInheritedPropertiesOnAck`
  anymore, since that one graduated to a production default (see above).
- `TestKeepAliveHook_*` (`keepalive_test.go`) — the Paho-specific
  `Keepalive==120 && Clean==true → 60` scaffolding hook. Not to be
  confused with `internal/broker`'s `MaxKeepAliveHook` above — that one
  is the real, general, production feature; this one only exists to
  satisfy one Paho test's exact magic numbers and must never be
  mistaken for it.

No broker-level regression test exists for `client_test5.py`'s shared
subscription scenario: its failure was entirely a bug in
`test/conformance/run_report.py` (a missing `topic_prefix` global), never
a Keel or mochi-mqtt behavior — nothing on the broker side needed
freezing.

### Result classification

Any conformance or protocol-test result is one of four, never a plain
pass/fail:

| Result | Meaning |
|---|---|
| `PASS` | expected behavior verified |
| `FAIL` | Keel (or mochi-mqtt, as used by Keel) behavior does not conform |
| `HARNESS` | the test/client library itself is the demonstrated cause, not the broker — must have a written investigation backing it, see `test/conformance/evidence/*.md` |
| `N/A` | requirement doesn't apply to this configuration/protocol version |

Never `SKIP` for any of these — it conflates causes that need different
follow-up (a real gap vs. an external tool's own bug vs. "doesn't apply
here") into one bucket, which is exactly what makes a conformance claim
hard to trust later.

**`HARNESS` is an asserted classification, not a fallback result.** It
is never the default reading of an inconvenient failure — it is a
specific claim that must earn its exit-code immunity. A `HARNESS`
result requires all of:

- documented evidence (packet capture, source inspection, an
  independent reproduction — not an assumption);
- reproducible behavior (not a one-off, flaky read);
- identification of the specific failing external component (which
  library, which code path — not "the test is probably wrong");
- proof that Keel's own protocol behavior remains correct (a `HARNESS`
  verdict on a case where the broker is *also* wrong is not a `HARNESS`
  verdict, it's a `FAIL` with an unrelated observation attached).

This is machine-enforced, not just a convention: `test/conformance/run_report.py`'s
`classify()` only accepts a failing test as `harness` if it's in the
reviewed `KNOWN_HARNESS_ISSUES` map **and** its evidence file exists
and structurally contains a Requirement/Test/Environment/Expected
behavior/Observed behavior/Evidence/Result — with the Result section
itself asserting `HARNESS` — see
`test/conformance/evidence/test_flow_control2.md` for the format that
satisfies it. Anything short of that — an unlisted failure, a listed
one whose evidence file is missing, or one whose evidence no longer
asserts `HARNESS` — falls back to a real `FAIL` and fails the run. This
fail-closed behavior is itself pinned by
`test/conformance/test_run_report.py` (run by `run.sh` before any real
infrastructure starts), covering exactly: two passes exiting clean, a
pass plus a genuine `HARNESS` exiting clean, a pass plus a real failure
exiting non-zero, and a "known" harness issue with missing or gutted
evidence exiting non-zero rather than silently disappearing.

**Conformance harness compatibility must never alter production
protocol semantics.** `--conformance-test` exists to validate the
protocol engine in isolation from Keel's own auth/ACL policy — not as a
place to accumulate workarounds that make an external suite pass. If a
fix belongs in production (a real spec-correctness gap), it goes in
`internal/broker` unconditionally, with its own regression test — see
`NoInheritedPropertiesOnAck`'s history for the precedent. If it's
scaffolding needed only to satisfy one specific test's assumptions, it
stays in `internal/conformance` and is documented as such, never quietly
promoted.

## Commit conventions

- No AI/LLM attribution or signature in commit messages — write commits as
  if authored directly by a human contributor.
- Prefer commit messages that explain *why* a change was made, not just
  what changed — the diff already shows the what.
- Keep commits focused; avoid bundling unrelated fixes with a feature.

## Proposing architectural changes

Before proposing a change to core architecture (clustering, session
ownership, routing, ACL model), read the relevant package docs and
existing comments — most non-obvious decisions (why Raft vs. Olric for a
given piece of state, TLS termination point, backpressure strategy) are
explained inline where they apply. If your change conflicts with an
existing decision, explain why in your PR description instead of
silently diverging from it.

For bug fixes, small features, or anything that doesn't touch a documented
architectural decision, no design-doc update is expected — just open a PR.

## Code style

- No comments explaining *what* code does — names should make that clear.
  Comments are for non-obvious *why* (a constraint, a workaround, an
  invariant a future reader could easily break).
- Follow existing package conventions (e.g. `internal/cluster/store`'s
  `ClusterStore` interface pattern for swappable backends) rather than
  introducing a new abstraction style for the same kind of problem.

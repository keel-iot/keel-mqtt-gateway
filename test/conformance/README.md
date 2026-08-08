# MQTT conformance suite

Runs the [Eclipse Paho MQTT interoperability suite](https://github.com/eclipse-paho/paho.mqtt.testing)
black-box, over the network, against a real `keel-mqtt-gateway` binary —
not a fork, not a mock, the same binary the Helm chart deploys.

## Why this exists

Keel wraps `mochi-mqtt` and adds its own multi-tenant auth/ACL on top.
Running an external protocol suite unmodified against it would test
Keel's *policy* (which credentials are valid, which topics a tenant may
use), not MQTT *protocol conformance* — the two need to be decoupled to
get a meaningful answer to "is the broker itself conformant?".

`--conformance-test` (see `internal/conformance`) solves this without
touching any production code path:

- a separate `AuthProvider` (`internal/conformance/auth.go`) that accepts
  any credentials, swapped in only for this mode;
- an extra mochi-mqtt hook (`internal/conformance/acl.go`) that allows any
  topic except a subscribe to `test/nosubscribe` — the exact, narrow
  semantics the suite's own `-n`/`--nosubscribe_topic_filter` documents,
  not a blanket publish+subscribe deny;
- refuses to start unless `--role=""` (standalone) — see
  `conformance.ValidateRole` — so it is structurally unreachable on a
  core/edge/combined cluster node, and never exposed by the Helm chart.

`internal/broker/hooks.go` (production ACL, live on Kimera) is never
imported by, or modified for, this mode — see
`TestProductionACL_Unchanged` in `internal/broker/hooks_test.go`.

**Conformance harness compatibility must never alter production
protocol semantics** — see `CONTRIBUTING.md`'s "Protocol regression
suite" section for that principle stated in full, and the four-valued
result classification (`PASS`/`FAIL`/`HARNESS`/`N/A`) used below and by
`run_report.py`.

## Running it

```
./test/conformance/run.sh
```

First runs `test_run_report.py` — a self-test of the report runner's own
classification semantics (no network/broker needed) — and aborts before
touching any real infrastructure if it fails. Then clones
`paho.mqtt.testing` at a pinned commit into `.cache/` (gitignored,
not vendored — separate project, separate license), starts a throwaway
Postgres container (required unconditionally by `internal/db.Migrate`,
same as any other run) and a `keel-server --conformance-test` instance,
then runs both suites and writes:

- `artifacts/mqtt_3_1_1.json`, `artifacts/mqtt_5.json` — machine-readable,
  one per suite, distinguishing real broker failures from known harness
  issues:
  ```json
  {"mqtt_5": {"passed": 26, "failed": 0, "harness": 1,
              "failed_tests": [],
              "harness_issues": [{"test": "test_flow_control2",
                                   "evidence": "evidence/test_flow_control2.md"}]}}
  ```
- `artifacts/report.md` — the same data as a human-readable table.

`artifacts/` is generated on every run (gitignored, a CI artifact, not a
source file) — the investigation backing each `harness` entry is
committed separately under `evidence/`, since that doesn't go stale the
way a raw pass/fail count does.

Exit code is non-zero only for a genuine (`failed`) broker regression —
a known, investigated harness issue does not fail the run, but is never
silently invisible either: it has its own count and its own evidence
file. Override ports via `KEEL_CONFORMANCE_{PG,MQTT,HTTP,METRICS}_PORT`
if the defaults collide with something already running locally.

## Results (last full run: 2026-08-07)

| Protocol | Passed | Failed (broker) | Harness issue |
|---|---|---|---|
| MQTT 3.1.1 | 10 | 0 | 0 |
| MQTT 5 | 26 | 0 | 1 |

**MQTT 3.1.1: 10/10 PASS** — including `test_subscribe_failure` (confirms
the conformance ACL's `test/nosubscribe` semantics end to end) and
`test_zero_length_clientid`.

**MQTT 5: 26 PASS, 1 HARNESS, 0 broker failures.** First run was 17/24
plus a hang (7 failures, 3 tests never reached); all but one were
root-caused the same day to two missing mochi-mqtt upstream compatibility
flags, one missing hook, and one bug in this harness's own runner — not
seven independent protocol defects.

**Important distinction, decided deliberately, not implicit:** this
result does **not** mean every policy choice the Paho suite exercises
was adopted in production, only that the protocol engine was validated
black-box with Keel's own application policy neutralized.

- **Production correctness fix — `NoInheritedPropertiesOnAck`**
  (`internal/broker/broker.go`, unconditional for every deployment):
  mochi-mqtt's own flag, documented upstream as "paho - spec violation".
  Without it, `server.go`'s `buildAck` echoes the original PUBLISH
  packet's `Properties` — including arbitrary User Properties — onto the
  PUBACK/PUBREC, which is never correct MQTT5 behavior regardless of any
  test suite. Fixed `test_retained_message`, `test_user_properties`, and
  very likely the post-`test_user_properties` hang too (a malformed
  PUBACK plausibly desyncs the suite's own socket framing for the next
  test — not independently isolated which exact symptom the hang was,
  since fixing the root cause removed the trigger for all of them at
  once). Pinned by `TestNew_NoInheritedPropertiesOnAckAlwaysEnabled` in
  `internal/broker/broker_test.go`.
- **Conformance-only — `ObscureNotAuthorized`** (`internal/conformance/compat.go`,
  never in `broker.go`): fixes `test_subscribe_failure` (SUBACK reason
  code 0x87 "Not Authorized" downgraded to the generic 0x80 the suite
  expects) — but this changes client-observable behavior on a live SUBACK.
  Deliberately **not** promoted to a production default: Kimera has real
  deployed clients today, and silently changing an ACL-denial reason code
  for all of them just to match a test suite isn't something to do
  implicitly. If wanted in production, it belongs behind an explicit
  config option (e.g. `security.obscureAuthorizationErrors`, default
  `false`), as a separate, deliberate change — not bundled into this one.
- **Fixed as a real feature — `test_server_keep_alive`**: `internal/broker.MaxKeepAliveHook`
  is a general, production, opt-in feature (`MAX_KEEPALIVE` env var /
  `maxKeepAlive` Helm value) — MQTT5-only by design (MQTT 3.1.1 has no
  Server Keep Alive property to inform the client, so it's left
  completely untouched; see the hook's doc for the full reasoning).
  `internal/conformance.KeepAliveHook` still exists separately as
  conformance-only scaffolding (the exact `Keepalive==120,Clean==true →
  60` magic-number match the suite asserts) — not to be confused with
  the real feature.
- **Fixed — `test_shared_subscriptions`**: not a keel/mochi issue at
  all — a gap in `run_report.py` itself, which didn't set the
  module-level `topic_prefix` global that `client_test5.py`'s own
  `__main__` block sets (`"client_test5/"`) before its shared-subscriptions
  test references it. Fixed in the wrapper, no broker change needed.

### Known harness deviations

**`test_flow_control2` — Result: `HARNESS`.** Full investigation:
[`evidence/test_flow_control2.md`](evidence/test_flow_control2.md).

Summary: the test expects a DISCONNECT (reason 0x93, Receive Maximum
exceeded) before the connection closes. Packet capture confirms Keel
writes DISCONNECT 0x93 synchronously, then closes cleanly (FIN, never an
RST it initiates). An independent Go client that reads every packet
strictly in order reproduces exactly the expected sequence — 1024
PUBREC, DISCONNECT 0x93, EOF — with zero errors. The Paho client's own
receive loop instead answers every PUBREC with an immediate PUBREL
write regardless of a pending disconnect, and one of those late writes
lands on a socket the server already closed — the resulting
`BrokenPipeError` happens in the *client's* PUBREC-handling code, never
while processing the DISCONNECT itself. No broker change was made: there
is no evidence of a Keel protocol violation, and "fixing" this would mean
changing correct behavior to accommodate a client-side ordering bug.

Local regression tests (no Paho suite dependency required) pinning
these results, each verified to actually fail when its corresponding
fix is reverted:

- `internal/broker/broker_test.go` — `TestNew_NoInheritedPropertiesOnAckAlwaysEnabled`
- `internal/broker/flow_control_test.go` — `TestFlowControl_ReceiveMaximumExceeded_DisconnectsWithReasonBeforeClose`
  (the Go reproduction behind the `test_flow_control2` HARNESS
  classification, kept permanently), `TestMaxKeepAlive_EndToEnd_ConnackServerKeepAliveProperty`
- `internal/broker/max_keepalive_test.go` — `TestMaxKeepAliveHook_*`
  (boundary matrix + MQTT 3.1.1 untouched guarantee)
- `internal/config/config_test.go` — `MAX_KEEPALIVE` validation
  (malformed, negative, wire-format boundary at exactly 65535s)
- `internal/conformance/compat_test.go` — `ObscureNotAuthorized`
- `internal/conformance/keepalive_test.go` — the conformance-only
  `KeepAliveHook` scaffolding

See `CONTRIBUTING.md`'s "Protocol regression suite" section for the full
inventory and the production-vs-conformance-only split, and
`docs/alternatives-and-future-work.md`'s roadmap for how this feeds into
the broader open-points list.

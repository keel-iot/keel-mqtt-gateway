# Keel MQTT Gateway — Feature Verification Matrix

What Keel actually verifies, not what it's assumed to support. Same
four-valued result model as `CONTRIBUTING.md`'s conformance
classification (`PASS` / `FAIL` / `HARNESS` / `N/A`), plus the internal
`UNRESOLVED` triage note for a result that's neither confirmed passing
nor confirmed failing yet — see that file for the full definitions.
`UNRESOLVED` is never a public result on its own: its row's public
status stays `FAIL` until it resolves one way or the other.

Every `Test reference` below is a real, greppable identifier — either an
`eclipse-paho/paho.mqtt.testing` test method name (run via
`test/conformance/run.sh`) or a Go test in this repo. No synthetic test
IDs are invented for rows that aren't actually covered by a test yet;
those are marked `NOT YET VERIFIED` instead of guessed at.

This is the baseline as of the "MQTT Correctness & Conformance
Baseline" milestone (2026-08-08) — see the pinned issue/milestone for
what "baseline" means here: future regressions are measured against
this table, not against a moving target.

## MQTT protocol — core

| Feature | MQTT 3.1.1 | MQTT 5 | Test reference | Status |
|---|---|---|---|---|
| Connect/Connack | YES | YES | `testBasic` / `test_basic` | PASS |
| QoS 0 | YES | YES | exercised across most tests below (no single dedicated test in either suite) | PASS |
| QoS 1 | YES | YES | `test_offline_message_queueing`, `test_redelivery_on_reconnect` | PASS |
| QoS 2 | YES | YES | `test_offline_message_queueing`, `test_redelivery_on_reconnect`; production ordering also pinned by `internal/broker/receive_maximum_test.go`'s `TestFlowControl_ReceiveMaximumExceeded_DisconnectsWithReasonBeforeClose` | PASS |
| Retained messages | YES | YES | `test_retained_messages` / `test_retained_message` | PASS |
| Persistent sessions (offline queueing) | YES | YES | `test_offline_message_queueing` | PASS |
| Redelivery on reconnect | YES | YES | `test_redelivery_on_reconnect` | PASS |
| Will message | YES | YES | MQTT5: `test_will_message`, `test_will_delay` — PASS. 3.1.1: **not exercised by this suite** (`client_test.py` has no will test) | MQTT5 PASS, 3.1.1 NOT YET VERIFIED |
| Overlapping subscriptions | YES | YES | `test_overlapping_subscriptions` | PASS |
| Unsubscribe | YES | YES | `test_unsubscribe` | PASS |
| Zero-length Client ID | YES | YES | `test_zero_length_clientid` | PASS |
| Keepalive (basic ping/pong) | YES | YES | `test_keepalive` | PASS |
| `$SYS` / dollar topics | YES | YES | `test_dollar_topics` | PASS |
| Subscribe failure (ACL deny → SUBACK reason) | YES | YES | `test_subscribe_failure` | PASS (see note below on `ObscureNotAuthorized`) |
| ACK properties not inherited from PUBLISH | N/A (no properties in 3.1.1) | YES | `internal/broker/broker_test.go`'s `TestNew_NoInheritedPropertiesOnAckAlwaysEnabled`; root-caused via `test_retained_message`/`test_user_properties` | PASS (production fix, unconditional) |

**Note on `test_subscribe_failure`**: PASS depends on `ObscureNotAuthorized`
being enabled, which is `--conformance-test`-only (see
`internal/conformance/compat.go`) — production SUBACK reason on ACL
denial is still 0x87 (Not Authorized), never downgraded to 0x80. This
row verifies the *mechanism* (a denied SUBSCRIBE gets a negative SUBACK
reason code at all), not that production emits the exact reason code
the suite expects. See `test/conformance/README.md` for the full
distinction.

## MQTT 5 — properties and extended features

| Feature | Test reference | Status |
|---|---|---|
| Shared Subscriptions (`$share/group/topic`) | `test_shared_subscriptions` | PASS |
| Receive Maximum | Paho: `test_flow_control2` — **HARNESS**, see `test/conformance/evidence/test_flow_control2.md`. Independently: `internal/broker/receive_maximum_test.go`'s `TestFlowControl_ReceiveMaximumExceeded_DisconnectsWithReasonBeforeClose` | Paho result: HARNESS. Underlying behavior: PASS (independently verified — DISCONNECT 0x93 before close, confirmed by packet capture + source inspection + a Go client reading in strict order) |
| Server Keep Alive (protocol mechanism) | `test_server_keep_alive` (validated via `internal/conformance/keepalive.go`'s conformance-only scaffolding hook, not production) | PASS |
| `MaxKeepAlive` (production feature) | `internal/broker/max_keepalive_test.go`'s `TestMaxKeepAliveHook_*` (boundary matrix + MQTT 3.1.1-untouched guarantee), `internal/broker/max_keepalive_integration_test.go`'s `TestMaxKeepAlive_EndToEnd_ConnackServerKeepAliveProperty` | PASS — opt-in, disabled by default, see `README.md`'s MQTT Conformance section |
| Session Expiry Interval | `test_session_expiry` | **UNRESOLVED** — failed once (host-load timing), passed on immediate retry, not yet reproducible either way. Tracked: [keel-iot/keel-mqtt-gateway#3](https://github.com/keel-iot/keel-mqtt-gateway/issues/3). **Public status: FAIL** until resolved — an unresolved flake is never silently treated as passing |
| Flow Control (general, Receive Maximum edge excluded) | `test_flow_control1` | PASS |
| Topic Alias (client → server) | `test_client_topic_alias` | PASS |
| Topic Alias (server → client) | `test_server_topic_alias` | PASS |
| Maximum Packet Size | `test_maximum_packet_size` | PASS |
| Payload Format Indicator | `test_payload_format` | PASS |
| Message Expiry Interval (Publication Expiry) | `test_publication_expiry` | PASS |
| Subscription Identifiers | `test_subscribe_identifiers` | PASS |
| Subscribe Options (No Local, Retain As Published, Retain Handling) | `test_subscribe_options` | PASS |
| Assigned Client Identifier | `test_assigned_clientid` | PASS |
| User Properties | `test_user_properties` | PASS |
| Request/Response (Response Topic + Correlation Data) | `test_request_response` | PASS |

## Not yet in this matrix

Deliberately absent rather than guessed at — these need either a Paho
test not yet run against Keel, or a dedicated local test, before they
get a row:

- Enhanced authentication (AUTH packet / SASL-style challenge flows) —
  not exercised by the current Paho run.
- Will Delay Interval boundary behavior beyond `test_will_delay`'s
  single scenario.
- Clean Start / Session Present interaction matrix beyond what
  `test_session_expiry` covers (currently `UNRESOLVED`).

## Cluster scenarios

Not yet populated. Next planned section — Core/Edge failover, session
ownership handoff, routing convergence, Redis primary promotion under
real client load, following the same evidence-and-test-reference
discipline as above rather than prose claims.

## Failure / chaos scenarios

Not yet populated. See `docs/alternatives-and-future-work.md`'s roadmap
(chaos test suite) for the planned scope — this section gets populated
once those tests exist, not before.

## Performance / scalability

Not yet populated. `internal/telemetry`'s metrics and the existing
devicesim load-test results (see project memory / prior session notes)
are inputs to this section once formalized into repeatable, referenced
benchmarks rather than one-off session logs.

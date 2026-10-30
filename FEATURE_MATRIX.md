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

**Phase 1 ("Business Readiness") froze on top of this baseline on
2026-08-10, commit `88bd2ac406402f0fe748224f941adcb98438338d`, tag
`phase1-business-readiness-baseline`** — see `ROADMAP.md`'s Phase 1
section for the full completion/deferred-items record. The rows below
already reflect that frozen state.

## MQTT protocol — core

| Feature | MQTT 3.1.1 | MQTT 5 | Test reference | Status |
|---|---|---|---|---|
| Connect/Connack | YES | YES | `testBasic` / `test_basic` | PASS |
| QoS 0 | YES | YES | exercised across most tests below (no single dedicated test in either suite) | PASS |
| QoS 1 | YES | YES | `test_offline_message_queueing`, `test_redelivery_on_reconnect`; `TestUpstreamQoS1SuccessReasonIsValidPubackCode`, `TestUpstreamInboundQoS1DoesNotPersistStatelessAck` in the Mochi fork | PASS |
| QoS 2 | YES | YES | `test_offline_message_queueing`, `test_redelivery_on_reconnect`; production ordering also pinned by `internal/broker/receive_maximum_test.go`'s `TestFlowControl_ReceiveMaximumExceeded_DisconnectsWithReasonBeforeClose` | PASS |
| Retained messages | YES | YES | `test_retained_messages` / `test_retained_message` | PASS |
| Persistent sessions (offline queueing) | YES | YES | `test_offline_message_queueing`; `TestOrdinaryOfflineDeliveryPreservesQoSState` and `TestDisconnectDuringPersistenceSeparatesOrdinaryAndSharedDelivery` in `thirdparty/mochi-mqtt-server/offline_delivery_regression_test.go` guard the local shared-fallback integration | PASS |
| Redelivery on reconnect | YES | YES | `test_redelivery_on_reconnect` | PASS |
| Will message | YES | YES | MQTT5: `test_will_message`, `test_will_delay` — PASS. 3.1.1: verified why the Paho suite never exercises it (`client_test.py`'s `will_message_test` isn't `test_`-prefixed, so `unittest`'s default discovery skips it) and closed the gap directly: `internal/broker/will_test.go`'s `TestWillMessage_MQTT311_UngracefulDisconnectTriggersWill`, `TestWillMessage_MQTT311_GracefulDisconnectDoesNotSendWill` (mutation-tested) | PASS (MQTT5 and 3.1.1) |
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
| Session Expiry Interval | `test_session_expiry` | **UNRESOLVED** — failed once (host-load timing), passed on immediate retry. Phase 2 investigation (source review of mochi-mqtt's 1s sweep granularity + 20/20 clean reproduction attempts against a fresh build) found no defect, but a clean run doesn't erase the original failure. Tracked: [keel-iot/keel-mqtt-gateway#3](https://github.com/keel-iot/keel-mqtt-gateway/issues/3). **Public status: FAIL** until resolved — an unresolved flake is never silently treated as passing |
| Session expiry — Redis cleanup (standalone) | `internal/broker/redis_session_expiry_cleanup_test.go`'s `TestOnDisconnectExpire_CleansSubscriptionsAndInflight`, `TestOnClientExpired_CleansAllSessionState` | PASS — real gap found and fixed: neither path cleaned up subscription/inflight Redis state before, only the client record (and only on the immediate-expiry path, not the periodic sweep at all). Mutation-tested. The cluster-only packet-ID-counter half of this same gap stays tracked under Phase 3 (`docs/testing/CLUSTER_CORRECTNESS_MATRIX.md`) |
| Flow Control (general, Receive Maximum edge excluded) | `test_flow_control1` | PASS |
| Topic Alias (client → server) | `test_client_topic_alias`; fork `TestUpstreamUnregisteredTopicAliasIsRefused` covers unknown aliases for QoS0/1/2 | PASS |
| Topic Alias (server → client) | `test_server_topic_alias` | PASS |
| Maximum Packet Size | `test_maximum_packet_size` | PASS |
| Explicit zero-valued CONNECT limits / Subscription Identifier | Fork `TestUpstreamZeroLimitsAreRefusedOnWire`; packet tests `TestUpstreamConnectDistinguishesAbsentAndZeroLimits`, `TestUpstreamSubscribeRejectsExplicitZeroIdentifier` cover absent/zero/positive properties | PASS |
| Payload Format Indicator | `test_payload_format` | PASS |
| Message Expiry Interval (Publication Expiry) | `test_publication_expiry` | PASS |
| Subscription Identifiers | `test_subscribe_identifiers` | PASS |
| Subscribe Options (No Local, Retain As Published, Retain Handling) | `test_subscribe_options`; controlled upstream/fork reproduction in `test/conformance/evidence/subscription_barrier_race.md` | Paho's initial No Local missing-message assertion: HARNESS only for the documented traceback; complete test passes with the correct subscriber's SUBACK barrier. Other assertions remain real failures if they fail |
| Assigned Client Identifier | `test_assigned_clientid` | PASS |
| User Properties | `test_user_properties`; fork `TestUpstreamProblemInfoDoesNotStripPublishProperties` checks PUBLISH vs ACK with Request Problem Information 0/1 | PASS |
| Request/Response (Response Topic + Correlation Data) | `test_request_response`; controlled upstream/fork reproduction in `test/conformance/evidence/subscription_barrier_race.md` | Paho's initial missing-message assertion: HARNESS only for the documented traceback; complete test passes with the correct subscriber's SUBACK barrier, including property and reply assertions. Other failures remain real failures |

## Transport (not MQTT protocol per se, but part of Phase 1's "deal breaker" gaps — see `ROADMAP.md`)

| Feature | Status | Notes |
|---|---|---|
| TCP (plain) | PASS | `internal/broker/broker.go`'s `listeners.NewTCP` |
| TLS / mTLS | PASS | `internal/broker/broker.go`, `CertReloader`, X.509 CN auth path |
| WebSocket / WSS | PASS | `internal/broker/broker.go`'s `MQTTWSPort`/`MQTTWSSPort`, sharing TLS cert/reload with the TLS-TCP listener. `TestWebSocket_EndToEnd_ConnectPublishSubscribe` (real CONNECT/SUBSCRIBE/server-push over a live WebSocket, production ACL enforced), `TestNew_WSSRequiresTLSCertDir` (config validation). Closed: [keel-iot/keel-mqtt-gateway#5](https://github.com/keel-iot/keel-mqtt-gateway/issues/5) |
| Proxy Protocol | PASS | `internal/broker/proxyproto_listener.go` (v1/v2 via `github.com/pires/go-proxyproto`), applied to the plain and TLS TCP listeners; requires an explicit trusted-CIDR allowlist, untrusted senders rejected outright. `TestProxyProtocol_TCP_TrustedSourceRealIP`, `TestProxyProtocol_TCP_UntrustedSourceRejected`, `TestNew_ProxyProtocolRequiresTrustedCIDRs`, `TestNew_ProxyProtocolInvalidCIDR`. Closed: [keel-iot/keel-mqtt-gateway#6](https://github.com/keel-iot/keel-mqtt-gateway/issues/6) |

## Security (part of Phase 1's "deal breaker" gaps — see `ROADMAP.md`)

| Feature | Status | Notes |
|---|---|---|
| Connection concurrency limit | PASS | `TenantGatewayConfig.MaxConnections`, enforced in `internal/broker/hooks.go`'s `OnConnectAuthenticate` |
| Connect-attempt rate limiting | PASS | `internal/broker/ratelimit.go`'s `keyedRateLimiter`, keyed by source IP (the real client IP when behind a trusted PROXY-protocol LB), checked before `authenticate()` runs. `TestConnectRateLimit_OverLimitRejectedBeforeAuth`, `TestConnectRateLimit_DifferentSourceIPIndependentBucket`, `TestConnectRateLimit_RefillAllowsReconnectAfterWait`, `TestConnectRateLimit_TrustedProxyProtocolIPUsedAsKey`, `TestConnectRateLimit_UntrustedProxyProtocolNeverReachesLimiter`, `TestKeyedRateLimiter_*` (unit). Closed: [keel-iot/keel-mqtt-gateway#8](https://github.com/keel-iot/keel-mqtt-gateway/issues/8) |
| Publish-rate limiting | PASS | Same `keyedRateLimiter`, keyed by tenant. MQTT5 QoS1/2 gets PUBACK/PUBREC reason 0x97 (Quota Exceeded); QoS0 and MQTT 3.1.1 are silently dropped (no ack, no disconnect) — no protocol reason-code mechanism exists there. `TestPublishRateLimit_BurstConsumedThenQuotaExceeded`, `TestPublishRateLimit_TenantIsolation`, `TestPublishRateLimit_MQTT5_QoS1_PubackQuotaExceeded`, `TestPublishRateLimit_MQTT5_QoS2_PubrecQuotaExceeded`, `TestPublishRateLimit_MQTT5_QoS0_DroppedConnectionSurvives`, `TestPublishRateLimit_MQTT311_DroppedNotDisconnected`, `TestPublishRateLimit_EmptyTenantNeverThrottled`. The MQTT5 QoS2 case required a fix: `github.com/mochi-mqtt/server/v2` v2.7.9 answered a QoS2 publish rejected via `OnPublish` with a PUBACK instead of a PUBREC (real wire-protocol violation, confirmed by source read + minimal standalone repro) — reported/fixed upstream ([issue #510](https://github.com/mochi-mqtt/server/issues/510), [PR #511](https://github.com/mochi-mqtt/server/pull/511)), patched locally in the interim via `thirdparty/mochi-mqtt-server` (see its `PATCH.md`), removed once a corrected release ships. Closed: [keel-iot/keel-mqtt-gateway#8](https://github.com/keel-iot/keel-mqtt-gateway/issues/8) |
| Rate-limit metric cardinality | `internal/broker/ratelimit_metrics_test.go`'s `TestRateLimitedTotal_BoundedCardinality` | PASS — production correctness contract of the rate-limiter design (`RateLimitedTotal` must carry only a fixed `type` enum, never ip/tenant/client_id), previously unenforced by any test. Triggers real rejections from several distinct source IPs/tenants and asserts the metric family's series count stays exactly 2. Mutation-tested |

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

| Behavior | Evidence | Scope |
|---|---|---|
| Lazy outbound storage with unchanged configured capacity | `TestPacketQueueStorageIsLazyAndLimitUnchanged`, `TestPacketQueueFullConfiguredCapacity`, `TestClientSlowWriterPreserves8192PendingLimit` in `thirdparty/mochi-mqtt-server/packet_queue_test.go` | Local tests including blocked writes with QoS 0/1/2; not cluster capacity certification |
| Independent shared snapshots without an intermediate copy | `TestSharedGatherSnapshotIsIndependent`, `TestSharedGatherDuringSubscriptionChanges`, `BenchmarkSharedGather12Members` in `thirdparty/mochi-mqtt-server/shared_snapshot_test.go` | Map ownership, concurrency, and helper allocation benchmark |
| Stateless inbound QoS1 ACK storage | Fork `TestUpstreamInboundQoS1DoesNotPersistStatelessAck`; `internal/broker/redis_stateless_qos1_test.go`'s `TestRedisStatelessInboundQoS1CannotDeleteOutboundDelivery` | Real Redis schemas 1/2: zero commands for the stateless callbacks, preserved equal-ID outbound record, and normal outbound completion |

Measurements and limitations: [client memory investigation](docs/testing/MEMORY_ALLOCATION_PROFILE.md).

Selective release-candidate backports and their protocol/race evidence:
[Mochi upstream adoption](docs/testing/MOCHI_UPSTREAM_ADOPTION.md), including
`TestUpstreamWrappedRefusalPreservesQoSAck`,
`TestUpstreamDisconnectRespectsProtocolAndPassiveMode`, and
`TestUpstreamRetainMessageWhileScanningDoesNotRace` in the fork.

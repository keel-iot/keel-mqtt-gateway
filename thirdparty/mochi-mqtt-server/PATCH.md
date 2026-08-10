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

Remove `replace github.com/mochi-mqtt/server/v2 => ./thirdparty/mochi-mqtt-server`
from `go.mod`, delete this directory, and bump the dependency once a
released version of `github.com/mochi-mqtt/server/v2` upstream contains
this fix.

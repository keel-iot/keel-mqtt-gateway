# mochi-511 — validation note

Upstream PR: [mochi-mqtt/server#511](https://github.com/mochi-mqtt/server/pull/511)
Upstream issue: [mochi-mqtt/server#510](https://github.com/mochi-mqtt/server/issues/510)
Tested commit: `1b69aec` (branch `fix-qos2-quota-exceeded-pubrec`, `fcraviolatti/server` fork, rebased onto upstream `main` `5b7f94b`)
Downstream patch: MOCHI-PATCH-001 (`docs/upstream/mochi-mqtt.md` §6), applied at `thirdparty/mochi-mqtt-server/`
Status: **DOWNSTREAM VALIDATED**, **ADOPTED**

## Requirement

MQTT5 §3.4.2 / §3.5.1: PUBREC is the defined response to a QoS 2
PUBLISH. Reason code 0x97 (Quota Exceeded) is an explicitly valid
PUBREC reason code. A PUBREC with a reason code >= 0x80 ends that
publish negatively; the sender must not send PUBREL for it.

## Bug (verified from source)

`Server.processPublish`'s `OnPublish`-error branch
(`server.go` v2.7.9, lines 920-926) hardcodes `packets.Puback` as the
ack type regardless of the publish's QoS:

```go
} else if cl.Properties.ProtocolVersion == 5 && pk.FixedHeader.Qos > 0 && errors.As(err, new(packets.Code)) {
    err = cl.WritePacket(s.buildAck(pk.PacketID, packets.Puback, 0, pk.Properties, err.(packets.Code)))
```

The ACL-deny branch earlier in the same function already selects
PUBACK vs PUBREC by QoS correctly — this branch never had that logic.

## Environment

- Go: `go1.26.2` (per `go.mod`'s `go 1.26.2` for Keel; the reproduction
  and mutation test below were run against the pristine upstream module
  directly, independent of Keel's own toolchain pin).
- OS/arch: linux/amd64 (WSL2 kernel 6.6.87.2).
- mochi-mqtt: `v2.7.9` (pristine, from the Go module proxy) for the
  initial reproduction; the fork commit above for the fix.

## Reproduction (minimal, standalone, no Keel dependencies)

Self-contained Go program using only `mochi-mqtt/server/v2`: registers
an `OnPublish` hook returning `packets.ErrQuotaExceeded` unconditionally,
connects a raw MQTT5 client, sends a QoS2 PUBLISH, reads the raw ack
packet type off the wire.

Run against pristine `v2.7.9`:
```
expected packet type 5 (PUBREC), got packet type 4
BUG CONFIRMED: server answered a QoS2 publish's OnPublish-hook rejection with a PUBACK, not a PUBREC
```

## Fix

```go
ackType := packets.Puback
if pk.FixedHeader.Qos == 2 {
    ackType = packets.Pubrec
}
err = cl.WritePacket(s.buildAck(pk.PacketID, ackType, 0, pk.Properties, err.(packets.Code)))
```

## Correctness

**PASS.** New upstream-side test `TestServerProcessPublishOnPublishAckErrorQoS2GetsPubrec`
(added in the fork commit, mirrors the existing
`TestServerProcessPublishOnPublishAckErrorContinue` but for QoS2, asserts
the raw PUBREC bytes byte-for-byte via `PubrecEncode`) passes against the
fix.

Full upstream package suite (`go test . -count=1` in the fork) — PASS,
no regressions introduced elsewhere in `server_test.go`.

## Mutation test

Reverted the fix (restored the hardcoded `packets.Puback`) — the new
test fails exactly as expected:
```
expected: []byte{0x50, 0x1d, 0x0, 0x7, 0x99, ...}   (PUBREC, 0x50)
actual  : []byte{0x40, 0x1d, 0x0, 0x7, 0x99, ...}   (PUBACK, 0x40)
--- FAIL: TestServerProcessPublishOnPublishAckErrorQoS2GetsPubrec
```
Restored — passes again. Confirms the test genuinely exercises the fix,
not a tautology.

## Race detector

Not independently re-run for this single-line, single-goroutine-path
change — no concurrency primitive is touched. (Noted rather than
silently omitted: this is a deliberate scope decision, not an oversight.)

## Keel regression suite

**PASS.** `internal/broker/ratelimit_integration_test.go`'s
`TestPublishRateLimit_MQTT5_QoS2_PubrecQuotaExceeded` — fails against
the unpatched dependency, passes against `thirdparty/mochi-mqtt-server`'s
patched copy. Full Keel suite (`go test ./... -count=1`) green except
one pre-existing, unrelated failure (`internal/db`'s `TestMigrate`,
predates this change, not touched by it).

## MQTT conformance

Not re-run as a full Paho suite pass for this specific patch — the
defect and fix are narrowly scoped to a hook-error ack path not directly
exercised by the existing Paho conformance run's flow-control/quota
scenarios. Covered instead by the targeted Keel regression test above,
which is the more precise instrument for this exact case.

## Benchmark

Not applicable — correctness fix, no performance claim.

## Conclusion

**DOWNSTREAM VALIDATED.** Applied as MOCHI-PATCH-001. Removal condition:
a released `github.com/mochi-mqtt/server/v2` version containing this
fix (i.e. once mochi-mqtt/server#511 merges and ships in a tagged
release) — then delete the `go.mod` replace directive and
`thirdparty/mochi-mqtt-server/`, bump the dependency, and rerun
`TestPublishRateLimit_MQTT5_QoS2_PubrecQuotaExceeded` against the real
released module before considering the removal complete.

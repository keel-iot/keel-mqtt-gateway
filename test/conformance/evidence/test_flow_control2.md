# test_flow_control2 — investigation record

## Requirement

MQTT5 Receive Maximum enforcement (spec §3.2.2.3.3, §3.14.2.1 reason code
0x93 "Receive Maximum exceeded"): a server that receives more concurrent
QoS1/2 publishes than the Receive Maximum it advertised in CONNACK must
disconnect the client, with reason code 0x93.

## Test

`paho.mqtt.testing`'s `client_test5.py::test_flow_control2`.

## Environment

- `keel-mqtt-gateway` built from this working tree, `--conformance-test`,
  standalone.
- mochi-mqtt v2.7.9 (vendored dependency, unmodified).
- Reproduced 2026-08-07, `test/conformance/run.sh` (Paho suite) and an
  independent minimal Go reproduction (below) against the same binary.
- Packet capture: `tcpdump -i lo -n 'tcp port <mqtt-port>'`, loopback,
  same host. Not stored in this repo (ephemeral capture, not a build
  artifact) — reproducible on demand via the steps below.

## Expected behavior

Server advertises Receive Maximum = 1024 (mochi-mqtt's own default,
`Options.Capabilities.ReceiveMaximum`, present in CONNACK). On the
1025th concurrent QoS2 PUBLISH, the server sends DISCONNECT (reason
0x93) to the socket, then closes the TCP connection — DISCONNECT must
precede the close, not race it.

## Observed behavior

**On the wire** (tcpdump, loopback, last packets of the exchange):
```
server -> client: length=4   (PUBREC, one of 1024 successful QoS2 publishes)
server -> client: length=31  (DISCONNECT, reason 0x93 + ReasonString property)
server -> client: Flags [F.] (clean FIN — graceful close, not RST)
client -> server: length=4   (a late PUBREL, sent by the Paho client after the FIN)
server -> client: Flags [R]  (RST — the OS's normal reply to data arriving on an
                               already-closed socket; not something Keel sends
                               proactively)
```

**In mochi-mqtt source** (`server.go`):
- `processPublish` checks `receiveQuota == 0` before anything else and
  calls `DisconnectClient(cl, packets.ErrReceiveMaximum)` — reason
  0x93, confirmed at the exact call site.
- `DisconnectClient` calls `cl.WritePacket(out)` (a real, synchronous
  `net.Conn.Write`, confirmed in `clients.go`'s `WritePacket` — not
  queued/async) **before** `cl.Stop(code)` closes the connection.
  Write-then-close, in that order, unconditionally.

**Independent reproduction** (Go, reads every packet strictly in order,
never writes back on receiving a PUBREC — see
`internal/broker/flow_control_test.go` for the same logic as a
permanent regression test):
```
CONNACK: reasonCode=0 receiveMax=1024
sent 1025 publishes
DISCONNECT received: reasonCode=147 (0x93)
read ended after 1025 packets: EOF
disconnectSeen=true reason=147 packetsRead=1025
post-disconnect read: EOF
```
1024 PUBRECs, then the DISCONNECT, then a clean EOF. No error, no
reset, no ambiguity.

**Harness behavior** (`paho.mqtt.testing`'s own client,
`mqtt/clients/V5/internal.py`'s `Receivers.receive`): on every PUBREC
received, it unconditionally sends a PUBREL in response
(`self.socket.send(self.pubrel.pack())`), regardless of how many PUBRECs
are still queued or whether the peer has already signaled it's closing.
Because TCP delivers bytes in order, the client must drain all 1024
PUBRECs — each triggering an outbound PUBREL — before it ever reaches
the DISCONNECT byte in the stream. By the time the client is still
mid-drain, the server has already sent DISCONNECT and closed; one of
the client's own late PUBREL writes lands on a socket the OS already
tore down, and the *next* write raises `BrokenPipeError` once the
kernel processes the resulting RST. The exception fires inside the
PUBREC-handling branch, never while handling the DISCONNECT itself —
confirmed from the traceback (`internal.py:85`, the
`PacketTypes.PUBREC` branch).

## Evidence

- Packet capture (tcpdump, loopback) — DISCONNECT (0x93) precedes FIN;
  RST is a reply to a late client write, not server-initiated.
- `mochi-mqtt` v2.7.9 source inspection — `processPublish`,
  `DisconnectClient`, `WritePacket` (synchronous write, write-before-close
  ordering).
- Independent Go reproduction reading packets strictly in order —
  matches expected behavior exactly, zero errors. Preserved as
  `internal/broker/flow_control_test.go`'s
  `TestFlowControl_ReceiveMaximumExceeded_DisconnectsWithReasonBeforeClose`.
- `paho.mqtt.testing` client source (`mqtt/clients/V5/internal.py`) —
  confirms the unconditional PUBREC→PUBREL write with no
  already-disconnecting check.

## Result

**HARNESS** — not a Keel failure, not a mochi-mqtt failure. Keel emits
the required DISCONNECT 0x93 before an orderly TCP close; the Paho
client's own receive loop keeps writing after the peer has begun
shutdown, causing a local `BrokenPipeError` in its own code path before
it ever processes the DISCONNECT packet already waiting in its buffer.

No broker change made. No workaround for this specific harness
behavior was added to `--conformance-test` either — see
`CONTRIBUTING.md`'s "Conformance harness compatibility must never alter
production protocol semantics."

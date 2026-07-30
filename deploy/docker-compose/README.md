# keel-mqtt-gateway — docker-compose PoC

3 core nodes, no edge nodes (per the design doc's phase-1 scope). The
gateway migrates its own schema on startup (see `internal/db`) against an
otherwise-empty Postgres — `devices.tenant_gateway_config` ends up empty
(the query's safe default applies); `AUTH_BACKEND=file` with
`credentials.yaml` supplies two test devices so no real device database is
needed.

```
docker compose up -d --build
```

## 1. Bootstrap from zero

```
curl -s http://localhost:18090/api/cluster/nodes | python3 -m json.tool
```

All three nodes should appear with `"raft_voter": true`; exactly one has
`"is_leader": true` (core-1 self-elects via `--bootstrap=true`, core-2/3
are added as voters once core-1's leader-election completes — see
`internal/cluster/membership`'s `reconcileVotersLoop`, not just the
join-event path, since the join events race the election on a genuinely
simultaneous 3-node start).

## 2. Late join

```
docker compose stop core-3
curl -s http://localhost:18090/api/cluster/nodes | python3 -m json.tool   # core-3 gone
docker compose start core-3
sleep 10
curl -s http://localhost:18090/api/cluster/nodes | python3 -m json.tool   # core-3 back, still a voter
```

core-3's raft log/snapshots persist on its named volume, so it catches up
rather than re-bootstrapping — confirmed by the startup log line
`cluster: raft log already exists, skipping bootstrap (restart)`.

## 3. Voluntary drain + rejoin

```
docker exec keel-mqtt-gateway-poc-core-1-1 keel-mqtt-gateway drain --management-addr=http://localhost:8090
docker compose logs core-1 --tail 5   # "lifecycle: leadership transferred", "lifecycle: left gossip cluster"
docker compose restart core-1
sleep 10
curl -s http://localhost:28090/api/cluster/nodes | python3 -m json.tool   # core-1 back on all peers' views
```

## 4. Cross-node routing (subscribe replication + dataplane forward)

Devices in this gateway are telemetry-publish-only / own-command-subscribe-only
(see `internal/broker/hooks.go`'s `OnACLCheck`) — no two devices can
legally pub/sub to each other's topics, so a fully device-driven "publish
on node A reaches subscriber on node B" test isn't reachable through MQTT
alone. This validates the same mechanism in two parts instead:

**4a. Raft replication of subscribe state** (fully MQTT-driven, ACL-legal):

```
mosquitto_sub -h localhost -p 21883 \
  -i "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" \
  -u "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb@11111111-1111-1111-1111-111111111111" \
  -P "testpass123" \
  -t "command/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" -v
```

While that's connected to **core-2**, check **core-1**'s view of the
routing table:

```
curl -s http://localhost:18090/api/cluster/routes | python3 -m json.tool
# {"command/bbbbbbbb-...": ["core-2"]}
```

Proves `OnSubscribed` → `Registry.Subscribe` → `raft.Apply` replicated the
subscription to every voter, not just core-2.

**4b. Dataplane forward → local delivery** (needs a tiny Go gRPC client
since no device can legally publish to `command/*`; the debug port
`27100` on core-2 is exposed exactly for this):

```go
// call clusterpb.DataplaneClient.Forward against localhost:27100 with
// Topic: "command/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", any payload
```

The still-connected `mosquitto_sub` from 4a receives the payload —
confirms `GRPCForwarder`'s inbound handler correctly calls
`mqttServer.Publish()` (requires `mqtt.Options.InlineClient = true`,
enabled in `internal/broker/broker.go`) and that delivery reaches a real
locally-connected client.

In production, `OnPublish` triggers 4b automatically for any topic a
device legally publishes to that also has cluster-wide subscribers (see
`forwardToClusterSubscribers` in `internal/broker/hooks.go`) — 4a+4b
together are the two halves of that same path, exercised separately here
only because of the ACL constraint above.

## 5. Activating the `keel-device-default` RBAC ruleset

RBAC ACL evaluation (`internal/cluster/acl`) is wired in additively
alongside the legacy hardcoded checks in `internal/broker/hooks.go`'s
`OnACLCheck` — see that function's doc comment. **No standard ruleset is
enabled by default** on a fresh cluster (this compose file included): the
`keel-device-default` ruleset ships defined in
`internal/cluster/acl/standard.go` but inert until explicitly turned on
via the management API/CLI, exactly like a custom role/binding. This is a
deliberate choice, not an oversight — enabling it changes real
authorization behavior, so it's opt-in per environment rather than
silently on everywhere.

Enable it against any core node's management API (the write is forwarded
to the raft leader automatically, see `CoreRegistry.EnableRuleset`):

```
docker exec keel-mqtt-gateway-poc-core-1-1 keel-mqtt-gateway acl ruleset-enable \
  --management-addr=http://localhost:8090 --name=keel-device-default
```

Confirm it replicated to every node:

```
curl -s http://localhost:18090/api/acl/rulesets | python3 -m json.tool
curl -s http://localhost:28090/api/acl/rulesets | python3 -m json.tool
curl -s http://localhost:38090/api/acl/rulesets | python3 -m json.tool
# all three: ["keel-device-default"]
```

Once enabled, `OnACLCheck` starts getting an explicit (non-nil-`Rule`)
RBAC decision for every device-ID-only topic shape the ruleset
reproduces (own `telemetry/%c/#` and `event/%c/#` publish, `t`/`e`/
`telemetry`/`event` short-alias publish, `status/heartbeat|ota|ca`
publish, own `cmd/%c`/`command/%c(/#)` subscribe — see the ruleset's own
doc comment for the exact list) — RBAC becomes authoritative for those
shapes instead of merely falling through to the legacy checks. Verify
with the same devices used in §4:

```
mosquitto_pub -h localhost -p 11883 \
  -i "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" \
  -u "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa@11111111-1111-1111-1111-111111111111" \
  -P "testpass123" \
  -t "t/self-test" -m "hello"   # allowed by keel-device-default's unconditional 't' alias
```

`keel-device-default` does **not** reproduce the real Hono
tenant-qualified `telemetry/<tenant>/<device>/...` ownership check or
`via/<uuid>/...` gateway delegation (RBAC's `EvaluateACL` only carries
clientID/username, no tenant — see `standard.go`'s doc comment for the
full rationale). Those shapes correctly abstain (nil `Rule`) and keep
falling through to the legacy `isAllowedPublish`/`isHonoTopicOwned` logic
in `hooks.go` even with the ruleset enabled — enabling
`keel-device-default` narrows, but does not eliminate, the legacy ACL
code path.

To disable again (e.g. to compare behavior with/without):

```
docker exec keel-mqtt-gateway-poc-core-1-1 keel-mqtt-gateway acl ruleset-disable \
  --management-addr=http://localhost:8090 --name=keel-device-default
```

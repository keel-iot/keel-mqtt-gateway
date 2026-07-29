#!/usr/bin/env bash
# Track B / task 1: measure QoS1 message loss when the edge node owning a
# disconnected client's persistent session is killed unplanned (docker kill,
# not a drain) — design doc risk #6.
#
#   1. bring up the core/edge split cluster (docker-compose.core-edge-split.yml)
#   2. register TWO independent persistent (clean_session=false) QoS1
#      subscriptions on edge-2, then disconnect both — mochi-mqtt owns each
#      offline queue entirely in edge-2's process memory (see
#      internal/broker/redis_session.go's package doc). Two independent
#      client/topic pairs, not one reused across both scenarios below: once
#      a session's queue is genuinely delivered somewhere (scenario A
#      succeeding is the whole point of this test), Redis correctly deletes
#      those messages on ack — reusing the same client/topic for scenario B
#      afterward would measure "nothing left to redeliver", not "did this
#      node's own restart recover its queue," an artifact of the test
#      sharing state across scenarios, not a real loss.
#   3. publish N QoS1 tagged messages to each topic from a different edge
#      while both subscribers stay disconnected
#   4. start a control pair (independent device/topic, continuously
#      connected) on a different edge to measure blast radius
#   5. docker kill edge-2 (unplanned crash, not docker-compose stop)
#   6. scenario A: reconnect subscriber-A to a DIFFERENT already-running
#      edge (edge-3) and count how many of its N messages are recovered
#   7. scenario B: restart edge-2 itself (docker start, same container) and
#      reconnect subscriber-B there — RedisSessionHook persists inflight
#      QoS messages, so edge-2's readStore() at boot may recover what
#      scenario A's node-change cannot
#   8. report exact loss counts for A and B, confirm no duplicate deliveries
#      (a raw-vs-unique seq count mismatch would mean double-delivery, which
#      a plain received/N count alone can't distinguish from "exactly
#      right"), and confirm the control pair's delivery was undisturbed
#      throughout (blast radius containment)
#
# Uses its own docker-compose project name + port overrides, same convention
# as olric-reconcile.sh / backup-restore.sh, to avoid colliding with another
# already-running stack on the same host.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

PROJECT="keel-e2e-qos-crash"
COMPOSE=(docker compose -f docker-compose.core-edge-split.yml -f test/e2e/qos-crash-loss.override.yml -p "$PROJECT")

TENANT="11111111-1111-1111-1111-111111111111"
DEVICE_A="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"   # scenario A: reconnect elsewhere
DEVICE_B_SCEN="b6ab420a-7ae7-4d45-974b-d895f2bd9b61" # scenario B: same-edge restart
DEVICE_CTRL="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"  # control pair, untouched by the crash
DEVICE_PWD="testpass123"

CONSUMER_USER="test-consumer"
CONSUMER_PWD="consumer-e2e-testpass"

EDGE1="localhost:29183"
EDGE2="localhost:29283"
EDGE3="localhost:29383"
MGMT_1="http://localhost:29190"

TOPIC_A="telemetry/$TENANT/$DEVICE_A"           # scenario A's own topic
TOPIC_B_SCEN="telemetry/$TENANT/$DEVICE_B_SCEN" # scenario B's own topic
TOPIC_CTRL="telemetry/$TENANT/$DEVICE_CTRL"     # control topic

N_MESSAGES=20
WORKDIR=$(mktemp -d)

log() { echo ">> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

cleanup() {
  log "tearing down"
  kill "${CTRL_SUB_PID:-}" "${CTRL_PUB_PID:-}" 2>/dev/null || true
  wait "${CTRL_SUB_PID:-}" "${CTRL_PUB_PID:-}" 2>/dev/null || true
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

wait_for_leader() {
  local mgmt="$1" timeout="$2"
  for ((i = 0; i < timeout; i++)); do
    if curl -sf "$mgmt/api/cluster/nodes" 2>/dev/null | grep -q '"is_leader":true'; then
      return 0
    fi
    sleep 1
  done
  return 1
}

routes_has() {
  local topic="$1" node="$2"
  curl -sf "$MGMT_1/api/cluster/routes" 2>/dev/null | jq -e --arg t "$topic" --arg n "$node" \
    '(.[$t] // []) | index($n) != null' >/dev/null 2>&1
}

# wait_for_route retries routes_has for up to timeout seconds — right after a
# fresh `compose up`, an edge's gossip/gRPC mesh join can lag a few seconds
# behind core leader election, so a single-shot check right after subscribing
# is flaky on a cold cluster.
wait_for_route() {
  local topic="$1" node="$2" timeout="$3"
  for ((i = 0; i < timeout; i++)); do
    routes_has "$topic" "$node" && return 0
    sleep 1
  done
  return 1
}

wait_for_mqtt() {
  local hostport="$1" timeout="$2"
  local host="${hostport%:*}" port="${hostport#*:}"
  for ((i = 0; i < timeout; i++)); do
    if (echo >"/dev/tcp/$host/$port") 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

post_json() {
  curl -sf -X POST -H "Content-Type: application/json" -d "$2" "$1" >/dev/null
}

# count_received extracts unique seq=N tokens from a mosquitto_sub output
# file and prints how many of 1..N_MESSAGES were actually seen.
count_received() {
  local file="$1"
  grep -o 'seq=[0-9]*' "$file" 2>/dev/null | sort -u | wc -l
}

# raw_received counts EVERY seq=N occurrence, duplicates included — comparing
# this against count_received's deduped total is how a silent double-delivery
# (as opposed to loss) would surface: raw > unique. A plain "N/N received"
# headline number alone can't distinguish "exactly right" from "some arrived
# twice, masking a loss elsewhere" — sort -u in count_received collapses
# duplicates by design, so it must never be the only check.
raw_received() {
  local file="$1"
  grep -o 'seq=[0-9]*' "$file" 2>/dev/null | wc -l
}

# assert_no_duplicates fails the run if file has more raw occurrences than
# unique ones for a given scenario label.
assert_no_duplicates() {
  local file="$1" label="$2"
  local raw unique
  raw=$(raw_received "$file")
  unique=$(count_received "$file")
  if [ "$raw" -gt "$unique" ]; then
    fail "$label: duplicate delivery detected — $raw raw messages vs $unique unique (diff: $((raw - unique)))"
  fi
  log "$label: no duplicates ($raw raw == $unique unique)"
}

log "1. bringing up the core/edge split cluster"
"${COMPOSE[@]}" up -d --build

wait_for_leader "$MGMT_1" 60 || fail "cluster never elected a leader"
log "cluster is up, leader elected — settling for full mesh join"
sleep 8

log "2. granting the test-consumer RBAC role subscribe on all three topics"
post_json "$MGMT_1/api/acl/roles" "$(jq -n --arg ta "$TOPIC_A" --arg tb "$TOPIC_B_SCEN" --arg tc "$TOPIC_CTRL" \
  '{name: "qos-crash-consumer", rules: [
    {topic_filter: $ta, actions: ["subscribe"], effect: "allow"},
    {topic_filter: $tb, actions: ["subscribe"], effect: "allow"},
    {topic_filter: $tc, actions: ["subscribe"], effect: "allow"}
  ]}')" || fail "create role"
post_json "$MGMT_1/api/acl/bindings" "$(jq -n --arg p "$CONSUMER_USER" \
  '{principal: $p, role_name: "qos-crash-consumer"}')" || fail "create binding"
sleep 1

log "3. registering two independent persistent QoS1 subscriptions on edge-2, then disconnecting both"
mosquitto_sub -h "${EDGE2%:*}" -p "${EDGE2#*:}" -i qos-crash-subscriber-a \
  -u "$CONSUMER_USER" -P "$CONSUMER_PWD" -c -q 1 -t "$TOPIC_A" -W 3 \
  >"$WORKDIR/register-a.log" 2>&1
mosquitto_sub -h "${EDGE2%:*}" -p "${EDGE2#*:}" -i qos-crash-subscriber-b \
  -u "$CONSUMER_USER" -P "$CONSUMER_PWD" -c -q 1 -t "$TOPIC_B_SCEN" -W 3 \
  >"$WORKDIR/register-b.log" 2>&1
sleep 1
wait_for_route "$TOPIC_A" "edge-2" 20 || fail "scenario A's persistent subscription never appeared in the routing table (edge-2)"
wait_for_route "$TOPIC_B_SCEN" "edge-2" 20 || fail "scenario B's persistent subscription never appeared in the routing table (edge-2)"
log "both persistent subscriptions confirmed on edge-2, both subscribers now offline"

log "4. starting control pair (independent device, continuously connected via edge-3)"
mosquitto_sub -h "${EDGE3%:*}" -p "${EDGE3#*:}" -i control-subscriber \
  -u "$CONSUMER_USER" -P "$CONSUMER_PWD" -q 1 -t "$TOPIC_CTRL" -v \
  >"$WORKDIR/control.log" 2>&1 &
CTRL_SUB_PID=$!
sleep 1
wait_for_route "$TOPIC_CTRL" "edge-3" 20 || fail "control subscription never appeared in the routing table (edge-3)"
(
  i=0
  while true; do
    i=$((i + 1))
    mosquitto_pub -h "${EDGE1%:*}" -p "${EDGE1#*:}" -i "control-pub-$$" \
      -u "$DEVICE_CTRL@$TENANT" -P "$DEVICE_PWD" -q 1 -t "$TOPIC_CTRL" -m "ctrl-seq=$i" 2>/dev/null
    sleep 0.5
  done
) &
CTRL_PUB_PID=$!
log "control pair running (control device -> edge-1 publish, edge-3 subscribe)"
sleep 2
CONTROL_BEFORE=$(count_received "$WORKDIR/control.log")
log "control messages received before crash: $CONTROL_BEFORE"

log "5. publishing $N_MESSAGES QoS1 tagged messages to EACH of the two scenario topics from edge-1, while both subscribers are offline"
for ((i = 1; i <= N_MESSAGES; i++)); do
  mosquitto_pub -h "${EDGE1%:*}" -p "${EDGE1#*:}" -i "device-a-pub" \
    -u "$DEVICE_A@$TENANT" -P "$DEVICE_PWD" -q 1 -t "$TOPIC_A" -m "seq=$i" \
    || fail "scenario A publish #$i failed"
done
for ((i = 1; i <= N_MESSAGES; i++)); do
  mosquitto_pub -h "${EDGE1%:*}" -p "${EDGE1#*:}" -i "device-b-pub" \
    -u "$DEVICE_B_SCEN@$TENANT" -P "$DEVICE_PWD" -q 1 -t "$TOPIC_B_SCEN" -m "seq=$i" \
    || fail "scenario B publish #$i failed"
done
log "all messages published to both topics, waiting for cross-node forward + Redis persist"
sleep 4

log "6. docker kill edge-2 (unplanned crash — a single crash event, both scenarios' queues were on it)"
docker kill "${PROJECT}-edge-2-1" >/dev/null || fail "docker kill edge-2 failed"
sleep 2

log "7a. scenario A: reconnecting subscriber-a to a DIFFERENT surviving edge (edge-3)"
mosquitto_sub -h "${EDGE3%:*}" -p "${EDGE3#*:}" -i qos-crash-subscriber-a \
  -u "$CONSUMER_USER" -P "$CONSUMER_PWD" -c -q 1 -t "$TOPIC_A" \
  -C "$N_MESSAGES" -W 15 -v >"$WORKDIR/scenarioA.log" 2>&1
RECEIVED_A=$(count_received "$WORKDIR/scenarioA.log")
LOSS_A=$((N_MESSAGES - RECEIVED_A))
log "scenario A: received $RECEIVED_A/$N_MESSAGES (loss: $LOSS_A)"
assert_no_duplicates "$WORKDIR/scenarioA.log" "scenario A"

CONTROL_MID=$(count_received "$WORKDIR/control.log")
log "control messages received right after crash+scenario A: $CONTROL_MID (control pair unaffected check)"

log "7b. scenario B: restarting edge-2 itself (same container) and reconnecting subscriber-b there — its queue was never touched by scenario A, independent topic/client"
docker start "${PROJECT}-edge-2-1" >/dev/null || fail "docker start edge-2 failed"
wait_for_mqtt "$EDGE2" 30 || fail "edge-2 never came back up on its MQTT port"
sleep 2
mosquitto_sub -h "${EDGE2%:*}" -p "${EDGE2#*:}" -i qos-crash-subscriber-b \
  -u "$CONSUMER_USER" -P "$CONSUMER_PWD" -c -q 1 -t "$TOPIC_B_SCEN" \
  -C "$N_MESSAGES" -W 15 -v >"$WORKDIR/scenarioB.log" 2>&1
RECEIVED_B=$(count_received "$WORKDIR/scenarioB.log")
LOSS_B=$((N_MESSAGES - RECEIVED_B))
log "scenario B: received $RECEIVED_B/$N_MESSAGES (loss: $LOSS_B)"
assert_no_duplicates "$WORKDIR/scenarioB.log" "scenario B"

sleep 2
CONTROL_AFTER=$(count_received "$WORKDIR/control.log")
kill "$CTRL_SUB_PID" "$CTRL_PUB_PID" 2>/dev/null || true
wait "$CTRL_SUB_PID" "$CTRL_PUB_PID" 2>/dev/null || true

log "control messages received total (before/mid/after): $CONTROL_BEFORE / $CONTROL_MID / $CONTROL_AFTER"

echo
echo "=================== RESULTS ==================="
echo "sent: $N_MESSAGES QoS1 messages to each of two independent topics, both queued offline on edge-2, single unplanned crash"
echo "scenario A (reconnect to different surviving edge-3):        received $RECEIVED_A/$N_MESSAGES, lost $LOSS_A"
echo "scenario B (edge-2 itself restarted, reconnect there, independent topic): received $RECEIVED_B/$N_MESSAGES, lost $LOSS_B"
echo "control pair (independent device <-> edge-1/edge-3, unrelated to the crash):"
echo "  received before crash: $CONTROL_BEFORE, right after crash+scenario A: $CONTROL_MID, at end: $CONTROL_AFTER"
echo "=================================================="

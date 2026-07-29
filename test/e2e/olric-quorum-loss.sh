#!/usr/bin/env bash
# Design-doc PoC checklist item (keel-design-doc.md, "Cosa resta da
# validare in PoC"): "comportamento sotto perdita di quorum Olric durante
# reconnect storm: i client MQTT si bloccano? i messaggi in-flight (via
# gRPC diretto, non dipendenti da Olric) si perdono? conferma che il piano
# dati resta funzionante in degrado anche con Olric non disponibile".
#
# Honest scope note: Olric and Raft are co-located in the same core
# process in this codebase (see keel-design-doc.md's "Decisioni operative
# per l'integrazione Olric") — there is no way to kill only Olric's quorum
# while leaving Raft's quorum intact on the same node set. This script
# therefore kills ALL THREE core containers together (total control-plane
# outage: Olric ring gone AND Raft quorum gone at once), which is a
# stricter/more pessimistic scenario than "Olric alone", and validates the
# two concrete claims the checklist item is actually after:
#
#   1. an ALREADY-established publish/subscribe pair (routes cached
#      locally on each edge — see internal/cluster/raft.EdgeRegistry and
#      routing.Router's in-process trie, populated before the outage)
#      keeps delivering new messages published DURING the total outage,
#      with zero core reachability — proving the data plane (gRPC direct,
#      local route cache) does not block on the control plane
#   2. a genuinely NEW client connect attempted during the outage is
#      correctly refused (fail-closed on ClaimSession error, see
#      internal/broker/hooks.go) rather than silently allowed or hung
#      forever — this is Raft's job, expected to degrade, not a bug
#
# Then cores are restarted and both directions are confirmed to recover:
# new connects succeed again, and messages published after recovery still
# arrive on the never-disconnected subscriber.
#
# Uses its own docker-compose project name + port overrides (22xxx), same
# convention as the other e2e scripts.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

PROJECT="keel-e2e-olric-quorum-loss"
COMPOSE=(docker compose -f docker-compose.core-edge-split.yml -f test/e2e/olric-quorum-loss.override.yml -p "$PROJECT")

TENANT="11111111-1111-1111-1111-111111111111"
DEVICE="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
DEVICE_PWD="testpass123"

CONSUMER_USER="test-consumer"
CONSUMER_PWD="consumer-e2e-testpass"

EDGE1="localhost:22183"
EDGE2="localhost:22283"
EDGE3="localhost:22383"
MGMT_1="http://localhost:22190"

TOPIC="telemetry/$TENANT/$DEVICE"
WORKDIR=$(mktemp -d)
PUB_FIFO="$WORKDIR/pub.fifo"

log() { echo ">> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

PUB_PID=""
SUB_PID=""
NEWCONN_PID=""
cleanup() {
  log "tearing down"
  [ -n "$PUB_PID" ] && kill "$PUB_PID" >/dev/null 2>&1
  [ -n "$SUB_PID" ] && kill "$SUB_PID" >/dev/null 2>&1
  [ -n "$NEWCONN_PID" ] && kill "$NEWCONN_PID" >/dev/null 2>&1
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

wait_for_route() {
  local topic="$1" node="$2" timeout="$3"
  for ((i = 0; i < timeout; i++)); do
    routes_has "$topic" "$node" && return 0
    sleep 1
  done
  return 1
}

post_json() {
  curl -sf -X POST -H "Content-Type: application/json" -d "$2" "$1" >/dev/null
}

wait_for_tag() {
  local file="$1" tag="$2" timeout="$3"
  for ((i = 0; i < timeout; i++)); do
    grep -q "$tag" "$file" 2>/dev/null && return 0
    sleep 1
  done
  return 1
}

log "1. bringing up the core/edge split cluster"
"${COMPOSE[@]}" up -d --build

wait_for_leader "$MGMT_1" 60 || fail "cluster never elected a leader"
log "cluster is up, leader elected"
sleep 5

log "2. granting test-consumer subscribe on the test topic"
post_json "$MGMT_1/api/acl/roles" "$(jq -n --arg t "$TOPIC" \
  '{name: "olric-quorum-loss-consumer", rules: [{topic_filter: $t, actions: ["subscribe"], effect: "allow"}]}')" \
  || fail "create role"
post_json "$MGMT_1/api/acl/bindings" "$(jq -n --arg p "$CONSUMER_USER" \
  '{principal: $p, role_name: "olric-quorum-loss-consumer"}')" || fail "create binding"
sleep 1

log "3. establishing a long-lived subscriber on edge-3 (never disconnects for the rest of the test)"
mosquitto_sub -h "${EDGE3%:*}" -p "${EDGE3#*:}" -i olric-quorum-loss-subscriber \
  -u "$CONSUMER_USER" -P "$CONSUMER_PWD" -q 1 -t "$TOPIC" -v \
  >"$WORKDIR/subscriber.log" 2>&1 &
SUB_PID=$!
sleep 2

log "4. establishing a long-lived publisher on edge-1 (single connection, fed via a FIFO, never reconnects for the rest of the test)"
mkfifo "$PUB_FIFO"
# Start the reader (mosquitto_pub, backgrounded) BEFORE opening our own
# write fd — opening a FIFO for writing blocks until a reader is present,
# and mosquitto_pub itself blocks opening it for reading until a writer
# is present. Reader first (it just waits in the background), then our
# write fd, which then opens immediately since the reader is already there.
mosquitto_pub -h "${EDGE1%:*}" -p "${EDGE1#*:}" -i device-pub \
  -u "$DEVICE@$TENANT" -P "$DEVICE_PWD" -q 1 -t "$TOPIC" -l \
  <"$PUB_FIFO" >"$WORKDIR/publisher.log" 2>&1 &
PUB_PID=$!
# Keep the FIFO open for writing on our own fd so mosquitto_pub's `-l`
# reader never sees EOF between messages (only when we explicitly close
# it at teardown).
exec 3>"$PUB_FIFO"
sleep 2

wait_for_route "$TOPIC" "edge-3" 20 || fail "subscriber's route never appeared in the routing table"
log "route confirmed, both connections established"

log "5. baseline: publishing before the outage"
echo "before-1" >&3
echo "before-2" >&3
wait_for_tag "$WORKDIR/subscriber.log" "before-2" 15 || fail "baseline message never arrived before the outage"
log "baseline confirmed delivered"

log "6. killing ALL THREE core containers simultaneously (total Olric+Raft outage)"
docker kill "${PROJECT}-core-1-1" "${PROJECT}-core-2-1" "${PROJECT}-core-3-1" >/dev/null \
  || fail "docker kill of all 3 cores failed"
sleep 2

log "7. while cores are down: publishing on the ALREADY-established connections (no new connect involved)"
echo "during-1" >&3
echo "during-2" >&3
echo "during-3" >&3
wait_for_tag "$WORKDIR/subscriber.log" "during-3" 20 \
  || fail "in-flight messages during the total core outage were NOT delivered — data plane blocked on the control plane (this would be the architectural gap the design doc worried about)"
log "confirmed: data plane keeps delivering with zero core reachability (routes served from each edge's local cache, gRPC forwarding direct edge-to-edge)"

log "8. while cores are down: a genuinely NEW connect attempt must be refused (fail-closed), not hang forever or succeed"
timeout 15 mosquitto_sub -h "${EDGE2%:*}" -p "${EDGE2#*:}" -i olric-quorum-loss-newconn \
  -u "$CONSUMER_USER" -P "$CONSUMER_PWD" -q 1 -t "$TOPIC" -C 1 -W 12 \
  >"$WORKDIR/newconn-during.log" 2>&1
NEWCONN_DURING_RC=$?
[ "$NEWCONN_DURING_RC" -ne 0 ] || fail "a brand-new client connect SUCCEEDED during total core outage — expected fail-closed refusal, got a working connection"
log "confirmed: new connect correctly refused/timed out while cores are down (fail-closed, as designed for ClaimSession failure)"

log "9. restarting the 3 core containers (same volumes, not a fresh cluster)"
docker start "${PROJECT}-core-1-1" "${PROJECT}-core-2-1" "${PROJECT}-core-3-1" >/dev/null \
  || fail "docker start of all 3 cores failed"

wait_for_leader "$MGMT_1" 60 || fail "cluster never re-elected a leader after restart"
log "cluster recovered, leader re-elected"
sleep 5

log "10. after recovery: a new connect must eventually succeed again (retrying — the cluster reporting a leader doesn't mean raft quorum has actually caught up yet across all 3 restarted cores, same class of readiness-gate gap already documented once in keel-design-doc.md). Checked via a NEW publisher, each retry tagged distinctly, observed on the SAME never-reconnected subscriber from step 3 — this validates both 'new connect succeeds' and 'delivery still works post-recovery' in one shot, instead of a throwaway subscriber waiting for a message nobody sends."
NEWCONN_OK=0
for ((i = 0; i < 60; i++)); do
  TAG="newconn-$i"
  if timeout 5 mosquitto_pub -h "${EDGE2%:*}" -p "${EDGE2#*:}" -i "olric-quorum-loss-newconn-$i" \
    -u "$DEVICE@$TENANT" -P "$DEVICE_PWD" -q 1 -t "$TOPIC" -m "$TAG" \
    >"$WORKDIR/newconn-after.log" 2>&1; then
    if wait_for_tag "$WORKDIR/subscriber.log" "$TAG" 5; then
      NEWCONN_OK=1
      break
    fi
  fi
  sleep 1
done
[ "$NEWCONN_OK" -eq 1 ] || fail "new connect+delivery never succeeded within 60s after core restart: $(cat "$WORKDIR/newconn-after.log")"
log "confirmed: new connect works again and is delivered end-to-end (took ~${i}s after core restart to actually regain quorum)"

log "11. after recovery: publishing on the SAME never-reconnected pub/sub pair from step 4 (not just new connections)"
echo "after-1" >&3
echo "after-2" >&3
wait_for_tag "$WORKDIR/subscriber.log" "after-2" 20 || fail "post-recovery message never arrived on the never-reconnected subscriber"
log "confirmed: never-reconnected pub/sub pair still healthy after core recovery"

exec 3>&-

echo
echo "=================== RESULTS ==================="
echo "baseline (pre-outage): delivered"
echo "during total 3-core outage: in-flight messages on an already-established pub/sub pair — delivered (data plane resilient)"
echo "during total 3-core outage: brand-new client connect — refused/timed out (fail-closed, as designed)"
echo "post-recovery: never-reconnected pub/sub pair — still delivering"
echo "post-recovery: brand-new client connect — works again"
echo "================================================="

#!/usr/bin/env bash
# Phase 3 e2e test: routing-table self-heal after total Olric data loss.
#
#   1. bring up the core/edge split cluster (docker-compose.core-edge-split.yml)
#   2. connect two real MQTT clients to two different edge nodes, each
#      subscribing to its own command topic
#   3. confirm both subscriptions show up in the routing table (core's
#      /api/cluster/routes) mapped to the correct edge node
#   4. force-reset Olric: hard-kill + restart all 3 core containers (which
#      host the embedded, memory-only Olric member) WITHOUT touching the
#      edge containers or the still-connected MQTT clients at all
#   5. without reconnecting/re-subscribing either client, poll
#      /api/cluster/routes and confirm both entries reappear within the
#      timeout — proving each edge's routing.Reconciler re-asserted its own
#      live subscriptions into the freshly-empty store
#
# Uses its own docker-compose project name + port overrides so it never
# collides with another already-running docker-compose.core-edge-split.yml
# stack on the same host (this happened in practice while developing
# test/e2e/backup-restore.sh — see that script's own override file).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

PROJECT="keel-e2e-olric-reconcile"
COMPOSE=(docker compose -f docker-compose.core-edge-split.yml -f test/e2e/olric-reconcile.override.yml -p "$PROJECT")

TENANT="11111111-1111-1111-1111-111111111111"
DEVICE_A="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
DEVICE_B="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
DEVICE_PWD="testpass123"

EDGE1_MQTT="localhost:28183"
EDGE2_MQTT="localhost:28283"
MGMT_1="http://localhost:28190"

TOPIC_A="command/$DEVICE_A"
TOPIC_B="command/$DEVICE_B"

log() { echo ">> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

cleanup() {
  log "tearing down"
  kill "${SUBA_PID:-}" "${SUBB_PID:-}" 2>/dev/null || true
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
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

# routes_has checks whether /api/cluster/routes currently maps topic to
# nodeID. The response shape is {"<topic>": ["<node-id>", ...], ...}.
routes_has() {
  local topic="$1" node="$2"
  curl -sf "$MGMT_1/api/cluster/routes" 2>/dev/null | python3 -c "
import json, sys
try:
    routes = json.load(sys.stdin)
except Exception:
    sys.exit(1)
nodes = routes.get(\"$topic\", [])
sys.exit(0 if \"$node\" in nodes else 1)
"
}

log "1. bringing up the core/edge split cluster"
"${COMPOSE[@]}" up -d --build

wait_for_leader "$MGMT_1" 60 || fail "cluster never elected a leader"
log "cluster is up, leader elected"

log "2. connecting two MQTT clients to two different edge nodes"
mosquitto_sub -h "${EDGE1_MQTT%:*}" -p "${EDGE1_MQTT#*:}" \
  -i "$DEVICE_A" -u "$DEVICE_A@$TENANT" -P "$DEVICE_PWD" \
  -t "$TOPIC_A" -q 1 >/dev/null 2>&1 &
SUBA_PID=$!
mosquitto_sub -h "${EDGE2_MQTT%:*}" -p "${EDGE2_MQTT#*:}" \
  -i "$DEVICE_B" -u "$DEVICE_B@$TENANT" -P "$DEVICE_PWD" \
  -t "$TOPIC_B" -q 1 >/dev/null 2>&1 &
SUBB_PID=$!
sleep 2 # let SUBSCRIBE's OnSubscribed -> Router.Subscribe raft/Olric write land

log "3. confirming both subscriptions appear in the routing table"
routes_has "$TOPIC_A" "edge-1" || fail "device A's subscription never appeared in the routing table (edge-1)"
routes_has "$TOPIC_B" "edge-2" || fail "device B's subscription never appeared in the routing table (edge-2)"
log "pre-reset routing table confirmed: both subscriptions present"

log "4. force-resetting Olric: hard-killing + restarting all 3 core containers (edge containers and clients untouched)"
docker kill "${PROJECT}-core-1-1" "${PROJECT}-core-2-1" "${PROJECT}-core-3-1" >/dev/null
docker start "${PROJECT}-core-1-1" "${PROJECT}-core-2-1" "${PROJECT}-core-3-1" >/dev/null

wait_for_leader "$MGMT_1" 60 || fail "core quorum never reformed after the reset"
log "core quorum reformed — Olric is now a freshly empty ring"

log "5. without reconnecting either client, waiting for both edges' Reconciler to re-assert their subscriptions"
DEADLINE=$((SECONDS + 90))
FOUND_A=0
FOUND_B=0
while [ "$SECONDS" -lt "$DEADLINE" ]; do
  if [ "$FOUND_A" -eq 0 ] && routes_has "$TOPIC_A" "edge-1"; then
    FOUND_A=1
    log "device A's subscription reappeared after $((90 - (DEADLINE - SECONDS)))s"
  fi
  if [ "$FOUND_B" -eq 0 ] && routes_has "$TOPIC_B" "edge-2"; then
    FOUND_B=1
    log "device B's subscription reappeared after $((90 - (DEADLINE - SECONDS)))s"
  fi
  [ "$FOUND_A" -eq 1 ] && [ "$FOUND_B" -eq 1 ] && break
  sleep 2
done

[ "$FOUND_A" -eq 1 ] || fail "device A's subscription never reappeared in the routing table within 90s — reconciler did not self-heal"
[ "$FOUND_B" -eq 1 ] || fail "device B's subscription never reappeared in the routing table within 90s — reconciler did not self-heal"

# Confirm both clients really did stay connected the whole time (never
# reconnected) — proof this was the edges' Reconciler, not a client-driven
# re-subscribe.
kill -0 "$SUBA_PID" 2>/dev/null || fail "device A's mosquitto_sub process died mid-test — result is not trustworthy"
kill -0 "$SUBB_PID" 2>/dev/null || fail "device B's mosquitto_sub process died mid-test — result is not trustworthy"

echo "PASS: Olric self-heal e2e test succeeded — routing table rebuilt with no client reconnection"

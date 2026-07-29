#!/usr/bin/env bash
# Track B / risk #6 follow-up: validate the single-core degraded mode for
# the co-located Redis primary+replica design (internal/cluster/membership's
# redisFailoverLoop) — only core-1 (+ its co-located redis-core-1, one edge,
# postgres) is ever started; core-2/3 and their redis-core-2/3 are never
# brought up at all, simulating a cluster that has only ever had one core.
#
# What this validates, explicitly:
#   1. core-1 designates itself as Redis primary (bootstrapRedisPrimary's
#      voterCount==1 branch) without ever attempting a REPLICAOF/SLAVEOF
#      call meant for a nonexistent peer — checked by asserting the
#      relevant log lines are ABSENT, not just that nothing crashed
#   2. QoS1 persistence still works end-to-end in this degraded mode (not
#      just the role designation) — a persistent offline subscriber's
#      queued messages are still recovered correctly
#
# Uses its own docker-compose project name + port overrides (24xxx), same
# convention as the other e2e scripts.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

PROJECT="keel-e2e-redis-single-core"
COMPOSE=(docker compose -f docker-compose.core-edge-split.yml -f test/e2e/redis-single-core.override.yml -p "$PROJECT")

TENANT="11111111-1111-1111-1111-111111111111"
DEVICE="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
DEVICE_PWD="testpass123"

CONSUMER_USER="test-consumer"
CONSUMER_PWD="consumer-e2e-testpass"

EDGE1="localhost:24183"
MGMT_1="http://localhost:24190"

TOPIC="telemetry/$TENANT/$DEVICE"
N_MESSAGES=10
WORKDIR=$(mktemp -d)

log() { echo ">> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

cleanup() {
  log "tearing down"
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

count_received() {
  local file="$1"
  grep -o 'seq=[0-9]*' "$file" 2>/dev/null | sort -u | wc -l
}

redis_role() {
  docker exec "${PROJECT}-redis-core-1-1" redis-cli INFO replication 2>/dev/null | grep '^role:' | tr -d '\r' | cut -d: -f2
}

log "1. bringing up ONLY core-1, redis-core-1, edge-1, postgres (no core-2/3, no redis-core-2/3)"
"${COMPOSE[@]}" up -d --build postgres redis-core-1 core-1 edge-1

wait_for_leader "$MGMT_1" 60 || fail "core-1 never self-elected as leader"
log "core-1 is up and leader (alone, as expected for a single-node raft cluster)"
sleep 10

log "2. verifying core-1 designated itself as redis primary"
role=$(redis_role)
[ "$role" = "master" ] || fail "expected redis-core-1 to be role:master (self-designated), got role:$role"
log "redis-core-1: role=$role"

log "3. verifying no replica-configuration attempt was ever made for a nonexistent peer"
if docker logs "${PROJECT}-core-1-1" 2>&1 | grep -q "configure redis replica"; then
  fail "found a 'configure redis replica' log line — the single-core guard should prevent this entirely, not just fail silently"
fi
log "confirmed: no replica-configuration attempt in core-1's logs (single-core guard held)"

if ! docker logs "${PROJECT}-core-1-1" 2>&1 | grep -q "designated initial redis primary"; then
  fail "expected to find 'designated initial redis primary' in core-1's logs"
fi
log "confirmed: core-1 logged its own primary self-designation"

log "4. functional check: QoS1 persistence works end-to-end in single-core mode"
post_json "$MGMT_1/api/acl/roles" "$(jq -n --arg t "$TOPIC" \
  '{name: "single-core-consumer", rules: [{topic_filter: $t, actions: ["subscribe"], effect: "allow"}]}')" \
  || fail "create role"
post_json "$MGMT_1/api/acl/bindings" "$(jq -n --arg p "$CONSUMER_USER" \
  '{principal: $p, role_name: "single-core-consumer"}')" || fail "create binding"
sleep 1

mosquitto_sub -h "${EDGE1%:*}" -p "${EDGE1#*:}" -i single-core-subscriber \
  -u "$CONSUMER_USER" -P "$CONSUMER_PWD" -c -q 1 -t "$TOPIC" -W 3 \
  >"$WORKDIR/register.log" 2>&1
sleep 1
wait_for_route "$TOPIC" "edge-1" 20 || fail "persistent subscription never appeared in the routing table"

for ((i = 1; i <= N_MESSAGES; i++)); do
  mosquitto_pub -h "${EDGE1%:*}" -p "${EDGE1#*:}" -i "device-pub" \
    -u "$DEVICE@$TENANT" -P "$DEVICE_PWD" -q 1 -t "$TOPIC" -m "seq=$i" \
    || fail "publish #$i failed"
done
sleep 2

mosquitto_sub -h "${EDGE1%:*}" -p "${EDGE1#*:}" -i single-core-subscriber \
  -u "$CONSUMER_USER" -P "$CONSUMER_PWD" -c -q 1 -t "$TOPIC" \
  -C "$N_MESSAGES" -W 15 -v >"$WORKDIR/recovery.log" 2>&1
RECEIVED=$(count_received "$WORKDIR/recovery.log")
[ "$RECEIVED" -eq "$N_MESSAGES" ] || fail "expected all $N_MESSAGES messages recovered in single-core mode, got $RECEIVED"

echo
echo "=================== RESULTS ==================="
echo "single-core mode: redis-core-1 self-designated primary (role:master), no replica-configuration attempts logged"
echo "functional check: $RECEIVED/$N_MESSAGES QoS1 messages recovered correctly"
echo "================================================="

#!/usr/bin/env bash
# Track B / risk #6 follow-up: validate the co-located Redis primary+replica
# failover (internal/cluster/membership's redisFailoverLoop +
# internal/cluster/redisrouter.WatchPrimary) — distinct from
# qos-crash-loss.sh, which validates message recovery across an EDGE crash
# assuming Redis itself stays healthy. This script instead kills the CORE
# that currently hosts the Redis PRIMARY (app container + its co-located
# redis-core-N container together, simulating a real co-located pod loss)
# while QoS1 messages are queued for an offline persistent subscriber, and
# measures:
#
#   1. exactly one surviving instance ever reports itself primary at any
#      point during/after the failover (split-brain check) — verified by
#      querying every survivor's own Redis INFO replication directly, not
#      just observing application-level behavior after the fact
#   2. how many of the N queued messages are lost in the async-replication
#      window (this design's declared, accepted residual risk) —
#      messages are published immediately before the kill, deliberately
#      minimizing the replication catch-up window, to measure the
#      realistic worst case rather than a padded best case
#   3. the routing/ACL-identity side of the affected client's session
#      still works after the failover (reusing the same recovery check
#      qos-crash-loss.sh already validates for the edge-crash case)
#
# Uses its own docker-compose project name + port overrides (27xxx), same
# convention as the other e2e scripts, to avoid colliding with an
# already-running stack on the same host.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

PROJECT="keel-e2e-redis-failover"
COMPOSE=(docker compose -f docker-compose.core-edge-split.yml -f test/e2e/redis-failover.override.yml -p "$PROJECT")

TENANT="11111111-1111-1111-1111-111111111111"
DEVICE="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
DEVICE_PWD="testpass123"

CONSUMER_USER="test-consumer"
CONSUMER_PWD="consumer-e2e-testpass"

EDGE1="localhost:27183"
EDGE3="localhost:27383"
MGMT_1="http://localhost:27190"
MGMT_2="http://localhost:27290"
MGMT_3="http://localhost:27390"

TOPIC="telemetry/$TENANT/$DEVICE"
N_MESSAGES=20
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

raw_received() {
  local file="$1"
  grep -o 'seq=[0-9]*' "$file" 2>/dev/null | wc -l
}

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

# redis_role queries container_name's own Redis for its current
# replication role directly — the ground truth, not an inference from
# application logs.
redis_role() {
  local container="$1"
  docker exec "$container" redis-cli INFO replication 2>/dev/null | grep '^role:' | tr -d '\r' | cut -d: -f2
}

# count_primaries_among counts how many of the given containers currently
# report role:master. Used for the split-brain assertion: this must never
# exceed 1, checked at multiple points, not just once after the dust settles.
count_primaries_among() {
  local count=0
  for c in "$@"; do
    local role
    role=$(redis_role "$c")
    if [ "$role" = "master" ]; then
      count=$((count + 1))
    fi
  done
  echo "$count"
}

assert_no_split_brain() {
  local label="$1"
  shift
  local n
  n=$(count_primaries_among "$@")
  if [ "$n" -gt 1 ]; then
    fail "$label: split-brain detected — $n Redis instances simultaneously report role:master among [$*]"
  fi
  log "$label: no split-brain ($n primary among [$*])"
}

log "1. bringing up the core/edge split cluster"
"${COMPOSE[@]}" up -d --build

wait_for_leader "$MGMT_1" 60 || fail "cluster never elected a leader"
log "cluster is up, leader elected — settling for redis primary designation + full mesh join"
sleep 10

log "2. identifying the current redis primary"
PRIMARY_CORE=""
for n in 1 2 3; do
  role=$(redis_role "${PROJECT}-redis-core-${n}-1")
  log "redis-core-${n}: role=$role"
  if [ "$role" = "master" ]; then
    PRIMARY_CORE="$n"
  fi
done
[ -n "$PRIMARY_CORE" ] || fail "no redis-core instance reports role:master after cluster settle"
log "current redis primary: core-${PRIMARY_CORE}"

assert_no_split_brain "pre-kill" "${PROJECT}-redis-core-1-1" "${PROJECT}-redis-core-2-1" "${PROJECT}-redis-core-3-1"

log "3. granting the test-consumer RBAC role subscribe on the test topic"
post_json "$MGMT_1/api/acl/roles" "$(jq -n --arg t "$TOPIC" \
  '{name: "redis-failover-consumer", rules: [{topic_filter: $t, actions: ["subscribe"], effect: "allow"}]}')" \
  || fail "create role"
post_json "$MGMT_1/api/acl/bindings" "$(jq -n --arg p "$CONSUMER_USER" \
  '{principal: $p, role_name: "redis-failover-consumer"}')" || fail "create binding"
sleep 1

log "4. registering a persistent QoS1 subscription on edge-1, then disconnecting"
mosquitto_sub -h "${EDGE1%:*}" -p "${EDGE1#*:}" -i redis-failover-subscriber \
  -u "$CONSUMER_USER" -P "$CONSUMER_PWD" -c -q 1 -t "$TOPIC" -W 3 \
  >"$WORKDIR/register.log" 2>&1
sleep 1
wait_for_route "$TOPIC" "edge-1" 20 || fail "persistent subscription never appeared in the routing table (edge-1)"
log "persistent subscription confirmed on edge-1, subscriber now offline"

log "5. publishing $N_MESSAGES QoS1 tagged messages, then IMMEDIATELY killing the primary — minimizing the async-replication catch-up window on purpose"
for ((i = 1; i <= N_MESSAGES; i++)); do
  mosquitto_pub -h "${EDGE1%:*}" -p "${EDGE1#*:}" -i "device-pub" \
    -u "$DEVICE@$TENANT" -P "$DEVICE_PWD" -q 1 -t "$TOPIC" -m "seq=$i" \
    || fail "publish #$i failed"
done

log "6. killing core-${PRIMARY_CORE} AND its co-located redis-core-${PRIMARY_CORE} together (simulating a real co-located pod loss)"
docker kill "${PROJECT}-core-${PRIMARY_CORE}-1" "${PROJECT}-redis-core-${PRIMARY_CORE}-1" >/dev/null \
  || fail "docker kill core-${PRIMARY_CORE}/redis-core-${PRIMARY_CORE} failed"

SURVIVORS=()
for n in 1 2 3; do
  [ "$n" != "$PRIMARY_CORE" ] && SURVIVORS+=("${PROJECT}-redis-core-${n}-1")
done

log "7. waiting for a surviving redis instance to be promoted"
DEADLINE=$((SECONDS + 60))
NEW_PRIMARY=""
while [ "$SECONDS" -lt "$DEADLINE" ]; do
  for c in "${SURVIVORS[@]}"; do
    if [ "$(redis_role "$c")" = "master" ]; then
      NEW_PRIMARY="$c"
      break 2
    fi
  done
  sleep 2
done
[ -n "$NEW_PRIMARY" ] || fail "no surviving redis instance was ever promoted to primary within 60s"
log "promoted: $NEW_PRIMARY"

# Split-brain check across the full failover window, not just the end
# state — poll repeatedly for a few seconds after promotion is first
# observed, since a bug could plausibly cause a SECOND promotion shortly
# after the first (e.g. a stale leader re-deciding).
log "8. verifying no split-brain during the settle window after promotion"
for i in $(seq 1 5); do
  assert_no_split_brain "post-promotion tick $i" "${SURVIVORS[@]}"
  sleep 1
done

log "9. waiting for cluster leader to stabilize post-crash (raft re-election if the killed core was leader)"
# Check a SURVIVING core's own mgmt API, not core-1's — core-1 may well be
# the one just killed, in which case its mgmt endpoint is gone along with
# it, and that's expected, not a failure to report on.
SURVIVOR_MGMT="$MGMT_1"
[ "$PRIMARY_CORE" = "1" ] && SURVIVOR_MGMT="$MGMT_2"
wait_for_leader "$SURVIVOR_MGMT" 60 || fail "cluster never re-elected a leader among survivors after the kill"

log "10. reconnecting subscriber to a surviving edge and measuring recovery"
mosquitto_sub -h "${EDGE3%:*}" -p "${EDGE3#*:}" -i redis-failover-subscriber \
  -u "$CONSUMER_USER" -P "$CONSUMER_PWD" -c -q 1 -t "$TOPIC" \
  -C "$N_MESSAGES" -W 20 -v >"$WORKDIR/recovery.log" 2>&1
RECEIVED=$(count_received "$WORKDIR/recovery.log")
LOST=$((N_MESSAGES - RECEIVED))
assert_no_duplicates "$WORKDIR/recovery.log" "recovery"

echo
echo "=================== RESULTS ==================="
echo "redis primary before kill: core-${PRIMARY_CORE}"
echo "redis primary after failover: $NEW_PRIMARY"
echo "sent: $N_MESSAGES QoS1 messages immediately before killing the primary core+redis together"
echo "recovered: $RECEIVED/$N_MESSAGES (lost to async-replication window: $LOST)"
echo "split-brain check: passed (never more than 1 simultaneous primary observed)"
echo "================================================="

#!/usr/bin/env bash
# Disaster-recovery e2e test for Phase 2 (backup/restore raft):
#   1. bring up the 3-node core cluster (docker-compose.yml)
#   2. create a session (real MQTT connect) + an ACL role/binding
#   3. `backup raft` against the leader
#   4. hard-kill all 3 core containers (docker kill, not graceful)
#   5. wipe their data volumes — simulates total quorum-loss + data loss
#   6. `restore raft` onto 3 fresh volumes from the backup
#   7. start the 3 nodes again and confirm quorum reforms with the
#      pre-backup session/ACL state intact
#
# Uses its own docker-compose project name (-p) so it never collides with
# a concurrently-running docker-compose.yml stack (e.g. test/e2e/cross_node_test.go
# or a manually-started deploy/docker-compose/README.md session).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

PROJECT="keel-e2e-backup-restore"
COMPOSE=(docker compose -f docker-compose.yml -f test/e2e/backup-restore.override.yml -p "$PROJECT")
IMAGE="keel-mqtt-gateway:dev"
VOTERS="core-1@core-1:7000,core-2@core-2:7000,core-3@core-3:7000"
BACKUP_HOST_DIR="$(mktemp -d)"

MGMT_1="http://localhost:19190"

log() { echo ">> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

cleanup() {
  log "tearing down"
  kill "${SUB_PID:-}" 2>/dev/null || true
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$BACKUP_HOST_DIR"
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

log "1. bringing up the 3-node core cluster"
"${COMPOSE[@]}" up -d --build

wait_for_leader "$MGMT_1" 60 || fail "cluster never elected a leader"
log "cluster is up, leader elected"

log "2. creating a session (real MQTT connect) + an ACL role/binding"
mosquitto_sub -h localhost -p 19283 \
  -i "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" \
  -u "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb@11111111-1111-1111-1111-111111111111" \
  -P "testpass123" \
  -t "command/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" -q 1 >/dev/null 2>&1 &
SUB_PID=$!
sleep 2 # let CONNECT's claimClusterSession raft.Apply replicate

curl -sf -X POST "$MGMT_1/api/acl/roles" \
  -d '{"name":"e2e-backup-role","rules":[{"topic_filter":"t/#","actions":["publish"],"effect":"allow"}]}' \
  >/dev/null || fail "create ACL role failed"
curl -sf -X POST "$MGMT_1/api/acl/bindings" \
  -d '{"principal":"e2e-backup-principal","role_name":"e2e-backup-role"}' \
  >/dev/null || fail "create ACL binding failed"
sleep 1

PRE_SESSIONS="$(curl -sf "$MGMT_1/api/cluster/sessions")"
PRE_ROLES="$(curl -sf "$MGMT_1/api/acl/roles")"
echo "$PRE_SESSIONS" | grep -q "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" || fail "session was never claimed before backup — nothing to prove survives restore"
echo "$PRE_ROLES" | grep -q "e2e-backup-role" || fail "ACL role was never created before backup"
log "pre-backup state confirmed: session + ACL role present"

log "3. backup raft against the leader (core-1)"
docker exec "${PROJECT}-core-1-1" keel-mqtt-gateway backup raft \
  --output /tmp/raft-backup --management-addr=http://localhost:8090
docker cp "${PROJECT}-core-1-1:/tmp/raft-backup" "$BACKUP_HOST_DIR/raft-backup"
[ -f "$BACKUP_HOST_DIR/raft-backup/meta.json" ] || fail "backup did not produce meta.json"
log "backup copied to $BACKUP_HOST_DIR/raft-backup"

log "4. hard-killing all 3 core containers (non-graceful)"
kill "$SUB_PID" 2>/dev/null || true
unset SUB_PID
docker kill "${PROJECT}-core-1-1" "${PROJECT}-core-2-1" "${PROJECT}-core-3-1" >/dev/null

log "5. simulating total data loss: wiping the raft data volumes"
"${COMPOSE[@]}" rm -f core-1 core-2 core-3 >/dev/null
for n in 1 2 3; do
  docker volume rm "${PROJECT}_core-${n}-data" >/dev/null 2>&1 || true
  docker volume create "${PROJECT}_core-${n}-data" >/dev/null
done

log "6. restoring raft onto 3 fresh volumes from the backup"
# --raft-bind is 127.0.0.1, not 0.0.0.0: this is a one-shot, disk-only
# operation (no real peer traffic happens here, see RecoverCluster's doc
# comment) not attached to the cluster's docker network, and hashicorp/raft
# rejects an unspecified (0.0.0.0) address as "not advertisable" even though
# nothing ever dials it during recovery. --voters still stores the real
# core-N:7000 hostnames the recovered node will actually use once started
# normally afterward.
for n in 1 2 3; do
  docker run --rm \
    -v "${PROJECT}_core-${n}-data:/data" \
    -v "$BACKUP_HOST_DIR/raft-backup:/backup:ro" \
    "$IMAGE" \
    restore raft \
      --snapshot /backup \
      --voters "$VOTERS" \
      --node-id "core-$n" \
      --raft-bind "127.0.0.1:7000" \
      --raft-data-dir /data/raft
done
log "restore raft completed on all 3 volumes"

log "7. starting the 3 nodes again on the recovered volumes"
"${COMPOSE[@]}" up -d core-1 core-2 core-3

wait_for_leader "$MGMT_1" 60 || fail "quorum never reformed after restore"
log "quorum reformed"

log "8. verifying pre-backup state survived the restore"
POST_SESSIONS="$(curl -sf "$MGMT_1/api/cluster/sessions")"
POST_ROLES="$(curl -sf "$MGMT_1/api/acl/roles")"

echo "$POST_SESSIONS" | grep -q "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" || fail "session lost after restore"
echo "$POST_ROLES" | grep -q "e2e-backup-role" || fail "ACL role lost after restore"

NODE_COUNT="$(curl -sf "$MGMT_1/api/cluster/nodes" | grep -o '"raft_voter":true' | wc -l)"
[ "$NODE_COUNT" -eq 3 ] || fail "expected 3 raft voters after restore, got $NODE_COUNT"

echo "PASS: backup/restore disaster-recovery e2e test succeeded"

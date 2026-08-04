#!/usr/bin/env bash
# Design-doc gap ("Gap architetturale trovato, non fixato"): a
# non-graceful core crash (kill, not a clean leave) has been observed
# leaving stale ownership entries in Olric's own internal
# partition-ownership table for 5+ minutes on a real cluster — well beyond
# keel's own 30s heartbeat-based routing-table purge (a completely
# separate mechanism, internal/cluster/lifecycle.Monitor, operating on
# keel's own routing.Router, not Olric's internal state).
#
# This script answers the cheap question first: is Olric's own
# RoutingTablePushInterval (default 1 minute, config.go) the actual lever,
# or does the "immediate node-left" path have a real bug that needs an
# upstream fix regardless of this setting?
#
# Runs the SAME non-graceful kill twice — once with the interval left at
# Olric's default, once tuned down — and measures, from each surviving
# core's own logs, how long it takes Olric to log the killed member being
# dropped from partition ownership ("has been deleted from the primary
# owners list"). If the tuned run drops this in a few seconds while the
# baseline run is still stale after a comparable wait, this is a config
# tuning issue, not an Olric bug — see keel-design-doc.md for the
# conclusion this script's output is expected to feed.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

PROJECT="keel-e2e-olric-nongraceful-crash"
COMPOSE=(docker compose -f docker-compose.core-edge-split.yml -f test/e2e/olric-nongraceful-crash.override.yml -p "$PROJECT")

MGMT_1="http://localhost:23190"
BASELINE_WAIT=90   # seconds to observe with Olric's own 1-minute default
TUNED_INTERVAL="3s"
TUNED_WAIT=20      # seconds to observe with the interval tuned down

log() { echo ">> $*"; }
fail() { echo "FAIL: $*" >&2; "${COMPOSE[@]}" down -v >/dev/null 2>&1; exit 1; }

wait_for_leader() {
  local timeout="$1"
  for ((i = 0; i < timeout; i++)); do
    if curl -sf "$MGMT_1/api/cluster/nodes" 2>/dev/null | grep -q '"is_leader":true'; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# run_case: brings the cluster up with OLRIC_ROUTING_TABLE_PUSH_INTERVAL
# set to $1, kills core-2 non-gracefully, and reports how long (in
# seconds, "not observed within ${2}s" if it never happens) it takes
# core-1 or core-3 to log core-2's Olric ownership entries being dropped.
run_case() {
  local interval="$1" observe_seconds="$2" label="$3"

  log "=== case: $label (OLRIC_ROUTING_TABLE_PUSH_INTERVAL=${interval:-<unset, Olric default 1m>}) ==="
  OLRIC_ROUTING_TABLE_PUSH_INTERVAL="$interval" "${COMPOSE[@]}" up -d --build >/dev/null 2>&1 \
    || fail "compose up failed for $label"

  wait_for_leader 60 || fail "cluster never elected a leader ($label)"
  sleep 5 # let routing/olric fully settle past initial join noise

  local core2_olric_addr
  core2_olric_addr=$(docker inspect -f '{{.NetworkSettings.Networks}}' "${PROJECT}-core-2-1" 2>/dev/null | grep -oE '172\.[0-9]+\.[0-9]+\.[0-9]+' | head -1)

  local t0 t1 elapsed
  t0=$(date +%s)
  log "killing core-2 non-gracefully (docker kill, SIGKILL) at t=0"
  docker kill "${PROJECT}-core-2-1" >/dev/null || fail "docker kill core-2 failed ($label)"

  local found=""
  for ((i = 0; i < observe_seconds; i++)); do
    if docker logs "${PROJECT}-core-1-1" 2>&1 | grep -q "deleted from the primary owners list" \
       || docker logs "${PROJECT}-core-3-1" 2>&1 | grep -q "deleted from the primary owners list"; then
      found="yes"
      t1=$(date +%s)
      elapsed=$((t1 - t0))
      break
    fi
    sleep 1
  done

  if [[ -n "$found" ]]; then
    log "RESULT ($label): Olric dropped core-2's ownership entries after ~${elapsed}s"
  else
    log "RESULT ($label): NOT observed within ${observe_seconds}s"
  fi

  "${COMPOSE[@]}" down -v >/dev/null 2>&1
}

run_case "$TUNED_INTERVAL" "$TUNED_WAIT" "tuned"
run_case "" "$BASELINE_WAIT" "baseline (Olric default)"

log "done — compare the two RESULT lines above"

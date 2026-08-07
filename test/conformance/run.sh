#!/usr/bin/env bash
# Runs the Eclipse Paho MQTT interoperability suite
# (eclipse-paho/paho.mqtt.testing) black-box against a real
# keel-mqtt-gateway binary in --conformance-test mode (see
# internal/conformance's package doc) — MQTT 3.1.1 (client_test.py) and
# MQTT5 (client_test5.py). Prints one JSON report line per suite, e.g.
# {"mqtt_3_1_1": {"passed": 10, "failed": 0}}, and exits non-zero if
# either suite has a failure.
#
# The suite itself is NOT vendored into this repo (a separate project,
# its own license) — cloned at a pinned commit into .cache/ (gitignored)
# for reproducibility, same spirit as pinning a Go module version.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

PAHO_REPO="https://github.com/eclipse-paho/paho.mqtt.testing.git"
PAHO_PIN="9d7bb80bb8b9d9cfc0b52f8cb4c1916401281103"
CACHE_DIR="test/conformance/.cache/paho.mqtt.testing"

PG_CONTAINER="keel-conformance-pg"
PG_PORT="${KEEL_CONFORMANCE_PG_PORT:-15433}"
MQTT_PORT="${KEEL_CONFORMANCE_MQTT_PORT:-1883}"
HTTP_PORT="${KEEL_CONFORMANCE_HTTP_PORT:-18080}"
METRICS_PORT="${KEEL_CONFORMANCE_METRICS_PORT:-19090}"

BIN="$(mktemp -d)/keel-server"
SERVER_LOG="$(mktemp)"

log() { echo ">> $*"; }
fail() { echo "FAIL: $*" >&2; cleanup; exit 1; }

cleanup() {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" >/dev/null 2>&1
  docker rm -f "$PG_CONTAINER" >/dev/null 2>&1
}
trap cleanup EXIT

# ── Fetch the suite at a pinned commit ──────────────────────────────────────
if [[ ! -d "$CACHE_DIR" ]]; then
  log "cloning paho.mqtt.testing @ $PAHO_PIN"
  git clone --quiet "$PAHO_REPO" "$CACHE_DIR" || fail "clone paho.mqtt.testing"
fi
git -C "$CACHE_DIR" fetch --quiet origin "$PAHO_PIN" 2>/dev/null
git -C "$CACHE_DIR" checkout --quiet "$PAHO_PIN" || fail "checkout pinned paho.mqtt.testing commit"

# ── Throwaway Postgres (keel-server requires it unconditionally for its
#    own schema migrations — see internal/db.Migrate — conformance mode
#    doesn't change that) ────────────────────────────────────────────────
log "starting throwaway Postgres on :$PG_PORT"
docker rm -f "$PG_CONTAINER" >/dev/null 2>&1
docker run -d --name "$PG_CONTAINER" \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=keel_devices \
  -p "$PG_PORT:5432" postgres:16-alpine >/dev/null || fail "start postgres"

for i in $(seq 1 30); do
  docker exec "$PG_CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break
  [[ "$i" -eq 30 ]] && fail "postgres did not become ready"
  sleep 1
done

# ── Build and start keel-server in conformance mode ─────────────────────────
log "building keel-server"
go build -o "$BIN" ./cmd/server || fail "go build"

log "starting keel-server --conformance-test on :$MQTT_PORT"
DATABASE_URL="postgres://postgres:postgres@localhost:$PG_PORT/keel_devices?sslmode=disable" \
MQTT_PORT="$MQTT_PORT" \
HTTP_PORT="$HTTP_PORT" \
METRICS_ADDR=":$METRICS_PORT" \
LOG_LEVEL=warn \
DEFAULT_TENANT_ID="00000000-0000-0000-0000-000000000001" \
"$BIN" --conformance-test > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!

for i in $(seq 1 30); do
  grep -q "mochi mqtt server started" "$SERVER_LOG" 2>/dev/null && break
  [[ "$i" -eq 30 ]] && { cat "$SERVER_LOG" >&2; fail "keel-server did not start"; }
  sleep 1
done
grep -q "MQTT CONFORMANCE MODE ENABLED" "$SERVER_LOG" || fail "conformance banner missing from startup log — refusing to trust auth/ACL are actually bypassed"

# ── Run both suites ──────────────────────────────────────────────────────────
EXIT=0
for spec in "client_test:mqtt_3_1_1" "client_test5:mqtt_5"; do
  module="${spec%%:*}"
  name="${spec##*:}"
  log "running $module"
  # timeout: observed at least one real, reproducible hang (a malformed
  # MQTT5 PUBACK/SUBACK during test_user_properties leaves the suite's own
  # client socket desynced, and the next test's connect() blocks forever
  # rather than erroring) — a broker-side conformance bug shouldn't also
  # be able to wedge CI indefinitely.
  if ! timeout 180 python3 test/conformance/run_report.py \
        --suite-dir "$CACHE_DIR/interoperability" \
        --module "$module" \
        --host localhost --port "$MQTT_PORT" \
        --name "$name" 2> >(tee "/tmp/keel-conformance-$module.log" >&2); then
    status=$?
    [[ "$status" -eq 124 ]] && log "$module TIMED OUT after 180s — see /tmp/keel-conformance-$module.log"
    EXIT=1
  fi
done

exit $EXIT

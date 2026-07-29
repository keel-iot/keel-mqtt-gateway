#!/usr/bin/env bash
# Track B PoC follow-up: validate the kafka-hono OutputConnector
# (internal/connector/kafka_hono.go) against a REAL Kafka-compatible broker
# (single-node Redpanda, no ZooKeeper — the simplest option, see
# keel-design-doc.md's "Connettore Kafka/Ditto" section) instead of only the
# existing unit tests, which mock the producer entirely.
#
# What this validates, explicitly:
#   1. the connector actually connects and produces to a real broker with
#      the wiring used in production config (OUTPUT_CONNECTOR=kafka-hono,
#      KAFKA_HONO_BROKERS, no SASL — the simplest working config; SASL
#      mechanism selection itself is already covered by unit tests)
#   2. real device telemetry publishes (via mosquitto_pub, going through the
#      full hook path: isAllowedPublish -> forwardToOutputConnector ->
#      BufferedConnector -> KafkaHonoConnector) land on the expected real
#      topic (`hono.telemetry.<tenant_id>`) with the expected payload and
#      headers (device_id, tenant_id) — not just that Init()/Forward() don't
#      error in isolation
#
# Uses its own docker-compose project name + port overrides (20xxx), same
# convention as the other e2e scripts.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

PROJECT="keel-e2e-kafka-redpanda"
COMPOSE=(docker compose -f docker-compose.core-edge-split.yml -f test/e2e/kafka-redpanda.override.yml -p "$PROJECT")

TENANT="11111111-1111-1111-1111-111111111111"
DEVICE="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
DEVICE_PWD="testpass123"

EDGE1="localhost:20183"
MGMT_1="http://localhost:20190"

TOPIC="telemetry/$TENANT/$DEVICE"
KAFKA_TOPIC="hono.telemetry.$TENANT"
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

redpanda_ready() {
  docker exec "${PROJECT}-redpanda-1" rpk topic list --brokers localhost:9092 >/dev/null 2>&1
}

wait_for_redpanda() {
  local timeout="$1"
  for ((i = 0; i < timeout; i++)); do
    redpanda_ready && return 0
    sleep 1
  done
  return 1
}

# wait_for_connector_ready polls edge-1's own logs for the connector's
# ready line instead of a fixed sleep — edge-1 first has to finish its own
# olric/routing bring-up before wiring the output connector, and that
# handshake's duration isn't fixed.
wait_for_connector_ready() {
  local timeout="$1"
  for ((i = 0; i < timeout; i++)); do
    if docker logs "${PROJECT}-edge-1-1" 2>&1 | grep -q "output connector: ready"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

log "1. bringing up the core/edge split cluster + redpanda"
"${COMPOSE[@]}" up -d --build

wait_for_leader "$MGMT_1" 60 || fail "cluster never elected a leader"
log "cluster is up, leader elected"

wait_for_redpanda 60 || fail "redpanda never became healthy"
log "redpanda is up"

log "2. confirming edge-1's kafka-hono connector initialized against the real broker"
wait_for_connector_ready 30 || fail "expected 'output connector: ready' in edge-1's logs — kafka-hono connector never initialized"
log "confirmed: kafka-hono connector initialized"

log "2b. pre-creating $KAFKA_TOPIC (franz-go, used by the connector, doesn't request auto-creation on produce — same assumption a real Ditto/Hono deployment makes, where topics are provisioned by ops, not by the producing client)"
docker exec "${PROJECT}-redpanda-1" rpk topic create "$KAFKA_TOPIC" --brokers localhost:9092 >/dev/null \
  || fail "failed to pre-create $KAFKA_TOPIC"

log "3. publishing $N_MESSAGES QoS1 tagged telemetry messages from the device"
for ((i = 1; i <= N_MESSAGES; i++)); do
  mosquitto_pub -h "${EDGE1%:*}" -p "${EDGE1#*:}" -i "device-pub" \
    -u "$DEVICE@$TENANT" -P "$DEVICE_PWD" -q 1 -t "$TOPIC" -m "seq=$i" \
    || fail "publish #$i failed"
done
sleep 3

log "4. reading back $KAFKA_TOPIC directly from redpanda (ground truth, not application logs)"
timeout 30 docker exec "${PROJECT}-redpanda-1" rpk topic consume "$KAFKA_TOPIC" \
  --brokers localhost:9092 -n "$N_MESSAGES" -o start --format json \
  >"$WORKDIR/consumed.jsonl" 2>"$WORKDIR/consume.err"
[ -s "$WORKDIR/consumed.jsonl" ] || fail "rpk topic consume produced no output: $(cat "$WORKDIR/consume.err")"

RECEIVED=$(jq -s '[.[].value] | unique | length' "$WORKDIR/consumed.jsonl" 2>/dev/null)
[ "$RECEIVED" -eq "$N_MESSAGES" ] || fail "expected $N_MESSAGES distinct messages on $KAFKA_TOPIC, got $RECEIVED (see $WORKDIR/consumed.jsonl)"
log "confirmed: $RECEIVED/$N_MESSAGES messages landed on $KAFKA_TOPIC"

log "5. verifying headers (device_id, tenant_id) on the first record"
# rpk's --format json pretty-prints each record as a multi-line JSON object
# back-to-back (not one-line-per-record/ndjson) — `jq -s` slurps the
# concatenated stream into an array so multi-line records parse correctly.
FIRST_RECORD=$(jq -s '.[0]' "$WORKDIR/consumed.jsonl")
DEVICE_HDR=$(echo "$FIRST_RECORD" | jq -r '.headers[]? | select(.key=="device_id") | .value')
TENANT_HDR=$(echo "$FIRST_RECORD" | jq -r '.headers[]? | select(.key=="tenant_id") | .value')
[ "$DEVICE_HDR" = "$DEVICE" ] || fail "expected device_id header '$DEVICE', got '$DEVICE_HDR'"
[ "$TENANT_HDR" = "$TENANT" ] || fail "expected tenant_id header '$TENANT', got '$TENANT_HDR'"
log "confirmed: device_id/tenant_id headers correct"

echo
echo "=================== RESULTS ==================="
echo "kafka-hono connector initialized against real redpanda broker (no SASL)"
echo "topic: $KAFKA_TOPIC"
echo "messages: $RECEIVED/$N_MESSAGES landed with correct payload"
echo "headers: device_id=$DEVICE_HDR tenant_id=$TENANT_HDR (expected)"
echo "================================================="

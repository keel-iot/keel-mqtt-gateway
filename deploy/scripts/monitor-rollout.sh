#!/usr/bin/env bash
# monitor-rollout.sh — read-only snapshot of keel-mqtt-gateway's state on a
# live K8s cluster, meant to be run from a jumpbox that already has kubectl
# pointed at the target cluster (this script never configures kubeconfig
# itself). Takes no mutating action — no sync, no scale, no exec that
# changes state. Safe to run repeatedly (e.g. in a watch loop) during a
# rollout/HPA-adapter activation.
#
# Usage:
#   ./monitor-rollout.sh [namespace] [outdir]
#   ./monitor-rollout.sh --watch [interval-seconds] [namespace] [outdir]
#
# Defaults: namespace=keel-mqtt-gateway, outdir=./keel-monitor-<timestamp>,
# watch interval=15s.
#
# Deliberately no `set -e`: almost every collection command below can fail
# legitimately mid-rollout (readyz 503 during startup, custom-metrics 404
# before the adapter is registered, a label selector matching zero pods
# yet) — none of those should abort the whole snapshot, only be captured
# as-is in their own output file.
set -uo pipefail

WATCH=false
INTERVAL=15
if [[ "${1:-}" == "--watch" ]]; then
  WATCH=true
  shift
  if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
    INTERVAL="$1"
    shift
  fi
fi

NAMESPACE="${1:-keel-mqtt-gateway}"
OUTDIR="${2:-./keel-monitor-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUTDIR"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# raw() — hits a Service port through the kube-apiserver's proxy subresource
# (no port-forward, no exec, no direct network path to the cluster needed
# beyond kubectl itself already working).
raw() {
  local svc="$1" port="$2" path="$3"
  kubectl get --raw "/api/v1/namespaces/${NAMESPACE}/services/${svc}:${port}/proxy/${path}" 2>&1
}

snapshot() {
  local ts stamp
  ts="$(date +%Y-%m-%dT%H:%M:%S)"
  stamp="$OUTDIR/$(date +%H%M%S)"
  mkdir -p "$stamp"
  log "snapshot -> $stamp"

  {
    echo "# keel-mqtt-gateway snapshot — $ts — namespace=$NAMESPACE"
  } > "$stamp/00-meta.txt"

  kubectl get pods -n "$NAMESPACE" -o wide > "$stamp/01-pods.txt" 2>&1
  kubectl get deploy,statefulset -n "$NAMESPACE" -o wide > "$stamp/02-workloads.txt" 2>&1
  kubectl get hpa -n "$NAMESPACE" -o wide > "$stamp/03-hpa.txt" 2>&1
  kubectl describe hpa -n "$NAMESPACE" > "$stamp/04-hpa-describe.txt" 2>&1
  kubectl get events -n "$NAMESPACE" --sort-by='.lastTimestamp' | tail -60 > "$stamp/05-events.txt" 2>&1
  kubectl top pods -n "$NAMESPACE" > "$stamp/06-top.txt" 2>&1 || echo "metrics-server not available" > "$stamp/06-top.txt"

  kubectl get apiservices v1beta1.custom.metrics.k8s.io -o yaml > "$stamp/07-custom-metrics-apiservice.txt" 2>&1
  kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/${NAMESPACE}/pods/*/keel_edge_load_score" \
    > "$stamp/08-custom-metrics-value.txt" 2>&1

  # Core Service name follows the chart's keel.core.fullname helper —
  # override CORE_SVC/EDGE_SVC env vars if the release name differs from
  # "keel-mqtt-gateway".
  local core_svc="${CORE_SVC:-keel-mqtt-gateway-core}"
  raw "$core_svc" mgmt "api/cluster/nodes" > "$stamp/09-cluster-nodes.txt" 2>&1
  raw "$core_svc" mgmt "api/cluster/sessions" > "$stamp/10-cluster-sessions.txt" 2>&1
  raw "$core_svc" metrics "readyz" > "$stamp/11-readyz.txt" 2>&1
  raw "$core_svc" metrics "healthz" > "$stamp/12-healthz.txt" 2>&1

  for pod in $(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=core -o name 2>/dev/null); do
    name="${pod#pod/}"
    kubectl logs -n "$NAMESPACE" "$name" --tail=80 --timestamps > "$stamp/logs-core-${name}.txt" 2>&1
  done
  for pod in $(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=edge -o name 2>/dev/null); do
    name="${pod#pod/}"
    kubectl logs -n "$NAMESPACE" "$name" --tail=80 --timestamps > "$stamp/logs-edge-${name}.txt" 2>&1
  done
}

if [[ "$WATCH" == true ]]; then
  log "watch mode: snapshot every ${INTERVAL}s into $OUTDIR (Ctrl-C to stop)"
  while true; do
    snapshot
    sleep "$INTERVAL"
  done
else
  snapshot
  log "done — see $OUTDIR"
fi

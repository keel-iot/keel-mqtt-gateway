# keel-mqtt-gateway

Helm chart for the core/edge MQTT clustering gateway.

## External dependencies — always external, never bundled

This chart never deploys PostgreSQL or Redpanda/Kafka itself. Both are
configured as pointers to pre-existing services:

```yaml
postgresql:
  external:
    host: postgres.example.internal
    port: 5432
    database: keel_devices
    username: keel
    existingSecret: keel-postgres-credentials   # must contain key "password"
    existingSecretPasswordKey: password

redpanda:
  enabled: true
  external:
    brokers:
      - redpanda-0.redpanda.svc.cluster.local:9092
      - redpanda-1.redpanda.svc.cluster.local:9092
    saslUsername: keel-gateway
    existingSecret: keel-redpanda-credentials
    existingSecretPasswordKey: password

outputConnector:
  kafkaHono:
    enabled: true
    external:
      brokers: ["kafka.customer.example.com:9092"]
      saslUsername: keel-hono
      existingSecret: keel-kafka-hono-credentials
```

For quick local/dev testing you can set an inline `password`/`saslPassword`
instead of `existingSecret` — the chart then creates its own Secret from
it (`templates/secrets.yaml`). Never set both; `existingSecret` wins if
present.

Redis is different: it's co-located with each core pod (primary+replica
across cores) and fully managed by this chart — not an "external"
dependency in the same sense, no host/credential pointer needed beyond
the optional shared `core.redis.password`.

## Topology

- `core`: StatefulSet, `core.replicaCount` (keep odd — raft quorum), one
  PVC for the raft log and one for the co-located Redis's AOF file per
  pod. No MQTT listener — pure control plane (raft session ownership,
  Olric routing table, ACL, Redis primary/replica failover).
- `edge`: Deployment, stateless, HPA-driven (`edge.autoscaling`) — the
  only role that terminates MQTT client connections.

Both roles gossip on the same mesh (`hashicorp/memberlist`); edges seed
from every core pod's stable DNS name.

## TLS

MQTT TLS termination happens inside the edge pods themselves, never at a
load balancer, via `internal/broker.CertReloader` watching a mounted
Secret. Either let cert-manager manage it (`tls.certManager.enabled: true`
with an `issuerRef`) or point at an existing Secret (`tls.existingSecret`).

## Readiness

`/readyz` (see `cmd/server/main.go`'s `newReadyzHandler`) checks TLS
certificate validity, Redis primary reachability, raft leader known, and
local Olric reachability — each independently, only for whatever's
actually configured.

## Backup/restore, ACL management

The binary ships `backup`/`restore`/`acl` CLI subcommands, not wired into
a CronJob or Job by this chart yet. Use `kubectl exec` against a core pod
in the meantime:

```
kubectl exec {{ "<release>-core-0" }} -- keel-mqtt-gateway backup raft --output /tmp/snap
```

## Automatic connection rebalancing (0.3.5+)

```yaml
core:
  rebalance:
    enabled: true
    interval: 2m
    dryRun: false
    imbalanceThreshold: 0.20
    maxEvictionsPerNode: 50
    evictStagger: 200ms
    excludeClientIDs: [] # e.g. ["bridge-consumer"]
```

Automatic rebalancing is disabled by default. When enabled, only the Raft
leader runs a pass, starting after the configured interval. Each pass selects
clients randomly from edges exceeding the average session count by the
configured fraction. It disconnects at most `maxEvictionsPerNode` clients per
overloaded edge, with `evictStagger` between successful disconnections.
Clients must reconnect; their destination is chosen by the external load
balancer. Rebalancing uses session counts, not CPU or message rate.

Use `dryRun: true` to log the proposed disconnections without executing them.
Leadership is checked before each scheduled disconnection; an RPC already
in flight can complete after leadership changes. Manual API passes remain
available and should not be triggered concurrently with scheduled passes.
Threshold, cap, stagger and exclusions also configure the manual API.
The interval must be positive. Apply these values with chart and image 0.3.5
or later; changing core arguments causes a core StatefulSet rolling update.

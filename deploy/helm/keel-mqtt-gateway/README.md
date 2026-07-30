# keel-mqtt-gateway

Helm chart for the core/edge MQTT clustering gateway — see
`keel-design-doc.md` (repo root, one level up from this project) for the
full architecture.

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
across cores, see risk #6 in the design doc) and fully managed by this
chart — not an "external" dependency in the same sense, no host/credential
pointer needed beyond the optional shared `core.redis.password`.

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

MQTT TLS termination happens inside the edge pods themselves (never at a
load balancer — see the design doc), via `internal/broker.CertReloader`
watching a mounted Secret. Either let cert-manager manage it
(`tls.certManager.enabled: true` with an `issuerRef`) or point at an
existing Secret (`tls.existingSecret`).

## Known limitation

`/readyz` today only reflects TLS certificate readiness (see
`cmd/server/main.go`'s `newReadyzHandler`), not "raft joined" or "Olric
joined" — a pod can report Ready before it has actually rejoined the
cluster's gossip mesh. Acceptable for now; revisit if it causes premature
traffic routing during rollouts.

## Backup/restore, ACL management

The binary ships `backup`/`restore`/`acl` CLI subcommands (see the design
doc's "Backup/restore" and "Sistema ACL configurabile" sections) — not
wired into a CronJob or Job by this chart yet. Use `kubectl exec` against
a core pod in the meantime:

```
kubectl exec {{ "<release>-core-0" }} -- keel-mqtt-gateway backup raft --output /tmp/snap
```

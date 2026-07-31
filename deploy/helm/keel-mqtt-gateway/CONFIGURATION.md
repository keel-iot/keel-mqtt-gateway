# Configuration guide

Step-by-step configuration for `keel-mqtt-gateway`. See `README.md` for a
quick overview and `values.yaml` for the full, commented default values.

## Prerequisites

- Kubernetes 1.25+
- A pre-existing PostgreSQL database reachable from the cluster
  (`keel_devices` database, migrated — this chart does not run
  migrations or create the database)
- Optional: a pre-existing Redpanda/Kafka broker if you want the
  keel-native event forwarding (`redpanda.enabled`) and/or the
  Kafka/Ditto output connector (`outputConnector.kafkaHono.enabled`)
- Optional: cert-manager, if you want `tls.certManager.enabled: true`
- Optional: a Prometheus + prometheus-adapter (or equivalent) installation
  if you want `edge.autoscaling.enabled` (the default) to actually scale —
  without it the HPA is created but never gets real metric values

## 1. PostgreSQL (required)

Create a Secret with the database password:

```sh
kubectl create secret generic keel-postgres-credentials \
  --from-literal=password='<db-password>'
```

Then in your values file:

```yaml
postgresql:
  external:
    host: postgres.example.internal
    port: 5432
    database: keel_devices
    username: keel
    sslmode: disable   # or "require"/"verify-full" depending on your Postgres setup
    existingSecret: keel-postgres-credentials
    existingSecretPasswordKey: password
```

For quick local/dev testing only, you can skip the Secret and set
`postgresql.external.password` inline instead — the chart then creates
its own Secret from it. Never set both `existingSecret` and `password`.

The gateway owns its schema — it applies its own versioned migrations
(`internal/db`, tracked in `devices.schema_migrations`) on every startup.
No separate init script or manual `CREATE TABLE` step is needed; a fresh
empty database is enough.

## 2. Auth backend

Three options (`auth.backend`):

- `postgres` (default) — devices authenticate against
  `devices.device_credentials` in the PostgreSQL database above. No
  further configuration needed.
- `file` — static YAML credential file, useful for air-gapped/dev
  deployments without a device-management backend:

  ```sh
  kubectl create secret generic keel-credentials \
    --from-file=credentials.yaml=./my-credentials.yaml
  ```

  ```yaml
  auth:
    backend: file
    file:
      existingSecret: keel-credentials
  ```

  See `cmd/devicesim/gen_credentials.go`'s output format, or
  `deploy/docker-compose/credentials.yaml` for the shape by hand.

- `grpc` — forwards to a future keel-core gRPC service:

  ```yaml
  auth:
    backend: grpc
    grpc:
      keelCoreAddr: keel-core.keel-core.svc.cluster.local:9000
  ```

### Per-tenant JWT device auth

Devices can also authenticate with a signed JWT instead of the
password/credential-file/gRPC options above (detected automatically —
MQTT passwords starting with `eyJ` are treated as a JWT). This is
independent of `auth.backend` and controlled per-tenant via
`devices.tenant_gateway_config` (`jwt_auth_enabled`, plus one of the two
key sources below):

- `jwt_public_key_pem` — a single static RSA/EC public key (PEM). Simple,
  but rotating the key means updating this column directly.
- `jwks_url` — a JWKS endpoint; the key is resolved per-token by its `kid`
  header and cached (`JWKS_CACHE_TTL` env var, default `5m`). Takes
  precedence over `jwt_public_key_pem` when both are set. This is the
  path a future Clavex-issued device-token integration would use — lets
  the IdP rotate signing keys without a config change here.

`TENANT_CACHE_TTL` (default `5m`) controls how long the tenant's gateway
config itself (which of the two sources is active, whether JWT is enabled
at all, ...) is cached before re-reading from PostgreSQL.

## 3. Redpanda/Kafka — commander only (optional)

Off by default. Enable only if you want the commander (platform→device
push commands) — this is the ONLY thing `redpanda.*` feeds now:

```sh
kubectl create secret generic keel-redpanda-credentials \
  --from-literal=password='<sasl-password>'
```

```yaml
redpanda:
  enabled: true
  external:
    brokers:
      - redpanda-0.redpanda.svc.cluster.local:9092
      - redpanda-1.redpanda.svc.cluster.local:9092
    saslUsername: keel-gateway
    saslMechanism: SCRAM-SHA-512   # or SCRAM-SHA-256
    existingSecret: keel-redpanda-credentials
    existingSecretPasswordKey: password
  topics:
    commands: platform.commands
```

keel's own device-state/OTA/CA event mirroring and Ditto/Hono compat used
to be configured here too, but that logic has moved out of the broker
entirely into a standalone OutputConnector plugin — this chart doesn't
deploy that plugin (not published yet).

## 4. Kafka/Ditto output connector (optional)

Separate from step 3 on purpose — in practice this is very often a
**different** Kafka cluster (your customer's Ditto/Hono deployment), not
the one keel uses for its own internal topics:

```sh
kubectl create secret generic keel-kafka-hono-credentials \
  --from-literal=password='<sasl-password>'
```

```yaml
outputConnector:
  kafkaHono:
    enabled: true
    external:
      brokers: ["kafka.customer.example.com:9092"]
      saslUsername: keel-hono
      existingSecret: keel-kafka-hono-credentials
    topicPrefix: hono   # topics become hono.telemetry.<tenant_id>, hono.event.<tenant_id>
```

**Note**: the underlying Kafka client does not auto-create topics on
produce — pre-create `<prefix>.telemetry.*`/`<prefix>.event.*` on the
target broker, the same assumption a real Ditto/Hono deployment makes.

## 5. TLS (MQTT client-facing listener)

Off by default (plain MQTT on port 1883 only). Two ways to enable:

**cert-manager** (recommended if already installed):

```yaml
tls:
  enabled: true
  certManager:
    enabled: true
    issuerRef:
      name: letsencrypt-prod
      kind: ClusterIssuer   # or "Issuer"
    dnsNames:
      - mqtt.example.com
```

**Existing Secret** (e.g. managed by an external ACME client):

```yaml
tls:
  enabled: true
  existingSecret: my-mqtt-tls-secret   # must contain tls.crt/tls.key
```

Certificate rotation is picked up automatically without a pod restart
(`internal/broker.CertReloader` watches the mounted Secret volume).

`tls.clientAuth` controls whether client certificates are requested:
`"none"`, `"request"` (default — needed for the existing X.509
auto-provisioning path), or `"require-and-verify"`.

## 6. Autoscaling (edge)

Enabled by default (`edge.autoscaling.enabled: true`), driven by the
already-implemented `keel_gateway_edge_load_score` composite metric
(connections + CPU, see the design doc's HPA section) exposed as
`custom.metrics.k8s.io`. Requires a Prometheus scraping every edge pod's
`/metrics` plus a prometheus-adapter serving that metric:

```yaml
metrics:
  serviceMonitor:
    enabled: true       # if you use the Prometheus Operator
  prometheusAdapter:
    installConfigMap: true
    namespace: monitoring   # wherever your prometheus-adapter deployment lives
```

The rendered `installConfigMap` ConfigMap alone does nothing — it must be
mounted by an actual prometheus-adapter deployment
(`--config=/etc/adapter/config.yaml`), not installed by this chart.

Tune `edge.edgeConnectionsLimit` to your real per-pod MQTT connection
target — it's the denominator of the load-score's connections term.

If you don't have a metrics pipeline yet, set
`edge.autoscaling.enabled: false` and manage `edge.replicaCount` manually
until one is in place.

## 7. Core sizing

`core.replicaCount` must stay odd (raft quorum) — 3 is the default and
the minimum recommended for production. Persistence:

```yaml
core:
  persistence:
    raft:
      size: 2Gi
      storageClassName: fast-ssd
    redis:
      size: 2Gi
      storageClassName: fast-ssd
  redis:
    password: "<optional-shared-password>"
```

The co-located Redis (primary+replica across core pods, per risk #6 in
the design doc) is managed entirely by this chart — there's no
external-service pointer for it, only the optional shared password above.

## 8. Verifying the deployment

```sh
kubectl exec <release>-core-0 -- \
  wget -qO- http://localhost:8090/api/cluster/nodes

kubectl exec <release>-core-0 -- \
  wget -qO- http://localhost:8090/api/cluster/routes
```

Both endpoints, plus `GET /api/metrics` (aggregated connections/msg-sec
across every edge) and `GET /api/live/clients`, are documented in
`keel-design-doc.md`'s "Osservabilità e controllo" section.

### Monitoring dashboard

A basic monitoring UI (connections, messages/sec, clients, topics) is
served at `GET /ui` on any core pod's management API:

```sh
kubectl -n <namespace> port-forward svc/<release>-core 8090:8090
# then open http://localhost:8090/ui
```

Not exposed via an Ingress/LoadBalancer by this chart — internal-only,
same as the rest of the management API.

## 9. Backup/restore, ACL management

Not wired into a CronJob by this chart yet — run manually via
`kubectl exec` against a core pod:

```sh
# Backup
kubectl exec <release>-core-0 -- \
  keel-mqtt-gateway backup raft --output /tmp/snapshot

# Restore (disaster recovery — see keel-design-doc.md's "Backup/restore" section)
kubectl exec <release>-core-0 -- \
  keel-mqtt-gateway restore raft --snapshot /tmp/snapshot --voters <release>-core-0,<release>-core-1,<release>-core-2

# ACL
kubectl exec <release>-core-0 -- \
  keel-mqtt-gateway acl role create backend-consumer
```

## Known limitations

- `/readyz` reflects TLS certificate readiness and Redis reachability
  (including, in cluster mode, whether a Redis primary has been designated
  yet) — but not "has actually rejoined raft/Olric": a pod can report Ready
  before its cluster state has fully converged on that front.
- No bundled CronJob for scheduled backups (see section 9) — manual or
  external scheduling for now.
- Retained messages are cluster-wide (Redis-backed) only when
  `REDIS_ADDR` is configured; without Redis they fall back to
  mochi-mqtt's own per-node, in-memory retained store (a subscriber only
  sees retained messages published on that exact node, and they're lost
  on restart). With Redis configured, wildcard retained lookups
  (`SUBSCRIBE state/#`) scan the full retained-topic index — fine at
  typical volumes, a known bottleneck at tens of thousands of unique
  retained topics. Retained backfill from Redis is also QoS 0 only,
  regardless of the subscriber's requested QoS.

# Keel MQTT Gateway

A **cloud-native clustered MQTT broker** written in Go, built on
[`mochi-mqtt`](https://github.com/mochi-mqtt/server).

Keel is designed as a simpler alternative to VerneMQ and EMQX for teams that
need horizontal scalability without adopting a symmetric CRDT-based cluster
or a BSL-licensed broker.

Instead of making every node equal, Keel separates the cluster into:

- **Stateless MQTT Edge nodes** (HPA-scalable, terminate client connections)
- **Strongly-consistent Core nodes** (StatefulSet, manage cluster metadata via Raft)
- **AP Distributed routing** (Olric)
- **Direct gRPC data plane** (Zero message payload replication through consensus)

---

## Highlights

- **MQTT 3.1.1 / MQTT 5** support
- **Core/Edge Architecture:** Strongly consistent control plane (Raft) + eventually consistent data plane (Olric)
- **Auth:** per-tenant password, mTLS (X.509 CN + trusted CA pool), and JWT/JWKS (any OIDC-compliant IdP) — credential storage backends (postgres/file) are a fixed set today, no staged plugin interface yet. Two companion device-side agents cover the two real-world tiers: [`keel-cert-manager`](../keel-cert-manager) (Clavex Device PKI, full certificate rotation + revocation) and [`keel-jwt-agent`](../keel-jwt-agent) (generic OAuth2 client-credentials, no real revocation beyond token TTL)
- **Zero-Loss QoS 1/2:** Co-located Redis (primary+replica) with automatic Raft-driven failover
- **Kubernetes-first:** StatefulSet for Core, Deployment + HPA for Edge
- **Output plugin architecture:** Publish → Transform → Forward runs out-of-process (`hashicorp/go-plugin`/gRPC sidecar), fully isolated from MQTT delivery
- **Bridging:** Kafka / Redpanda / HTTP bridge out-of-the-box
- **Observability:** Prometheus metrics + OpenTelemetry tracing, tenant-aware sampling
- **Single binary:** (`--role=edge`, `core`, `combined`)
- **Apache 2.0 License** (No BSL, no SSPL)

---

## High-Level Architecture

```mermaid
flowchart TB

classDef edge fill:#E8F5E9,stroke:#2E7D32,color:#000
classDef control fill:#F3E5F5,stroke:#6A1B9A,color:#000
classDef data fill:#E0F7FA,stroke:#00838F,color:#000
classDef persist fill:#FFF3E0,stroke:#EF6C00,color:#000

Client["MQTT Clients"]

subgraph EDGE["Edge Layer (Stateless)"]
    Broker["mochi-mqtt\n\nAuth\nACL Cache\nPlugins\nTopic Match\nRouting"]
end

subgraph CORE["Core Layer (Stateful)"]
    subgraph CP["Control Plane"]
        Memberlist["Memberlist"]
        Raft["Raft"]
        FSM["FSM"]
        Memberlist --> Raft
        Raft --> FSM
    end
    subgraph DP["Data Plane"]
        Olric["Olric\n\nRouting Table\nPub/Sub\nTTL\nCache"]
        Redis["Redis\n\nQoS\nOffline Queue\nSession Persistence\nRetained Messages"]
    end
end

subgraph Integration
    Kafka
    Redpanda
    HTTP
    MQTTBridge["MQTT Bridge"]
end

Client --> Broker
Broker --> FSM
FSM --> Olric
FSM --> Redis
Broker --> Kafka
Broker --> Redpanda
Broker --> HTTP
Broker --> MQTTBridge

class Broker edge
class Memberlist,Raft,FSM control
class Olric data
class Redis persist
```

---

## Why not VerneMQ or EMQX?

- **VerneMQ** clusters nodes symmetrically using CRDTs (`vmq_swc`), which can
  produce non-deterministic merges under heavy subscribe/unsubscribe churn and
  offers no clean separation between "pod ready" and "cluster ready".

- **EMQX** moved to **BSL 1.1** from v5.9.0, limiting free commercial usage.

Keel adopts the same architectural direction taken by modern distributed
systems (EMQX Mria Core/Replicant, RabbitMQ Quorum Queues, NATS JetStream):
- Core nodes (stateful) for cluster metadata
- Stateless edge nodes for connection termination
- Strongly consistent metadata, independent AP data plane

---

## Architecture Details

Keel separates responsibilities into independent layers to avoid bottlenecks on the hot path.

| Layer | Responsibility | Technology |
|--------|---------------|------------|
| Transport | MQTT protocol | mochi-mqtt |
| Broker | Auth, ACL, plugins, routing | Internal |
| Control Plane | Cluster metadata (Session Ownership, Redis Primary) | Raft + Memberlist |
| Data Plane | Routing table, cache, pub/sub | Olric |
| Persistence | QoS persistence, offline queue, retained | Redis |
| Integration | Kafka, HTTP, MQTT bridge | Internal |

### Data Ownership

- **Raft stores only cluster metadata:** Session ownership, cluster configuration, ACL version, Redis primary election.
- **Olric stores only derivable state:** Routing table, topic registry, distributed cache, TTL.
- **Redis stores only durable MQTT state:** QoS persistence, offline queue, session persistence, retained messages.

No MQTT payload is replicated through Raft. No MQTT packet is routed through Olric. The MQTT data plane is always **direct gRPC** between broker nodes.

---

## Node roles

| | Core | Edge |
|------|------|------|
| MQTT Listener | Optional | ✔ |
| Raft | ✔ | ✖ |
| Memberlist | ✔ | ✔ |
| Olric | ✔ | ✖ |
| Redis | ✔ | ✖ |
| State | ✔ | ✖ |
| Kubernetes | StatefulSet | Deployment + HPA |

---

## Data Flows

### 1. CONNECT sequence

When a client connects, the edge node claims the session via Raft to guarantee global uniqueness.

```mermaid
sequenceDiagram
    participant C as Client
    participant E as Edge Node
    participant R as Raft (Core)

    C->>E: CONNECT
    E->>R: ClaimSession(clientID)
    R-->>E: OK (Ownership Acquired)
    E->>E: ACL Check (Local Cache)
    E->>C: CONNACK
```

### 2. PUBLISH sequence

Messages are routed directly between edge nodes using a local cache, falling back to Olric only when necessary.

```mermaid
sequenceDiagram
    participant P as Publisher
    participant E1 as Edge 1
    participant E2 as Edge 2
    participant O as Olric (Core)

    P->>E1: PUBLISH
    E1->>E1: Check Local Topic Cache
    alt Subscriber on E1
        E1->>P: PUBACK
        E1->>P_sub: Local Delivery
    else Subscriber on E2
        E1->>E2: Forward via gRPC
        E2->>P_sub: Remote Delivery
        E1->>P: PUBACK
    else Unknown Route
        E1->>O: Lookup Topic
        O-->>E1: Return Node List
        E1->>E2: Forward via gRPC
    end
```

### 3. FAILOVER sequence

If a core node crashes, Raft elects a new leader and Redis is promoted to avoid QoS data loss.

```mermaid
sequenceDiagram
    participant Core1 as Core 1 (Leader)
    participant Core2 as Core 2 (Follower)
    participant Core3 as Core 3 (Follower)
    participant Redis as Redis Primary

    Core1--xCore1: CRASH!
    Note over Core2,Core3: Heartbeat timeout
    Core2->>Core2: Request Election
    Core3->>Core2: Vote
    Core2->>Core2: Elected Leader

    Note over Core2,Redis: Redis Failover Loop triggers
    Core2->>Redis: PROMOTE REPLICA (SLAVEOF NO ONE)
    Redis-->>Core2: OK
    Core2->>Core3: Update Raft State (New Primary)

    Note over Core2,Core3: Traffic Continues
```

---

## Kubernetes Deployment

```mermaid
graph TD
    LB[LoadBalancer / Ingress] --> EDP[Edge Deployment]

    subgraph Edge Layer
        EDP --> EP1[Edge Pod 1]
        EDP --> EP2[Edge Pod 2]
        EDP --> EP3[Edge Pod N...]
    end

    EP1 & EP2 & EP3 --> CS[Core StatefulSet]

    subgraph Core Layer
        CS --> CP1["Core Pod 1<br>Raft + Olric + Redis P"]
        CS --> CP2["Core Pod 2<br>Raft + Olric + Redis R"]
        CS --> CP3["Core Pod 3<br>Raft + Olric + Redis R"]
    end
```

---

## Observability

- **Metrics:** Prometheus exposed on `/metrics` (`cfg.MetricsAddr`) — connections, auth duration, message throughput, raft apply duration, forwarder drops, and more (`internal/telemetry/metrics.go`).
- **Tracing:** OpenTelemetry via OTLP/gRPC, enabled by setting `OTLP_ENDPOINT`. Sampling is 10% by default, with per-tenant opt-in to 100% (`tracing_enabled` in tenant gateway config). Spans cover connection auth, publish, raft apply, and forwarder retries (`internal/telemetry/tracing.go`).

---

## Quick start

Requires Docker and Docker Compose.

```sh
docker compose up -d --build
```

The default topology starts a **combined** cluster (Core + Broker in the same
process) suitable for development.

### Test it in 30 seconds

Open two terminals to test pub/sub:

**Terminal 1 (Subscriber):**
```sh
mosquitto_sub -h localhost -p 1883 -t "test/topic" -v
```

**Terminal 2 (Publisher):**
```sh
mosquitto_pub -h localhost -p 1883 -t "test/topic" -m "Hello Keel!"
```

For production-like deployments (Core / Edge split, TLS, Redis failover), see:
- `deploy/docker-compose/README.md`
- `docker-compose.core-edge-split.yml`
- `docker-compose.tls.yml`

---

## Status & Roadmap

Keel is currently production-ready for its core feature set.

**Validated:**
- Reconnect storms (tested up to 10,000 devices, 0 message loss on QoS 1/2)
- Redis failover (Split-brain prevention, 0 lost messages on primary crash)
- JWKS auto-fetch and key rotation
- Core/edge pod kills (leader failover, edge pod restart) on a real K8s cluster
- MQTT 5.0 Shared Subscriptions (`$share/group/topic`), exactly-once delivery per group across the whole cluster
- Output plugin isolation: a slow/failing Kafka-Redpanda producer can no longer block MQTT delivery (fixed 2026-07-31, see design doc)
- StatefulSet rolling update (Olric cold-start sequencing) — `/readyz` (TLS, Redis, raft leader, local Olric reachability, present since v0.2.3) has been through several real rolling updates on GKE (v0.2.0 → v0.2.5) with no issues observed
- HPA edge load metric end-to-end on GKE: `prometheus-adapter` (VictoriaMetrics-backed) registering `custom.metrics.k8s.io`, HPA reading real `keel_edge_load_score` values (`ScalingActive: True`, `ValidMetricFound`) — confirmed on Kimera 2026-08-03

**Roadmap / Known Gaps:**
- Auth/ACL staged plugin interface (JWT/OAuth/other providers today require code changes, not a plugin) — deliberately deferred
- Advanced K8s Operator for automated cluster scaling.

---

## Documentation

See:
- `CONTRIBUTING.md`

---

## License

Apache License 2.0

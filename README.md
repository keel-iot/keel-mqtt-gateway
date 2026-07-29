# Keel MQTT

A **cloud-native clustered MQTT broker** written in Go, built on
[`mochi-mqtt`](https://github.com/mochi-mqtt/server).

Keel is designed as a simpler alternative to VerneMQ and EMQX for teams that
need horizontal scalability without adopting a symmetric CRDT-based cluster
or a BSL-licensed broker.

Instead of making every node equal, Keel separates the cluster into:

- **Stateless MQTT Edge nodes**
- **Strongly-consistent Core nodes**
- **Distributed routing and cache**
- **Direct data plane**

---

## Highlights

- MQTT 3.1.1 / MQTT 5
- Stateless edge nodes
- Strongly consistent control plane (Raft)
- Distributed routing with Olric
- Kubernetes-first architecture
- Plugin pipeline
- Kafka / Redpanda / HTTP bridge
- Single binary (`--role=edge`, `core`, `combined`)
- Apache 2.0

---

# High-Level Architecture

```mermaid
flowchart TB

classDef edge fill:#E8F5E9,stroke:#2E7D32,color:#000
classDef control fill:#F3E5F5,stroke:#6A1B9A,color:#000
classDef data fill:#E0F7FA,stroke:#00838F,color:#000
classDef persist fill:#FFF3E0,stroke:#EF6C00,color:#000

Client["MQTT Clients"]

subgraph EDGE["Edge Layer (Stateless)"]

Broker["mochi-mqtt

Auth
ACL Cache
Plugins
Topic Match
Routing"]

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

Olric["Olric

Routing Table
Pub/Sub
TTL
Cache"]

Redis["Redis

QoS
Offline Queue
Session Persistence"]

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

# Why not VerneMQ or EMQX?

- **VerneMQ** clusters nodes symmetrically using CRDTs (`vmq_swc`), which can
  produce non-deterministic merges under heavy subscribe/unsubscribe churn and
  offers no clean separation between "pod ready" and "cluster ready".

- **EMQX** moved to **BSL 1.1** from v5.9.0.

Keel adopts the same architectural direction taken by modern distributed
systems:

- Core nodes (stateful)
- Stateless edge nodes
- Strongly consistent metadata
- Independent data plane

Similar concepts can be found in:

- EMQX (Mria Core/Replicant)
- RabbitMQ (Quorum Queues)
- NATS JetStream
- Redpanda

---

# Architecture

Keel separates responsibilities into independent layers.

| Layer | Responsibility | Technology |
|--------|---------------|------------|
| Transport | MQTT protocol | mochi-mqtt |
| Broker | Auth, ACL, plugins, routing | Internal |
| Control Plane | Cluster metadata | Raft + Memberlist |
| Data Plane | Routing table, cache, pub/sub | Olric |
| Persistence | QoS persistence, offline queue | Redis |
| Integration | Kafka, HTTP, MQTT bridge | Internal |

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
| Kubernetes | StatefulSet | Deployment |

---

## Publish path

```mermaid
sequenceDiagram

participant Client

participant Edge

participant Core

participant Olric

participant Remote

Client->>Edge: PUBLISH

alt Local subscribers

Edge->>Edge: Deliver

else Remote subscribers

Edge->>Core: Resolve ownership

Core->>Olric: Lookup route

Core-->>Edge: Remote node

Edge->>Remote: gRPC

Remote->>Remote: Deliver

end
```

---

## Connect path

```mermaid
sequenceDiagram

participant Client

participant Edge

participant Raft

participant Olric

Client->>Edge: CONNECT

Edge->>Edge: Authentication

Edge->>Raft: Resolve session owner

Raft-->>Edge: Core

Edge->>Olric: Load session

Olric-->>Edge: Session

Edge-->>Client: CONNACK
```

---

## Data ownership

Raft stores only **cluster metadata**:

- Session ownership
- Cluster configuration
- ACL version
- Redis primary election

Olric stores only **derivable state**:

- Routing table
- Topic registry
- Distributed cache
- TTL
- Pub/Sub

Redis stores only **durable MQTT state**:

- QoS persistence
- Offline queue
- Session persistence

No MQTT payload is replicated through Raft.

No MQTT packet is routed through Olric.

The MQTT data plane is always **direct gRPC** between broker nodes.

---

## Kubernetes deployment

```mermaid
flowchart TB

LB["LoadBalancer"]

LB --> E1["Edge"]

LB --> E2["Edge"]

LB --> E3["Edge"]

subgraph StatefulSet

C1["Core"]

C2["Core"]

C3["Core"]

end

E1 --> C1

E2 --> C2

E3 --> C3

C1 <-- Raft --> C2

C2 <-- Raft --> C3

C3 <-- Raft --> C1

C1 <-- Olric --> C2

C2 <-- Olric --> C3

C3 <-- Olric --> C1
```

---

## Quick start

Requires Docker and Docker Compose.

```sh
docker compose up -d --build
```

The default topology starts a **combined** cluster (Core + Broker in the same
process) suitable for development.

For production-like deployments (Core / Edge split), see:

- `deploy/docker-compose/README.md`
- `docker-compose.core-edge-split.yml`
- `docker-compose.tls.yml`

---

## Documentation

See:

- `keel-design-doc.md`
- `CONTRIBUTING.md`

---

## License

Apache License 2.0

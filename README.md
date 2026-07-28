# keel-mqtt-gateway

A clustered MQTT broker in Go, built on [`mochi-mqtt`](https://github.com/mochi-mqtt/server),
designed as a simpler alternative to VerneMQ and EMQX for teams that need
horizontal scaling without adopting a CRDT-based clustering model or a
BSL-licensed broker.

## Why not VerneMQ or EMQX?

- **VerneMQ** clusters nodes symmetrically via CRDTs (`vmq_swc`), which can
  produce non-deterministic merges between nodes with divergent history
  under load, and offers no clean separation between "pod ready for K8s"
  and "node ready for the application cluster".
- **EMQX**, the most mature OSS alternative, moved to BSL 1.1 from v5.9.0 —
  not freely usable in commercial production beyond certain thresholds.

keel replaces "symmetric, eventually-consistent consensus" with a
**restricted-quorum, strongly-consistent core + stateless edge** model — the
same architectural pattern adopted by EMQX itself (Mria core/replicant),
RabbitMQ (quorum queues), Redpanda, and NATS JetStream.

## Architecture, in brief

Two node roles, same binary, selected via `--role`:

| | Core nodes | Edge nodes |
|---|---|---|
| Responsibility | Raft quorum: session ownership, ACL/cluster config | Terminate MQTT client connections (`mochi-mqtt`) |
| State | Yes — Raft log on disk | No |
| K8s workload | StatefulSet + PVC | Deployment |
| Scaling | Manual, explicit (`AddVoter`/`RemoveServer`) | Automatic HPA on real MQTT load |

- **Control plane**: [`hashicorp/raft`](https://github.com/hashicorp/raft) for session ownership only (small, low-write-volume FSM).
- **Membership**: [`hashicorp/memberlist`](https://github.com/hashicorp/memberlist) (SWIM gossip) — works identically on K8s, bare VMs, or Docker Compose.
- **Routing table** (topic subscriptions): [Olric](https://github.com/olric-data/olric), an embedded AP key/value store — derivable/rebuildable state doesn't need Raft's strong consistency, and this avoids the CRDT-merge bottleneck that affects VerneMQ under subscribe/unsubscribe churn.
- **Data plane**: direct gRPC between nodes, never routed through Raft or Olric — a control-plane outage degrades routing (stale table), it never blocks message delivery.

See [`keel-design-doc.md`](../keel-design-doc.md) for the full rationale,
every decision's history, and validated PoC results (scale tests up to
10k simulated devices, disaster recovery, TLS, backpressure).

## Quick start

Requires Docker and Docker Compose.

```sh
docker compose up -d --build
```

This starts a 3-node all-in-one cluster (`--role=combined`, each node runs
both Raft/Olric and the MQTT broker — the minimal PoC topology, no
dedicated edge tier). Check cluster state:

```sh
curl -s http://localhost:18090/api/cluster/nodes | python3 -m json.tool
```

For a core/edge split topology (closer to a real K8s deployment) and other
test scenarios (late join, drain/rejoin, TLS, backup/restore), see
[`deploy/docker-compose/README.md`](deploy/docker-compose/README.md) and
the `docker-compose.core-edge-split.yml` / `docker-compose.tls.yml` files.

## Development

```sh
go build ./...
go vet ./...
go test ./...
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for conventions and how to propose
architectural changes.

## License

Apache License 2.0 — see [`LICENSE`](LICENSE).

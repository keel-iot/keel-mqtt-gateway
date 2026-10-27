# Scoped transitive dependency updates — 2026-10-27

Based on v0.3.1. These updates ship in v0.3.2;
production deployment is managed separately.

## Scope

Keep Olric, Raft, memberlist and their dependency graph at the versions
selected by v0.3.1. The comparison covers the 118 selected module versions
reachable from those three modules' requirements, including shared dependencies.
All 118 versions are unchanged after dependency resolution and `go mod tidy`.
This records the current maintenance boundary, not a permanent version policy.

In particular, keep armon/hashicorp metrics, bitset, consistent, btree, DNS,
logging/color dependencies, go-winio, logrus and shared Prometheus dependencies
unchanged. franz-go is already at the latest release found in this review.

## Updates

| Module | v0.3.1 | v0.3.2 |
| --- | --- | --- |
| docker/go-connections | 0.7.0 | 0.8.2 |
| ebitengine/purego | 0.10.1 | 0.11.2 |
| grpc-gateway/v2 | 2.30.0 | 2.31.0 |
| magiconair/properties | 1.8.10 | 1.18.12 |
| moby/go-archive | 0.2.0 | 0.3.3 |
| moby/moby/api | 1.55.0 | 1.56.1 |
| moby/moby/client | 0.5.0 | 0.6.2 |
| moby/sys/user | 0.4.0 | 0.5.0 |
| moby/sys/userns | 0.1.0 | 0.2.1 |
| oklog/run | 1.1.0 | 1.2.0 |
| shirou/gopsutil/v4 | 4.26.6 | 4.26.9 |

The Docker client updates resolve associated archive, OpenAPI, terminal,
telemetry and generated API dependencies. The complete selected graph changes
44 module versions; only required selections are retained in go.mod/go.sum.

The latest grpc-gateway 2.33.0 would advance x/tools, x/mod and x/telemetry
shared with the protected graph. Resolve to 2.31.0, which retains their baseline
versions. No application-code adaptation or dependency replacement was needed.

## Validation

- Main module: `go build ./...`, `go vet ./...`, `go test ./...`.
- `govulncheck ./...`: zero affected vulnerabilities and zero affected imported
  packages; the existing unused OpenPGP module-only advisory remains.
- Compile all tagged cluster tests with `go test -tags cluster ./test/cluster
  -run '^$'`.
- Run a real Testcontainers/Docker smoke check with Redis 7.4.12: create a
  dedicated network, attach a container with an alias, map a port, copy a file
  into the container through the archive API, execute a command, read logs,
  and verify Redis PING/SET/GET. Container and network cleanup succeeded.
- Verify all 118 protected selected module versions against the v0.3.1 graph
  after the final update resolution.

These checks validate the exercised Linux gateway and Docker test paths;
they are not a full multi-node cluster load or failure-injection campaign.

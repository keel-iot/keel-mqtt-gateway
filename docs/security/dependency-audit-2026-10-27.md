# Dependency review — 2026-10-27

Based on release v0.3.0 (d8259da). These updates ship in v0.3.1;
the published v0.3.0 tag and image are unchanged.

## Findings and changes

Trivy 0.76.1 scanned the published `ghcr.io/keel-iot/keel-mqtt-gateway:0.3.0`
image. It reported two musl package entries for CVE-2026-107891 on Alpine
3.20.10, and 20 advisory entries in the gateway binary (7 High, 8 Medium,
4 Low, 1 Unknown). Entries are package/advisory matches, not proof that
all affected features are exposed or exploitable in this deployment.

The source baseline used the locally selected Go 1.26.2 toolchain.
`govulncheck` reported 36 unique advisories with symbol-level traces across
all main-module packages. This is a source-analysis result, not a count of
vulnerabilities in the production image: the old Docker builder floated
within Go 1.26 and could already include newer standard-library fixes.

Updated components include:

| Component | Previous | v0.3.1 |
| --- | --- | --- |
| Minimum Go toolchain, all three modules | 1.26.2; fork 1.21 | 1.26.10 |
| Docker Go builder | floating 1.26-alpine | 1.26.10-alpine3.24 |
| Gateway runtime base | alpine:3.20 | alpine:3.24.3 |
| Redis image, chart and test environments | floating 7-alpine / 7.4-alpine | 7.4.12-alpine |
| go-redis/v9 | 9.18.0 | 9.23.0 |
| gRPC | 1.80.0 | 1.85.0 |
| pgx/v5 | 5.9.1 | 5.11.0 |
| chi/v5 | 5.2.5 | 5.5.0 |
| OpenTelemetry | 1.43 / 1.44 | 1.47.0 |
| x/crypto, x/net, x/text | 0.54, 0.56, 0.40 | 0.58, 0.61, 0.43 |
| klauspost/compress | 1.18.6 | 1.20.3 |
| bbolt | 1.3.5 | 1.3.12 (same minor line) |
| franz-go / kadm in local vendor-pkg | 1.21.0 / 1.18.0 | 1.22.2 / 1.19.0 |

Also updated compatible fsnotify, PROXY protocol, Prometheus, protobuf,
synchronization, rate-limiting, testing and fork dependency versions.
The local Mochi replacement and its gateway patches remain in place.
The fork's standalone Dockerfile also uses the updated Go and Alpine bases.
Three HTTP listener tests now retry until the asynchronous server is ready
instead of assuming a one-millisecond startup window; the initial test failure
left the shared test port bound and caused later tests to fail.

## Remaining findings and deferred updates

- `govulncheck` after the changes reports no symbol-level or package-level
  vulnerability findings in the main module or either nested module. The main
  module retains one module-only advisory, GO-2026-5932, for the unmaintained
  `golang.org/x/crypto/openpgp` package. The gateway's dependency import graph
  does not include OpenPGP. No scanner suppression was added.
- The rebuilt gateway image has zero Alpine package findings and one Unknown
  binary advisory, the same OpenPGP module match. There are no High or Critical
  findings in that image with the database used for this review.
- The Redis 7.4.12 Alpine image has zero scanner findings in this review. Its
  base is Alpine 3.21.9, whose ordinary support ends on 2026-11-01. Review the
  upstream image base again before that date; the official 7.4.12 tag currently
  offers Alpine 3.21 or Debian Bookworm, not Alpine 3.24.
- Redis 7.4.12 fixes a cluster-bus security issue. This gateway runs Redis in
  standalone primary/replica mode, so the affected cluster-bus path is not
  enabled. This patch does not establish that the earlier ENOSPC/AOF issue is
  fixed.
- Raft 1.8.0 and memberlist 0.7.0 are available but intentionally deferred.
  They change metrics integration and Raft 1.8 persists the commit index in
  the LogStore. Coordinated dependency and mixed-version/storage-recovery
  testing belongs in a separate change. No reachable advisory in the retained
  versions remains in the updated source scan. The Raft BoltDB adapter remains
  2.3.1 and bbolt stays within the 1.3 patch line.
- This review covers this repository and its images. External PostgreSQL,
  Redpanda, console services and the GKE/node images were not scanned.

## Validation

- Main module: build, vet and complete default test suite.
- Both nested modules: complete default test suites and `govulncheck`.
- Main module: `govulncheck` with Go 1.26.10; CI now runs pinned
  `govulncheck@v1.3.0` to detect future reachable Go advisories.
- Gateway Docker build with the updated runtime base and Trivy image scan.
- Redis 7.4.12 Alpine image scan; Helm lint and rendering.

Additional runtime checks passed:

- Live Redis 7.4.12 storage and routing tests with the race detector.
- Docker-based authenticated Redis-sidecar failure, promotion, client
  redirection and recovery, with the race detector and go-redis 9.23.0.
- The rebuilt container starts and runs its CLI help as the non-root user.

The first full fork run exposed asynchronous HTTP test startup timing. After
replacing the fixed startup assumption with bounded availability retries, the
complete fork suite passed. Gateway production listener code was unchanged.

## Sources

- [Go vulnerability management](https://go.dev/doc/security/vuln/)
- [Go vulnerability database](https://vuln.go.dev/)
- [Alpine release support](https://alpinelinux.org/releases/)
- [Redis 7.4.12 release](https://github.com/redis/redis/releases/tag/7.4.12)
- [Raft 1.8.0 release](https://github.com/hashicorp/raft/releases/tag/v1.8.0)
- [memberlist 0.7.0 release](https://github.com/hashicorp/memberlist/releases/tag/v0.7.0)

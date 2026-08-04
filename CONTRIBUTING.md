# Contributing to keel-mqtt-gateway

## Running tests locally

```sh
go build ./...
go vet ./...
go test ./...
```

Unit tests require no external services. For cluster/e2e scenarios that do
(Redpanda, TLS certs, a multi-node compose topology), see
`test/e2e/*.sh` and `deploy/docker-compose/README.md` — these are run
manually, not yet part of CI (see below).

## Commit conventions

- No AI/LLM attribution or signature in commit messages — write commits as
  if authored directly by a human contributor.
- Prefer commit messages that explain *why* a change was made, not just
  what changed — the diff already shows the what.
- Keep commits focused; avoid bundling unrelated fixes with a feature.

## Proposing architectural changes

Before proposing a change to core architecture (clustering, session
ownership, routing, ACL model), read the relevant package docs and
existing comments — most non-obvious decisions (why Raft vs. Olric for a
given piece of state, TLS termination point, backpressure strategy) are
explained inline where they apply. If your change conflicts with an
existing decision, explain why in your PR description instead of
silently diverging from it.

For bug fixes, small features, or anything that doesn't touch a documented
architectural decision, no design-doc update is expected — just open a PR.

## Code style

- No comments explaining *what* code does — names should make that clear.
  Comments are for non-obvious *why* (a constraint, a workaround, an
  invariant a future reader could easily break).
- Follow existing package conventions (e.g. `internal/cluster/store`'s
  `ClusterStore` interface pattern for swappable backends) rather than
  introducing a new abstraction style for the same kind of problem.

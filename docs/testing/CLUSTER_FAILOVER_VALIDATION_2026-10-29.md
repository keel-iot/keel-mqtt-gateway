# Cluster and Redis failover validation — 2026-10-29

Executed against the current working tree, including the selected Mochi backports and Keel-specific persistence changes. The checkout contains pre-existing uncommitted work; HEAD alone does not identify the tested source.

- Git HEAD: `7b73147648d9e7eb774e01fe2f8ca536e4769146` (dirty working tree).
- Built application image: `sha256:f957f435352b978a66b42c462ec1d11a50a2a5e6f20fdc293a0b98f0700b0423`.
- Cluster suite: **19 passed, 0 failed, 0 skipped**, package elapsed 544.7 seconds (includes image build and infrastructure setup).
- Dedicated Redis sidecar failover/rejoin test: **passed**, 21.62 seconds.
- No new benchmark runs were started. At the user’s request the laboratory was stopped after validation; Docker has no running containers and no Python/Erlang load generators remain on the Raspberry Pi.

## Coverage

Fresh real Docker topologies cover Core follower/leader death and rejoin, session ownership/reconnect on another Edge, QoS1 offline recovery, QoS2 Edge failure, packet-ID collision, Redis per-client expiry cleanup, shared subscriptions and inflight recovery, cross-node routing/unsubscribe/wildcards, and mixed-image rolling persistence migration. Core/Edge/Redis/Postgres are separate processes; Redis is provisioned per Core.

The rolling test uses release **0.3.2** as the old Edge image and the freshly built `keel-mqtt-gateway:cluster-test` as the compatibility/current image. It exercises schema 1, pre-activation rollback, schema 2 activation and directional rehydration.

| Scenario | Result | Test wall time (s) |
|---|---|---:|
| `TestCoreFollowerDeath_ClusterStaysOperational` | PASS | 111.33 |
| `TestCoreFollowerRejoin_ReconvergesAfterRestart` | PASS | 24.07 |
| `TestCoreLeaderDeath_NewLeaderElectedAndProgressRestored` | PASS | 28.14 |
| `TestCoreLeaderRejoin_ConvergesAfterRestart` | PASS | 41.73 |
| `TestDeadEdgeForwarding_DocumentedLossConfirmed` | PASS — confirms known loss limitation | 23.69 |
| `TestDirectionalIFMRealRollingLifecycle` | PASS | 34.26 |
| `TestMultiNodeHappyPath` | PASS | 20.06 |
| `TestOfflineSession_QueuedQoS1Delivery_ReconnectOnDifferentEdge` | PASS | 22.84 |
| `TestOwnerEdgeDies_ReconnectSucceedsOnAnotherEdge` | PASS | 20.41 |
| `TestDuplicateClientID_NewConnectionEvictsOldOwner` | PASS | 22.03 |
| `TestPacketID_CollisionAcrossLiveAndOfflineCounters` | PASS | 28.13 |
| `TestQoS2EdgeFailure_HandshakeContinuesAndLiveForwardingSurvives` | PASS | 20.74 |
| `TestCrossNodeUnsubscribeStopsRemoteDelivery` | PASS | 26.21 |
| `TestSessionExpiry_CleansPerClientRedisState` | PASS | 24.41 |
| `TestSharedInflightRegression_TwentyLiveMembersNoOfflineGrowth` | PASS | 12.86 |
| `TestSharedInflightRegression_SameEdgeResumeDeliversExactlyOnce` | PASS | 10.18 |
| `TestOfflineSharedSubscription_QueuesExactlyOneMember` | PASS | 28.15 |
| `TestOfflineSharedSubscription_LiveMemberSuppressesOfflineQueue` | PASS | 25.16 |
| `TestCrossNodeWildcard_BareTopicMatchesHashFilter` | PASS | 20.28 |

## Redis failover

`TestRedisSidecarFailure_FailoverAndRejoinWithLiveCore` kills the primary Redis while its Core stays alive, verifies another Redis is promoted, then checks the returned Redis rejoins as a replica. It uses real Redis containers and host networking. The first setup attempt lacked the test’s required `redis:7.4-alpine` image (`--pull=never`); after explicitly pulling that image, the unmodified test passed. Both attempts are preserved.

## Confirmed limitation

`TestDeadEdgeForwarding_DocumentedLossConfirmed` is a passing regression **for an existing gap**. Best-effort forwarding to a destination Edge that dies can discard a message after the ingress broker has acknowledged the publisher. Route cleanup and independent surviving-Edge traffic recover; that does not recover the discarded message. The successful suite is not an end-to-end no-loss certification.

No deterministic network partition, GKE upgrade, ENOSPC/AOF-corruption reproduction or production PVC/storage fault was injected. These tests are functional recovery checks, not a saturation/failover-under-load benchmark.

## Artifacts and deferred work

Raw JSONL, the image-pull/setup logs, `SUMMARY.json` and source hashes are in `/home/fabio/keel-benchmark-lab/validation-20261029/`.

Comparison preparation is retained outside the repository: `compose.comparison-20261029.yaml` and `hivemq-comparison.xml` in `/home/fabio/keel-benchmark-lab/`. EMQX 6.3.2 and HiveMQ CE 2026.5 images were downloaded, but no comparative benchmark was executed. Future comparisons must document single-node Community-edition scope and distinguish Keel with PostgreSQL from broker-only resource totals.

No application fix, version bump, commit, push or production deployment was performed as part of this validation run.

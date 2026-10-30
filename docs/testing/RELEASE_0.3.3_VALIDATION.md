# Release 0.3.3 integration validation

The release branch integrates the allocation/persistence work and the join/offline fixes with the routing and dependency updates already published in 0.3.2. Conflicts were resolved without dropping exact per-shared-filter forwarding metadata. The ID-only lookup now returns both node targets and per-filter choices.

Validation used a separate clean worktree, not the original workspace containing uncommitted formal/specification material.

- Main default test suite and go vet: passed.
- Complete Mochi fork default suite: passed; the release CI now runs this nested module explicitly.
- Broker, routing, dataplane and Mochi server/packets with race detector: passed.
- Broker persistence tests with real Redis 7.4.12: passed.
- Redis-sidecar-only crash, promotion/redirection and replica rejoin integration: passed (20.91 seconds). This does not satisfy the entire real-Raft/core-loss definition of issue #15.
- Helm lint and rendering: passed, including default auto mode and explicit schema 2 with Redis authentication; both core and edge receive the selected mode.
- Conformance result classifier: 13 tests passed. This is not a new full Paho conformance campaign.
- govulncheck: 0 affected vulnerabilities, 0 vulnerabilities in imported packages, 3 module-level advisories with no vulnerable calls reported.
- Full real multi-container cluster suite: 22 passed, 0 failed.

The case confirming loss to a dead forwarding target passes by observing the documented limitation; it does not establish lossless delivery for arbitrary failures.

Validated application image: sha256:3dad4c2c94ec98c638f26edcd29004a6bb30a9dd79cf9904c27507d593bd8263. The final documentation/CI commit does not change runtime code. No new performance campaign was run after integrating the previously published dependency/shared-routing changes; historical benchmark figures remain scoped to their recorded images.

| Cluster scenario | Result | Duration |
|---|---|---:|
| TestCoreFollowerDeath_ClusterStaysOperational | PASS | 30.82s |
| TestCoreFollowerRejoin_ReconvergesAfterRestart | PASS | 24.43s |
| TestCoreLeaderDeath_NewLeaderElectedAndProgressRestored | PASS | 33.87s |
| TestCoreLeaderRejoin_ConvergesAfterRestart | PASS | 26.42s |
| TestDeadEdgeForwarding_DocumentedLossConfirmed | PASS | 20.65s |
| TestDirectionalIFMRealRollingLifecycle | PASS | 33.35s |
| TestMultiNodeHappyPath | PASS | 21.75s |
| TestOfflineSession_QueuedQoS1Delivery_ReconnectOnDifferentEdge | PASS | 22.08s |
| TestOfflineSession_OriginalEdgeOwnerDoesNotDuplicate | PASS | 24.68s |
| TestOwnerEdgeDies_ReconnectSucceedsOnAnotherEdge | PASS | 20.89s |
| TestDuplicateClientID_NewConnectionEvictsOldOwner | PASS | 19.59s |
| TestPacketID_CollisionAcrossLiveAndOfflineCounters | PASS | 21.71s |
| TestQoS2EdgeFailure_HandshakeContinuesAndLiveForwardingSurvives | PASS | 32.50s |
| TestCrossNodeUnsubscribeStopsRemoteDelivery | PASS | 25.08s |
| TestSessionExpiry_CleansPerClientRedisState | PASS | 24.02s |
| TestSharedInflightRegression_TwentyLiveMembersNoOfflineGrowth | PASS | 12.32s |
| TestSharedInflightRegression_SameEdgeResumeDeliversExactlyOnce | PASS | 10.22s |
| TestSharedRouting_MultipleGroupsAndOrdinaryFanout | PASS | 13.91s |
| TestOfflineSharedSubscription_QueuesExactlyOneMember | PASS | 32.51s |
| TestOfflineSharedSubscription_LiveMemberSuppressesOfflineQueue | PASS | 25.71s |
| TestSharedRouting_SharedLiveAndOrdinaryRouteMustNotDuplicate | PASS | 11.15s |
| TestCrossNodeWildcard_BareTopicMatchesHashFilter | PASS | 19.49s |

All open issues were reviewed against their complete acceptance criteria. The per-issue progress and remaining work are recorded in [release notes](../releases/0.3.3.md). None is automatically closed.

Artifacts: /home/fabio/keel-benchmark-lab/release-0.3.3-validation. Publication is blocked until authorized Git credentials are available: HTTPS Git has no credential, SSH is unauthorized, and the connected GitHub app rejects Git tree writes with HTTP 403 Resource not accessible by integration. Local release commits and tag can be prepared without publishing them.

# Redis primary failure and recovery

Each core hosts a standalone Redis with AOF persistence. The gateway's
Raft leader configures asynchronous replication, promotes a replica, and
records the primary node ID in Raft. All gateways redirect their shared
Redis client to the designated primary. This topology does not use Redis
Cluster or Sentinel.

## Failure detection

The controller reconciles every two seconds. A primary is unavailable if
its core is absent from gossip or its Redis `PING` fails. A probe uses the
configured Redis password and a two-second deadline without client retries.
Connection refusal, timeout, and Redis `LOADING` errors count as failed
observations. This handles a Redis container crash or failed AOF load while
the gateway container in the same pod continues participating in gossip.

The default failure window is 30 seconds. A successful `PING` clears the
window; an observed loss of Raft leadership clears the local observations.
Actual recovery time also includes probing, promotion, and client redirection.
The controller does not reconfigure replicas toward an unavailable primary
during the grace window.

After the window expires, the Raft leader tries other cores in node-ID order.
It skips candidates whose Redis `PING` fails. Only a successful promotion
is followed by a Raft primary designation update. If promotion fails, the
designation stays unchanged and the controller retries on a later tick.
A timeout can hide an already-executed promotion, so the controller does
not promote another candidate in that same attempt. Loss of leadership
observed after a probe stops the controller from issuing a promotion.

When the old Redis becomes reachable, it is configured to replicate from
the new primary. An AOF format error can prevent Redis from starting at all;
the operator must preserve its files and recover or rebuild that replica.
The controller does not edit corrupted persistence files.

## Verification after a failure

Check the gateway logs for `redis primary unavailable, starting failure window`
and `redis primary failover complete`. The first log includes the failed
probe's error; a completed failover increments
`keel_gateway_storage_failovers_total`.

Use `redis-cli INFO replication` in each core's Redis container. The new
primary should report `role:master`; recovered replicas should report
`role:slave`, the new primary as `master_host`, `master_link_status:up`, and
`master_sync_in_progress:0`. When authentication is enabled, supply credentials
through `REDISCLI_AUTH`. Check gateway readiness and successful MQTT traffic
as well as Redis replication.

## Validation

Unit tests cover the Redis-only failure with unchanged core membership,
continuous `LOADING`, the grace window, transient recovery, unavailable
candidates, failed promotions, and reconfiguration of the recovered primary.
A stalled TCP peer verifies that the real Redis client's probe honors its
deadline.

```sh
go test -race ./internal/cluster/membership ./internal/cluster/redisrouter ./cmd/server
```

The Linux integration test uses Docker host networking and two authenticated
Redis 7.4 containers. It stops only the primary Redis while retaining core
membership, verifies promotion through a real Raft registry and client
redirection, checks previously replicated data and new writes, and restarts
the old Redis to verify it becomes a synchronized replica. It uses a
single-voter Raft test node with the topology count overridden; it does not
validate elections or network partitions across a real multi-core cluster.

```sh
docker pull redis:7.4-alpine
go test -tags integration ./internal/cluster/membership -run '^TestRedisSidecarFailure_' -count=1 -v
```

## Limits

This change restores availability after a Redis-only failure. It does not
prevent AOF corruption or disk exhaustion. The evidence below identifies
ENOSPC during the October 25, 2026 incident; a causal link to the GKE upgrade
has not been established. Asynchronous replication can lose writes that
have not reached the promoted replica. Candidate ordering is deterministic
but does not rank replication freshness; key count alone does not prove
which copy is newest. Raft records the selected primary, but the current
Redis topology has no write fencing to guarantee a single writable Redis
during every network partition. These durability and partition guarantees
require separate design work and fault testing.

## October 25, 2026 incident evidence

The original Redis 7.4.11 log contains these events (UTC):

- 01:25:26: a background AOF rewrite opens `appendonly.aof.108022.incr.aof`.
- 01:25:40.272: the rewrite child reports `No space left on device`.
- 01:25:40.272: the parent reports a short AOF write: 1,595 of 3,035 bytes.
- 01:25:40.273: the parent reports that writing works again and continues.
- 01:27:17: Redis starts again; the cause of that restart remains unconfirmed.
- 01:27:40: Redis aborts while loading the malformed incremental AOF.

The preserved file has a valid prefix ending at byte 27,691,461, followed
by exactly 1,595 zero bytes, then a structurally valid RESP suffix starting
at byte 27,693,056. The entire 25,296,419-byte suffix passes redis-check-aof.
The base RDB checksum and preceding incremental AOF also pass validation.

Redis 7.4.11's `openNewIncrAofForAppend` opens a new incremental file with
`O_WRONLY|O_TRUNC|O_CREAT`, without `O_APPEND`. After a short write,
`flushAppendOnlyFile` truncates to the previous size but does not reset the
descriptor's current offset. It retries the retained write buffer on a
later call. Because ftruncate does not change that offset, this sequence
can insert a zero-filled hole exactly as long as the partial write before
the next successful write. This source-level reconstruction matches the
incident byte for byte; a small local filesystem reproduction confirms
the offset/hole behavior, but a fault-injected full Redis reproduction
has not yet been run.

The gap may therefore be inserted padding rather than erased commands;
the retained write buffer is retried in full after successful truncation.
Any reconstruction must be evaluated on an isolated copy, not replayed
into the recovered production primary. Preserve the original artifacts.

Sources: [Redis 7.4.11 AOF implementation](https://github.com/redis/redis/blob/7.4.11/src/aof.c),
[ftruncate semantics](https://pubs.opengroup.org/onlinepubs/9799919799/functions/ftruncate.html).

Disk capacity and free-space alerting must account for simultaneous base,
incremental, RDB, rewrite temporary, and abandoned temporary files. The
preserved corrupt directory itself consumes space on the production PVC
until the forensic copy is moved off that volume.

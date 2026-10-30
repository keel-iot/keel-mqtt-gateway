# Client queue memory investigation (2026-10-28)

## Cause and implementation

Mochi's default `MaximumClientWritesPending` is 8,192. Previously every
client constructed a channel with that many packet-pointer slots, including
clients that never received a publication. On a 64-bit process this reserves
64 KiB per connection: about 400 MiB at 6,400 connections. This transport
write queue is separate from MQTT QoS inflight state.

`thirdparty/mochi-mqtt-server/packet_queue.go` replaces that channel with a
bounded FIFO. Packet storage starts on first enqueue, grows from 16 slots to
the configured limit, clears consumed references, and releases larger arrays
when drained. The default capacity remains 8,192; enqueue remains nonblocking
when full. Explicit zero capacity preserves rendezvous behavior. Stopping a
client releases transport copies without deleting durable QoS inflight state.

Shared-subscription gathering now constructs its result under the registry
read lock instead of first making an intermediate snapshot. Returned
group/member maps remain independent of the registry. Subscription values
retain the existing shallow-copy semantics. Selection and routing are unchanged.

Mosquitto 2.0.22 instead uses linked packet objects allocated as needed for
its transport queue. Its separate QoS queue allocates client-message records
referencing message storage. Its limits and persistence model are not identical.

Sources: [Mochi defaults](https://github.com/mochi-mqtt/server/blob/v2.7.9/server.go),
[Mosquitto transport queue](https://github.com/eclipse-mosquitto/mosquitto/blob/v2.0.22/lib/packet_mosq.c),
[Mosquitto message storage](https://github.com/eclipse-mosquitto/mosquitto/blob/v2.0.22/src/database.c).

## Measurements

Controlled variants use release 0.3.2 (`8fab41ecbc2f46a4d31b70eb258f851af0c7106d`)
plus only these changes. The working checkout has other pre-existing changes;
these measurements must not be attributed to all of them.

Broker: Ubuntu 24.04, Core Ultra 7 155H, container limited to two CPUs and
4 GiB without swap. Separate generator: Raspberry Pi 4, 4 GiB, ARM64,
Ethernet. Standalone conformance mode, no TLS/auth/Redis, 6,400 publishers,
12 shared consumers, QoS 1, 256-byte payloads, one message/second/publisher.

| Measurement | Release 0.3.2 | Queue + shared snapshot changes |
|---|---:|---:|
| Live Go heap after forced GC under traffic | 449.12 MiB | 39.64 MiB |
| Allocation rate between samples (~30 seconds) | 79.05 MiB/s | 64.05 MiB/s |
| Peak combined container memory, broker plus Postgres, unprofiled | 918.80 MiB | 505.58 MiB |
| Average CPU cores, broker plus Postgres | 0.451 | 0.477 |
| Publish-to-receive p99, unprofiled run | 6.833 ms | 7.377 ms |

Each unprofiled run measured 30 seconds after warmup: 192,000 messages sent,
acknowledged, and uniquely received, with no missing messages or duplicates.
The final profiled run delivered all 384,000 messages over 60 seconds.
Forced GC affects timing; latency comes from separate unprofiled runs.
These are individual experiments, not confidence intervals, capacity
certification, production cluster tests, or soak tests.

The isolated 12-member shared-gather benchmark changed from 4,912 B/op and
14 allocations/op to 2,456 B/op and 7 allocations/op. Three single-CPU samples
measured 3.13–3.39 microseconds/op before and 1.36–1.39 after. This measures
one helper, not a corresponding broker-wide speedup.

Stacks, temporary allocations, allocator behavior, and Go's release of pages
still account for a substantial resident footprint. Heap size and container
memory are different. Keel's memory usage is not yet equal to Mosquitto's.

Raw artifacts outside the repository, in `/home/fabio/keel-benchmark-lab`:

- `profiling/traffic-20261028T220241Z` (release profiles).
- `profiling/memory-optimized-traffic-20261028T224746Z` (combined profiles).
- `results/doubling-multi-balanced-20261028T223754Z` (release unprofiled run).
- `results/doubling-multi-balanced-20261028T224635Z` (combined unprofiled run).
- `profiling/shared-gather-{before,after}-benchmark.log`.

## Regression coverage

Fork tests cover lazy allocation, exact configured capacity, overflow, FIFO
through wrapped growth, clearing references, cancellation/close, unbuffered
operation, concurrent producers, and a blocked socket with QoS 0/1/2.
`TestClientStopReleasesQueueButKeepsQoSState` checks that transport cleanup
leaves inflight state intact. Shared snapshot tests cover independent result
maps and gathering concurrently with subscription changes.

The full fork suite, full main-module suite, targeted race runs, build, and
`go vet` passed. Database-dependent tests requiring `TEST_DATABASE_URL` were
not exercised by the main-module unit run. Performance traffic is QoS 1;
QoS 2 slow-writer coverage is a local regression test.

Mutation checks on an isolated release checkout confirmed that tests fail
when restoring eager allocation, reducing capacity by one slot, or aliasing
shared member maps; restoring the implementation makes them pass again.
See `profiling/memory-mutations.json` and corresponding logs.

The black-box Paho run is not fully passing: MQTT 3.1.1 offline queueing,
and MQTT 5 offline queueing/publication expiry fail. A copy of the original
working tree with only these memory changes removed reproduces all three
failures. They must remain reported as failures, not classified as harness
issues or silently attributed to this optimization. The separately documented
MQTT 5 `test_flow_control2` harness issue remains classified by the existing
runner. Logs are `profiling/memory-optimized-conformance.log` and
`profiling/before-memory-changes-conformance.log`.

Follow-up (2026-10-29): the offline delivery failures were traced to Keel's
shared-fallback integration rather than upstream Mochi. The correction and
upstream comparison are documented in
[offline delivery review](OFFLINE_DELIVERY_UPSTREAM_REVIEW.md). The failures
above describe the earlier checkout and remain part of the profiling record.

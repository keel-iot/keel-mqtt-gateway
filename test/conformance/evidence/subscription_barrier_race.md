# Paho MQTT 5 subscription barrier race

## Requirement

A publisher may rely on a subscription after the subscribing client receives
its SUBACK. A broker cannot guarantee delivery for a subscription that has
not reached it yet on a different TCP connection.

## Test

Pinned Paho commit `9d7bb80bb8b9d9cfc0b52f8cb4c1916401281103`:
`client_test5.Test.test_request_response` and the initial No Local scenario
in `client_test5.Test.test_subscribe_options`.

## Environment

Reproduced 2026-10-29 on Mochi upstream v2.7.9
(`5b7f94bde4072edf6db62b69345f9454cae4936f`) and the local Keel Mochi fork.
The same minimal TCP broker fixture uses each module in turn, with allow-all
authentication and a loopback listener. The Keel standalone conformance run
also exhibited both failures in separate runs; the other test passed on each
of those runs.

## Expected behavior

After client B subscribes, the test waits for **B's** SUBACK before client A
publishes. Request/response then verifies ResponseTopic, CorrelationData and
the reply; the subscription-options test verifies No Local and its remaining
retain options.

## Observed behavior

Both tests call `bclient.subscribe(...)`, then wait on
`callback.subscribeds` (A's callbacks), not `callback2.subscribeds` (B's).
A's list already contains its earlier SUBACK, so the wait returns immediately.
A's PUBLISH can reach the broker before B's SUBSCRIBE. The observed failures
are exactly `AssertionError: 0 != 1 : []`, at lines 692 and 561 respectively.

## Evidence

A transparent MQTT TCP proxy holds B's SUBSCRIBE for 300 ms while allowing
A's separate connection to proceed. It records packet times. No broker or
message semantics are modified. For each broker, each original test fails
with A's PUBLISH preceding B's SUBACK. Changing only the incorrect callback
wait to `callback2.subscribeds` makes the complete test pass, including all
of its existing assertions. This verifies the broker path independently of
the faulty barrier; no production workaround or weakened assertion is used.

The probe and broker fixture are preserved with logs/JSON in
`/home/fabio/keel-benchmark-lab/profiling/offline-fix-20261029/` as
`keel-request-response-probe.py`, `keel-mochi-probe.go`, and
`keel-request-response-probe.json`. To reproduce, compile the Go fixture once
from the upstream checkout and once from Keel's module as
`/tmp/keel-mochi-probe-upstream` and `/tmp/keel-mochi-probe-fixed`, then run
the Python probe. It uses the pinned suite in the current workspace, performs
all eight broker/test/barrier combinations, asserts their outcomes, and
checks packet ordering. Each broker fixture binds only `127.0.0.1:18886`.

## Result

**HARNESS** for the two observed missing-message assertions caused by the
wrong subscription barrier. This classification does not cover other
failures in these multi-assertion tests. The report runner requires the exact
pinned source location and empty-message assertion in the traceback, as well
as this evidence file; missing evidence, missing traceback, or a different
assertion remains a real FAIL. The external suite source remains unchanged.

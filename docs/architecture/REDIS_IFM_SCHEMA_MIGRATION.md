# Redis IFM schema migration

The inflight hash is controlled by `keel:gw:IFM:SCHEMA`.

| marker | read | write |
|---|---|---|
| absent or `1` | numeric legacy fields (`7`) | numeric legacy fields |
| `2` | `in:<id>`/`out:<id>` plus migratable numeric residue | directional fields only |

The marker is never inferred from hash contents and is never downgraded by a
gateway. An absent marker is treated as schema 1 for compatibility with
existing production data.

## Activation procedure

1. Set `keel:gw:IFM:SCHEMA` to `1` explicitly if the marker is absent.
2. Deploy the compatibility release while the marker is `1`. It reads and
   writes only the legacy representation, so it can coexist with old writers.
3. Confirm that no old gateway can still write the shared Redis session store.
4. Build and validate the schema-2-compatible release, obtain its immutable
   registry digest, add the exact full image reference to the embedded
   allowlist in `deploy/k8s/ifm-schema2-rollback-guard.yaml`, and review it as
   a cluster-scoped platform change. Do not approve a tag or an unverified
   digest.
5. Install the Kubernetes admission policy
   `deploy/k8s/ifm-schema2-rollback-guard.yaml`, then label the Keel namespace
   with `keel.io/ifm-schema-required=2`. Verify that an Edge Deployment using
   `0.2.30` is rejected before continuing. The policy is bound only to
   Edge-labelled Deployments, ReplicaSets and Pods; Core is unaffected.
6. Set the marker from `1` to `2` using the explicit activation operation,
   for example a guarded Redis script (never an automatic pod-start action):

   ```sh
   redis-cli EVAL 'if redis.call("GET",KEYS[1]) ~= "1" then return redis.error_reply("IFM schema is not 1") end; return redis.call("SET",KEYS[1],"2")' 1 keel:gw:IFM:SCHEMA
   ```
7. Restart/roll the compatibility release so every process loads schema 2
   immediately. This is still the preferred operational cutover, although
   the compatibility writer also refreshes and fences its mode on each IFM
   mutation, so a running compatibility pod cannot commit a new legacy field
   after activation.
8. Verify that per-client IFM hashes contain only directional fields (apart
   from a short, idempotently migrated residue window).

The Kubernetes label is a deployment barrier, not the Redis authority. Redis
must be changed only after the policy is active and the old Edge count is zero.
The policy rejects both the `0.2.30` tag and the known `0.2.30` image digest,
as well as any Edge Deployment/ReplicaSet/Pod template that does not declare
`keel.io/ifm-schema-max: "2"`. A direct Kubernetes API mutation through those
marked Edge resource paths is therefore covered during normal operation;
deleting or disabling the cluster-scoped policy still requires explicit
administrative privilege.

The capability label is declarative metadata only. The admission policy also
requires an exact immutable image reference present in its embedded approved
digest allowlist. The allowlist is intentionally empty until a validated
schema-2-compatible image is published; this makes schema-2 admission fail
closed rather than trusting a tag or arbitrary label. Each future release
must add its digest to the cluster-scoped policy before deployment.

The Helm chart provides defense in depth: `ifm.schemaRequired: "2"` fails
rendering unless `edge.ifmSchemaMax: "2"` and an immutable image digest are
provided. Helm validation does not replace admission enforcement or the
policy's exact digest allowlist.

Before activation, rollback to `v0.2.30` is supported while the namespace is
not marked as requiring schema 2. After activation, rollback is restricted to
the compatibility release or a later schema-2-capable release; rollback to
`v0.2.30` is unsupported and admission-blocked. The namespace marker and
admission policy must be preserved during GitOps sync and rollback.

The application does not activate schema 2 automatically when a pod starts.
After activation, old numeric-only binaries must not be started: they cannot
represent simultaneous `in:<id>` and `out:<id>` state. A rollback before
activation to the legacy release is supported. After activation, the minimum
rollback version is the compatibility release; rollback to a numeric-only
release is unsupported.

During schema-2 rehydration, a directional field is authoritative over an
equivalent or conflicting numeric field. The numeric field is removed by an
atomic Redis script after the directional field is confirmed or created. A
conflict is counted by `keel_gateway_redis_ifm_schema_conflicts_total` with a
bounded reason label.

## Startup selection and mutation cost

Set `REDIS_IFM_SCHEMA=auto|1|2` (Helm: `core.redis.ifmSchema`).
The default is `auto`; explicit modes validate the marker at startup and
never activate or follow a schema transition. A missing marker means schema 1,
so selecting 2 against an unactivated database fails startup. This setting
applies to the broker hook and the auxiliary offline-session hooks.

Use auto on every writer during activation. Pinned modes reject activation
through that hook and reject inflight mutations after a marker mismatch;
restart with a compatible setting to restore progress. Configuration does not
replace the operator's requirement to remove incompatible old writers.

Inflight saves serialize the stored message once per attempt and use the
locally selected schema as a hint. Saves and completions/drops check the actual
marker atomically in Redis before HSET/HDEL. A successful steady-state mutation
requires one script request, with no separate marker GET from the client.
In auto mode, a rejected fence refreshes the marker and retries once using the
new field format. Redis errors propagate; a mismatched fence changes no fields.
Directional deletion retains the numeric compatibility fallback and preserves
the opposite direction's equal-numbered packet ID.

The marker GET inside Lua is retained in all modes. This reduces network
round trips, not the need for a compatibility fence, and script-internal
commands still appear in Redis command statistics.

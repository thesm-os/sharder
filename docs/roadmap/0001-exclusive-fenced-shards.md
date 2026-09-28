---
milestone: 0001
title: Exclusive fenced shards in one cell
status: Planned
depends-on: none
released-in: unscheduled
deadline: none
deadline-source: none
prd: none
rfc: RFC-0001, RFC-0002, RFC-0003, RFC-0004, RFC-0005, RFC-0006, RFC-0007, RFC-0008, RFC-0009
---

# Milestone 0001: Exclusive fenced shards in one cell

## Goal

An application can embed sharder to spread exclusive shards over its processes, in one cell per namespace, so that its shards move on deploys, failures and rebalances within a disruption budget, and every transaction of a stale writer fails at the application's own fence.

## Done when

### The boundary

- [ ] `builtin.Support` lists `fixed-slots` and `singleton`, `planner`, `primary-only`, `exclusive`, `break-before-make`, `embedded` and `watch`, opens the constraint, goal and hash axes, requires the capability `fenced`, and admits no approvals.
- [ ] `Resolve` rejects every other built-in name on every axis, a strategy that an application registers on a closed axis, a non-empty approvals section, `hold`, `scale`, and a replica count other than one. Each rejection is an error of class `errs.Unsupported` that names the axis and the value, and a table test covers every name.
- [ ] `builtin.Registry` registers `exclusive-worker`, `consumer-group`, `actor-entities` and `leader-election` and no other preset, and each of the four resolves without a problem or a warning.
- [ ] `Change` rejects an edit of every field of the mapping, `hashTags` alone included, with an error of class `errs.Invalid` that names a migration to a new namespace. It allows edits of placement, the budget, the load model and the cell bounds. A table test covers every field of both supported partitioners.
- [ ] `controller.New` rejects `store/memory` without `WithVolatileStore`, and `replica.Start` rejects an `exclusive` namespace without a fence guard, each with an error of class `errs.Invalid`.
- [ ] `examples/` contains one runnable example per supported preset. Each example's handler records its callback keys in the transactions of its storage, and its test replays every callback of a move.
- [ ] A test fails when the README's support table and `builtin.Support` differ.

### Failure traces in the simulator

CI runs each trace over every supported preset from fixed seeds, at `GOMAXPROCS` 1 and 4, and fails when the two event traces differ.

- [ ] A controller crash just before and just after every event of a preset's first move, as `sim.CrashPoints` runs it, resumes the move with the same operation ID, keys and epochs. No handler effect that an example records happens twice.
- [ ] A commit whose reply is lost, before or after it applies, is settled by a retry with the same commit ID or by `Outcome`. No abort path starts from a presumed rejection.
- [ ] An `add` whose reply is lost after it took effect leads to the abort path. The fallback acquires the fence at the reserved abort epoch, every later write of the new node's tenure fails at the fence, and no epoch is granted twice.
- [ ] A callback for a node's earlier incarnation, delivered after the node restarted, is refused before any handler call.
- [ ] A forward callback and an abort callback with the same step index have different keys, and a key that arrives again with another payload is refused.
- [ ] A writer paused beyond its TTL resumes after a new tenure acquired the fence. Its fenced writes fail, and a write transaction that checked the fence before the acquisition makes the acquisition wait until it ends.
- [ ] A controller that fails after it granted a disruption, and before the maintenance started, is replaced by one that keeps the reservation and admits nothing that the reservation would exceed.
- [ ] Two planned disruptions that race an unplanned failure are admitted one at a time, each unavailable replica counts once, and the repair of the failure proceeds.
- [ ] The first move of each supported preset completes under the preset's default budget.
- [ ] Reports that arrive twice and in reverse order return neither the controller's view of a node nor a router's route to an older state.
- [ ] Two namespaces that reuse every node, cell, shard and disruption ID on one controller and one network remain isolated. Each call affects only its own namespace, and a delayed message of a deleted incarnation is refused.
- [ ] An identity that is not an operator changes nothing through `Submit` of any kind, `Disrupt` of another node, or `Done` of another owner's reservation.
- [ ] A `Close` whose context expires while its node still has a replica leaves the reservation in place, and another node's disruption gains no budget from it.
- [ ] `router.Do` does not retry an application write whose reply is lost.
- [ ] During an outage of the cell's controller, of the root's leader and of the store, writers, routers with routes and routers that start during the outage serve as the core's outage table states, under `stop` and under `continue-fenced`.
- [ ] A change of a node's capacity or labels that invalidates the current assignment is repaired without a writer-safety violation, and a drain completes when it makes the balance worse.
- [ ] The simulator checks a predicate for every safety property of the core after every event, and a run whose last quarter has no fault ends converged.

### The etcd adapter

- [ ] `store/storetest` passes on `store/memory` and on the etcd adapter, with commits lost before and after they apply.
- [ ] On a real etcd cluster, a commit whose reply is lost after it applies is settled by a retry or by `Outcome`, with one durable effect.
- [ ] On a real etcd cluster, two controllers that race to acquire one scope end with one acquisition, and the other controller is fenced. The winner reads the scope only after its acquisition applied.
- [ ] On a real etcd cluster, a watch whose history is compacted during a paged snapshot starts a new snapshot, and the reader keeps its last complete state until the new snapshot is complete.
- [ ] `Fits` agrees with the encoded transaction of every supported shape at the cluster's configured limits, and a mutation that does not fit fails before it is sent.

### The transport

- [ ] Over `httptransport` and through `protocol.Local`, a call of another major version, or one that requires an unknown feature, is refused with an error of class `errs.Unsupported` before any effect.
- [ ] Over `httptransport`, a server returns `NotOwnerError` only for a request that it did not start, and `router.Do` returns a transport error without a retry.

### Operations and measurements

- [ ] The operator documentation describes recovery with a new namespace incarnation, over a fresh application-data namespace or behind an administrative barrier, and states that restoring a store from a backup is not supported.
- [ ] The release notes publish the writer gap of planned moves on etcd at p50, p95 and p99, against its target of 2 seconds at p99, and the planner's six benchmark cases at increasing cell sizes.
- [ ] The release notes publish the supported limits of a cell, from the etcd feasibility measurements.

### Dispositions of the review's findings

The design review of 2026-09-28 raised 35 findings against the RFCs. The RFC revision of the same day fixed or clarified each finding in the first release's scope, and moved each other finding out of the scope with a rule that rejects it. The checklist above closes each one:

| Finding | Review | Disposition | Closed by |
|---|---|---|---|
| A lease and a lock delay cannot make a resource without a fence exclusive | OWN-01 | Unfenced exclusive is outside the support matrix, and writer safety is stated precisely | The paused-writer trace, and the rejection of unfenced exclusive |
| An abort can make the old writer write while the new one may still write | OWN-02 | Contract clarified: the four requirements of break-before-make's abort path | The lost-`add`-reply trace |
| A new controller cannot resume a move from the transition's record | OWN-03, STO-02 | Fixed: the record has the operation, the endpoints with their incarnations, the reserved epochs, the protocol version and the cursors | The crash-point trace |
| The majority rule counts learners | OWN-04 | Definition corrected, and consensus models are outside the support matrix | Milestone 0002 |
| A role's bound blocks make-before-break when every slot is full | OWN-05 | The repair rule is fixed, and the temporary allowance is outside the matrix with `secondary-only` | Bound-admission tests, and milestone 0003 |
| The budget's defaults block every primary-only move | OWN-06 | Fixed: a presence-aware budget and planned interruption | The first-move trace of every preset |
| A callback is not bound to its node's incarnation | OWN-07 | Fixed | The restarted-node trace |
| Callback deduplication has no durable contract | OWN-08 | Contract clarified, and verified during implementation | The crash-point and equal-step-index traces, and the examples' replay tests |
| `Token` contradicts `ContinueFenced` | OWN-09 | Fixed: the token table | Token tests under each lease-loss mode, and the outage trace |
| A split reuses a retired shard ID | PLC-01 | Outside the matrix with resharding. The fence key contains the namespace incarnation | A test that recreates a namespace, and milestone 0004 |
| A slot-count change moves keys between cells, and a reshape lacks its inputs | PLC-02, PLC-05 | Outside the matrix, and rejected | Rejection tests, and milestone 0004 |
| A large layout change does not fit one commit | PLC-03 | Outside the matrix with resharding. The initial placement across commits is clarified | An initial placement under a tiny commit limit |
| A spec edit can change the mapping | PLC-04 | Fixed: the mapping is fixed for the namespace incarnation | `Change`'s rejection table, and the hash vectors |
| Constraints cannot read shard labels or moves per node | PLC-06 | Contract clarified | The external constraint in the conformance suite, and the drain-repair trace |
| The planner's bounds and determinism have no contract | PLC-07 | Contract clarified, and verified during implementation | Determinism under shuffled inputs, termination under the work limits, and the benchmarks |
| A commit that returns an error may have applied | STO-01 | Fixed | The lost-commit-reply trace, and the same case on etcd |
| A disruption grant does not survive a failover, and `Close` releases budget early | STO-03, API-08 | Fixed | The grant-then-crash, racing-disruptions and `Close`-timeout traces |
| A cell split has no durable protocol | STO-04 | Outside the matrix, and rejected | Milestone 0005 |
| A new controller cannot form its first conditional commit | STO-05 | Fixed: `Head`, then a conditional acquisition | The acquisition race on etcd |
| Commit limits do not follow from the physical schema | STO-06 | Contract clarified: five transaction shapes, and `Limits` and `Fits` per scope | The transaction-shape check on etcd |
| Snapshots, filtered watches and compaction do not compose | STO-07 | Contract clarified, and verified during implementation | The compaction-during-snapshot case on etcd |
| Calls do not name their namespace | API-01 | Fixed: every message names its scope and its namespace incarnation | The two-namespace trace |
| A computed route cannot know an election's epoch | API-02 | Outside the matrix, and rejected | A rejection test |
| Route updates can regress | API-03 | Contract clarified: a generation per slot | The reversed-report trace |
| Reports have no order | API-04 | Fixed: sequenced full snapshots | The reversed-report trace |
| A rule for the whole namespace lets unsigned kinds through | API-05 | Fixed: authorization per kind | The non-operator trace |
| Signed history is not fresh after a restart | API-06 | Outside the matrix with approvals | Nobody has scheduled approvals |
| Field numbers are not an upgrade protocol | API-07 | Contract clarified, and verified during implementation | The version and feature refusal tests |
| The router can replay a request that ran | API-09 | Fixed, and `hold` is outside the matrix | The lost-write-reply trace |
| The interfaces have no compiling examples | API-10 | Verified during implementation | One example per supported preset |
| Some invariants cannot hold under every fault | VER-01 | Fixed: properties with a stated scope | A predicate per safety property |
| The simulator's execution is not deterministic | VER-02 | Contract clarified, and verified during implementation | Identical traces at `GOMAXPROCS` 1 and 4 |

The review's scenarios for a voter set with learners, for split, merge and layout changes, and for cell transfers go with the milestones that include those features.

## Why now

This milestone is the first code of the library, and every other milestone extends its core: its agent, its controller, its store, its protocol and its budget.

It needs these from outside this repository:

- A core release that contains `fsm`, `task`, `coretest/epochtest`, the fence laws and the `Policy`, `Signature` and `Resolver` of `crypto/sign`, as core does at commit `8edf26a`. The latest release, v0.6.1, predates all five.
- XXH64 in core, which `fixed-slots` hashes with.
- `version.ErrOutcomeUnknown` in core, which the store returns for a commit whose outcome is unknown.
- Binary encoding methods on core's `id.ID`, through which every message encodes its namespace incarnation.
- kanon's generator, and its runtime packages `go.thesmos.sh/kanon` and `go.thesmos.sh/kanon/wire`, which the generated codecs import.

Milestones 0002 to 0005 wait for this one.

## Scope

The scope is the support matrix of the first release and its enforcement: `Resolve`, the built-in presets, the examples and the documentation all read `builtin.Support`. The root module, the etcd adapter, `httptransport` and `sharderctl` are in scope. The RFCs contain the design.

## Not in this milestone

- Delegated ownership, `elect`, consensus models and the voter-aware availability rule: milestone 0002.
- `secondary-only`, `primary-secondary`, replica counts above one, and scaling on load: milestone 0003.
- Resharding, `hash-range` and `key-range`: milestone 0004.
- Namespaces of more than one cell, and cell splits: milestone 0005.
- `explicit`, the function placers and `static`: nobody has scheduled them.
- The `service` topology with `sharderd`, the `leaderless` topology, `lookup` and `compute` routing, and `hold`: nobody has scheduled them.
- `affinity` and `overlapping` ownership, and `make-before-break`, `graceful` and `staged` handoffs: nobody has scheduled them.
- The gRPC transport and approvals: nobody has scheduled them.
- Restoring a store from a backup: not supported, and nobody has scheduled it.

## Risks to the sequence

| Risk | What it delays | What we would do |
|---|---|---|
| Core releases XXH64, `version.ErrOutcomeUnknown` and the encoding of `id.ID` after the library's own work is done | This milestone, and milestones 0002 to 0005 | Develop against core through a replace directive, as the ledger does, and release once a core release contains the three |
| kanon's generator is not released when the library needs its codecs | This milestone, and milestones 0002 to 0005 | Keep the `go:generate` directives, and generate the codecs from kanon's development tree until it releases |
| The etcd feasibility check shows that a supported transaction shape does not fit etcd's default limits | This milestone, and milestones 0002 to 0005 | Split the shape into smaller commits, or publish lower limits for a cell |

## Changes

| Date | What changed | Why |
|---|---|---|
| 2026-09-28 | Created, with the support matrix as its scope and the review's acceptance plan as its exit criteria | The design review of 2026-09-28 found the full design not ready to build at once. The first release supports the subset whose contracts the RFC revision completed |

## References

| What | Where |
|---|---|
| Design review of the sharder RFCs, 2026-09-28: 35 findings and 24 acceptance scenarios | `.scratch/rfc-review.md` and the four reviews beside it, which are not under version control |
| Feature requests to core for XXH64, `version.ErrOutcomeUnknown` and the encoding of `id.ID` | `feat-xxh64-hash.md`, `feat-version-outcome-unknown.md` and `feat-id-binary-encoding.md` at the root of the core repository |
| etcd v3.6 API guarantees, operation completion | https://etcd.io/docs/v3.6/learning/api_guarantees/ |

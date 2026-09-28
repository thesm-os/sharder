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
- [ ] `Resolve` merges a selection of the preset's strategy over the preset's configuration as a JSON Merge Patch, and starts a selection of another strategy from that strategy's defaults. The root stores the expanded result. A table test covers `null`, an array, `[]` and a change of strategy.
- [ ] `Resolve` rejects a store that has leases paired with `membership/static`, and a value below one for `maxTransitions`, `maxNodeTransitions` or `nodeCallbacks`.
- [ ] A live spec edit that selects a factory of the wrong type, or one whose `New` fails, leaves the accepted version unchanged, and `Submit` returns the spec path.
- [ ] A spec that places shards with `match-label` leaves every shard unplaced, with the constraint as the reason, because the supported partitioners label no shard.
- [ ] `controller.New` rejects `store/memory` without `WithVolatileStore`, and a controller without `WithNode`. `replica.Start` rejects an `exclusive` namespace without a fence guard. Each rejection is an error of class `errs.Invalid`.
- [ ] `examples/` contains one runnable example per supported preset. Each example's handler records its callback keys in the transactions of its storage. It reads its shard's data only from a state that contains the acquisition: a new transaction on its authoritative store, or a replica that has applied the acquisition. Its tests replay every callback of a move. They also pause a replica before it applies the old writer's last write, and check that the handler does not read the shard from that replica.
- [ ] A test fails when the README's support table and `builtin.Support` differ.

### The public composition

- [ ] An external-package example that uses only exported API starts two processes against an empty etcd cluster, with different initial specs. Exactly one spec is committed, and both processes resolve it. The example hosts the cell through a root `add`, serves an application shard, replaces the cell's host, replaces the root's leader, and closes every component.
- [ ] A root `add` replayed across lost replies starts one cell controller per term.
- [ ] A router that `router.New` builds refuses a namespace whose recorded mapping differs from its partitioner's, and its `Close` returns after its watches have stopped.

### Failure traces in the simulator

CI runs each trace over every supported preset from fixed seeds, at `GOMAXPROCS` 1 and 4, and fails when the two event traces differ.

- [ ] A controller crash just before and just after every event of each `sim.CrashPoints` scenario of a preset resumes the move with the same operation ID, keys, digests and epochs, on the forward and on the abort path. The agent returns the result that it kept for a resent key, and no handler effect that an example records happens twice.
- [ ] A fallback that restarts during an abort, or whose handler returns an error that is not transient, abandons the abort path. One commit ends the transition, leaves the shard unplaced, and keeps `Reserved` and `Sequence`. The next pass places the shard at epochs above every reserved one, and late callbacks and reports of the abandoned operation do not change the assignment.
- [ ] An abort `drop` to a node that has restarted completes, and repeated restarts of fallbacks end with a serving writer once the faults stop.
- [ ] The first placement of a shard of each preset ends with a serving writer or an unplaced shard, on one and on two live nodes, when its `add` is lost before or after its effect, fails with a permanent error, or is followed by a restart of its node. No epoch is granted twice.
- [ ] A commit whose reply is lost, before or after it applies, is settled by a retry with the same commit ID or by `Outcome`. No abort path starts from a presumed rejection.
- [ ] An `add` whose reply is lost after it took effect leads to the abort path. The fallback acquires the fence at the reserved abort epoch, every later write of the new node's tenure fails at the fence, and no epoch is granted twice.
- [ ] A callback for a node's earlier incarnation, delivered after the node restarted, is refused before any handler call.
- [ ] A forward callback and an abort callback with the same step index have different keys, and a key that arrives again with another payload is refused.
- [ ] A writer paused beyond its TTL resumes after a new tenure acquired the fence. Its fenced writes fail, and a write transaction that checked the fence before the acquisition makes the acquisition wait until it ends.
- [ ] A controller that fails after it granted a disruption, and before the maintenance started, is replaced by one that keeps the reservation and admits nothing that the reservation would exceed.
- [ ] A controller that fails after it deferred a disruption, and before its admission, is replaced by one that continues the drain. The pending reservation ends at the disruption's `End` when the client has stopped retrying, a non-negotiable disruption is admitted at its `Start`, the same ID with another payload is refused, and a restart of the node during the drain ends the reservation.
- [ ] Two planning passes that each fit a move into a node's last free capacity start one transition to it, also with the first source's drop paused, after an abort, and after a controller restart.
- [ ] A permanent fault in one replica on a live node moves that shard to another node, and the node's other shards remain in place.
- [ ] A drain in a cell below the load thresholds completes, also when it needs more than one pass. A node with 200 replicas drains with at most `maxNodeTransitions` of its transitions in flight, and at most `maxTransitions` transitions of the cell are in flight at once.
- [ ] A terminal abort under `maxTransitions` of 1, followed by a move of another shard, reports one transition in flight and two shards without a serving writer, as two separate counts.
- [ ] Repair with `maxSourceShards` of 1 moves shard 1 and then shard 0, where shard 0 can move only to the node that shard 1 fills. Repair behind a shard that can never be placed still examines the other shards, and completes a drain. Each pass replays exactly from its recorded input, at `GOMAXPROCS` 1 and 4.
- [ ] Two planned disruptions that race an unplanned failure are admitted one at a time, each unavailable replica counts once, and the repair of the failure proceeds.
- [ ] The first move of each supported preset completes under the preset's default budget.
- [ ] Reports that arrive twice and in reverse order return neither the controller's view of a node nor a router's route to an older state.
- [ ] Two namespaces that reuse every node, cell, shard and disruption ID on one controller and one network remain isolated. Each call affects only its own namespace, and a delayed message of a deleted incarnation is refused.
- [ ] An identity that is not an operator changes nothing through `Submit` of any kind, `Disrupt` of another node, or `Done` of another owner's reservation.
- [ ] A `Close` whose context expires while its node still has a replica leaves the reservation in place, and another node's disruption gains no budget from it.
- [ ] `router.Do` does not retry an application write whose reply is lost.
- [ ] During an outage of the cell's controller, of the root's leader and of the store, writers, routers with routes and routers that start during the outage serve as the core's outage table states, under `stop` and under `continue-fenced`. A callback that the controller sent before a store outage acquires when its node has installed the callback's placement by the time the callback starts the tenure, including a placement that an event buffered before the outage installs during it.
- [ ] A change of a node's capacity or labels that invalidates the current assignment is repaired without a writer-safety violation, and a drain completes when it makes the balance worse.
- [ ] A constraint that a spec edit adds while a transition is in flight does not stop the transition. A replica that the transition places against the constraint is reported and repaired.
- [ ] A controller of an older release refuses a scope with a newer `Head.Schema` before any commit, also when the schema rises between its head read and its acquisition.
- [ ] An unknown commit followed by a takeover leaves `Outcome` without a receipt, and no component reports the commit as rejected. `sharderctl` reports a proposal's outcome from `Applied`.
- [ ] A read with pages, a watch and a membership watch that resume across a recreation of the namespace under the same name, at an equal generation, return `shard.ErrIncarnation`. A delayed `Join` of the old incarnation changes nothing in the new one.
- [ ] A callback of a new controller that arrives before the agent's watch delivers the controller's record returns `errs.Transient` and succeeds on a retry. A caller that claims a higher controller epoch without a matching record does not change the agent's state.
- [ ] A removal that the watch delivers after a newer `add` on the same node incarnation leaves the newer tenure active. Within one transition, a move away from A removes A's tenure at e and the abort path restores A at e+2, and the delayed removal of the tenure at e has no effect. A cleanup still stops a replica that a late callback started after its operation ended.
- [ ] A watch that compaction or a full buffer breaks after a tenure has started, and past the commit that removes the tenure, ends in a replacement snapshot that omits it, and the agent revokes and drops the tenure. A cleanup that fails transiently on the removal event, continues after the snapshot and repeats after its removal arrives again performs its transactional effect once, under the key that both paths build.
- [ ] An obsolete `add`, delivered before, during and after the installation of that snapshot without a controller takeover, either never starts its tenure or starts it from the old state and is dropped at the snapshot's comparison. When the gap covers both the commit that placed the tenure and the commit that removed it, the `add` never starts. An `add` whose commit follows the start of the snapshot's read starts once the watch delivers that commit, and the snapshot does not stop it.
- [ ] An `add` that passes its last listing check while the agent installs a replacement snapshot is either in the snapshot's comparison as a starting tenure, or checks the installed snapshot before it starts. No tenure starts without a comparison that covers it.
- [ ] When a namespace is recreated at a lower and at an equal generation, while an old redirect and an old `Refresh` are outstanding, the router routes with the new mapping and caches. No `Refresh` waits for a generation of the old incarnation. A server of the new incarnation refuses a request of the old one before the request starts. `router.Do` retries no application write after any other error.
- [ ] A manual drain and two disruptions that drain the same node keep the node draining until the last of the three ends, in every order of expiry, cancellation and undrain, with a controller takeover between the steps. `undrain` removes only the manual drain.
- [ ] Under `stop`, a node that keeps sending renewals while every reply is lost ends its session at the deadline of its last accepted renewal, and a late success reply neither extends nor reopens the session.
- [ ] The simulator checks a predicate for every safety property of the core after every event. It checks convergence within the bound that it derives from the spec, from the moment the last fault has ended. A pause that lasts into the last quarter of a run does not cause a false convergence failure. When a pause ends, the process handles the timers that expired during the pause.

### The etcd adapter

- [ ] `store/storetest` passes on `store/memory` and on the etcd adapter, with commits lost before and after they apply, the order of `Commit`'s checks, and a scope deleted and created again.
- [ ] The feasibility check documents how the adapter maps a scope's generation to etcd revisions, and a fresh adapter resumes a watch at the right revision while other scopes commit.
- [ ] `Fits` computes the deletion of every old per-node index key from the mutation alone, for moves, aborts and removals.
- [ ] The state that `Read` returns for the largest supported cell, with its transitions, reservations, drains and applied proposals, fits the supported limits.
- [ ] The adapter's membership sessions renew at the spec's interval and expire at the spec's TTL.
- [ ] On a real etcd cluster, a commit whose reply is lost after it applies is settled by a retry or by `Outcome`, with one durable effect.
- [ ] On a real etcd cluster, two controllers that race to acquire one scope end with one acquisition, and the other controller is fenced. The winner reads the scope only after its acquisition applied.
- [ ] On a real etcd cluster, a watch whose history is compacted during a paged snapshot starts a new snapshot, and the reader keeps its last complete state until the new snapshot is complete. An agent whose watch is compacted past the removal of one of its tenures drops the tenure after the new snapshot.
- [ ] On a real etcd cluster, a node-filtered watch lists in `Event.Removed` each shard whose placement stops naming the node, also after compaction has discarded the previous value of the node's index key.
- [ ] `Fits` agrees with the encoded transaction of every supported shape at the cluster's configured limits, and a mutation that does not fit fails before it is sent.

### The transport

- [ ] Over `httptransport` and through `protocol.Local`, a call of another major version, or one that requires an unknown feature, is refused with an error of class `errs.Unsupported` before any effect.
- [ ] Over `httptransport`, a server returns `NotOwnerError` only for a request that it did not start, and `router.Do` returns a transport error without a retry.
- [ ] `httptransport`'s framing is specified and tested: the endpoint of each call, the encoding of its message and of `CallError`, the metadata headers, the largest message that a server accepts, and cancellation through the request's context. Each supported call round-trips through its encoding.
- [ ] The report of a node with the most replicas that the supported limits allow fits in one message within the transport's largest message.

### Operations and measurements

- [ ] The operator documentation describes recovery with a new namespace incarnation, over a fresh application-data namespace or behind an administrative barrier, and states that restoring a store from a backup is not supported.
- [ ] The operator documentation lists the store credentials of every process role, and states that the processes of a namespace that can run a controller trust each other.
- [ ] The release notes publish the writer gap of planned moves on etcd at p50, p95 and p99, against its target of 2 seconds at p99. They publish the delay of the watch delivery that each `add` waits for at the same percentiles, and the planner's six benchmark cases at increasing cell sizes.
- [ ] The release notes publish the supported limits of a cell, from the etcd feasibility measurements.
- [ ] The feasibility workload and the published limits count the root's membership sessions, election candidates and watches as well as the cells' sessions, and state any lease that sessions share.

### Dispositions of the first review's findings

The first design review of 2026-09-28 raised 35 findings against the RFCs. The RFC revision of the same day fixed or clarified each finding in the first release's scope, and moved each other finding out of the scope with a rule that rejects it. The checklist above specifies the acceptance coverage of each one:

| Finding | Review | Disposition | Covered by |
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

### Dispositions of the second review's findings

The second design review of 2026-09-28 raised 21 findings against the revised RFCs, and checking them found 2 more. The second RFC revision of the same day fixed or clarified each one. The checklist above specifies the acceptance coverage of each one:

| Finding | Review | Disposition | Covered by |
|---|---|---|---|
| A new controller's resent callback has another digest, so the agent refuses its key | O-01 | Fixed: the digest excludes the controller epoch, and a request has no generation | The crash-point scenarios |
| An abort cannot finish when its fallback's incarnation ends | O-02 | Fixed: a definite failure abandons the abort path, and the shard is placed again at fresh epochs | The fallback-restart trace |
| Acquiring first does not make a lagging replica current | O-03 | Contract clarified: the first read sees the acquisition | The examples' reads |
| A shard's first placement has no abort destination | O-04 | Fixed: an abort without a fallback ends with no assigned writer | The first-placement trace |
| Capacity admission ignores transitions in flight | P-01 | Fixed: a moving shard's load counts once on its destination and its fallback | The two-pass capacity trace |
| The planner cannot see a faulted replica on a live node | P-02 | Fixed: `Input.Faults` | The live-node fault trace |
| The load thresholds suppress repair | P-03 | Fixed: repair runs whatever the thresholds report, outside the churn budget | The idle-drain trace |
| Stickiness has no baseline | P-04 | Fixed: `State.Initial` | The external stickiness goal in the conformance suite |
| A live edit can select a factory that does not build | P-05 | Fixed: the root builds every strategy before it accepts a spec | The wrong-factory test |
| A store with leases hides a static membership | P-06 | Fixed: capabilities per adapter role | The leased-store rejection |
| Fused floating-point operations break determinism across machines | P-07 | Contract narrowed to the same binary | Identical traces at `GOMAXPROCS` 1 and 4 |
| The public API cannot host the root and a cell | S-01 | Fixed: `WithNode`, the controller's `AgentServer`, `Dispatch`, initial specs and `router.New` | The two-process example |
| A deferred drain loses its disruption on failover | S-02 | Fixed: pending reservations | The crash-before-admission trace |
| A controller refuses a newer schema only after it has acquired the scope | S-03 | Fixed: the schema check precedes the acquisition, and a protocol version raises the schema | The newer-schema trace |
| A takeover deletes the evidence of an old outcome | S-04 | Contract clarified: a barrier prevents future application only, and `sharderctl` reads `Applied` | The unknown-commit takeover trace |
| A resumed read or watch does not name its incarnation | S-05 | Fixed: reads, page tokens, watches and membership calls are bound to an incarnation | The recreation trace, and `store/storetest` |
| The outage table claims that no newer tenure acquires during a store outage | A-01 | Corrected | The outage trace |
| A simulated pause stops the process's clock | A-02 | Fixed: the clock advances during a pause | The simulator check |
| Crash points and the convergence check overstate coverage | A-03 | Fixed: crash scenarios, and a convergence bound that starts when the last fault ends | The crash-point scenarios and the simulator check |
| A lagging watch denies a valid new controller | A-04 | Fixed: `errs.Transient` for an epoch above the agent's record | The lagging-watch trace |
| The security motivation overstates what the checks protect | A-05 | Contract clarified: the processes of a namespace trust each other | The operator documentation |
| `Commit` lists its rejections without an order | Checking the review | Fixed: incarnation, fence, receipt, generation | `store/storetest` |
| A drain moves 2 replicas per node per pass | Checking the review | Fixed: repair is outside the churn budget, and the budget bounds transitions in flight | The idle-drain trace |

### Dispositions of the third review's findings

The third design review of 2026-09-28 found no blocker and raised 7 findings against the second revision. The third RFC revision of the same day fixed or clarified each one. The checklist above specifies the acceptance coverage of each one:

| Finding | Review | Disposition | Covered by |
|---|---|---|---|
| Bounded repair can examine the same infeasible shards in every pass | P3-01 | Fixed: a repair cursor in each pass's recorded input, with an eventual fairness guarantee | The one-source repair trace |
| A cleanup from the watch has no request identity, and a delayed removal can stop a newer tenure | A3-01 | Fixed: a local `Cleanup` path whose key contains the removed tenure's epoch, run only while that tenure is active | The delayed-removal trace |
| A redirect's generation does not name its incarnation | A3-02 | Fixed: `NotOwnerError` and `Refresh` name the incarnation, and the `Watcher` rediscovers the namespace | The recreation-redirect trace |
| All drains of a node share one node-only removal | S3-01 | Fixed: drains keyed by node and disruption, with one shared manual drain | The overlapping-drain trace |
| A failed renewal can move the lease deadline | S3-03 | Contract clarified: only an accepted renewal moves the deadline | The lost-renewal trace |
| The transition bound is described as a bound on writer gaps | P3-02 | Corrected: the settings bound transitions in flight | The terminal-abort count trace |
| Root sizing leaves out the processes that run a controller | S3-02 | Corrected: one root session and one candidate per process that runs a controller | The feasibility workload |

### Disposition of the verification's finding

The verification of the third revision, on 2026-09-28, closed six of the third review's findings, and found the cleanup incomplete after a replacement snapshot. The fourth RFC revision of the same day fixed it. The checklist above specifies its acceptance coverage:

| Finding | Review | Disposition | Covered by |
|---|---|---|---|
| A replacement snapshot reports no removal, and a delayed `add` can start a tenure that no later event removes | C4-01 | Fixed: an agent starts a tenure only while its watched placements list it, compares each replacement snapshot with its tenures, and builds a cleanup's key from the callback that started the tenure | The compacted-watch and obsolete-`add` traces, and the removal checks on etcd |

## Why now

This milestone is the first code of the library, and every other milestone extends its core: its agent, its controller, its store, its protocol and its budget.

It needs these from outside this repository:

- A core release that contains `fsm`, `task`, `coretest/epochtest`, the fence laws and the `Policy`, `Signature` and `Resolver` of `crypto/sign`, as core does at commit `8edf26a`. The latest release, v0.6.1, predates all five.
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
| Core releases `version.ErrOutcomeUnknown` and the encoding of `id.ID` after the library's own work is done | This milestone, and milestones 0002 to 0005 | Develop against core through a replace directive, as the ledger does, and release once a core release contains both |
| kanon's generator is not released when the library needs its codecs | This milestone, and milestones 0002 to 0005 | Keep the `go:generate` directives, and generate the codecs from kanon's development tree until it releases |
| The etcd feasibility check shows that a supported transaction shape does not fit etcd's default limits | This milestone, and milestones 0002 to 0005 | Split the shape into smaller commits, or publish lower limits for a cell |

## Changes

| Date | What changed | Why |
|---|---|---|
| 2026-09-28 | Checked removed shards on etcd in place of removal epochs, measured the watch delivery that each `add` waits for, and added a check of an `add` that passes its listing check during a snapshot's installation | The closure review of the fourth revision simplified `Event.Removed` to shard IDs and asked for both checks |
| 2026-09-28 | Added the acceptance criteria and the disposition of the verification's finding | The verification of the third revision found that cleanup did not survive a replacement snapshot, and the fourth revision closed it |
| 2026-09-28 | Added the acceptance criteria and the dispositions of the third design review, and described the disposition tables as acceptance coverage | The third design review of 2026-09-28 found 7 contract gaps in the second revision, and the third revision closed each one |
| 2026-09-28 | Added the acceptance criteria and the dispositions of the second design review | The second design review of 2026-09-28 found 21 contract gaps in the revised RFCs, and the second revision closed each one |
| 2026-09-28 | Dropped XXH64 in core from the prerequisites | sharder takes XXH64 from `github.com/cespare/xxhash/v2`, which exists today |
| 2026-09-28 | Created, with the support matrix as its scope and the review's acceptance plan as its exit criteria | The design review of 2026-09-28 found the full design not ready to build at once. The first release supports the subset whose contracts the RFC revision completed |

## References

| What | Where |
|---|---|
| First design review of the sharder RFCs, 2026-09-28: 35 findings and 24 acceptance scenarios | `.scratch/rfc-review.md` and the four reviews beside it, which are not under version control |
| Second design review of the sharder RFCs, 2026-09-28: 21 findings | `.scratch/review-2026-09-28-r2/review.md` and the four reviews beside it, which are not under version control |
| Third design review of the sharder RFCs, 2026-09-28: 7 findings | `.scratch/review-2026-09-28-r3/review.md` and the four reviews beside it, which are not under version control |
| Verification of the third revision, 2026-09-28: 1 finding | `.scratch/review-2026-09-28-r4/review.md` and the three reviews beside it, which are not under version control |
| Closure review of the fourth revision, 2026-09-28: no finding | `.scratch/review-2026-09-28-r5/review.md`, which is not under version control |
| Feature requests to core for `version.ErrOutcomeUnknown` and the encoding of `id.ID` | `feat-version-outcome-unknown.md` and `feat-id-binary-encoding.md` at the root of the core repository |
| etcd v3.6 API guarantees, operation completion | https://etcd.io/docs/v3.6/learning/api_guarantees/ |

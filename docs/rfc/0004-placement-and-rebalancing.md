---
rfc: 0004
title: Placement and rebalancing strategies
author: Roy Klopper
status: Draft
created: 2026-09-27
updated: 2026-09-28
discussion: none
supersedes: none
superseded-by: none
produces-adr: tbd
research: Research-0001, Research-0002, Research-0003
related: RFC-0001, RFC-0002, RFC-0003, RFC-0005, RFC-0006, RFC-0009
---

# RFC-0004: Placement and rebalancing strategies

## Summary

A placer selects the nodes that host each shard's replicas. We propose two kinds of placer and one adapter:

- The `planner` computes placements in a controller from the current assignment, load and constraints. Hard constraints reject moves, and weighted goals in priority tiers score them. A local search applies the best moves within limits on its work. Applications register their own constraints and goals.
- A function placer computes each shard's nodes from the layout and the node list alone, so every node computes the same placement without a coordinator. The built-ins are `rendezvous`, `consistent-hash`, `bounded-load`, `jump` and `maglev`.
- The `static` adapter places shards where an operator lists them.

The support matrix contains the planner. The function placers and `static` are designs outside it.

A planning pass is a function of its input: the same binary gives the same plan for the same input at every degree of parallelism, unless the pass's timeout ends it. The core checks every plan against the constraints and the state model before it commits anything. A plan from a faulty placer that violates either is rejected. A placer places the shards of one cell on the nodes of that cell, so cells place in parallel, each in its own controller.

## Motivation

Placement needs differ more than any other axis across the use cases this library targets:

- Within SM alone, 55% of applications balance on shard count and 20% on one metric. The applications that balance on more than one metric use 65% of the servers, and a third of the servers of geo-distributed deployments belong to applications with regional placement preferences.
- SM's allocator started as hand-written heuristics that became "complex, brittle, and hard to extend". A rewrite as simpler heuristics was abandoned halfway. The constraint solver that replaced them cut the allocator to about 20% of its lines.
- Solvers have limits. SM reports that MIP solves millions of assignment variables in tens of minutes when SM needs billions in tens of seconds, so SM uses local search. Service Fabric tried linear and integer programming and genetic algorithms, found them too slow or too far from optimal, and uses simulated annealing.
- Hashing alone balances counts, not load. A measurement of Facebook's TAO cache traffic found that standard consistent hashing put 53% more load than the average on the busiest servers.
- Some applications run without a controller: a client that spreads keys over cache nodes computes owners from the node list with a hash function.
- Frameworks that place work already make the strategy pluggable, and each provides more than one. The Kubernetes scheduler composes filter and score plugins, and multiplies each plugin's 0-to-100 score by its configured weight. Kafka offers range, round-robin, sticky and cooperative-sticky assignors on the client, and uniform and range assignors on the broker. Akka and Pekko cluster sharding take a shard allocation strategy and check for rebalancing every 10 seconds. Orleans has seven placement strategies and changed its default from random to resource-optimised placement in version 9.2.

One placer cannot serve these needs. A generic library needs cheap hash functions for leaderless use, and an extensible optimiser for everything else, behind one interface that the core can check.

## Detailed design

### Interfaces

```go
// Package placement decides which nodes host the replicas of each shard.
package placement

import (
 "context"

 "go.thesmos.sh/core/epoch"
 "go.thesmos.sh/core/rand"
 "go.thesmos.sh/sharder/partition"
 "go.thesmos.sh/sharder/replication"
 "go.thesmos.sh/sharder/shard"
)

// Input is everything a placer may read about one cell. A placer must not modify it.
type Input struct {
 // Snapshot names the state that the pass reads.
 Snapshot Snapshot
 Cell     shard.CellID
 Layout   partition.Layout
 Current  shard.Assignment
 // Transitions maps each shard with a transition in flight to the nodes on which the
 // transition can place the shard's replica. The planner plans no other move for
 // such a shard.
 Transitions map[shard.ID]InFlight
 // Faults maps a shard to the nodes whose replica of the shard has a permanent fault,
 // as the nodes' live incarnations reported it. The planner treats each such replica
 // as missing, and does not place a replica of the shard on those nodes.
 Faults map[shard.ID][]shard.NodeID
 // RepairFrom is the repair cursor: a pass examines the shards that need repair in ID
 // order from the first one at or above RepairFrom, and wraps around after the
 // highest ID. It is empty in a controller's first pass.
 RepairFrom shard.ID
 // Nodes lists the cell's live nodes that accept new replicas.
 Nodes []shard.Node
 // Draining lists live nodes whose replicas must move off. They do not accept new replicas.
 Draining map[shard.NodeID]bool
 // ShardLoad is each shard's load over the namespace's load window.
 ShardLoad map[shard.ID]shard.Load
 // Replicas is the desired replica count of each shard.
 Replicas map[shard.ID]int
 Model    replication.Model
 // Emergency is true when shards are under-replicated after failures, or have a
 // replica that Faults lists. A placer must then restore replica counts first, and
 // may leave soft goals unimproved.
 Emergency bool
 // Rand is the only source of randomness a placer may use. The core seeds it for
 // each pass from Snapshot, so a pass can be replayed.
 Rand rand.Rand
}

// Snapshot identifies the state that one planning pass reads. The controller logs it
// with each plan. The simulator records it with the pass's whole Input, and replays a
// recorded pass from them.
type Snapshot struct {
 // Term is the epoch of the controller that runs the pass.
 Term epoch.Epoch
 // SpecVersion is the version of the spec that the pass applies.
 SpecVersion epoch.Epoch
 // Generation is the cell's committed generation that Current is at.
 Generation epoch.Epoch
 // Membership is the revision of the membership view that Nodes lists.
 Membership epoch.Epoch
 // Reports counts the agents' reports that the controller accepted in its term
 // before the pass, and so identifies the loads in ShardLoad and the faults in
 // Faults.
 Reports uint64
}

// InFlight is the part of a transition in flight that placement accounts for: the
// nodes on which it can place the shard's replica. Fallback is empty when the
// transition has none.
type InFlight struct {
 To       shard.NodeID
 Fallback shard.NodeID
}

// Plan is a placer's proposal for the next assignment.
type Plan struct {
 // Targets maps every shard of the layout to its target.
 Targets map[shard.ID]Target
 // Cost is the value of the placer's objective for Targets. Lower is better.
 Cost float64
 // Incomplete reports that the pass ended on its timeout or on a work limit before
 // its search finished. The core validates and commits an incomplete plan like any
 // other, and never reports it as converged.
 Incomplete bool
 // RepairNext is the repair cursor for the next pass: the ID of the shard that
 // follows, in the rotation, the last shard that the pass examined for repair,
 // whether or not the pass found a move for it. It equals Input.RepairFrom when the
 // pass examined no shard for repair.
 RepairNext shard.ID
}

// Target is a placer's proposal for one shard. The shard's desired count is
// len(Replicas) plus Deficit.
type Target struct {
 // Replicas lists the desired replicas that have a feasible node. The core assigns
 // epochs.
 Replicas []shard.Replica
 // Deficit counts the desired replicas without a feasible node. The core keeps a
 // shard's current replicas that the target does not move, so a target with a
 // deficit repairs what it can and leaves the rest in place.
 Deficit int
 // Reason explains a deficit, such as the constraint that rejected every node.
 Reason error
}

// Placer proposes an assignment. Given the same Input, the same binary returns the
// same Plan, unless its timeout ends the pass.
type Placer interface {
 Place(ctx context.Context, in Input) (Plan, error)
}

// Function is a placer whose plan depends only on the layout, the node list and the
// replica counts. Every node computes the same plan, so no coordinator is needed.
type Function interface {
 Placer
 // Assign returns the nodes of every shard's replicas, most preferred first.
 // The first node takes the model's writer role, if the model has one.
 Assign(layout partition.Layout, nodes []shard.Node, replicas map[shard.ID]int) map[shard.ID][]shard.NodeID
}
```

A target states the repair it can make. Suppose a shard of three replicas has lost two nodes, and one spare node is feasible. Its target lists the surviving replica and the spare, with a deficit of one. The core adds the spare and keeps the survivor, and the shard has two serving replicas instead of one.

### The planner

The planner has three parts, each configured in the spec: hard constraints, weighted goals in tiers, and a search.

```go
package placement

import (
 "go.thesmos.sh/sharder/partition"
 "go.thesmos.sh/sharder/shard"
)

// Move adds, removes, relocates or re-roles one replica.
type Move struct {
 Shard shard.ID
 From  shard.NodeID // empty when the move adds a replica
 To    shard.NodeID // empty when the move removes a replica
 Role  shard.Role
}

// State is the planner's working copy of the assignment, indexed for goals and
// constraints. Only the planner applies moves to it, on one goroutine, between
// evaluations. Constraints and goals read it from as many goroutines as the planner
// evaluates in parallel, and never modify it. Slices that it returns are sorted by ID,
// and the caller must not modify them.
type State interface {
 // Shard returns the layout's descriptor of shard id, with its range and labels.
 Shard(id shard.ID) (partition.Shard, bool)
 Replicas(id shard.ID) []shard.Replica
 // Initial returns the replicas of shard id at the start of the pass, as
 // Input.Current lists them.
 Initial(id shard.ID) []shard.Replica
 OnNode(n shard.NodeID) []shard.ID
 Node(n shard.NodeID) (shard.Node, bool)
 Nodes() []shard.NodeID
 ShardLoad(id shard.ID) shard.Load
 // NodeLoad returns the load on node n: the load of each shard that has a replica on
 // n, or a transition in flight whose destination or fallback is n, counted once per
 // shard.
 NodeLoad(n shard.NodeID) shard.Load
 // Moved returns the number of moves applied since the pass started.
 Moved() int
 // MovedNode returns the number of moves applied since the pass started that add a
 // replica to n or remove one from it.
 MovedNode(n shard.NodeID) int
 // MovedShard returns the number of moves applied since the pass started to the
 // replicas of shard id.
 MovedShard(id shard.ID) int
}

// Constraint is a hard rule. The planner never applies a move that a constraint rejects,
// and the core rejects a plan that adds or moves a replica that a constraint rejects.
type Constraint interface {
 Name() string
 // Check returns nil when st may apply m, and otherwise an error that explains why not.
 Check(st State, m Move) error
}

// Goal is a soft objective with a weight and a tier from the spec. The planner minimises
// the weighted sum of goal costs within each tier, one tier at a time, in ascending tier
// order.
type Goal interface {
 Name() string
 Cost(st State) float64
 // Delta returns the cost after m minus the cost before m, without applying m.
 Delta(st State, m Move) float64
}
```

`Delta` exists so that the planner scores a candidate move by touching only the nodes and shards the move affects. SM's solver evaluates moves the same way, updating only the parts of its objective that a move can change.

The built-in constraints and goals use only the exported methods of `State`, so a constraint that an application registers from its own package can do everything a built-in does. The conformance suite contains an external package that implements `match-label` and `capacity` again, and checks that they reject the same moves as the built-ins. It also implements `stickiness` again, and checks for moves away, back and between two new nodes that `Delta` equals the change of `Cost`.

#### Built-in constraints

| Constraint | Rejects a move that | Configuration |
|---|---|---|
| `capacity` | Puts a node above its capacity on any metric | `headroom`: fraction of capacity kept free, default 0 |
| `labels` | Places a replica on a node whose labels fail the rule | Rules with the operators `in`, `notIn`, `exists` and `notExists`, per role |
| `isolation` | Puts more than `max` replicas of one shard under one value of a label | `label`, such as `zone`; `max`, default 1 |
| `drain` | Adds a replica to a draining node | none |
| `max-per-node` | Gives a node more than `max` replicas of any shard, or of one role | `max`, `role` |
| `match-label` | Places a replica on a node whose value of a label differs from the shard's value of the same label, or places any replica of a shard without the label | `label`, such as `jurisdiction` |

`match-label` keeps the shards of a regulated tenant in the tenant's jurisdiction when the partitioner labels each shard with it. A shard without the label has no feasible node, so its target reports a deficit, with the constraint as the reason. The partitioners of the support matrix label no shard. A rule that no built-in covers is a Go factory of `Constraint` that the application registers, and the spec names it in the planner's configuration.

#### Built-in goals

| Goal | Cost it measures | Configuration |
|---|---|---|
| `count` | Variance of replica count per node, weighted by role | `roleWeights`, such as `{"primary": 2, "secondary": 1}` |
| `balance` | Variance of one metric's utilisation across nodes | `metric` |
| `utilisation` | Sum of utilisation above a threshold | `metric`, `above`: 0.9 |
| `spread` | Replicas of one shard that share a value of a label | `label`, such as `zone` or `region` |
| `prefer` | Replicas outside the label value that a shard prefers. A shard without the label costs nothing | `label`, `from`: the shard label that names the preferred value |
| `stickiness` | Replicas that differ from the assignment at the start of the pass, which `State.Initial` returns | none |
| `colocate` | Shards of a group that are not on the same nodes. A shard without the label is in no group | `group`: the shard label that names the group |

A goal list for a replicated store that prefers each shard's home region:

```json
{
  "goals": [
    {"name": "spread", "tier": 0, "weight": 1, "config": {"label": "zone"}},
    {"name": "prefer", "tier": 0, "weight": 2, "config": {"label": "region", "from": "home"}},
    {"name": "utilisation", "tier": 1, "weight": 4, "config": {"metric": "cpu", "above": 0.9}},
    {"name": "balance", "tier": 2, "weight": 1, "config": {"metric": "cpu"}},
    {"name": "balance", "tier": 2, "weight": 0.5, "config": {"metric": "bytes"}},
    {"name": "stickiness", "tier": 2, "weight": 0.25}
  ]
}
```

Tiers encode priority. The planner never accepts a move that worsens the cost of a lower tier to improve a higher one. SM orders its goals the same way: region preference, replica spread, planned maintenance, utilisation threshold, then global and regional balance.

Kafka's assignors are planner configurations, not placers of their own. `count` alone spreads partitions as round-robin does, and `count` with `stickiness` keeps them in place as the sticky assignors do. No built-in goal keeps a member's partitions contiguous, as the range assignor does, and an application that needs that registers a goal.

#### Repair

A constraint that the spec adds, or a node whose capacity or labels change, can make the current assignment invalid before the planner runs. The planner and the core treat that assignment as a starting point to repair, not as a plan to reject:

- The core checks a replica that the current assignment has and a plan keeps as the move that would add it. A replica that fails is a violation. The core reports it and does not reject the plan for it.
- The planner plans repair before every goal tier. A replica on a failed node, on a draining node, with a permanent fault or in violation of a constraint moves to the best feasible node, even when the move raises a goal's cost. A drain therefore completes when it makes the balance worse for a while.
- A repair move must pass every constraint. A replica without a feasible node remains where it is, and its shard's target reports the deficit.
- Repair runs at every interval of `triggers` while repair work exists, and after every change that creates repair work, whatever the load thresholds of `triggers` report. A drain, a changed constraint or an unplaced shard in an idle cell is therefore repaired.
- The search's work limits bound every repair pass. The churn budget does not, because it limits improvement only. The engine starts the repair moves within the budget's bounds on transitions in flight.
- A repair cursor spreads the examination over passes. A pass examines the shards that need repair in ID order, starting at the first one whose ID is at or above `Input.RepairFrom`, so a cursor whose shard no longer needs repair starts the pass at the next ID above it. After the highest ID, the pass continues from the lowest, and it examines each shard at most once. It advances the cursor past every shard that it examined, whether it found a move, found none, or the engine admitted none, and returns the cursor in `Plan.RepairNext`. The controller passes the cursor to its next pass, whose recorded input contains it, and a new controller starts with an empty cursor.
- With a stable set of R shards that need repair, and when no other limit ends a pass first, every shard of the set is examined within ⌈R / `maxSourceShards`⌉ passes. `maxMoves`, `maxEvaluations` and the timeout can each end a pass first, so the general guarantee is eventual: once one controller keeps the cell, and each pass examines at least one shard, every shard that needs repair is examined within a finite number of passes, and again after each full rotation of the cursor. Fair examination does not guarantee that the heuristic finds every feasible assignment.

#### Search

```json
{
  "search": {
    "algorithm": "local",
    "maxMoves": 500,
    "maxEvaluations": 200000,
    "maxSourceShards": 5000,
    "sample": 16,
    "timeout": "60s"
  },
  "churn": {"maxShardFraction": 0.09, "perNode": 2, "perShard": 1},
  "triggers": {"interval": "30s", "minImbalance": 1.1, "activeAbove": 0.25, "onMembership": true}
}
```

The planner runs in one of two modes, as SM's allocator does:

- **Emergency.** When shards are under-replicated after node failures, or when `Input.Faults` lists a replica with a permanent fault outside the `delegated` mode, the planner places each missing or faulted replica on the best feasible node: every constraint filters the nodes, and the goals score the rest. It never chooses a node on which the shard's replica has the fault. It spreads the replicas of a failed node over many nodes, so that recovery work runs in parallel. It does not move healthy replicas, and a fault in one replica of a node leaves the node's other replicas in place.
- **Periodic.** At each interval, the planner repairs, then improves the assignment tier by tier, unless the load thresholds of `triggers` skip the improvement. For each tier, it takes the nodes with the highest cost and evaluates moving each of their shards to a sample of target nodes. It applies the best move that improves the tier's cost and passes the constraints. It evaluates larger shards first, because one large move fixes more than many small ones. SM's solver samples targets across groups of nodes and tries large shards first for the same reasons.

The `algorithm` field selects the search:

- `greedy` applies the best move per shard once.
- `local`, the default, repeats improving moves until no move improves the cost or a limit ends the pass.
- `anneal` accepts a worsening move within a tier with a probability that falls over the pass, as Service Fabric does. It never accepts a move that worsens a lower tier.

A pass ends at the first of four limits:

- `maxMoves` accepted moves.
- `maxEvaluations` evaluated candidate moves.
- `maxSourceShards` shards examined as the source of a move.
- `timeout`.

The first three bound a pass's search whatever the size of the cell, including a pass that finds no improving move: it stops after `maxSourceShards` shards and `maxEvaluations` evaluations. The planner builds its `State` from the `Input` once per pass, which costs time proportional to the shards and nodes of the cell. It then maintains its index of the nodes' costs with each applied move, so finding the costliest nodes does not scan the cell again. A pass that the timeout or a work limit ends returns its best plan with `Incomplete` set, and the core reports the cell as not converged.

The planner samples `sample` target nodes per candidate move, not every node. Mitzenmacher's analysis of randomised load balancing supports the choice: a system in which each item chooses among a small number of random candidates "can perform almost as well as a perfect load balancing system with global load knowledge". Orleans's activation-count placement samples two silos by default, and the Kubernetes scheduler scores 50% of the nodes of a 100-node cluster, falling to 5% on large clusters.

A pass is deterministic: the same binary gives the same `Plan` for the same `Input`, whatever the number of goroutines and whatever the order of Go's map iteration, unless the pass's timeout ends it. The planner applies these rules:

- It visits nodes and shards in the order of their IDs, never in the order of a map.
- It takes every random choice from `Input.Rand`, a `rand/pcg` source from core, on one goroutine, because a `rand/pcg` source is not safe for concurrent use.
- It evaluates the candidate targets of a move in parallel with `task.Each` from core, bounded by the controller's CPU limit, and reduces the results in candidate order, not in the order in which they complete.
- Of two candidates with equal cost, it takes the one with the lower shard ID, then the one with the lower target node ID.
- It rejects a cost that is not a finite number, and reports the goal that returned it.
- It reads the timeout through the controller's `clock.Clock`, so the simulator replays a pass exactly under a virtual clock.

The same binary is part of the contract. The Go specification lets a compiler fuse floating-point operations, even across statements, so builds for two architectures can resolve a near tie between two moves differently. The timeout is the one other source of machine dependence. A pass that the timeout ends has a result that depends on the machine's speed, and its plan is marked `Incomplete`.

The planner has two targets at the default cell bounds of 5,000 nodes and 1,000,000 shards, on 4 cores. Emergency mode places the replicas of one failed node within 1 second. A periodic pass ends within 60 seconds, the default `timeout`. We have not measured either. CI benchmarks measure them at increasing cell sizes, in six cases: each supported preset's goals, a pass with no improving move, a cell where every move is infeasible, skewed load, many label constraints, and one failed node. For each case, a benchmark reports these values at p50, p95 and p99:

- the planner's CPU time and allocations
- the time to build its `State`
- the plan's cost against the starting cost
- the latency of the commits that apply the plan
- the time until every moved replica serves

A measurement that misses a target is recorded as a defect. The deterministic simulator checks the plans, never the elapsed times.

Each cell plans in its own controller, so a namespace of many cells plans its cells in parallel on as many processes.

The churn budget caps the improvement of one periodic pass: the fraction of shards that it may move, and the moves that it may make per node and per shard. The fraction's default, 9%, is Slicer's per-adjustment key churn budget. The simulator measures the balance and the moves that the defaults give, and a preset can set its own budget. `triggers` skip the improvement of a periodic pass while the most loaded node is below `activeAbove` of its capacity, or while the ratio of the highest to the mean node load is below `minImbalance`. Slicer suppresses rebalancing below 25% CPU for the same reason: when no node is at risk, a move has a cost and no benefit. Neither the churn budget nor `triggers` limits repair.

### Function placers

The function placers are outside the support matrix.

| Strategy | Maps a shard to nodes by | Cost per shard | Moved on one node change | Restriction | Configuration and defaults |
|---|---|---|---|---|---|
| `rendezvous` | The k nodes with the highest hash of shard ID and node ID, weighted by capacity | O(N) | Only the shards whose top k included the changed node | none | `hash`: `xxhash64`, `weighted`: true |
| `consistent-hash` | The next k distinct nodes clockwise on a ring of virtual nodes | O(log(N·v)) | About 1/N of the shards | More virtual nodes balance better but give each node more distinct replica neighbours, so more combinations of two failures lose a shard | `vnodes`: 100, Dapr's default for its actor placement ring |
| `bounded-load` | Consistent hashing in which a shard that maps to a full node moves on to the next node below c times the mean count | O(1/ε²) nodes probed, where c = 1+ε | O(1/ε²) times the shards that plain consistent hashing moves | Placement depends on which nodes are full, so it needs the whole layout | `capacityFactor`: 1.25, the value Vimeo used in production |
| `jump` | Jump consistent hash over node ordinals | O(log N) | 1/(N+1) of the shards when a node is added, all to the new node | Nodes must be numbered 0 to N-1, and only the highest can leave without remapping others | `ordinalLabel`: `"ordinal"` |
| `maglev` | A lookup table of prime size filled by per-node permutations | O(1) after a table build | About twice as many as a ring, by Envoy's simulations | Table must be rebuilt on every node change. Its authors size it above 100 × N for a 1% balance | `tableSize`: 65537, the authors' default |

N is the node count, k the replica count, v the virtual nodes per node, and M the table size. The core counts the shards each function moves when membership changes, and exports the count as a metric, because a function placer does not read the previous assignment.

### The static adapter

`static` is outside the support matrix. It places each shard on the nodes and roles an operator lists in the spec. It reports a shard whose listed node is not live as unplaced, unless its `failover` field names another placer to fill the gap.

An application that uses an optimiser in another process registers a `Placer` that calls it. The core validates the plan that it returns like any other.

### Load

Agents report each shard's load per metric, averaged over the spec's load window. Nodes report their capacity per metric at registration. The pseudo-metric `count` is the number of replicas, weighted by role. A shard with no report yet has the mean load of its cell, so placement treats a new shard as an average shard.

The planner balances one namespace. Namespaces whose processes share machines rely on the platform's isolation of those processes, such as a container per namespace, or declare each namespace's share of a machine in `Node.Capacity`. Meta layers its fleet the same way: Twine gives each application containers that isolate it from the others on a server, and SM places shards only within an application's own containers.

### Plan validation in the core

The core checks every plan before it commits it, whatever placer produced it:

- Each shard in the layout has a target.
- Each target has at most one replica per node, and its replicas plus its deficit equal the shard's desired count.
- Every replica that a target adds or moves passes every constraint of the spec, checked as the move that adds it. A replica that a target keeps and that fails a constraint is a violation, which the core reports.
- The constraints see the whole plan. A shard's load counts once on each node among its current replicas, its target's replicas, and the destination and fallback of its transition in flight. Two targets that each fit a node's last free capacity alone therefore fail together.
- A target leaves a shard with a transition in flight unchanged.
- The core builds the transfers of the plan and checks them together before it starts any of them. A fallback other than the transfer's source counts the shard's load on its node, as a destination does, so two transfers cannot both take a third node's last free capacity for their fallbacks.
- No target puts more replicas in a role than the state model's bound allows, counting started transitions.

A plan that fails a check is rejected whole. The current assignment remains in effect, and the core reports the placer and the failed check.

`sharderctl move` and `drain` each become a plan that the core validates the same way. The core refuses such a plan before anything is committed, and reports the check that failed. `reshard` and `cell split` are outside the support matrix.

In a namespace with `delegated` ownership, outside the support matrix, the application's authority chooses each shard's replicas, and the core takes them from the agents' reports. A plan there may change only which of a shard's reported replicas is the writer.

## Alternatives considered

### A mathematical programming solver

Formulate placement as a mixed-integer program or a constraint program and solve it with CP-SAT or a MIP solver. The solver proves optimality or bounds the gap, and new constraints are formulas.

**Why not:** SM found MIP too slow at its scale and moved to local search. OR-Tools has no officially supported Go binding. Its maintainers describe the Go wrapper as community-built and dependent on Bazel. A cgo solver also removes determinism across platforms. An application can still register a placer that calls such a solver in another process.

### Simulated annealing as the only search

Service Fabric's approach: random moves, with worsening moves accepted at a probability that falls over time. It escapes local minima in which greedy search stops.

**Why not as the default:** annealing needs more moves per pass to converge, and its random walk makes results harder to explain to an operator. It is available as `"algorithm": "anneal"`.

### A timeout as the only bound on a pass

End a pass only on its timeout, with the best plan found so far. The search then needs no limits on its work, and a fast machine searches further.

**Why not:** a pass that the timeout ends depends on the machine's speed, so its plan cannot be replayed on another machine, and a slow machine ends every pass at the timeout. Work limits end a pass at the same point on every machine, and the timeout remains as a safety bound.

### Consistent hashing as the only placer

Place every shard with a hash ring. A ring runs without a controller and moves few shards on a membership change. Every node computes the same ring.

**Why not:** a ring cannot honour capacity, zone spread, regional preference or a label rule, and it balances only on counts, not on measured load. It remains available as a function placer for applications that need nothing more.

### One planner across namespaces

Plan every namespace whose processes share machines in one pass, over their combined load, as Helix's WAGED rebalancer does across all resources of a cluster and as Service Fabric's resource manager does across services. Idle capacity of one namespace then serves another, and no two planners move load onto the same machine at once.

**Why not:**

- One planner would span the cells of several namespaces, so its problem grows with the sum of their shards instead of one cell's bound. Helix's maintainers report partial rebalances of 30 to 40 seconds on clusters of more than 25,000 partitions, 1/40 of one cell's bound of 1,000,000 shards here.
- A bad spec or a planner fault in one namespace could move the shards of every namespace that shares its machines.
- Leaderless namespaces have no planner to join.

### One scoring pass per shard

Filter the feasible nodes and pick the highest-scoring one for each shard, as a pod scheduler does. This is simple and fast, and it is what emergency mode does.

**Why not as the only mode:** placing shards one at a time never revisits a placement, so the assignment drifts as load changes. The Kubernetes scheduler shows the gap: it decides from its view of the cluster "at that point of time when a new pod appears", and moving running pods takes a separate component, the descheduler. Periodic local search moves already-placed shards when that lowers the cost.

## Drawbacks

- The planner is the largest component of the library: six constraints, seven goals, three search algorithms and two modes. By our estimate it is 4,000 lines of code.
- We have not measured the planner. SM reports 30 seconds for 75,000 shards on 1,000 servers with a mature, heavily optimised solver, growing to 205 seconds for 375,000 shards on 5,000 servers. A 60-second pass over 1,000,000 shards ends on a work limit or its timeout, not at a converged plan.
- Building a pass's `State` costs time proportional to the cell, on every pass, before the work limits apply.
- A plan is reproducible only by the same binary, and only when a work limit or the search, not the timeout, ends the pass.
- The planner counts a moving shard's load on its destination and on its fallback until the transition ends. A node therefore keeps capacity for an abort that rarely happens, and a full cell rebalances more slowly.
- Namespaces that share machines without isolation do not see each other's load. Capacity that one namespace leaves idle is not available to another.
- Tiers and weights are configuration that users must understand. A goal list with badly chosen weights moves shards without improving anything that matters.
- Function placers, outside the matrix, ignore load and can overload a node that serves hot shards. `bounded-load` limits the imbalance in shard count, not in measured load.
- Placement never crosses a cell. A cell whose nodes are full cannot use the free capacity of another cell until an operator moves nodes between them.

## Open questions

None.

## Unresolved and future work

- The function placers and `static` are outside the support matrix.
- Placement across namespaces, where a goal in one namespace reads another namespace's assignment, is not proposed here beyond the `colocate` goal within one namespace.
- Machine-wide utilisation as a load metric, which would let each namespace's planner avoid machines that other namespaces load, is not proposed here.
- Forecasting load and placing ahead of it is not proposed here.

## References

| What | Where |
|---|---|
| Lee et al., "Shard Manager", SOSP '21, §2.2 and §5 | https://dl.acm.org/doi/10.1145/3477132.3483546 |
| Adya et al., "Slicer", OSDI '16, §4.4 | https://www.usenix.org/system/files/conference/osdi16/osdi16-adya.pdf |
| Kakivaya et al., "Service Fabric", EuroSys '18, §5.3 | https://dprg.cs.uiuc.edu/data/files/2018/ServiceFabricEuroSys2018.pdf |
| Thaler and Ravishankar, "Using Name-Based Mappings to Increase Hit Rates", IEEE/ACM ToN 1998 | https://doi.org/10.1109/90.663936 |
| Karger et al., "Consistent Hashing and Random Trees", STOC '97 | https://doi.org/10.1145/258533.258660 |
| Mirrokni, Thorup and Zadimoghaddam, "Consistent Hashing with Bounded Loads" | https://arxiv.org/abs/1608.01350 |
| Huang et al., load imbalance of consistent hashing in Facebook's TAO, HotNets '14 | http://conferences.sigcomm.org/hotnets/2014/papers/hotnets-XIII-final128.pdf |
| Envoy v1.39.1, Maglev and ring hash load balancers | https://github.com/envoyproxy/envoy/blob/v1.39.1/docs/root/intro/arch_overview/upstream/load_balancing/load_balancers.rst |
| Apache Cassandra 5.0, virtual nodes and availability | https://cassandra.apache.org/doc/5.0/cassandra/architecture/dynamo.html |
| Lamping and Veach, "A Fast, Minimal Memory, Consistent Hash Algorithm" | https://arxiv.org/abs/1406.2294 |
| Eisenbud et al., "Maglev: A Fast and Reliable Software Network Load Balancer", NSDI '16 | https://www.usenix.org/conference/nsdi16/technical-sessions/presentation/eisenbud |
| Kubernetes scheduling framework | https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/ |
| Kubernetes v1.37.1, score weighting and nodes scored | https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/scheduler/schedule_one.go |
| Kubernetes descheduler | https://github.com/kubernetes-sigs/descheduler |
| Mitzenmacher, "The Power of Two Choices in Randomized Load Balancing", IEEE TPDS 2001 | https://www.eecs.harvard.edu/~michaelm/postscripts/tpds2001.pdf |
| Apache Kafka 4.3, consumer rebalance protocol and assignors | https://kafka.apache.org/43/operations/consumer-rebalance-protocol/ |
| Apache Pekko 1.7.0, cluster sharding configuration | https://github.com/apache/pekko/blob/v1.7.0/cluster-sharding/src/main/resources/reference.conf |
| Microsoft Learn, Orleans grain placement | https://learn.microsoft.com/en-us/dotnet/orleans/grains/grain-placement |
| Dapr v1.18.4, placement service options | https://github.com/dapr/dapr/blob/v1.18.4/cmd/placement/options/options.go |
| PingCAP, "Placement Rules" | https://docs.pingcap.com/tidb/stable/configure-placement-rules/ |
| google/or-tools issue 5042, Go support | https://github.com/google/or-tools/issues/5042 |
| Apache Helix wiki, the WAGED rebalancer | https://github.com/apache/helix/wiki/Weight-Aware-Globally-Even-Distribute-Rebalancer |
| Apache Helix wiki, WAGED rebalance pipeline redesign | https://github.com/apache/helix/wiki/WAGED-Rebalance-Pipeline-Redesign |
| Microsoft Learn, Service Fabric metrics | https://learn.microsoft.com/en-us/azure/service-fabric/service-fabric-cluster-resource-manager-metrics |
| Guo and Kooburat, "Scaling services with Shard Manager", Engineering at Meta, 2020 | https://engineering.fb.com/2020/08/24/production-engineering/scaling-services-with-shard-manager/ |
| Yu, "Twine: Efficient, reliable cluster management", Engineering at Meta, 2019 | https://engineering.fb.com/2019/06/06/data-center-engineering/twine/ |
| Go specification, range over maps | https://go.dev/ref/spec#For_range |
| Go specification, floating-point operators and fused operations | https://go.dev/ref/spec#Floating-point_operators |

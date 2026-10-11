---
adr: 0015
title: Placement declares only the planner of the support matrix, with sparse plans and a concrete state
status: Accepted
date: 2026-10-11
supersedes: none
superseded-by: none
rfc: RFC-0003, RFC-0004
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0015: Placement declares only the planner of the support matrix, with sparse plans and a concrete state

## Status

Accepted

## Context

The design of `placement` declares more than the support matrix uses:

- `Function`, its five function placers and `static` place shards without a controller or by hand. The matrix contains the planner alone.
- A `Move` can change the role of a replica. The matrix contains the model `primary-only`, which has one role.
- The field `algorithm` selects one of three searches: `greedy`, `local` or `anneal`. No preset of the matrix selects a search other than `local`.

The plan and the state of the design also have costs at the bounds of a cell:

- `Plan.Targets` maps every shard of the layout to its target. A probe that copies the layouts of the types built such a map for 1,000,000 shards in about 100 ms, with 214 MiB and about 1,000,000 objects. A map of the 500 shards that a periodic pass moves at most took 107 KiB. Emergency mode has a target of 1 second to place the replicas of a failed node.
- `Plan.Cost` is one number, and one number cannot order the costs of two tiers.
- `State` is an interface. The validation of a plan in the core needs the same index of loads, with the load of a shard counted once on each node of its current replicas, its target and its transition in flight.

The design also predates facts of the repository:

- gocritic's hugeParam refuses a parameter of 80 bytes or more, and an `Input` is larger. A `Move` is 72 bytes.
- The registered name of a strategy is a `shard.StrategyName`.
- The constructor of a built-in takes only its parsed configuration, as `partition/fixedslots` does. A pass then has no other way to receive the clock and the CPU limit of the controller.
- The partitioners of the matrix cover every position of a cell, and resharding is outside the matrix.

## Decision

We will build `placement`, its planner, its constraints, its goals, its kit and `partition.Layout` with only the planner of the support matrix, sparse plans and a concrete `State`, because the processes of the matrix call nothing else and a plan of every shard costs 214 MiB at the bounds of a cell.

The scope:

- `placement` has no `Function`, no function placers and no `static`, and a `Move` does not change the role of a replica.
- The planner has the local search alone, so `planner.Config` has no field `algorithm`.

The plan and the state:

- `Plan.Targets` lists the shards whose replicas the plan changes, and the shards with a deficit. The core does not change the replicas of any other shard.
- `Plan.Costs` lists one cost per tier of goals, in ascending tier order.
- `State` is a struct. `placement.NewState` builds it from an `Input`, and `State.Apply` applies one move.
- `placement.Validate` checks a plan with the same index, and returns the violations of the replicas that the plan keeps. It checks a replica that the plan adds as the move that adds it to the state of the whole plan.
- `ErrPlan` reports a plan that `Validate` rejects, and `ErrCost` a cost that is not finite. Both classify as `errs.Invalid`.

The input and the placer:

- `Input` has no `Replicas`. The Max of each role of the model gives the replicas of every shard.
- `Input.Metrics` lists each load metric with the capacity of a node that it fills.
- `Input.Clock` is the clock of the controller, and `Input.Parallelism` is the most evaluations that a pass runs at once.
- `Placer.Place` takes an `*Input`, and `Placer.Schedule` returns the interval and the flag `onMembership` of the triggers.
- The triggers apply to each load metric of the input and to the replica count. A pass skips the improvement only when no metric shows a node at risk, and `activeAbove` applies only to a metric whose capacity the nodes declare.

The constraints, the goals and their configuration:

- `Name` of a constraint and of a goal returns a `shard.StrategyName`.
- `Check` and `Delta` do not allocate. A built-in constraint returns one static error, and the kit measures `Check` and `Delta` with `MaxAllocs`.
- Each built-in exports a configuration type with JSON tags, `Validate` and `New`. `planner.New` takes a `*planner.Config` with the constraints and the goals that the configuration names, already built.

The layout:

- `partition.Shard` embeds `Span` and adds the ID that the core assigned.
- `partition.Layout` has `Cell` and `Shards`, and no version.
- `Layout.Locate` returns `(shard.ID, bool)`, and `partition` has no `ErrNoShard`.

The benchmarks:

- `BenchmarkPlanner` measures the six cases of the design at 1,000, 10,000 and 100,000 shards, with 200 shards per node. Its flag `-bound` adds 1,000,000 shards on 5,000 nodes.
- It reports the CPU time, the allocations, the time to build the `State`, and the cost of each tier before and after the pass. `make bench-go` runs it, and `make check` does not.

## Alternatives Considered

### The declarations of the design

Each package declares the placers, the searches, the role changes and the fields of the design, and the processes of the matrix refuse the ones outside it.

Rejected because every refused declaration needs code and tests of its refusal, and no preset of the matrix uses them.

### A target for every shard

`Plan.Targets` maps every shard of the layout to its target, so a reader of a plan does not need a rule for a shard without a target.

Rejected because the map costs about 100 ms, 214 MiB and 1,000,000 objects per pass at 1,000,000 shards. A map of the 500 changed shards costs 107 KiB.

### The searches greedy and anneal

The planner keeps `greedy` and `anneal` beside `local`, and `algorithm` selects one of them.

Rejected because `local` already applies the best move of each shard and repeats until no move improves a tier. The random walk of `anneal` also makes a plan harder to explain to an operator. A preset of the matrix on which `local` stalls in a local minimum would bring `anneal` back.

### State as an interface

`State` remains an interface, so a placer of an application can use an index of its own.

Rejected because `Validate` needs the same index, and an interface adds an indirect call to each of the 200,000 evaluations of a pass.

### Inputs passed by value

`Place` takes its `Input` by value, as the design declares.

Rejected because hugeParam refuses the parameter, and each call copies the whole struct.

## Consequences

**Positive:**

- A pass that moves 500 of 1,000,000 shards returns a plan of about 107 KiB.
- The kit checks the plans of every placer with `Validate`, the check that the core runs.
- A constraint or a goal of an application reads the same exported methods of `State` as a built-in.
- The benchmarks compare the cost of each tier with its cost at the start of the pass.

**Negative:**

- A design that adds function placers, `static`, role changes or another search adds them to the contracts and the configuration.
- The core treats a shard without a target as unchanged, so a placer that omits a shard leaves it as it is, and `Validate` does not report the omission.
- A placer of an application builds its plans against the concrete `State` of `placement`.
- The triggers compare every load metric of the input, also a metric that the goals do not balance.
- The benchmarks do not measure the latency of the commits of a plan or the time until moved replicas serve, because both need the engine and a store.

**Neutral:**

- The planner follows the design for the two modes, the repair cursor and the work limits.

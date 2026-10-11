---
adr: 0016
title: The improvement weighs every tier at once and examines a sample of the shards of each node
status: Accepted
date: 2026-10-11
supersedes: none
superseded-by: none
rfc: RFC-0004
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0016: The improvement weighs every tier at once and examines a sample of the shards of each node

## Status

Accepted

## Context

A periodic pass of the planner improves the assignment after its repair. The design improves one tier of goals at a time, in ascending tier order. For each tier, the planner takes the nodes with the highest cost and evaluates the move of each of their shards to a sample of nodes. It applies at most one move per visited node in each sweep.

`maxSourceShards` bounds the shards that one pass examines, 5,000 by default. The tiers use this budget in their order:

- At 200 shards per node, a cell of 10,000 shards has 50 nodes, and one sweep of a tier examines 10,000 shards.
- The preset exclusive-worker has `spread` in tier 0 and `count` in tier 1. With one replica per shard, `spread` cannot improve. Its first sweep used the whole budget, so tier 1 never ran. A pass at 10,000 shards returned no target and an incomplete plan, and the cost of `count` remained 816.4.
- The preset actor-entities has `count` alone. Its pass at 10,000 shards examined 5,000 shards for 25 moves, one per visited node, and returned an incomplete plan.

Repair already compares the candidates of a replica in every tier at once, from the lowest tier up.

## Decision

We will choose each move of the improvement by its deltas in every tier at once, and examine at most `sample` shards of a node in each sweep, because a tier without an improving move otherwise uses up the source budget of the tiers above it.

- A move improves the assignment when it lowers the cost of a tier by more than the tolerance of the tier, and does not raise the cost of a lower tier by more than the tolerance of that tier.
- On each node, the planner applies the improving move with the lowest delta in the lowest tier where two moves differ, so a move that lowers a lower tier takes precedence over a move that lowers only a higher tier.
- A sweep examines the `sample` largest shards of each node that the improvement may move. It evaluates the move of each of them to `sample` nodes.
- The churn budget, the work limits and the triggers keep their rules and defaults.

## Alternatives Considered

### The order of the design

Improve one tier at a time, and examine every shard of each visited node.

Rejected because a tier that cannot improve uses the whole source budget. At 10,000 shards, a pass of exclusive-worker then moved no shard.

### Every tier at once, with every shard of a node

Weigh every tier at once, and keep the examination of every shard of each visited node.

Rejected because one move per visited node still costs the shards of the whole node. At 10,000 shards, 5,000 examined shards then yield at most 25 moves.

### A sample of the shards of a node, one tier at a time

Examine `sample` shards of each visited node, and keep the order of the tiers.

Rejected because the tiers still use one budget in their order. At the bound of 5,000 nodes, one sweep of a tier examines 80,000 shards, so a tier that cannot improve still uses the whole budget.

## Consequences

**Positive:**

`BenchmarkPlanner` measured one pass per case on 4 cores, with 200 shards per node:

- A pass at 10,000 shards of exclusive-worker lowered the cost of `count` from 816.4 to 212.4. In the order of the design, the cost remained 816.4.
- A pass at 10,000 shards of actor-entities lowered the cost of `count` from 816.4 to 212.4, against 625.2 in the order of the design.
- At 100,000 shards, the same pass lowered the cost from 80.40 to 8.248, against 61.60 in the order of the design.

**Negative:**

- One pass at 100,000 shards of the three presets took 67 to 84 ms, against 54 to 68 ms in the order of the design.
- When a constraint keeps the `sample` largest shards of a node in place, the improvement does not move the other shards of that node, because it examines only the largest shards.
- A sweep can apply a move that lowers only a higher tier before it visits a node with a move that lowers a lower tier.
- Each evaluation computes the delta of every tier, also for a candidate that raises a lower tier.
- `sample` sets two quantities: the shards of a node that a sweep examines, and the target nodes of each of them.

**Neutral:**

- Repair and the improvement compare moves the same way.

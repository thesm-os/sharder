---
adr: 0024
title: A controller makes the source of a move its fallback, or gives the move none
status: Accepted
date: 2026-10-12
supersedes: none
superseded-by: none
rfc: RFC-0004, RFC-0006, RFC-0009
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0024: A controller makes the source of a move its fallback, or gives the move none

## Status

Accepted

## Context

The abort path of a move makes its fallback the writer. A move without a fallback aborts with one commit that leaves the shard without an assigned writer, and the next planning pass places the shard again.

We defined the fallback of a transfer as "From, unless From is draining or has a fault, in which case the planner chooses another node". The core also checks the fallbacks of a plan together, so that two transfers cannot both take the last free capacity of a third node.

The placement contract does not have a place for that choice:

- A `placement.Target` has the replicas of a shard and a deficit. A plan does not have a fallback.
- `placement.Validate` checks the replicas that a plan adds and keeps. It does not check a fallback.
- The placer of the support matrix is the planner, and its passes return such plans.

A source that has left the membership view cannot take the writer role back, because its incarnation has ended.

The root moves cells with the same planner, transition engine and handoff protocol that the controller of a cell uses for shards. In the root, a shard is a cell, and a node is a controller process. The view of the root contains the controller processes that joined the root scope. A controller records a drain from a drain proposal or from a call of the disruption budget. It takes the faults of replicas from reports. The root refuses drain proposals, the calls of the disruption budget and reports.

## Decision

We will make the source of a move its fallback only while the source can serve the shard, because a plan does not have a fallback that a placer could choose.

- The controller of each cell and the root apply the same rule.
- The source can serve the shard while these conditions are true:
  - The view has the source.
  - The source does not drain.
  - Its replica of the shard does not have a fault.
- In the root, only the first condition can be false.
- The controller sets the fallback when it starts the move, after the plan has passed `Validate`.
- A move that only adds a replica does not have a source, so it does not have a fallback.
- A move that aborts without a fallback ends without an assigned writer, and the commit that ends it starts a pass.

## Alternatives Considered

### The planner chooses another node

When the source drains or has a fault, the planner chooses a third node as the fallback, and `Validate` counts the load of the shard on that node.

Rejected because neither the plan nor `Validate` has a fallback. Adding one changes the accepted placement contract for an abort path that rarely runs.

## Consequences

**Positive:**

- The placement contract and the planner do not change.
- The load of a shard already counts on its source, so a fallback does not need a capacity check across the moves of a plan.
- The root and the controllers of the cells share one implementation of the rule.

**Negative:**

- A move off a draining node or a faulted replica that aborts leaves the shard without a writer until a new pass places it. That costs one more commit, one pass and the `add` of the new writer.
- A move off a node that has left the view does not get a fallback either, also when another node could take the shard at once.
- In the root, such an abort leaves a cell without a controller until a new pass of the root places the cell. Its agents keep their replicas, but the cell does not start a transition until then.

**Neutral:**

- A move of a healthy writer between two live nodes keeps its source as the fallback, as the design states.

## References

| What | Where |
|---|---|
| The fallback of each move | `internal/control/plan.go`, `move` |
| The calls that the root refuses | `internal/machine/root/root.go`, `step` |
| The abort paths of break-before-make | `handoff/breakbeforemake/protocol.go` |
| The targets of a plan | `placement/placer.go`, `Plan` and `Target` |

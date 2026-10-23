---
adr: 0043
title: An emergency pass that restores no replica repairs the other replicas
status: Accepted
date: 2026-10-13
supersedes: none
superseded-by: none
rfc: RFC-0004
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0043: An emergency pass that restores no replica repairs the other replicas

## Status

Accepted

## Context

RFC-0004 runs the planner in emergency mode while shards are under-replicated after failures, or have a replica with a permanent fault. An emergency pass places the missing and faulted replicas and does not move any other replica. RFC-0004 also states that repair finishes a drain and a changed constraint.

ADR-0040 leaves a shard without a writer when every live node incarnation failed its add, and every pass is then an emergency. A constraint can keep a shard off every node in the same way, such as a label that no live node has. The review of 2026-10-13 reproduced the consequence with the test harness of the controller: shard Y fails on node A and on node B, and node B drains. Every later pass is an emergency, and no pass moves the replicas of the other shards off B. The drain never finishes.

## Decision

We will let an emergency pass repair the other replicas once it cannot restore a replica:

1. An emergency pass repairs the missing and faulted replicas first, as RFC-0004 states.
2. When that repair does not apply a move, and no limit of its work has ended the pass, the pass repairs again with the rules of a periodic pass. A replica on a draining node and a replica that a constraint rejects then need repair too. The second repair starts at the repair cursor of the input, as the first does.
3. The pass skips the improvement in an emergency, as before.
4. The controller keeps the emergency predicate of ADR-0040.

## Alternatives Considered

### A shard that no node can take is no emergency

The controller sets `Input.Emergency` only for a shard that an accepting node can take, a node without a fault of the shard.

Rejected because a constraint keeps a shard off every node in the same way, and the controller does not evaluate constraints. The controller would need the feasibility check of the planner.

### Alternate emergency and periodic passes

After an emergency pass without a target, the controller runs the next pass as a periodic pass.

Rejected because the drains then move in every second pass alone, and the controller keeps the outcome of the last pass as state of its own.

### A fixed share of each emergency pass for the other repair

An emergency pass spends a fixed part of its limits on drains and violations.

Rejected because that part delays the restoration of a missing writer while the emergency can still make progress. We have no measurement that sets the part.

## Consequences

**Positive:**

- A drain and a violation finish while a shard remains without a node that can take it.
- An emergency that can restore a replica uses the whole pass for its repair.

**Negative:**

- An emergency pass that restores no replica examines its shards twice, and evaluates each shard without a feasible node again in the second repair.
- The drains wait while the emergency restores replicas, also when they take many passes.

**Neutral:**

- `placement.Input` and `placement.Plan` do not change.

## References

| What | Where |
|---|---|
| Emergency and periodic mode, and repair | RFC-0004, The planner |
| A shard whose add every node failed | ADR-0040 |
| The repair of a pass | `placement/planner/pass.go`, `repair` and `bad` |
| The emergency of a pass | `internal/control/plan.go`, `emergency` |

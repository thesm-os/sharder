---
adr: 0041
title: The controller admits each move against the replicas that the transitions in flight can place
status: Accepted
date: 2026-10-13
supersedes: none
superseded-by: none
rfc: RFC-0004, RFC-0011
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0041: The controller admits each move against the replicas that the transitions in flight can place

## Status

Accepted

## Context

RFC-0004 makes every constraint a hard rule. The core checks every plan before it commits anything, and it counts the load of a moving shard on the destination and on the fallback of its transition. `config.Budget.MaxUnavailable` states that the budget does not admit planned unavailability under an explicit zero. RFC-0011 keeps the input of a pass to committed state, so a recorded input replays its plan.

The runtime checks a plan against the input of its pass, and the controller starts the moves of the plan in a later step. The review of 2026-10-13 reproduced three defects in that gap with the test harness of the controller:

- Concurrent plans overfill a node. Three move proposals to an empty node C under `max-per-node` 2 each pass `placement.Validate` against the same input, and C ends with three writers.
- `max-per-node` does not count the transitions in flight. `State.OnNode` and `State.RoleCount` count the replicas of the placements alone. Three moves to C, each submitted after the commit of the one before it, pass while their transitions run, and C ends with three writers.
- A move proposal bypasses the budget. `Controller.moves` asks `admits` before it starts the move of a pass, and `Controller.moved` starts a move proposal without it. Under `maxUnavailable` 0 a proposal moves a healthy writer.

The same design has these gaps as well:

- `placement.Validate` checks the state after every move of a plan. The controller then leaves out the moves that a drain, the budget or the bounds of the engine refuse, and starts the rest. That subset can break a constraint that the whole plan passes. An example is a move into node A whose room depends on a move out of A that the controller left out.
- A transition that aborts returns its shard to its fallback, so a count bound that is true once every transition completes can break.

Under `maxUnavailable` 0 the budget refuses the repair of a writer with a permanent fault, and the planner proposes the repair again at every pass. `admits` treats a writer as unavailable only when the view does not have its node at the incarnation of the placement, or when an admitted reservation covers its node. A writer whose replica has a permanent fault is unavailable as well.

## Decision

We will admit every move in the step that starts it. Admission counts the replicas that the transitions in flight can place:

1. `placement.InFlight` gains the field `Role`, the role of the replica that the transition places. A zero `Role` counts in no role.
2. `placement.State` counts the peak occupancy of each node, as `PeakLoad` counts its peak load. `State.PeakCount(n)` returns the number of shards that have a replica on node n, had one when the pass started, or have a transition in flight whose destination or fallback is n. `State.PeakRoleCount(n, r)` counts the same shards in role r. Each shard counts once on each node.
3. `max-per-node` checks the peak counts in place of `OnNode` and `RoleCount`. A plan that it passes is then also valid for every subset of its moves, and for every outcome of the transitions in flight.
4. The controller admits each move of a plan and each move proposal in the step that takes the plan. It builds one `placement.State` over the committed assignment and every transition of the engine, including the transitions that the engine started and has not committed. It checks each move against the budget and then against the constraints of the spec, in the order of the shard IDs. Each admitted move counts as a transition in flight for the moves after it. A move that admission refuses does not start, and a later pass plans its shard again. The input of a pass keeps committed state alone, so the replay of RFC-0011 is unchanged.
5. The budget admits a move proposal as it admits the move of a pass. A proposal that the budget refuses fails with a new sentinel `control.ErrBudget`, classified as `errs.Conflict`. A proposal that a constraint refuses at admission fails with `ErrStale`.
6. The budget and the counter of the moves after a failure use one predicate for a failed writer. The writer of a placement has failed when the view does not have its node at the incarnation of the placement, or when the last report of that node incarnation lists its replica at the epoch of the placement with a permanent fault. The budget also admits the move of a writer whose node an admitted reservation covers.

A force flag for move proposals is out. The budget of the spec binds every move, and an operator who needs the move of a healthy writer raises `maxUnavailable` with a spec proposal. A force flag would need an authority, a scope and an audit record of its own. We have no requirement for one.

## Alternatives Considered

### One plan at a time

The controller runs one pass or one proposal at a time, and does not start a move between the input of a plan and its result.

Rejected because a move proposal then waits for the placement pass in flight, up to the timeout of the pass. The input of a pass also lacks the transitions that the engine has started and not committed, so the plan still needs a check against them.

### A sequence number per node

The controller refuses a move whose destination changed after the input of its pass, as the coarse-grained conflict detection that the authors of Omega measured.

Rejected because any change of a node refuses the move, also a change that leaves room for it. Omega measured two to three times as many conflicts this way as with a check of the resources themselves.

### Reject a plan whole

The controller checks the whole plan against the current state, and starts none of its moves when one move fails.

Rejected because one refused move, such as a target on a node that started to drain, then stops every other move of the plan, a drain of a thousand shards included. Omega measured about twice as many conflicts with all-or-nothing transactions as with incremental transactions. An incremental transaction accepts every change that does not conflict.

### An admission state for the whole term

The controller keeps one `placement.State` for its term, and updates it with each commit and with each transition that starts or ends.

Rejected because `State` would need operations that release a transition and change a node, and the controller would mirror every change of its assignment in a second structure. One build per plan costs less code, at the cost that the Consequences state.

### Reserve the destination alone

A transition reserves its destination, and frees its source when it starts.

Rejected because a move into the source after the start of the transition breaks the count bound once the transition aborts and returns the shard to its source.

## Consequences

**Positive:**

- Every constraint and the budget apply to every move, whether a pass, a proposal or a repair started it, and under every outcome of the transitions in flight.
- A move proposal obeys `maxUnavailable` 0.
- The repair of a writer with a permanent fault passes the budget under `maxUnavailable` 0.

**Negative:**

- A node keeps room for the fallback of each transition in flight, so a full cell rebalances more slowly. RFC-0004 states the same drawback for load.
- In a full cell the planner can plan a chain of moves of which admission starts one link per pass.
- The step that takes a plan with moves builds one `placement.State`. `NewState` measured 15 ms and 20 MB at 100,000 shards, and 1.1 ms at 10,000 shards. Its cost grows as O(s log s), which puts 1,000,000 shards at about 180 ms. That figure is an estimate. A plan without targets builds no state.
- A registered constraint that counts replicas must read the peak counts. Otherwise it passes a move that breaks its bound while transitions run.

**Neutral:**

- `placement.InFlight` gains a field and `placement.State` gains two methods. Both keep the behaviour of the existing callers.
- Package `control` gains the sentinel `ErrBudget`.

## References

| What | Where |
|---|---|
| Constraints as hard rules, and the load of a transition in flight | RFC-0004, The planner and Plan validation in the core |
| The input of a pass without the uncommitted transitions | RFC-0011, Alternatives considered |
| `maxUnavailable` | `config/spec.go`, `Budget` |
| The fallback of a move | ADR-0024 |
| The start of the moves of a pass and of a proposal | `internal/control/plan.go`, `moves` and `moved` |
| The budget of a move | `internal/control/budget.go`, `admits` |
| The peak load of a node | `placement/state.go`, `PeakLoad` |
| Schwarzkopf, Konwinski, Abd-El-Malek and Wilkes, "Omega: flexible, scalable schedulers for large compute clusters", EuroSys '13, §3.4 and §5.2 | <https://research.google/pubs/omega-flexible-scalable-schedulers-for-large-compute-clusters/> |
| The serial scheduling cycle and the Reserve phase of the Kubernetes scheduler | <https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/> |

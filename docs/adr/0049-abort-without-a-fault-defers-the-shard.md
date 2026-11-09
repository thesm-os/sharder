---
adr: 0049
title: A move that aborts without a fault defers the moves of its shard for a backoff
status: Accepted
date: 2026-11-09
supersedes: ADR-0024 in part, ADR-0033 in part
superseded-by: none
rfc: RFC-0004, RFC-0006
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0049: A move that aborts without a fault defers the moves of its shard for a backoff

## Status

Accepted

## Context

A pass skips a shard with a transition in flight, so the end of a transition starts the pass that places its shard again. These commits start a pass:

- A commit that ends the transition of a shard and leaves the shard without a writer, such as the end of an abort path without a fallback (ADR-0024).
- A commit that ends the transition of a shard whose writer is not live (ADR-0033).
- A commit that ends the transition of a shard whose writer is on a node that drains.
- A commit that ends a transition while the bounds of the engine refused a move of the last plan.

ADR-0040 makes a definite failure of an add a fault of the shard on the node incarnation of the add, so the pass after the end places the shard on another node. These failures abort a move without a fault:

- A drop that fails definitively. ADR-0023 fails a drop at its refusal. The refusal can come from a later incarnation of the node before the view shows that incarnation. Under break-before-make the abort path of the drop is empty, so the shard keeps its writer on the source.
- A step of the script that fails after its attempts. ADR-0040 does not record a fault for it. Once the retry budget of the engine is spent, such a step fails at its first attempt.

The pass after such an end reads the same placement, view, faults and drains as the pass that planned the move, so it plans the same move. The simulation of a namespace found this loop for a refused drop:

1. The agent of a node ends its session at its local deadline. It then refuses each callback for the ended incarnation.
2. The store lists the session until its lease expires at TTL × maxDriftRate after the deadline. The default maxDriftRate is 0.01, and the specs of the simulation use 0.5.
3. The node drains, so each end of the move starts a pass. Each pass sends the same drop.

One replay sent a drop every 17 steps at one instant until the limit of 10,000 steps. In production the loop runs at the rate of the commits of the store until the lease expires. Once the retry budget is spent, the loop of a step that fails after its attempts runs at the rate of the replies of the failing node.

The records do not state when the next move of such a shard starts. The workqueue of a Kubernetes controller delays each item that fails. Each failure of the item doubles its delay from 5 ms up to 1,000 s, and a success clears the count with `Forget`. The other items of the queue run during the delay.

## Decision

We will defer the moves of a shard for a backoff after a move of the shard aborts without a fault, because the next pass reads the same state and plans the same move.

- The transition engine passes the shard of each transition that ends on its abort path to `Config.Aborted`. `Engine.Applied` calls the function once the batch that ends the transition has applied. An abandoned transition ends on its abort path too.
- A move aborts without a fault when its transition ends on its abort path without a definite failure of an add. `Config.Refused` of the engine reports each such failure.
- The commit that ends such a move arms a Timer of the shard. The delay of the Timer is `resilience.Backoff` of the number of aborts of the shard in a row. The backoff has full jitter and the bounds `control.MoveRetryBase` and `control.MoveRetryMax`. They equal the bounds of the retries of a callback, `engine.RetryBase` and `engine.RetryMax`.
- Every pass defers the move of the shard until the Timer ends. The other moves of the pass start.
- The commit does not start a pass for the shard. It still starts the pass of the moves that the bounds refused.
- The end of the Timer starts a pass.
- A view that ends a node incarnation ends the deferral of every shard and starts a pass, because the view can end the incarnation whose refusal aborted the move. The count of the aborts in a row remains.
- A move of the shard that takes effect clears its backoff. A move takes effect when its transition ends at the end of its script.
- The commit that ends a move after a definite failure of an add starts a pass at once, as ADR-0040 decides. It does not change the backoff of the shard.
- A move proposal of an operator starts during the deferral.
- The controller logs each deferral with the shard and the delay.

## Alternatives Considered

### A reason of a refusal in a CallError

The agent reports that the incarnation of the callback has ended. The engine then completes the drop, as RFC-0006 states.

Rejected because a `protocol.CallError` has only a class and a message. ADR-0023 rejected the same completion for that reason. A new field changes the protocol and covers the refused drop alone, so a step that fails after its attempts keeps its loop.

### A backoff of the pass alone

The end of the transition does not start a pass, and the Timer starts the pass later. Every pass plans every shard.

Rejected because the passes of other triggers plan the same move at once. The end of another transition while the bounds refused a move starts such a pass. Under a bound of one transition, the shard whose move keeps failing then takes the only place of each pass, and the other shards of a drain do not move.

### A fault of a drop

A definite failure of a drop is a fault of the shard on the node incarnation of the drop. Each pass then keeps the shard on that incarnation until the view ends it.

Rejected because a step that fails after its attempts does not record a fault, so its loop remains.

### A backoff after every abort

The backoff also follows a move whose add failed definitively.

Rejected because the pass after such a move places the shard on another node (ADR-0040). The backoff would delay that pass while another node can take the shard.

## Consequences

**Positive:**

- A move that aborts without a fault starts again at most once per backoff, and each backoff is below `engine.RetryMax`.
- The other moves of a pass start while a shard waits for its backoff.
- The pass of a view that ends an incarnation places each deferred shard at once, so the repair of a writer on the ended incarnation does not wait for the backoff.

**Negative:**

- A node that stops failing its callbacks ends the cause of the aborts without a view. Its shards then wait for the rest of their backoff, and the wait is below `engine.RetryMax`.
- A shard without a writer whose abort did not record a fault waits for its backoff before its repair. After its first such abort the wait is below `engine.RetryBase`.
- Each pass is an emergency while such a shard waits. When a node can take the shard, the planner plans its restoration, and the second repair of ADR-0043 does not run. The drains and the violations of the other shards then wait for the end of the backoff.
- Each deferral keeps a Timer of the shard until the Timer ends.

**Neutral:**

- The controller keeps the count of the aborts of each shard in memory, so a controller of a new term starts each shard without a backoff.
- The recovery of the simulation of RFC-0017 counts the longest backoff of a shard once, so the bound of the convergence of its presets grows from 9m40s to 10m0s.

## References

| What | Where |
|---|---|
| The ends of the moves and the backoff of a shard | `internal/control/controller.go`, `apply` and `lost` |
| The deferral of a move in a pass | `internal/control/plan.go`, `moves` |
| The transitions that end on their abort path | `internal/engine/engine.go`, `Applied` |
| The bounds of the backoff of a shard | `internal/control/plan.go`, `MoveRetryBase` and `MoveRetryMax` |
| The bounds of the retries of a callback | `internal/engine/engine.go`, `RetryBase` and `RetryMax` |
| The recovery of the simulation of a namespace | `internal/machine/machinetest/converge.go`, `recovery` |
| The per-item backoff of a Kubernetes controller | <https://github.com/kubernetes/client-go/blob/master/util/workqueue/default_rate_limiters.go>, `DefaultTypedControllerRateLimiter` |

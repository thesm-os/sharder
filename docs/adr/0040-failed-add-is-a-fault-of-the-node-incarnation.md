---
adr: 0040
title: A definite failure of an add is a fault of the shard on the node incarnation
status: Accepted
date: 2026-10-13
supersedes: none
superseded-by: none
rfc: RFC-0004, RFC-0006
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0040: A definite failure of an add is a fault of the shard on the node incarnation

## Status

Accepted

## Context

RFC-0006 fails a step definitively when the node incarnation that the step addresses has ended. A step also fails definitively when the handler of the node returned an error that is not transient. A definite failure of the forward path starts the abort path. A move without a fallback ends without an assigned writer, and so does an abort path that fails definitively. The commit of that end starts a pass, and the pass places the shard again.

The planner avoids a node for a shard only when `placement.Input.Faults` lists the node for the shard. The controller filled `Faults` from the reports of the agents alone. A pass after a failed add planned the same node again.

The property of package `controller` found a livelock of the leading root in about 1 of 140 runs:

- A controller process restarted. The view of the root still listed its old incarnation because the watch of the views waited for a backoff after an outage.
- The root planned a cell onto the old incarnation. The agent of the new incarnation refused the add as stale, and the transition ended without a writer.
- The commit of the end started a pass, and the pass planned the same move. Each turn took two commits and no Timer.
- Inside `testing/synctest` the loop kept every goroutine busy. The virtual time never advanced, so the backoff never ended. The memory store kept every commit, and the test binary grew to 12.5 GB in 31 seconds.

In production the same loop runs at full speed until the backoff of the watch ends. The backoff lasts up to 10 seconds.

A move of a shard that has a writer has a related gap. Its abort path returns the shard to its source, and a later pass can plan the same move again.

## Decision

We will treat a definite failure of an add as a permanent fault of the shard on the node incarnation of the add:

- The transition engine passes the shard and the endpoint of each add that fails definitively to `Config.Refused`. It reports the adds of the forward path and of the abort path. It does not report a drop or an add that fails after its retries.
- The controller records the shard on the node while the view has the node at the incarnation of the endpoint. It ignores a failure on an incarnation that the view has ended.
- Each pass lists the node in the `Faults` of the shard until the view ends that incarnation. A new incarnation of the node starts without the fault.
- A pass is an emergency when a shard without a transition in flight has no writer on a live node, or has its writer on a node that `Faults` lists for the shard. RFC-0004 defines an emergency by the replicas that `Faults` lists, so a fault on a node without a replica of the shard is not an emergency.

The rule applies to the root and to every cell.

## Alternatives Considered

### A backoff before the pass

The pass that a definite failure starts waits for a backoff.

Rejected because the pass after the backoff plans the same node, and the add fails again. The shard also waits for the backoff when another node can take it.

### The end of the incarnation

The controller treats a refusal from a later incarnation as the end of the addressed incarnation in its passes.

Rejected because a handler error for one shard also fails an add definitively without ending the other replicas of the node.

### A fault only for a shard without a writer

The controller lists the fault only while the shard has no writer. A fault in `Faults` then always marks an emergency.

Rejected because a later pass moves the shard to the same node again once the shard has a writer on another node. In a test of this variant, the periodic pass sent a second add of the same shard to the node that had refused the first. Under break-before-make each such move leaves the shard without a writer for a writer gap.

### A new field of the input of a pass

A field such as `Refused` keeps the failed adds apart from the reported faults.

Rejected because every placer already avoids the nodes that `Faults` lists for a shard, and `placement.Validate` refuses a plan that places the shard there. A new field changes the accepted placement contract, and each placer would have to read it.

## Consequences

**Positive:**

- The pass after a failed add places the shard on another node.
- A node incarnation does not get another add of a shard after an add of the shard failed there definitively.
- A rebalance does not move a shard back to a node incarnation that failed its add.

**Negative:**

- A shard whose add every live node failed definitively remains without a writer until the view ends one of those incarnations or adds a node.
- A pass does not place a shard on a node again for the rest of its incarnation after a handler error that is not transient. A new operation of the shard could still succeed there.
- `Faults` now lists nodes without a replica of the shard. A placer that reads every entry of `Faults` as a faulted replica counts these entries wrongly.

**Neutral:**

- The controller records the shards whose adds failed on each node incarnation of its view. Each such set has at most the shards of the scope.
- `engine.Config` has one more required field.

## References

| What | Where |
|---|---|
| Definite failures of a step | RFC-0006, The transition engine |
| The input of a pass and its emergency | RFC-0004, Interfaces and Search |
| The fallback of a move | ADR-0024 |
| The report of a failed add | `internal/engine/engine.go`, `fail` |
| The record of a failed add | `internal/control/members.go`, `refuse` and `faults` |
| The emergency of a pass | `internal/control/plan.go`, `emergency` |

---
adr: 0033
title: A placement records the incarnation of its writer
status: Accepted
date: 2026-10-13
supersedes: none
superseded-by: none
rfc: RFC-0005, RFC-0006, RFC-0007
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0033: A placement records the incarnation of its writer

## Status

Accepted

## Context

RFC-0007 requires that a node that restarts restores no writer replica. The core gives a writer role back only with a new epoch, because another node may have written under the old epoch. The node restores every non-writer replica at once. RFC-0005 gives fencing tokens only to the replicas in the writer role, and a model has one writer role with one replica. A node that restarts joins its scope with a new incarnation. ADR-0026 gives an agent a new incarnation after its session ends, also when its process keeps running.

A placement lists each replica with its node and its role, and `Placement.Epoch` is the fence epoch of its writer. The placement does not record the incarnation of the node of the writer. A view that ends the incarnation of a node from the previous view of the controller starts a pass. The planner treats a replica as missing when `Nodes` does not list its node, or when `Faults` lists the replica.

A node that restarts within its lease is in both views, at two incarnations. The planner finds its node in `Nodes` and moves none of its writers. A controller that takes over after a failover has no earlier view to compare. The property of the controller found three such cases:

- After a restart of every controller process, each cell kept its old process as its writer, and no process ran the controller of the cell.
- A node of a cell joined again at a new incarnation. Its four placements kept the node for 30 seconds, and the controller did not start a move.
- A node restarted while a shard of the node was in transition. The pass after the restart skipped the shard, because a pass plans no move for a shard in transition. The transition then ended with its writer on the old incarnation. The next pass started only at the next interval.

## Decision

We will record the incarnation of the node of the writer in the placement, beside the epoch of the writer:

- `shard.Placement` has the field `Incarnation epoch.Epoch`, which is field 7 of its encoding. It is zero while the shard has no assigned writer.
- A commit step of the transition engine sets the incarnation with the epoch, from the endpoint of its side. The controller gives each endpoint the incarnation of its node in the view when the move starts.
- A writer is live while the view of the controller has its node at the incarnation of its placement.
- The input of a pass leaves out each writer whose node the view has at another incarnation. The planner then places the shard again in emergency mode, as it does for a writer on a node that has left. A writer on a node that has left the view remains in the input, and the planner repairs it as before.
- A commit that ends the transition of a shard whose writer is not live starts a pass. The rule covers a writer of an ended incarnation and a writer on a node that has left the view.
- The source of a move has the incarnation of its placement. A move from a source whose incarnation has ended has no fallback. The controller counts the move as a failure, and the budget admits it as it admits a move from a node that has left.

The rule applies to the root and to every cell. The store keeps the incarnation. A controller that takes over applies the rule from its first view. The engine already completes a drop on an incarnation that has ended. The protocol break-before-make already scripts a move from one incarnation of a node to the next.

## Alternatives Considered

### An incarnation on every replica

`shard.Replica` records the incarnation of its node. The controller treats every replica of an ended incarnation as lost.

Rejected because a node that restarts restores its non-writer replicas, while their placements keep the old incarnation. The controller would then move every secondary of a restarted node in `primary-secondary`, and every replica of the node in `secondary-only`. The planner also compares replicas as values. The controller would then have to copy every replica without its incarnation into the input of each pass.

### Writers that a full report leaves out

The controller treats each writer of its placements that the full report of the node leaves out as lost.

Rejected because the root refuses reports (ADR-0024), so the rule does not cover the cells of a controller process that restarts. A report in flight can also race an add, and the controller would then move a writer that the node has just added.

### A new root leader moves every cell

The root moves every cell once when it takes over.

Rejected because every failover of the root would restart every controller of a cell. The rule also does not cover a node of a cell that restarts.

## Consequences

**Positive:**

- A node that restarts within its lease gets its writers back with new epochs, as RFC-0007 requires.
- A controller that takes over detects the writers of ended incarnations from the store and its first view.
- A drop that a later incarnation refuses before the view lists that incarnation leaves the writer of the old incarnation in the placement (ADR-0023). The next pass now places that shard again.
- The non-writer replicas of a restarted node remain in their placements, as RFC-0007 requires of the models outside the support matrix.

**Negative:**

- The encoding of a placement grows by up to 11 bytes.
- The input of a pass copies the placement of each writer that it leaves out, with one allocation for each.

**Neutral:**

- `shard.Replica` remains a node and a role, so the planner compares replicas as before.
- The agents and the routers read the node of each replica, and do not change.

## References

| What | Where |
|---|---|
| A node that restarts restores no writer | RFC-0007, Start-up without the controller |
| The writer role and its fencing tokens | RFC-0005, Replica state models |
| The end of an incarnation in the engine | ADR-0023 |
| A move from one incarnation of a node to the next | `handoff/breakbeforemake/protocol.go`, `Script` |
| The input of a pass | `internal/control/assignment.go`, `current` |
| The passes that a commit starts | `internal/control/controller.go`, `apply` and `lost` |

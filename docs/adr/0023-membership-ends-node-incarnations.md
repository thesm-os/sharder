---
adr: 0023
title: The transition engine takes the end of a node incarnation from the membership view
status: Accepted
date: 2026-10-11
supersedes: none
superseded-by: none
rfc: RFC-0006
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0023: The transition engine takes the end of a node incarnation from the membership view

## Status

Accepted

## Context

Each callback addresses the incarnation of a node that the record of its transition names. A node that restarts has a new incarnation, and the new incarnation restores no writer. So a drop on an incarnation that has ended can complete, and an add on such an incarnation cannot succeed.

The design ends an incarnation in two cases. A later incarnation of the node refuses a callback, or the lease of the session of the incarnation lapses and the lock delay passes.

A refusal and the membership view give the controller this information:

- The agent refuses a callback for another node incarnation with an error of class `errs.Conflict`. It refuses a callback for a lower epoch, and a callback of an operation that it has seen end, with the same class.
- The transport of the controller returns a `protocol.CallError` in place of the error of the agent. A `CallError` has the class and the message of the error, and no other field.
- The membership view of the controller lists each live node with the incarnation of its session. A session lasts as long as its lease.

## Decision

We will end the incarnation of a node when the membership view of the controller has a later incarnation of the node or no longer has the node, because a refusal of a callback does not show which incarnation refused it.

- A drop on an incarnation that has ended completes. When the node has left the view, the drop completes after the lock delay of the spec.
- An add on an incarnation that has ended fails definitively.
- A view that ends an incarnation cancels the callback, the backoff or the round that waits for that incarnation.
- A refusal fails its step definitively, as every error that is not transient does.

## Alternatives Considered

### A refusal by a later incarnation

A drop completes when a later incarnation of its node refuses it.

Rejected because every refusal classifies as `errs.Conflict`, and a `CallError` has no other field. So a refusal by a later incarnation has the same class as a refusal by the incarnation that the drop addresses, and the engine has no field to separate the two.

## Consequences

**Positive:**

- A drop on a restarted node completes without an answer from the node.
- The simulation ends an incarnation with the same views as production.

**Negative:**

- A drop that a later incarnation refuses before the view shows that incarnation fails definitively. Its transition then ends through the abort path of the drop, and the placement keeps its replica on the restarted node.
- A callback to a node that has crashed fails with its timeout and its retries until the view drops the node.

**Neutral:**

- The lock delay starts when the view drops the node, and the view drops the node when the lease of its session lapses.

## References

| What | Where |
|---|---|
| The class and the message of an error that a transport returns | `protocol/callerror.go` |
| The end of an incarnation in the engine | `internal/engine/engine.go`, `View` and `lapse` |

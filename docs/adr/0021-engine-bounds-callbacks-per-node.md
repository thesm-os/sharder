---
adr: 0021
title: The transition engine bounds the callbacks to each node with a queue of its own
status: Accepted
date: 2026-10-11
supersedes: none
superseded-by: none
rfc: RFC-0001, RFC-0006
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0021: The transition engine bounds the callbacks to each node with a queue of its own

## Status

Accepted

## Context

The controller of a cell sends the callbacks of its transitions to the agents of its nodes. The budget of the spec bounds them with two settings:

- `nodeCallbacks` is the most callbacks in flight to one node.
- `maxNodeTransitions` is the most transitions in flight that count against one node.

The design bounds the callbacks to one node in the production driver, with a `resilience.Bulkhead` of core for each node. These facts constrain where the bound can run:

- A machine takes one event in `Step` and appends its effects. It does not start a goroutine. The production driver and the simulation perform the effects of the same machines.
- The simulation runs the machines without the production driver, so a bound in the driver does not apply in a simulated cell.
- `Bulkhead.Acquire` waits in a `select` on its channels until a permit is free. A machine runs on one goroutine and cannot wait there.

## Decision

We will bound the callbacks to each node inside the transition engine, with `nodeCallbacks` permits and a first-in, first-out queue for each node, because the simulation must apply the bounds of the spec and does not run the production driver.

- A callback takes a permit of its node when the engine appends it. Its result returns the permit.
- A callback without a free permit waits in the queue of its node. A returned permit sends the oldest callback of the queue.
- Before the first view of the membership, every callback waits in the queue of its node.
- A view that ends the incarnation of a queued callback removes the callback from the queue.
- The production driver performs each `Callback` effect when the machine returns it.

## Alternatives Considered

### A bulkhead in the production driver

Callbacks to one node pass through a `resilience.Bulkhead` in the driver. Its `Limit` is `nodeCallbacks`, and its `Queue` is `maxNodeTransitions` minus `nodeCallbacks`.

Rejected because the simulation does not run the driver. A simulated cell would send more callbacks to one node than its spec allows.

### A bulkhead inside the engine

The engine acquires a permit of a `resilience.Bulkhead` before it appends a callback.

Rejected because `Acquire` blocks its goroutine until a permit is free, and the engine runs inside the `Step` of its machine.

## Consequences

**Positive:**

- The simulation and the production driver apply the same bound to the callbacks of a node.
- The callbacks of a node leave its queue in the order in which they joined it.
- The driver does not keep state for each node.

**Negative:**

- The engine keeps a queue and a count of permits for every node that a callback has addressed during the term of its controller.
- A runtime that delays a callback keeps its permit for the delay, because the permit counts from the moment the engine appends the callback.

## References

| What | Where |
|---|---|
| core v0.12.0, `resilience.Bulkhead` and `Acquire` | <https://github.com/thesm-os/core/blob/v0.12.0/resilience/bulkhead.go> |
| The queue and the permits of a node | `internal/engine/node.go` |

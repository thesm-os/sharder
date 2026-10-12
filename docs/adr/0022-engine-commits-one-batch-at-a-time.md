---
adr: 0022
title: The transition engine commits its changes with one batch in flight
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

# ADR-0022: The transition engine commits its changes with one batch in flight

## Status

Accepted

## Context

The transition engine commits these changes to the scope of its controller for each transition:

- The first commit writes the record of the transition, and reserves the epochs and the sequence of the move in the placement.
- A commit after each step writes the next step of the record. A commit step also writes the placement.
- The last commit removes the record.

The agent of a node starts a tenure only while the placements that it watches list the tenure. So the engine sends the callback of a step only after the commit before the step has applied.

The design commits the changes of every transition that is ready in the same tick in one commit, up to the bound of a commit of the store. A machine has no tick. It takes one event in each `Step`, and the result of each callback is one event.

## Decision

We will commit the changes of the transitions in batches with at most one batch in flight, because a machine has no tick and a commit for each event costs one commit for each step of each transition.

- Every change that arises while a batch is in flight joins the next batch.
- `Batch` returns the changes that wait, in the order in which they arose, while `Config.Fits` accepts the mutation. The first change always joins a batch.
- `Batch` returns nil while a batch is in flight.
- The machine commits each batch through its scope, and calls `Applied` once the commit has applied. The engine then sends the callbacks of the steps that the batch committed.

## Alternatives Considered

### The changes of one step of the machine

The machine commits the changes of each `Step` as soon as the step returns them.

Rejected because the result of each callback arrives in a step of its own. A rebalance of hundreds of moves would cost one commit for each step of each transition.

## Consequences

**Positive:**

- A commit contains every change that arose while the commit before it was in flight, up to the bound of the store.
- The callback of a step follows the commit of its record, so the agent finds the record and the placement of the step.

**Negative:**

- A change that arises while a batch is in flight waits for that batch to apply. So the callback of a step can wait for two commits.
- The engine depends on the machine to call `Applied`. Every step that follows a commit waits until the machine reports the batch.

**Neutral:**

- `Config.Fits` is the bound of the store for one commit of the scope, so the store can apply every batch in one commit.

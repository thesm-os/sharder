---
adr: 0017
title: Handoff declares only the protocol of the support matrix, with steps on the sides of a transfer
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

# ADR-0017: Handoff declares only the protocol of the support matrix, with steps on the sides of a transfer

## Status

Accepted

## Context

A protocol of the design turns a transfer into a script of steps. A step has a node, a callback, a role, an epoch, a peer, a wait condition and a timeout, and a step without a node commits the placement of the shard. Before the core commits a transition, it checks that each step is on a node of the transfer and at an epoch that the core reserved. The design has these gaps:

- A commit step does not have a node, so the step that commits the new node as the writer does not have a field for that node.
- The check accepts an add on the new node at the epoch of the old writer, and an add at epoch zero. The first grants one epoch to two nodes, and the fence admits the writes of both. The second starts a writer without a fence.
- The design declares the lifecycle as `var Lifecycle, errLifecycle = ...Build()`, and the package does not check the error.
- A step has a timeout, and the design does not give the timeout a source.

The design declares more than the support matrix uses:

- `Script` takes the ownership mode and the lease. The matrix has one mode, and the protocol of the matrix does not read the mode or the lease.
- Each action of the matrix has one wait condition, so a field for the condition allows only wrong pairs. The lapse of a lease completes each drop whose incarnation has ended, and the engine applies that rule.
- A transfer has the shard, the roles of both sides, a flag for a move of the writer and the three epochs. The protocol of the matrix does not read any of them.
- The design declares five protocols, and the matrix contains `break-before-make` alone.

These facts of the repository postdate the design:

- The phase of a transition is `shard.Phase`, a `uint8` enumeration. `protocol.Call` has only `add` and `drop`, and `ownership` does not declare a mode.
- Core v0.10.0 adds `fsm.Must`, which panics with the error of `Build` while the package initialises.

## Decision

We will build `handoff` and `handoff/breakbeforemake` with steps on the sides of a transfer and with only the protocol of the support matrix, because the side of a step then determines its node and its epoch.

The steps:

- A `Step` has an `Action`, a `Side` and a `Timeout`. `Action` is `Add`, `Drop` or `Commit`, and `Side` is `From`, `To` or `Fallback`.
- A step runs at the epoch that the core reserved for its side: the old epoch on `From`, the new epoch on `To` and the abort epoch on `Fallback`. A `Commit` with the zero `Side` commits the placement without a writer.
- The engine takes the node, its incarnation and its role from the endpoint of the side in the record of the transition.
- Each action has one completion condition:
  - An add completes when its callback succeeds.
  - A drop completes when its callback succeeds, or once the incarnation that it addresses has ended.
  - A commit completes when it applies.

The contract:

- A `Transfer` contains the node of each side, and an empty node is a side that the move does not have.
- `Protocol` has `Name`, `Version` and `Script(t Transfer) (Script, error)`. The engine records the name and the version in the transition.
- `Script.Validate` refuses a script without steps, a number of abort paths other than the number of steps, an action that is not valid, a step on a side without a node, an add or a drop without a positive timeout, and a commit with a timeout. Each problem wraps `ErrScript`.
- `Script` returns an error that wraps `ErrTransfer` for a transfer that the protocol cannot script. Both errors classify as `errs.Invalid`.
- `Lifecycle` is the result of `fsm.Must` for the table of the design over `shard.Phase` and `Event`. `Event` is a `uint8` enumeration that starts at 1.
- `handoff` does not have a conformance kit, because the contract has one implementation.

The protocol `break-before-make`:

- The scripts follow the table of the design. The script has the drop only for a transfer with a `From`. For a transfer without a `Fallback`, the abort path of the add is one commit without a writer.
- `Script` refuses a transfer without a `To`. It accepts a `From` and a `To` on one node, which are two incarnations of the process of the node.
- `Config` has `addTimeout` and `dropTimeout`. The factory of the built-in applies 30 seconds and 10 seconds as their defaults.
- `New` builds the scripts once, so `Script` does not allocate. The version is 1, and a change of a timeout does not change it.

## Alternatives Considered

### The steps of the design

In the design, a step has a node and an epoch. The core checks that both belong to the transfer.

Rejected because a commit step does not have a field for its writer. The check also accepts an epoch that two nodes then share.

### A wait condition on each step

In the design, a step has the wait condition `Ack`, `Serving`, `LeaseLapse` or `Published`.

Rejected because each action of the matrix has one condition. The tables of the design describe the lapse of the lease for each drop, and the engine applies that rule to every drop.

### The mode and the lease as arguments of Script

In the design, `Script` takes the ownership mode and the lease of the namespace.

Rejected because `ownership` does not declare a mode, and the protocol of the matrix does not read the lease.

### A build error beside the lifecycle

In the design, `Lifecycle` and the error of `Build` are two package variables.

Rejected because the package does not check the error.

## Consequences

**Positive:**

- A script cannot grant one epoch to two nodes, or use an epoch that the core did not reserve.
- A step does not have a node, so the script of a transfer depends only on the sides that the transfer has.
- `Script` and `Validate` do not allocate.

**Negative:**

- The protocol `elect` runs its steps on the new node at the old epoch, because the core does not grant epochs in the delegated mode. A design that adds `elect` needs a step whose epoch differs from the epoch of its side.
- A design that adds prepare calls, a peer or another wait condition adds them to `Step`.
- A protocol does not see the shard, the roles or the epochs of a move.
- A change of a timeout needs a new namespace, because a spec edit does not change the handoff configuration.
- sharder requires core v0.10.0.

**Neutral:**

- The lifecycle keeps the edges of the design.

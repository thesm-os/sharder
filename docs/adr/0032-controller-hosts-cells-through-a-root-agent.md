---
adr: 0032
title: A controller process hosts its cells through an agent of the root
status: Accepted
date: 2026-10-12
supersedes: none
superseded-by: none
rfc: RFC-0007, RFC-0009
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0032: A controller process hosts its cells through an agent of the root

## Status

Accepted

## Context

The root of a namespace places each cell on a controller process, as the controller of a cell places a shard on an application node. RFC-0007 makes `Controller` serve the callbacks of each root as a `protocol.AgentServer`. Its `Do` checks each callback "as an agent checks the callbacks of its cell, against the root's leader, this process's session in the root and the root's epochs". `Do` starts the controller of the cell of an add at the epoch of the request, and returns once the acquisition of the scope of the cell has applied. A drop returns once the controller of the cell has stopped. The RFC does not state how a process makes these checks.

The agent machine makes each of these checks for the callbacks of a cell. It keeps the result of each key and refuses a callback of another controller or of an earlier node incarnation. It starts a tenure only while the placements that it watches list it and cleans up a tenure that they no longer list. Milestone 0001 requires that a root add replayed across lost replies starts one cell controller per term. The kept results of the agent provide that.

ADR-0024 made the root refuse reports and disruptions.

## Decision

We will run one agent machine in a root mode for each namespace of a controller process. The agent already makes every check that RFC-0007 asks of `Do`. The root mode works as follows:

- The agent of the root joins the membership of the root scope with the node of the process. The capacity `cells` of that node is the cell capacity of the process.
- It watches the placements of the process in the root scope, and checks each callback of the root as the agent of a cell checks a callback of its controller.
- It does not send reports, because the root refuses them.
- It does not acquire a fence for an add. The handler of the controller starts the cell machine at the epoch of the request, and the acquisition of the scope of the cell at that term fences every older controller of the cell.
- The handler returns from an add once the cell machine replies to `Started`, and from a drop once the driver of the cell machine has returned.
- The agent of the root takes its settings from the spec of the namespace, as the agent of a cell does.

A controller process runs the root of each namespace, the agent of each root and the cell machine of each hosted cell. Each of these machines runs under a driver of its own, and `Run` starts the drivers in one task group.

## Alternatives Considered

### A host machine of its own

A small machine of the controller watches the placements of the process in the root scope, and starts and stops its cells.

Rejected because it repeats the checks of the agent for keys, epochs, incarnations and replacement snapshots. The property of the agent checks those over generated orders of events.

### A fence guard that does nothing

The agent of the root keeps its fence acquisition, and the controller gives the driver a guard whose acquisition returns at once.

Rejected because the guard reports an acquisition that it does not make. The acquisition of the scope of the cell is the fence, and it runs in the handler.

## Consequences

**Positive:**

- One implementation checks the callbacks of a cell and of a root.
- A replayed add of a cell returns its kept result and does not start a second controller.

**Negative:**

- The agent machine has a mode that changes three of its steps: the scope that it joins, its reports and the acquisition of an add.
- An outage of the store between a callback of the root and the acquisition of the cell delays the reply to the root by the retries of the acquisition, up to the timeout of the add.

**Neutral:**

- The controller ends its machines with `Stopped` when the context of `Run` ends, so the agent of a root does not run a `Close`.

## References

| What | Where |
|---|---|
| The controller | RFC-0007, The controller |
| The checks of a callback | `internal/machine/agent/callback.go` |
| The root refuses reports and disruptions | ADR-0024 |

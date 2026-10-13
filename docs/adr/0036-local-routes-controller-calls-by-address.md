---
adr: 0036
title: The local transport routes the calls of a controller by the address in the record of the cell
status: Accepted
date: 2026-10-13
supersedes: none
superseded-by: none
rfc: RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0036: The local transport routes the calls of a controller by the address in the record of the cell

## Status

Accepted

## Context

RFC-0008 declares the in-process transport of package `protocol/local`. `Network.AddController(s, srv)` makes `srv` the controller of the scope `s`, and a `Local` passes each call of a `ControllerClient` to the controller of the scope of its message.

The controller of a cell runs on the controller process where the root places the cell, and the root moves the cell to another process when that process stops. Every call of a `ControllerClient` passes the record of the cell, a `shard.Cell`. The record contains the address and the identity of the controller that has acquired the scope. A transport adapter dials that address, so its calls follow a cell that moves.

The examples of RFC-0010 run several controller processes in one OS process over `protocol/local`. Under the routing by scope, an example has to register the scope of each cell on the network, and register it again after each move of the cell.

## Decision

We will route each call of a `ControllerClient` of `protocol/local` to the controller process at the address in the record of the cell:

- `Network.AddController(address, srv)` makes `srv` the controller server of the process at `address`. A later call for the same address replaces `srv`.
- A `Local` passes a call of `Report`, `Submit`, `Disrupt` or `Done` to the server of the address in the `Controller` field of the record that the caller passes.
- A `Local` returns an error that wraps `protocol.ErrUnreachable` for a record without an address, and for an address without a server, as a transport adapter does for a process that it cannot reach.
- A callback keeps its routing by the namespace of the request and the node.

## Alternatives Considered

### The routing by scope of RFC-0008

Each example registers the scope of each cell, and registers it again after the root moves the cell.

Rejected because no transport adapter routes this way. Each example would contain code that only the in-process transport needs.

### The address first, and the scope when the record has no address

Rejected because one call would have two routes, and a test could pass on the route that a transport adapter does not have.

## Consequences

**Positive:**

- A report, a disruption and the end of a reservation reach the controller of a cell after the root moves the cell, as over a transport adapter.
- `protocoltest.AssertTransport` checks the local transport with the record of the controller that it already passes to every call.

**Negative:**

- A process registers its controller server under its address. Every caller of `AddController` changes.

**Neutral:**

- RFC-0008 keeps its declaration of `Local`, and this ADR replaces the routing of the calls of a controller.

## References

| What | Where |
|---|---|
| The declaration of the local transport | RFC-0008, The protocol |
| The record of a cell | `shard.Cell` |
| The catalogue of examples | RFC-0010 |

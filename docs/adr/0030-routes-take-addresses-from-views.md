---
adr: 0030
title: A route takes the address of each replica from the view of its cell
status: Accepted
date: 2026-10-12
supersedes: none
superseded-by: none
rfc: RFC-0007, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0030: A route takes the address of each replica from the view of its cell

## Status

Accepted

## Context

A route has one endpoint for each replica of a shard, with the node, the role and the address that a client dials. A placement has the node and the role of each replica, but not the address. The design of the router does not state where the address comes from.

A view of the membership of a cell lists each live node with the address that its process serves. The controller of a cell sends each callback to the node of its latest view, so a callback already takes its address from a view.

## Decision

We will make a route take the address of each replica from the latest view of the cell, because the views are the only record of the address of a node and the callbacks of the controller already take their addresses from them.

- The router watches the views of each cell, and keeps the address of each node of the latest view.
- A route has an endpoint only for a replica whose node the latest view lists.
- `Route` returns `ErrNoRoute`, an error of class `errs.Transient`, for the keys of a shard whose route has no endpoint, as for a shard without a placement.
- A new view changes the endpoints of the routes whose nodes it adds, removes or gives another address. The generation of each route does not change.

## Alternatives Considered

### A route keeps the last address of a node that has left the view

A replica whose node leaves the view keeps the endpoint from the last view that listed the node.

Rejected because a node that has left the view has no session in its cell. Under `stop`, its writer stops at the lease deadline, and the controller moves its shards to other nodes, so the old address does not serve the shard. Without an endpoint, the client gets an error of class `errs.Transient` and routes the key again.

## Consequences

**Positive:**

- A route and a callback dial the same address of a node.
- A route never lists a node whose session the view has ended.

**Negative:**

- From the end of the session of a writer until the commit that moves its shard, the keys of the shard have no route. Under `continue-fenced`, the writer can still serve in that time.
- A router keeps a watch of the views of each cell beside the watch of its scope.
- Each new view makes the router compare the endpoints of every route of the cell.

**Neutral:**

- In the support matrix, each placement has one replica, its writer, so a route has one endpoint.

## References

| What | Where |
|---|---|
| The endpoints of a route | `internal/machine/routes/cell.go` |
| The node of a callback | `internal/engine/engine.go`, `dispatch` |

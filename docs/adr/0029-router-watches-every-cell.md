---
adr: 0029
title: The router watches every cell of the cell map
status: Accepted
date: 2026-10-12
supersedes: none
superseded-by: none
rfc: RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0029: The router watches every cell of the cell map

## Status

Accepted

## Context

A router in watch mode keeps the routes of a cell from a watch of the scope of the cell. The router was specified to watch only the cells that it has routed to, so a router starts the watch of a cell at the first route into the cell.

The router runs as a state machine. `Route` runs on the goroutines of the application, outside the steps of the machine, and reads the routes that the machine publishes. It does not pass events to the machine. A watch that starts at the first route needs such an event: `Route` has to pass the miss of a route into an unwatched cell to the machine, and the machine then starts the reads of the cell.

Both rules watch the same cell in the support matrix, because a namespace there has one cell and a router routes into it from its first request.

## Decision

We will make the router watch every cell of the cell map from the read of the root scope, because `Route` then does not pass an event to the machine, and the router watches the same cell under either rule in the support matrix.

- The router reads the scope of each cell with one follower, and the views of each cell with one watch.
- The router starts the reads of a cell that the root adds to the cell map at once.
- The router replies to its start once every cell has its first snapshot and its first view.

## Alternatives Considered

### The router watches the cells that it has routed to

The router starts the reads of a cell at the first route into the cell, as specified.

Rejected because `Route` has to pass the miss of the first route into a cell to the machine as an event from outside a step, and that route then waits for the first snapshot of the cell. In the support matrix, the router routes into the only cell from its first request, so the rule does not save a watch.

## Consequences

**Positive:**

- `Route` does not pass an event to the machine. It reads the published table of routes with one atomic load per lookup.
- A router has every route of the namespace when its start returns, so its first request does not wait for a snapshot.

**Negative:**

- A router of a namespace with many cells keeps one follower and one watch of views per cell, also for a cell that it never routes into. The store serves each of those watches.
- The start of a router waits for the first snapshot and the first view of every cell.

**Neutral:**

- In the support matrix, a namespace has one cell, so a router keeps one follower and one watch of views for the cells of a namespace.

## References

| What | Where |
|---|---|
| The reads of the root scope and of each cell | `internal/machine/routes/discovery.go`, `internal/machine/routes/cell.go` |
| The lookup of a route | `internal/machine/routes/route.go`, `Machine.Route` |

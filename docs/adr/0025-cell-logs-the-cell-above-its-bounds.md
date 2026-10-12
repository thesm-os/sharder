---
adr: 0025
title: The controller of a cell logs the cell above its bounds
status: Accepted
date: 2026-10-12
supersedes: none
superseded-by: none
rfc: RFC-0002, RFC-0009
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0025: The controller of a cell logs the cell above its bounds

## Status

Accepted

## Context

The spec bounds the size of each cell with `cells.maxNodes` and `cells.maxShards`. By default a cell has at most 5,000 nodes and 1,000,000 shards. An operator decides what happens to a cell above a bound, because the support matrix does not split a cell.

We specified that the root reports a cell above either bound. The root does not have the sizes of a cell:

- The view of the root contains the controller processes that joined the root scope. The nodes of a cell join the scope of the cell.
- The assignment of the root has one placement per cell. The layout and the placements of the shards of a cell are in the scope of the cell.

The controller of a cell has both sizes. Its view contains the live nodes of the cell, and its layout contains the shards of the cell.

## Decision

We will make the controller of a cell log the cell above either bound, because the controller of the cell has both sizes and the root has neither.

- The controller checks both bounds after each event that it handles, with the bounds of the last spec that it applied.
- It logs a warning when its view rises above `cells.maxNodes`, and a warning when its layout rises above `cells.maxShards`.
- It logs each rise once. The controller logs the cell again when the cell falls below a bound and rises above it again.
- The root does not check the bounds.

## Alternatives Considered

### The root reports the cell

The root watches the membership and the layout of each cell, and reports a cell above either bound.

Rejected because the root then needs a watch of the membership and the layout of every cell to count what the controller of each cell already has. Without that watch, the root reads only the root scope.

## Consequences

**Positive:**

- The root reads only the root scope, whatever the number of nodes and shards in its cells.
- The check uses the view and the layout that the controller of the cell keeps for its passes.

**Negative:**

- To find the cells above their bounds, an operator reads the logs of the controller of each cell. No log of the root lists them.
- No instrument reports the sizes of a cell, so an alert on a bound has to match the log records.
- A cell whose controller does not run is not checked.

**Neutral:**

- In the support matrix, a namespace has one cell, so the logs of the controller of that cell cover the namespace.

## References

| What | Where |
|---|---|
| The check of the bounds | `internal/machine/cell/bounds.go`, `bound` |
| The sizes of a cell | `internal/control/controller.go`, `Size` |
| The bounds and their defaults | `config/spec.go`, `CellPolicy`, and `config/resolve.go`, `cells` |

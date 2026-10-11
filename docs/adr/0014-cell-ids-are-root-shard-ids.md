---
adr: 0014
title: The ID of a cell is the canonical text of its shard ID in the root
status: Accepted
date: 2026-10-11
supersedes: none
superseded-by: none
rfc: RFC-0009
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0014: The ID of a cell is the canonical text of its shard ID in the root

## Status

Accepted

## Context

Each cell of a namespace is a shard of the root, and the cell takes the ID of that shard. The core assigns the ID of a shard as a ULID of core's `id/ulid`, which `shard.ID` embeds. A `shard.CellID` is a string, because node labels and messages contain it as text. A placement in the root contains the ID of a cell as a ULID, and every other record contains it as a string.

## Decision

We will make the `CellID` of a cell the canonical text that `ulid.Format` returns for its shard ID in the root, because the cell map then follows from the placements of the root alone.

- The root assigns every cell ID. No operator of the support matrix writes one, because a node without a cell label joins the only cell of its namespace.
- The `cell` label of a node contains the same text.
- The store does not keep a record of the cells beside the placements of the root.

## Alternatives Considered

### Cell IDs that operators choose

Operators name the cells, and the root scope keeps a list of the cells with their names beside its placements.

Rejected because the operators of the support matrix do not name cells, and the list repeats what the placements of the root contain.

## Consequences

**Positive:**

- The cell map follows from the placements of the root, so a reader of the root does not need another record.
- A cell ID is unique within its namespace incarnation, as the ID of a shard is.

**Negative:**

- A cell ID is a ULID in text, which an operator reads in node labels and in messages instead of a chosen name.
- A design that lets operators name cells needs a list of cells in the root scope beside its placements.

**Neutral:**

- `shard.CellID` is still a string type, documented as the text of the shard ID of the cell.

## References

| What | Where |
|---|---|
| The core assigns shard IDs from core's identifiers | [ADR-0005](0005-core-assigns-shard-ids.md) |

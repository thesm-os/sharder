---
adr: 0007
title: Partitioners divide a range into spans, and the core assigns their IDs
status: Accepted
date: 2026-10-10
supersedes: none
superseded-by: none
rfc: RFC-0003, RFC-0009
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0007: Partitioners divide a range into spans, and the core assigns their IDs

## Status

Accepted

## Context

In the design, `partition.Partitioner` has four methods:

- `Position` maps a key to its position.
- `Mapping` returns the description of the function that computes the position.
- `Initial` returns the layout of a new cell, with an ID for each shard.
- `Reshape` proposes the next layout from the current layout and the load of each shard, and returns the splits and merges as changes between shard IDs.

The core assigns the ID of each new shard, so a partitioner does not know the IDs of new shards. Reshaping is outside the support matrix, and `Reshape` of `fixed-slots` and `singleton` returns `ErrFixedLayout`. A reshape also needs three inputs that `Reshape` does not receive:

- a split point from the reports of the agents
- the time of the last change of each shard
- a logical time

## Decision

We will reduce `partition.Partitioner` to `Position`, `Mapping` and `Initial`, and let `Initial` return the spans of a range without IDs, because a partitioner computes only the positions of keys and the ranges of shards.

```go
type Partitioner interface {
	Position(dst, key []byte) []byte
	Mapping() shard.Mapping
	// Initial returns the shards of a new cell whose range is r, in position order.
	Initial(r shard.Range) ([]Span, error)
}

// Span is a shard that a partitioner proposes. The core gives it an ID when it
// commits its first placement.
type Span struct {
	Range  shard.Range
	Labels tag.Tags
}
```

- The spans of `Initial` cover `r` without gaps or overlaps. A partitioner that cannot divide `r` returns an error that wraps `partition.ErrRange`.
- `Initial` divides the position space, and every range whose start and end are bounds of the spans of the position space, because a cell starts and ends at the bounds of shards. The conformance kit of partitioners checks both.
- `Layout` lists the shards of a cell with the IDs that the core assigned, and `Layout.Locate` returns the ID of the shard whose range contains a position.
- `Reshape`, `Stats`, `Change`, `ChangeKind` and `ErrFixedLayout` are not declared.

## Alternatives Considered

### The interface of the design

`Initial` returns a `Layout` with IDs, and `Reshape` remains in the interface, so that a partitioner that reshapes does not need a change of the contract.

Rejected because a partitioner cannot assign the IDs that the core assigns, and every partitioner of the support matrix would implement `Reshape` only to refuse it.

## Consequences

**Positive:**

- A partitioner is a pure function of keys and ranges, without IDs and without a method that it refuses.
- `fixed-slots` and `singleton` implement every method of the contract.

**Negative:**

- A partitioner that reshapes needs a new method of the contract, which a decision about resharding declares with the inputs that a reshape needs.
- The core joins the spans of a partitioner with the IDs that it assigns before it commits a placement.

---
adr: 0005
title: The core assigns shard IDs from core's identifiers
status: Superseded
date: 2026-10-10
supersedes: none
superseded-by: ADR-0018
rfc: RFC-0001, RFC-0003, RFC-0004, RFC-0006, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0005: The core assigns shard IDs from core's identifiers

## Status

Superseded by [ADR-0018](0018-shard-ids-are-128-bit-identifiers.md)

## Context

In the design, `shard.ID` is a string, and the partitioner derives the ID of a shard from the shard's address:

- `fixed-slots` gives a shard the number of its slot, from `"0"` to the slot count minus one.
- `hash-range` gives a shard the bounds of its range in hexadecimal.
- `explicit` gives a shard the name that the application configures.

An address does not identify one shard across a reshard. A split of slot 0 gives one child the ID `"0"`, which the child's own fence acquisition retires. A split followed by a merge creates a retired range ID again. The fence key of a shard is its namespace incarnation and its ID, so a reused ID gives a new shard the fence record of a retired one.

A string type also does not keep a shard ID apart from other strings. The namespace incarnation already uses core's `id.ID`, embedded in a struct type of its own.

## Decision

We will declare `shard.ID` as `struct{ id.ID }` and let the core assign the ID of each new shard with core's `id/ulid`, because an ID that the core never assigns twice identifies one shard across every reshard.

- The core assigns the ID when it commits the first placement of the shard. The ID of a shard never changes, and no later shard of the namespace incarnation gets it.
- A partitioner returns the ranges of the shards of a layout, without IDs. The slot or the range of a shard is in the `Range` of its placement.
- `explicit` turns the configured names into labels of the shards.
- The planner sorts shards with `id.ID.Compare`, which orders ULIDs by their creation time. The repair cursor examines shards in the same order.

## Alternatives Considered

### Strings that the partitioner derives

The partitioner derives each ID from the shard's address, as in the design. Every process computes the same IDs without the store. An operator reads a slot number as the ID of a `fixed-slots` shard.

Rejected because a derived ID repeats after a reshard. A string type also lets a shard ID take the place of any other string.

## Consequences

**Positive:**

- A reshard does not need a rule that gives the new shards new IDs.
- The compiler rejects a namespace incarnation, a node ID or a cell ID where a shard ID belongs.
- A shard ID is a comparable value of fixed size, so a map keyed by shard ID and a fence key do not allocate for the ID.

**Negative:**

- An operator sees a ULID where the design showed a slot number, such as `"17"`.
- `Partitioner.Initial` and `Partitioner.Reshape` return ranges, so the API of package `partition` differs from the design.
- An application of `explicit` cannot choose the IDs of its shards. It finds a shard by a label.

**Neutral:**

- A shard ID encodes as the 16 bytes of a ULID, through the binary methods of `id.ID`.

## References

| What | Where |
|---|---|
| go.thesmos.sh/core v0.8.0, `id/id.go`, `id/binary.go` and `id/ulid` | https://github.com/thesm-os/core/tree/v0.8.0/id |

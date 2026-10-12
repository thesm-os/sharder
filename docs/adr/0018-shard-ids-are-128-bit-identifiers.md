---
adr: 0018
title: The core assigns shard IDs as 128-bit identifiers of core
status: Accepted
date: 2026-10-11
supersedes: ADR-0005
superseded-by: none
rfc: RFC-0001, RFC-0003, RFC-0004, RFC-0006, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0018: The core assigns shard IDs as 128-bit identifiers of core

## Status

Accepted

## Context

`shard.ID` is `struct{ id.ID }`, and the core assigns every shard ID with core's `id/ulid`. Core's `id.ID` stores identifiers of 128, 160 and 256 bits in one type. It consists of an array of 32 bytes and a size byte. A ULID has 16 bytes, so 17 of the 33 bytes of every shard ID are always zero.

A map keyed by `id.ID` hashes and compares all 33 bytes on each access, and each slot of the map has room for all 33. The input of the planner, its plans, the index of its state and the scopes of the memory store key maps by shard ID.

An issue in core measured maps of 1,000,000 random 128-bit identifiers. A map keyed by `id.ID` took 67.7 MB of heap, and a map keyed by an array of 16 bytes took 28.6 MB. A lookup in the first map took 215 to 229 ns, and a lookup in the second took 173 to 186 ns. In a CPU profile of one pass of the planner over 1,000,000 shards, `runtime.mapaccess2` took 0.23 s of the 0.45 s that the profile counted in `Place`.

Core v0.12.0 adds `id.ID128`. It is an array of 16 bytes with the methods `ID`, `Compare` and `String`. `id.ID.As128` converts an `id.ID` of 16 bytes into an `id.ID128`, and `ID128.ID` converts it back.

## Decision

We will declare `shard.ID` as `struct{ id.ID128 }`, because a map keyed by shard ID then hashes and compares the 16 bytes of a ULID instead of the 33 bytes of an `id.ID`.

The assignment does not change:

- The core assigns the ID of each new shard with core's `id/ulid` when it commits the first placement of the shard. The ID of a shard never changes, and no later shard of the namespace incarnation gets it.
- A partitioner proposes the shards of a layout without IDs. `explicit` turns the names that the application configures into labels of its shards.
- The zero ID is no shard. The core never assigns it, because a ULID starts with the millisecond of its creation.

The type:

- `ID.Compare` orders IDs by their bytes, and `id.ID.Compare` orders the same identifiers the same way. The planner sorts shards in this order, and its repair cursor examines them in the same order.
- An ID encodes as the 16 bytes of its ULID, and the zero ID encodes as no bytes. The binary methods of `id.ID` write the same bytes for the same identifier.
- `UnmarshalBinary` refuses the 20 or 32 bytes of a wider identifier, and it refuses 16 zero bytes. Each error wraps `shard.ErrID`, and `ErrID` classifies as `errs.Invalid`.
- `ID.ID` returns the identifier as an `id.ID`. Code passes that value to `ulid.Format` and to every other API of core that takes an `id.ID`.

## Alternatives Considered

### Core's id.ID

`shard.ID` embeds `id.ID`, so a shard ID can contain any identifier of core.

Rejected because the core assigns only ULIDs. Every map keyed by shard ID then hashes and compares 17 bytes that are always zero.

### Maps keyed by the 16 bytes of an id.ID

The issue in core proposed a method `id.ID.Key128` that returns the 16 bytes of a 128-bit identifier as an array. `shard.ID` keeps `id.ID`, and each map keyed by shard ID takes the array as its key.

Rejected because each map then converts its key at every access, and the type of its key differs from the type of the shard IDs that the API passes. Core added `id.ID128` with `Compare` and `String` instead of `Key128`.

## Consequences

**Positive:**

- One pass of the planner over 1,000,000 shards takes 22% to 24% less time and allocates 38% fewer bytes. The pass returned the same plans before and after the change.
  - Under `exclusive-worker` a pass took 395.2 ms instead of 504.6 ms, and it allocated 178.6 MiB instead of 286.0 MiB.
  - Under `failed-node` a pass took 332.3 ms instead of 438.0 ms, and it allocated 175.1 MiB instead of 280.7 MiB.
- A shard ID takes 16 bytes in every struct, map slot and slice that contains one, instead of 33.
- The encoding does not change, so every encoding that a store or a peer recorded decodes to the same ID.

**Negative:**

- A shard ID cannot contain an identifier of 160 or 256 bits. A design that assigns shard IDs with another generator, such as a KSUID generator, needs a new type and a new rule for the encoding.
- `UnmarshalBinary` refuses 16 zero bytes. `id.ID` decodes the same bytes as an identifier.
- A test that numbers its shards starts at one, because the ID whose 16 bytes are all zero is the zero ID. `placement.NewState` refuses a layout that contains it.
- Code that passes a shard ID to an API of core that takes `id.ID` converts the ID with `ID.ID`.
- sharder requires core v0.12.0.

**Neutral:**

- For an assigned ID, `String` returns the text that it returned before: `id:` and the 32 hexadecimal digits of the 16 bytes. For the zero ID it returns `id:` and 32 zeros. It returned `id:` before.

## References

| What | Where |
|---|---|
| The issue in core that measured maps keyed by `id.ID` against maps keyed by 16 bytes | https://github.com/thesm-os/core/issues/26 |
| go.thesmos.sh/core v0.12.0, `id/id.go` and `id/fixed.go` | https://github.com/thesm-os/core/tree/v0.12.0/id |
| `BenchmarkPlanner` with `-bound` at ef832a0 and on the change, 3 alternating rounds of 3 runs per side, `GOMAXPROCS=4`, compared with benchstat | `placement/planner/planner_test.go` |

---
rfc: 0003
title: Partitioning strategies and resharding
author: Roy Klopper
status: Accepted
created: 2026-09-27
updated: 2026-10-10
discussion: none
supersedes: none
superseded-by: none
produces-adr: tbd
research: Research-0001, Research-0002, Research-0003
related: RFC-0001, RFC-0002, RFC-0004, RFC-0006, RFC-0009
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# RFC-0003: Partitioning strategies and resharding

## Summary

A partitioner maps an application key to a position, an ordered byte string, and defines the shards of each cell as contiguous ranges of positions, called the cell's layout. Every lookup in every strategy is the same algorithm: compute the key's position, then find the shard whose range contains it. We propose one `Partitioner` interface and five built-in strategies:

- `fixed-slots` hashes keys into a fixed number of slots.
- `hash-range` hashes keys into a 64-bit space whose ranges split and merge with load.
- `key-range` keeps keys in order in ranges that split and merge with size and load.
- `explicit` takes the application's list of shards.
- `singleton` has one shard.

The support matrix contains `fixed-slots` and `singleton`. In both, the mapping from keys to positions is fixed when the root creates the namespace: the partitioner, the slot count, the hash function and its version, and hash tags. A hash function has a version, and every controller and router compares the version it runs with the one the namespace recorded. The other three strategies and resharding are designs outside the matrix. A split or merge would produce a new layout version and leave the data on the nodes that already store it, with the new writer's fence acquisition retiring each parent, so that a paused writer of a parent cannot write into a child.

## Motivation

The key-to-shard mapping is the choice on which published systems disagree most directly, and each disagreement has a use case behind it:

- Slicer hashes every key into a 63-bit space so that adjacent hot keys spread out, and the framework splits and merges ranges without the application. SM rejects this because hashing destroys key locality, and one of its applications serves prefix scans as 9% of its queries.
- SM found static sharding, a fixed mapping from keys to a numbered set of shards, about three times as common as consistent hashing across Facebook's applications. Resharding was rare enough that a fixed shard count was acceptable.
- Range-partitioned databases keep keys in order and split a range when it grows past a size or load threshold.
- Some applications already know their shards, such as one per tenant or one per region, and want the library to place them, not to compute them.

A library with one mapping serves one of these groups. The partitioner is also where a generic library can break correctness quietly. When a range splits while its writer is paused, the writer resumes with a token for the parent shard and can write keys that now belong to a child. A mapping that changes while processes disagree about it routes one key to two shards. A partitioner therefore needs a mapping that every process computes identically, and a resharding protocol that closes the stale-writer window for every strategy.

At the target scale, one namespace spans many cells, and each cell holds up to 1,000,000 shards by default. The key-to-shard mapping therefore has to divide the key space into cells as well as into shards, with one lookup structure for both.

## Detailed design

### Interface

```go
// Package partition maps application keys to positions and defines the shards of each
// cell as ranges of positions.
package partition

import (
 "errors"

 "go.thesmos.sh/core/epoch"
 "go.thesmos.sh/core/errs"
 "go.thesmos.sh/core/tag"
 "go.thesmos.sh/sharder/shard"
)

// ErrFixedLayout is returned by Reshape for strategies whose shard set never changes.
var ErrFixedLayout = errors.New("partition: layout is fixed")

// ErrNoShard is returned by Layout.Locate when no shard covers a position. Only the
// explicit strategy leaves positions uncovered. It classifies as errs.NotFound.
var ErrNoShard = errs.WithClass(errors.New("partition: no shard covers the position"), errs.NotFound)

// Layout lists the shards of one cell in position order, at one Version, which
// increases with every change to the set or to the shards' ranges. The shards' ranges
// do not overlap and lie within the cell's range.
type Layout struct {
 Cell    shard.CellID
 Version epoch.Epoch
 Shards  []Shard
}

// Locate returns the shard whose range contains pos. It searches Shards by binary
// search and does not allocate. It returns ErrNoShard when no range contains pos.
func (l Layout) Locate(pos []byte) (shard.ID, error)

// Shard describes one shard in a layout.
type Shard struct {
 ID    shard.ID
 Range shard.Range
 // Labels contains attributes that placement goals and constraints read, such as a
 // tenant or a preferred region. The partitioners of the support matrix set none.
 Labels tag.Tags
}

// Stats describes the load of one shard and its proposed split point, for Reshape.
type Stats struct {
 Load shard.Load
 // SplitAt is the position that the shard's writer proposes as a split point, such
 // as the position of the median key. Only key-range partitioners read it. It may
 // be nil.
 SplitAt []byte
}

// ChangeKind names a layout change.
type ChangeKind string

const (
 Split ChangeKind = "split"
 Merge ChangeKind = "merge"
)

// Change records one split or merge between two consecutive layouts.
type Change struct {
 Kind ChangeKind
 From []shard.ID // one shard for a split, two or more adjacent shards for a merge
 To   []shard.ID
}

// Partitioner maps keys to positions and defines the shards of each cell.
// Implementations are deterministic: the same key yields the same position, and the
// same inputs yield the same layout, in every process and on every platform.
// Implementations must not perform I/O.
type Partitioner interface {
 // Position appends the position of key to dst and returns the extended slice. It
 // does not allocate when dst has room for the position.
 Position(dst, key []byte) []byte
 // Mapping returns what decides Position: the partitioner's registered name, its
 // configuration, and the name and version of its hash function. The factory sets
 // it when it builds the partitioner.
 Mapping() shard.Mapping
 // Initial returns the layout of a new cell whose range is r.
 Initial(cell shard.CellID, r shard.Range) (Layout, error)
 // Reshape proposes the next layout from the current layout and per-shard stats.
 // It returns the current layout and no changes when no split or merge is due, and
 // ErrFixedLayout when the strategy never changes its shard set. The new layout
 // covers the same cell range as the current one.
 Reshape(layout Layout, stats map[shard.ID]Stats, nodes int) (Layout, []Change, error)
}

// Hash is a 64-bit hash of keys. A partitioner's configuration names its hash, and the
// registry builds it under the kind "hash".
type Hash interface {
 // Sum64 returns the hash of key. It returns the same value for the same key in every
 // process and on every platform, and does not allocate.
 Sum64(key []byte) uint64
 // Version identifies the function that Sum64 computes. It increases whenever Sum64
 // returns another value for any key, so a registered name and a version identify
 // one function.
 Version() uint32
}
```

`Position` and `Layout.Locate` are on the request path of every router, so their cost matters. We measured a binary search over 1,000,000 sorted ranges with 16-byte starts at 241 to 245 ns per lookup, without an allocation, at 24 bytes per entry plus the bytes of each start. A `fixed-slots` router does not search: a position is a slot number, which indexes an array of routes.

### Built-in strategies

| Strategy | Position of a key | Shards | Shard IDs | Reshapes | Keeps key order | Default configuration | In the support matrix |
|---|---|---|---|---|---|---|---|
| `fixed-slots` | `hash(key) mod slots`, as 4 bytes big-endian | One per slot | `"0"` to `"slots-1"` | No | No | 1024 slots, `xxhash64`, hash tags off | Yes |
| `singleton` | The empty string | One, covering every position | `"0"` | No | Yes | none | Yes |
| `hash-range` | `hash(key)`, as 8 bytes big-endian | Ranges of the 64-bit space | Hexadecimal range bounds, such as `"4000000000000000-8000000000000000"` | Yes, on load | No | 16 initial shards, `xxhash64`, split and merge thresholds below | No |
| `key-range` | The key | Ranges of keys | Generated at each split, stable afterwards | Yes, on size and load | Yes | One initial shard, split at 512 MiB or on load | No |
| `explicit` | The key | The configured prefixes and ranges. A prefix p covers every key that starts with p | Configured by the application | Only by a spec edit that adds a shard | Yes | none, the list is required | No |

#### fixed-slots

```json
{"slots": 1024, "hash": "xxhash64", "hashTags": true}
```

With `hashTags` on, only the bytes between the first `{` and the first `}` after it are hashed, when at least one byte lies between them. Keys `user:{42}:cart` and `user:{42}:orders` then map to the same slot. Redis Cluster uses the same rule, so applications that already group keys this way keep their groups.

Every field of the configuration decides a key's position, so the whole configuration is part of the namespace's mapping. The slot count, the hash function and hash tags are fixed for the life of the namespace incarnation, and `Change` rejects an edit of any of them. A namespace that needs another slot count migrates to a new namespace. A namespace chooses its slot count for its largest deployment: every slot is one entry of each router's route array, so 1,024 slots cost a router about 8 KiB of pointers.

The shard set of `fixed-slots` never changes, so a shard's ID remains its slot number for the life of the namespace incarnation, and no shard is ever retired.

Neither `fixed-slots` nor `singleton` labels a shard. A constraint that requires a shard label, such as `match-label`, therefore rejects every node for every shard in the support matrix, and each shard remains unplaced with the constraint as the reason.

#### singleton

`singleton` maps every key to the empty position and has one shard, `"0"`, whose range is every position. Its mapping has no hash function. Leader election uses it: the shard's one writer is the leader.

#### hash-range

`hash-range` is outside the support matrix.

```json
{
  "initialShards": 16,
  "hash": "xxhash64",
  "split": {"aboveMeanLoad": 2.0, "maxShardsPerNode": 150},
  "merge": {"belowMeanLoad": 1.0, "minShardsPerNode": 50, "maxKeyspaceFraction": 0.01},
  "metric": "cpu",
  "cooldown": "5m"
}
```

`Reshape` splits a range at its midpoint when its load is at least `aboveMeanLoad` times the mean shard load and the cell has fewer than `maxShardsPerNode` shards per node. It merges two adjacent ranges when their combined load is below `belowMeanLoad` times the mean, while the cell has more than `minShardsPerNode` shards per node, until the merged ranges in one pass cover `maxKeyspaceFraction` of the key space. The defaults are the constants Slicer uses: split at twice the mean below 150 slices per task, and merge above 50 slices per task with at most 1% of the key space moving per adjustment. Slicer's authors chose them by observation and did not measure the system's sensitivity to them. A shard is not split or merged again within `cooldown` of its last change.

#### key-range

`key-range` is outside the support matrix.

```json
{
  "initialSplits": ["g", "n", "t"],
  "split": {"aboveBytes": 536870912, "aboveLoad": {"metric": "qps", "value": 2500}},
  "merge": {"belowBytes": 134217728},
  "cooldown": "10m"
}
```

The writer of each shard reports the shard's size as the `bytes` load metric and proposes a split position in `Stats.SplitAt`, because only the application knows the distribution of its keys. `Reshape` splits at the proposed position when the shard is above either threshold, and merges two adjacent shards when both are below `belowBytes` and their combined load is below half of the split threshold. The defaults come from CockroachDB. A CockroachDB range splits at 512 MiB. Its merge queue merges only neighbours below 128 MiB whose merged load is below half the load-split threshold, so that a merged range does not split again at once.

#### explicit

`explicit` is outside the support matrix.

```json
{
  "shards": [
    {"id": "eu", "prefix": "eu/", "labels": {"region": "eu-west-1"}},
    {"id": "us", "prefix": "us/", "labels": {"region": "us-east-1"}},
    {"id": "archive", "range": {"start": "z", "end": ""}}
  ]
}
```

Each shard has either a key prefix or a key range. Prefixes and ranges must not overlap, and `Layout.Locate` returns `ErrNoShard` for a key that matches neither. Adding a shard is a spec edit, which needs rules for publishing the new layout to every router before the matrix can include it. Removing a shard or narrowing its prefix or range is a resharding operation, because keys move. An application whose mapping is more than prefixes and ranges, such as a lookup table, registers its own partitioner.

### Hash functions

A hash function is part of the mapping, because two processes that disagree on it route the same key to different shards. The one built-in, `xxhash64`, is XXH64 with seed 0 over the key bytes, at version 1. It calls `Sum64` of `github.com/cespare/xxhash/v2` v2.3.0, which computes the digest with a zero seed, in assembly on amd64 and arm64. The module has no dependencies of its own.

The namespace's mapping records the hash function's registered name and its version. Every controller and every router compares both with the hash function it runs when it starts, and refuses the namespace with an error of class `errs.Unsupported` when either differs. A process of a release whose hash function returns other values therefore refuses the namespace instead of routing keys to other shards. A registered hash function whose values change for any key increments its version, and a namespace cannot move to the new version by a spec edit.

In its conformance test, `xxhash64` passes the XXH64 vectors of xxHash's reference implementation, from the table `XSUM_XXH64_testdata` of its sanity check, with seed 0. The xxHash specification itself contains no test vectors. Every other hash function passes fixed vectors of its own, and `fixed-slots` passes vectors of keys and slots, with and without hash tags, so that a change to its encoding of positions also fails a test.

An application that migrates with an existing key-to-slot mapping registers that mapping's hash as a `partition.Hash` of its own. With Redis Cluster's CRC16 and 16,384 slots, `fixed-slots` with hash tags on reproduces Redis Cluster's slot of every key. With Kafka's murmur2, masked to 31 bits, and one slot per partition, it reproduces Kafka's default partition of every keyed record.

We measured four candidates over 1,048,576 keys in 1,024 buckets: XXH64 through `cespare/xxhash` v2.3.0, XXH3 through `zeebo/xxh3` v1.1.0, and FNV-1a and CRC-32C from the standard library. The balance columns give chi-square per degree of freedom: 1.0 is uniform, and the 99.9% bound is about 1.14. The speed column is one core of a Ryzen 9 9950X3D, with Go 1.27.1:

| Hash | Top 10 bits, keys `user:<n>` | Top 10 bits, 8-byte counters | ns per key of 16, 64 and 256 bytes |
|---|---|---|---|
| XXH64 | 1.02 | 1.00 | 2.6, 5.6, 11.5 |
| XXH3, 64-bit | 1.01 | 1.02 | 2.2, 2.9, 11.3 |
| FNV-1a, 64-bit | 73.3 | 3,680 | 4.8, 27.6, 163 |
| CRC-32C | 0.05 | 0.00 | 4.0, 5.3, 11.2 |

`hash-range` splits the 64-bit space by its top bits, so FNV-1a would put most sequential keys into a few shards. CRC-32C is linear, and its 32 bits cannot address the 64-bit space of `hash-range`. XXH64 and XXH3 are both uniform. The 3 ns between them is about 1% of a route lookup, so the library uses XXH64, the simpler algorithm. `zeebo/xxh3` v1.1.0 also requires `klauspost/cpuid/v2` and `golang.org/x/sys`, and `cespare/xxhash` requires nothing.

The Go runtime's `hash/maphash` is excluded because its seed differs between processes. Core's `crypto.Hasher` implementations are cryptographic hashes, and the routing path needs neither their collision resistance nor their cost.

### Layouts and cells

Each cell covers a contiguous range of positions and has its own layout. Every shard's range lies inside its cell's range. A namespace starts with one cell whose range is every position, and its layout is the partitioner's `Initial` for that range. In the support matrix, the namespace keeps that one cell and that one layout.

The store keeps each shard's range in the shard's placement, and the layout version in the cell's state. A layout of any size is therefore stored in bounded keys, and no single value contains a whole layout.

The initial placement of a layout can span more than one commit. The core commits the placements of the `Initial` layout in as many generations as the store's commit limits require, and each generation is a valid assignment. A shard whose placement is not committed yet is unplaced: the root reports it, and a router returns an error of class `errs.Transient` for its keys, as for a shard without a serving writer. A layout of 1,024 slots takes about 35 commits at 30 placements per commit.

### Resharding, outside the support matrix

The support matrix contains no partitioner that reshapes, so no namespace in it splits or merges a shard. `Resolve` rejects every strategy that provides `reshapes`, and `Submit` rejects a proposal of the kind `reshard` with an error of class `errs.Unsupported`. This section records the design and the changes it needs before the matrix can include it.

The design applies a split or merge in one generation of the cell, so that no router sees a layout whose placements are missing:

1. The planning pass calls `Reshape` with the current layout and per-shard stats.
2. For each change, the core places every new shard on the nodes and roles of the shard it comes from. For a merge, the placer first co-locates the parents on the same nodes. A merge whose parents are not co-located waits until they are.
3. The core commits the new layout version and the new placements in one generation. Each new shard's epoch is one higher than the highest epoch among its parents.
4. The core instructs each node to add the new shards and drop the parents, through the namespace's handoff protocol. A node whose handler implements the agent's optional `Resharder` interface receives one call per change, so that it can re-key local data in one step. Without it, the node receives `AddShard` for each new shard and then `DropShard` for each parent, and moves its data inside those calls.
5. The new writer's fence acquisition retires the parents. The instruction for a new shard lists the parents in `Supersedes`, and the application's fence raises each parent's watermark to `fence.Retired` in the same transaction as the acquisition. `fence.Retired` is above every epoch that the core issues, so fenced storage rejects every later write under a parent's token with `epoch.ErrFenced`, and the parent's writer abdicates.

Step 5 closes the stale-writer window. Suppose a writer of the parent pauses before step 4 and resumes after step 5. Its token refers to the parent, whose watermark is above the token's epoch, so its write fails. Without the retirement, the parent's watermark would still equal the old epoch. The write would then succeed on keys that belong to a child.

The design needs four changes before the support matrix can include it:

- **Fresh shard identities.** The built-in partitioners derive a shard's ID from its address: `fixed-slots` from the slot number, and `hash-range` from the range bounds. Doubling the slot count would give the child of slot 0 the ID `"0"`, which step 5 retires in the acquisition that makes it writable, and a split followed by a merge recreates a retired range ID. The core has to give every new shard an ID that no earlier shard of the namespace incarnation used, and keep each shard's slot or range in a separate field.
- **Mapping changes across cells.** Multiplying the slot count by k sends the keys of slot i to the slots i, i+n, …, i+(k-1)n, where n is the old count, and slot i+n can lie in another cell. The inherited placement of step 2 would then put a shard on nodes outside its cell. A change of the slot count is a migration of keys between cells, which the cell design does not provide, or a migration to a new namespace. Elasticsearch, which also routes by modulo, splits an index only into a new index, from a source that is read-only, with a shard count that is a multiple of the source's.
- **Bounded publication.** A change with more placements than one commit admits cannot be published in one generation. The design needs either reshape operations that each fit in one commit, or staged descriptors behind a small activation record that one commit switches.
- **Split inputs.** `Reshape` receives no split point from the agents' reports, no durable record of when a shard last changed, and no logical time. It cannot apply `key-range`'s split positions, or keep a cooldown across a restart of the controller.

In a namespace with `delegated` ownership, also outside the support matrix, the application's authority splits and merges shards instead of the partitioner, so the core does not call `Reshape`. An agent reports each shard that the authority created with its range and its parents. The core applies the change in one generation once the reported children cover their parents' ranges exactly. Each child keeps the epoch that the authority granted, and its first fence acquisition retires its parents, as in step 5. A partitioner whose layout the spec fixes, such as `explicit`, can match an authority's initial layout, but it cannot follow a split without a spec edit. The application registers a partitioner that takes its initial layout from the authority and leaves every later change to the agents' reports.

## Alternatives considered

### Consistent hashing as the only mapping

Map keys straight to nodes on a hash ring, as Dynamo and Cassandra do. There is no shard entity to manage, and adding a node moves only about 1/N of the keys.

**Why not:** the library needs a unit to fence, to move with its state, to report load for and to place under constraints. A ring has no such unit, only the arcs between tokens. Dynamo's authors compared three partitioning strategies and found that fixed, equal-sized partitions balanced load best and let a partition move "as a unit by simply transferring the file". This proposal keeps consistent hashing where it helps, as function placers that map shards to nodes.

### A lookup method per strategy

Give each partitioner a `Locate(layout, key)` method, so that `fixed-slots` computes its shard by arithmetic and the range strategies search.

**Why not:** cells divide the same space that shards divide, and routers cache parts of layouts. With one position space, the cell map, a layout and a router's cache share one search over sorted ranges. A `fixed-slots` router still indexes slot positions directly, because a slot's position is its index.

### Application-defined shards only

Let every application list its shards, as SM does. The library places what it is given and never computes a mapping.

**Why not:** caches, actor systems and queues do not want to maintain shard lists, and each of them would write the same hash-to-slot code. `explicit` keeps this option for the applications that want it.

### Hashed ranges only

Hash every key and split ranges on load, as Slicer, Dicer and Splitter do. The framework balances load without application input, and hot keys spread out.

**Why not:** hashing removes key order. Range scans and prefix queries then touch every shard. SM rejected hashing for this reason, for an application whose queries are 9% prefix scans. `hash-range` is one strategy among five here.

### A directory of key locations

Store the location of each key or small key group in a directory, as Facebook's Akkio does with micro-shards and Vitess does with lookup indexes. Placement becomes arbitrarily flexible.

**Why not as a built-in:** the directory is itself a sharded, replicated datastore on the request path, and a partitioner performs no I/O. An application with a directory resolves a key to its micro-shard first and routes the micro-shard's ID, which `key-range` or `explicit` then maps to a shard.

### Hash functions identified by name alone

Record only the hash function's registered name in the mapping, and rely on the conformance vectors to keep the function unchanged across releases.

**Why not:** the vectors catch an accidental change in a test run, but a deliberate change under the same name would still be deployed. With a version, a process of a release whose function returns other values refuses the namespace when it starts, instead of routing keys to other shards.

## Drawbacks

- Five built-in strategies are about 1,500 lines of code by our estimate, plus the conformance suite each must pass. The support matrix contains two of them.
- In the support matrix, `fixed-slots` never changes its slot count. A namespace that starts with too few slots for its largest deployment migrates to a new namespace. Too many slots add one route to each router's array per slot.
- A hash function's implementer must increment its version on any change of its values. The version check does not detect a missed increment, and the conformance vectors do.
- Resharding, outside the matrix, adds a path that runs rarely and must be correct the first time: co-locating merge parents, a new layout and new placements in one generation, and retirement of parent tokens. Each application's fence must raise the parents' watermarks in the same transaction as the acquisition. A fence that gets this wrong reopens the stale-writer window.
- `key-range`, outside the matrix, depends on the application to report sizes and split positions. An application that reports neither never splits.
- A router of a range partitioner in watch mode keeps the layouts of the cells it watches. A sorted index of 1,000,000 ranges takes 24 bytes per entry, measured, plus the bytes of each start: about 40 MB with 16-byte starts.

## Open questions

None.

## Unresolved and future work

- Resharding is outside the support matrix. It needs fresh shard identities, a migration protocol for mapping changes across cells, bounded publication, and split inputs before the matrix can include it.
- `hash-range`, `key-range` and `explicit` are outside the support matrix.
- Labels on ranges of slots in `fixed-slots`, which would give the label constraints and goals of placement a source in the support matrix, are not proposed here.
- Detecting a hot key and splitting it into a shard of its own, as Dicer does, is not proposed here. `hash-range` splits a hot range only at its midpoint.
- A directory-based built-in partitioner is not proposed here.
- Moving a shard from one cell to another is not proposed here. Cells change by splitting at a shard boundary.

## References

| What | Where |
|---|---|
| Adya et al., "Slicer: Auto-Sharding for Datacenter Applications", OSDI '16, §4.4.1 | https://www.usenix.org/system/files/conference/osdi16/osdi16-adya.pdf |
| Lee et al., "Shard Manager", SOSP '21, §2.2.1 and §3.1 | https://dl.acm.org/doi/10.1145/3477132.3483546 |
| DeCandia et al., "Dynamo", SOSP '07, partitioning strategies | https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf |
| Redis Cluster specification, hash slots and hash tags | https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/ |
| Apache Kafka, `BuiltInPartitioner.partitionForKey` and `Utils.toPositive` | https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java |
| Elasticsearch split index API | https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-split |
| CockroachDB range size and splits | https://www.cockroachlabs.com/docs/stable/architecture/distribution-layer |
| Annamalai et al., "Sharding the Shards: Managing Datastore Locality at Scale with Akkio", OSDI '18 | https://www.usenix.org/conference/osdi18/presentation/annamalai |
| xxHash specification, XXH64, version 0.2.0 | https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md |
| cespare/xxhash v2.3.0, `Sum64` | https://github.com/cespare/xxhash |
| xxHash reference implementation, `XSUM_XXH64_testdata` in `cli/xsum_sanity_check.c` | https://github.com/Cyan4973/xxHash/blob/dev/cli/xsum_sanity_check.c |
| Our measurement of sorted slices and B-trees, 2026-09-27 | Research-0003 |

---
rfc: 0003
title: Partitioning strategies and resharding
author: Roy Klopper
status: Draft
created: 2026-09-27
updated: 2026-09-27
discussion: none
supersedes: none
superseded-by: none
produces-adr: tbd
research: Research-0001, Research-0002, Research-0003
related: RFC-0001, RFC-0002, RFC-0004, RFC-0006, RFC-0009
---

# RFC-0003: Partitioning strategies and resharding

## Summary

A partitioner maps an application key to a position, an ordered byte string, and defines the shards of each cell as contiguous ranges of positions, called the cell's layout. Every lookup in every strategy is the same algorithm: compute the key's position, then search the sorted layout. We propose one `Partitioner` interface and five built-in strategies:

- `fixed-slots` hashes keys into a fixed number of slots.
- `hash-range` hashes keys into a 64-bit space whose ranges split and merge with load.
- `key-range` keeps keys in order in ranges that split and merge with size and load.
- `explicit` takes the application's list of shards.
- `singleton` has one shard.

A split or merge produces a new layout version and leaves the data on the nodes that already store it. The new writer's fence acquisition raises each parent's watermark above every epoch, so that a paused writer of a parent cannot write into a child.

## Motivation

The key-to-shard mapping is the choice on which published systems disagree most directly, and each disagreement has a use case behind it:

- Slicer hashes every key into a 63-bit space so that adjacent hot keys spread out, and the framework splits and merges ranges without the application. SM rejects this because hashing destroys key locality, and one of its applications serves prefix scans as 9% of its queries.
- SM found static sharding, a fixed mapping from keys to a numbered set of shards, about three times as common as consistent hashing across Facebook's applications. Resharding was rare enough that a fixed shard count was acceptable.
- Range-partitioned databases keep keys in order and split a range when it grows past a size or load threshold.
- Some applications already know their shards, such as one per tenant or one per region, and want the library to place them, not to compute them.

A library with one mapping serves one of these groups. The partitioner is also where a generic library can break correctness quietly. When a range splits while its writer is paused, the writer resumes with a token for the parent shard and can write keys that now belong to a child. The resharding protocol has to close that window for every strategy.

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
 // Labels contains attributes that placement goals and guardrails read, such as a
 // tenant or a preferred region.
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
 // Initial returns the layout of a new cell whose range is r.
 Initial(cell shard.CellID, r shard.Range) (Layout, error)
 // Reshape proposes the next layout from the current layout and per-shard stats.
 // It returns the current layout and no changes when no split or merge is due, and
 // ErrFixedLayout when the strategy never changes its shard set. The new layout
 // covers the same cell range as the current one.
 Reshape(layout Layout, stats map[shard.ID]Stats, nodes int) (Layout, []Change, error)
}
```

`Position` and `Layout.Locate` are on the request path of every router, so their cost matters. We measured a binary search over 1,000,000 sorted ranges with 16-byte starts at 241 to 245 ns per lookup, without an allocation, at 24 bytes per entry plus the bytes of each start. Routers that cache only part of a layout use the structure that the router describes.

### Built-in strategies

| Strategy | Position of a key | Shards | Shard IDs | Reshapes | Keeps key order | Default configuration |
|---|---|---|---|---|---|---|
| `fixed-slots` | `hash(key) mod slots`, as 4 bytes big-endian | One per slot | `"0"` to `"slots-1"` | No, except by a resharding operation | No | 1024 slots, `xxhash64`, hash tags off |
| `hash-range` | `hash(key)`, as 8 bytes big-endian | Ranges of the 64-bit space | Hexadecimal range bounds, such as `"4000000000000000-8000000000000000"` | Yes, on load | No | 16 initial shards, `xxhash64`, split and merge thresholds below |
| `key-range` | The key | Ranges of keys | Generated at each split, stable afterwards | Yes, on size and load | Yes | One initial shard, split at 512 MiB or on load |
| `explicit` | The key | The configured prefixes and ranges. A prefix p covers every key that starts with p | Configured by the application | Only by a spec edit that adds a shard | Yes | none, the list is required |
| `singleton` | The empty string | One, covering every position | `"0"` | No | Yes | none |

#### fixed-slots

```json
{"slots": 1024, "hash": "xxhash64", "hashTags": true}
```

With `hashTags` on, only the bytes between the first `{` and the first `}` after it are hashed, when at least one byte lies between them. Keys `user:{42}:cart` and `user:{42}:orders` then map to the same slot. Redis Cluster uses the same rule, so applications that already group keys this way keep their groups.

The slot count is fixed for the life of the namespace, except through a resharding operation that multiplies or divides it by an integer factor. With `hash mod slots`, multiplying the count by k sends every key of slot i to one of the slots i, i+n, …, i+(k-1)n of the new layout, where n is the old count. Every new slot takes keys from exactly one old slot, and the core places each new slot on the nodes of its old slot before any data moves. Elasticsearch has routed new indices by the same modulo since 9.4.0, allows a split only to a multiple of the shard count, and fills target shard t from source shard t mod n. Any other change of the slot count remaps nearly every key and is refused.

#### hash-range

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

```json
{
  "shards": [
    {"id": "eu", "prefix": "eu/", "labels": {"region": "eu-west-1"}},
    {"id": "us", "prefix": "us/", "labels": {"region": "us-east-1"}},
    {"id": "archive", "range": {"start": "z", "end": ""}}
  ]
}
```

Each shard has either a key prefix or a key range. Prefixes and ranges must not overlap, and `Layout.Locate` returns `ErrNoShard` for a key that matches neither. Adding a shard is a spec edit. Removing a shard or narrowing its prefix or range is a resharding operation, because keys move. An application whose mapping is more than prefixes and ranges, such as a lookup table, registers its own partitioner.

### Hash functions

A hash function is part of the layout, because two processes that disagree on it route the same key to different shards. The name is recorded in the layout, and changing it is a resharding operation.

| Name | Definition | Source |
|---|---|---|
| `xxhash64` | XXH64 with seed 0 over the key bytes | `github.com/cespare/xxhash/v2` v2.3.0, `Sum64` |
| `fnv1a64` | 64-bit FNV-1a over the key bytes | `hash/fnv` in the standard library |

The Go runtime's `hash/maphash` is excluded because its seed differs between processes. Core's `crypto.Hasher` implementations are cryptographic hashes, and the routing path needs neither their collision resistance nor their cost.

### Layouts and cells

Each cell covers a contiguous range of positions and has its own layout. Every shard's range lies inside its cell's range, and a split or merge stays inside one cell. A namespace starts with one cell whose range is every position, and its layout is the partitioner's `Initial` for that range. An operator splits a cell at a shard boundary, and each half keeps its shards, their epochs and their placements.

The store keeps each shard's range in the shard's placement, and the layout version in the cell's state. A layout of any size is therefore stored in bounded keys, and no single value contains a whole layout.

### Resharding

A split or merge changes which shard serves a key. The core applies a layout change in one generation of the cell, so that no router sees a layout whose placements are missing:

1. The planning pass calls `Reshape` with the current layout and per-shard stats.
2. For each change, the core places every new shard on the nodes and roles of the shard it comes from. For a merge, the placer first co-locates the parents on the same nodes. A merge whose parents are not co-located waits until they are.
3. The core commits the new layout version and the new placements in one generation. Each new shard's epoch is one higher than the highest epoch among its parents.
4. The core instructs each node to add the new shards and drop the parents, through the namespace's handoff protocol. A node whose handler implements the agent's optional `Resharder` interface receives one call per change, so that it can re-key local data in one step. Without it, the node receives `AddShard` for each new shard and then `DropShard` for each parent, and moves its data inside those calls.
5. The new writer's fence acquisition retires the parents. The instruction for a new shard lists the parents in `Supersedes`, and the fence adapter raises each parent's watermark to `fence.Retired` in the same transaction as the acquisition. `fence.Retired` is above every epoch that the core issues, so fenced storage rejects every later write under a parent's token with `epoch.ErrFenced`, and the parent's writer abdicates.

Step 5 closes the stale-writer window. Suppose a writer of the parent pauses before step 4 and resumes after step 5. Its token refers to the parent, whose watermark is above the token's epoch, so its write fails. Without the retirement, the parent's watermark would still equal the old epoch. The write would then succeed on keys that belong to a child.

In a namespace with `delegated` ownership, the application's authority splits and merges shards instead of the partitioner, so the core does not call `Reshape`. An agent reports each shard that the authority created with its range and its parents. The core applies the change in one generation once the reported children cover their parents' ranges exactly. Each child keeps the epoch that the authority granted, and its first fence acquisition retires its parents, as in step 5. A partitioner whose layout the spec fixes, such as `explicit`, can match an authority's initial layout, but it cannot follow a split without a spec edit. The application registers a partitioner that takes its initial layout from the authority and leaves every later change to the agents' reports.

## Alternatives considered

### Consistent hashing as the only mapping

Map keys straight to nodes on a hash ring, as Dynamo and Cassandra do. There is no shard entity to manage, and adding a node moves only about 1/N of the keys.

**Why not:** the library needs a unit to fence, to move with its state, to report load for and to place under constraints. A ring has no such unit, only the arcs between tokens. Dynamo's authors compared three partitioning strategies and found that fixed, equal-sized partitions balanced load best and let a partition move "as a unit by simply transferring the file". This proposal keeps consistent hashing where it helps, as function placers that map shards to nodes.

### A lookup method per strategy

Give each partitioner a `Locate(layout, key)` method, so that `fixed-slots` computes its shard by arithmetic and the range strategies search.

**Why not:** cells divide the same space that shards divide, and routers cache parts of layouts. With one position space, the cell map, a layout and a router's cache share one search over sorted ranges. `fixed-slots` loses its arithmetic lookup, and a router in lookup mode indexes slot positions directly instead.

### Application-defined shards only

Let every application list its shards, as SM does. The library places what it is given and never computes a mapping.

**Why not:** caches, actor systems and queues do not want to maintain shard lists, and each of them would write the same hash-to-slot code. `explicit` keeps this option for the applications that want it.

### Hashed ranges only

Hash every key and split ranges on load, as Slicer, Dicer and Splitter do. The framework balances load without application input, and hot keys spread out.

**Why not:** hashing removes key order. Range scans and prefix queries then touch every shard. SM rejected hashing for this reason, for an application whose queries are 9% prefix scans. `hash-range` is one strategy among five here.

### A directory of key locations

Store the location of each key or small key group in a directory, as Facebook's Akkio does with micro-shards and Vitess does with lookup indexes. Placement becomes arbitrarily flexible.

**Why not as a built-in:** the directory is itself a sharded, replicated datastore on the request path, and a partitioner performs no I/O. An application with a directory resolves a key to its micro-shard first and routes the micro-shard's ID, which `key-range` or `explicit` then maps to a shard.

## Drawbacks

- Five built-in strategies and two hash functions are about 1,500 lines of code by our estimate, plus the conformance suite each must pass.
- Resharding adds a path that runs rarely and must be correct the first time: co-locating merge parents, a new layout and new placements in one generation, and retirement of parent tokens. Each fence adapter must raise the parents' watermarks in the same transaction as the acquisition. An adapter that gets this wrong reopens the stale-writer window.
- `key-range` depends on the application to report sizes and split positions. An application that reports neither never splits.
- `fixed-slots` cannot change its slot count except by an integer factor. A namespace that starts with too few slots for its largest deployment must reshard. Too many slots add per-slot overhead to every router.
- A router in watch mode keeps the layouts of the cells it watches. A sorted index of 1,000,000 ranges takes 24 bytes per entry, measured, plus the bytes of each start: about 40 MB with 16-byte starts.

## Open questions

1. Is `xxhash64` the right default, or should the default be `fnv1a64`, which is in the standard library but distributes short keys less evenly?
2. Should `fixed-slots` offer compatibility hashes, such as Redis Cluster's CRC16 or Kafka's murmur2, for applications that migrate with their existing key-to-slot mapping?
3. Should `hash-range` split on more than one metric at once, or on one?

## Unresolved and future work

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
| Elasticsearch split index API | https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-split |
| CockroachDB range size and splits | https://www.cockroachlabs.com/docs/stable/architecture/distribution-layer |
| Annamalai et al., "Sharding the Shards: Managing Datastore Locality at Scale with Akkio", OSDI '18 | https://www.usenix.org/conference/osdi18/presentation/annamalai |
| cespare/xxhash v2.3.0 | https://github.com/cespare/xxhash |
| Our measurement of sorted slices and B-trees, 2026-09-27 | Research-0003 |

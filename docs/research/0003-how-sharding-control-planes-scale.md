---
research: 0003
title: How do sharding control planes, stores and routers scale to huge deployments, and which structure should a route cache use?
author: Claude, for Roy Klopper
status: Answered
created: 2026-09-27
updated: 2026-09-27
freshest-source: 2026-04-13
supersedes: none
superseded-by: none
---

# Research-0003: How do sharding control planes, stores and routers scale to huge deployments, and which structure should a route cache use?

## The question

A sharding library must serve namespaces of millions of shards on tens of thousands of nodes. Which part of a sharding system reaches its limit first, how do published systems divide that part, does gossip-based membership remove a limit, and which in-memory structure should a client's route cache use?

An answer is a published limit or a published division of work from a system that runs at that scale, or a measurement that someone can rerun. The answer would change if a system of that scale ran one undivided control plane, or if gossip removed the need to divide membership.

## The answer

Every component that sees the whole namespace reaches a limit: the control-plane instance, the consistent store, the membership pool and the client's routing table. Published systems divide each of them by the same unit. Shard Manager divides large applications into partitions of thousands of servers, places every replica of a shard inside its partition, and assigns partitions to control-plane instances through a partition registry. Bigtable and CockroachDB resolve locations through a hierarchy that clients cache lazily.

Gossip does not remove the membership limit. HashiCorp recommends at most 5,000 Consul agents per gossip pool, because churn can generate gossip messages faster than a pool transmits them, and gossip provides no lease that a fence can rely on. Dividing membership by partition removes the limit for a store-backed pool as well.

A route cache that changes one entry at a time and publishes immutable snapshots should be a copy-on-write B-tree. We measured a sorted slice at 12.7 ms per published change at 1,000,000 entries, against 2.9 to 3.7 µs for two B-trees, at a lookup cost of 241 ns against 291 ns.

## Findings

Each subsection states what the sources state. A paragraph that opens with "We conclude" gives our own conclusion.

### Control planes

- Shard Manager's authors state that its control plane "is not scalable enough to manage millions of servers and billions of shards", and that they "divide SM's control plane into multiple mini-SMs so that each mini-SM manages a subset of servers and shards".
- They divide a large application "into non-overlapping partitions, where each partition typically comprises thousands of servers and hundreds of thousands of shard replicas". "The replicas of a shard are always placed on servers that belong to the same partition."
- A partition registry assigns partitions to mini-SMs, and "a mini-SM can manage multiple partitions". When the load of two partitions diverges, "we use tools to initiate server or shard migration across partitions".
- The largest mini-SMs manage about 50,000 servers and 1.3 million shards, each on an 18-core machine with 64 GB of memory, at about 30% CPU and 38% memory at the 90th percentile. SM manages nearly 100 million shards on over one million servers in total.
- Meta's post states that the Shard Manager scheduler "is internally sharded to scale out horizontally", and names support for "tens of millions of shards per application" through "smaller independent partitions" as its direction.

We conclude that a library for this scale needs a unit like SM's partition, with its own controller, its own store keyspace and its own nodes, and a registry that assigns units to controller processes.

### Stores

| Limit | etcd's value | Source |
|---|---|---|
| Request size | 1.5 MiB by default | System limits page |
| Storage | 2 GiB by default, 8 GiB suggested maximum | System limits page |
| Operations per transaction | 128 by default | Server defaults at v3.7.2 |
| Throughput | "more than 30,000 requests per second under heavy load" for a three-member cluster | Performance page |
| Benchmarked writes | 44,341 to 50,104 per second, 256-byte values, 1,000 clients, etcd 3.2 on 8 vCPUs | Performance page |
| Benchmarked reads | 141,578 linearizable and 185,758 serializable per second, same setup | Performance page |
| Watch memory | 17 KB per client connection, 18 KB per watch stream and 350 bytes per watching | Watch memory benchmark |

The watch benchmark states that etcd "aims to support O(10k) clients, O(100K) watch streams (O(10) streams per client) and O(10M) total watchings". Kubernetes' KEP-589 reports that node heartbeats reached etcd's storage limit in clusters of more than 2,000 nodes with 10 to 15 images per node, and moved the heartbeats to separate Lease objects, which halved etcd's size on 5,000-node clusters.

We conclude that one etcd cluster holds the state of about one million shards, not of a hundred million, and that per-client watches of a whole namespace do not scale to hundreds of thousands of clients.

### Membership

- SWIM's authors state that "the expected time to first detection of each process failure, and the expected message load per member, do not vary with group size". Its suspicion mechanism "reduces (but does not eliminate)" false detections, and Lifeguard cut the false positives by more than 50 times.
- HashiCorp recommends "a maximum of 5,000 Consul client agents in a single gossip pool". It states that "gossip stability risk is determined by the rate of agent churn rather than the number of nodes", that churn can lead to "gossip messages being generated faster than they can be transmitted", and that "a gossip pool with mostly static nodes may be able to operate effectively with more than 5,000 agents".

We conclude that gossip pools need the same division by partition that store-backed membership needs, and that gossip adds no capacity once membership is divided. A gossip view also cannot back a lease for fencing.

### Routing

- Bigtable uses "a three-level hierarchy analogous to that of a B+tree" to store tablet locations, which addresses 2^34 tablets. "The client library caches tablet locations." With an empty cache the lookup "requires three network round-trips", and with a stale cache "up to six round-trips, because stale cache entries are only discovered upon misses". The client library prefetches: "it reads the metadata for more than one tablet whenever it reads the METADATA table".
- CockroachDB stores range locations in "a two-level index" of meta ranges. "Each node caches values of the meta2 range it has accessed before", and a request that reaches a non-leaseholder fails "with an error pointing at the replica's last known leaseholder", which the gateway retries transparently.

We conclude that routers of a huge namespace should cache routes lazily, resolve misses through a small first level, prefetch neighbouring entries and correct entries on redirects.

### Route cache structures, measured

We measured three structures for a cache of ranges that a router searches for the entry with the greatest start at or below a key:

| Structure | Lookup | Publish one change | Allocated per publish | Memory per entry, keys excluded |
|---|---|---|---|---|
| Sorted slice, `slices.BinarySearchFunc` | 241 to 245 ns | 12.66 to 12.74 ms | 24 MB in 1 allocation | 24.0 bytes |
| `github.com/google/btree` v1.1.3, degree 32 | 291 ns | 3.61 to 3.67 µs | 7.1 KB in 13 allocations | 52.1 bytes |
| `github.com/tidwall/btree` v1.8.1 | 292 ns | 2.89 to 2.99 µs | 5.1 KB in 15 allocations | 35.6 bytes |

No lookup allocated. Publishing a change means producing a new immutable snapshot that differs from the current one in one entry: a full copy for the slice, `Clone` for `google/btree` and `Copy` for `tidwall/btree`, followed by one insert. A test checked that the three structures return the same entry for 4,096 random keys.

We conclude that a cache that changes one route at a time needs a copy-on-write B-tree, and that `tidwall/btree` publishes 20% faster with 32% less memory per entry than `google/btree`, at the same lookup cost. A structure that is rebuilt whole, such as a layout, is best as a sorted slice.

#### Method

Go 1.27.1 on linux/amd64, AMD Ryzen 9 9950X3D, `GOMAXPROCS=4`, with `go test -bench . -benchtime=200ms -count=3 -cpu=1 -benchmem`. Each structure holds 1,000,000 entries with random 16-byte keys from a PCG source seeded with (1, 2). Each entry is a string and a pointer. Memory is the difference in `runtime.MemStats.HeapAlloc` after two garbage collections, with the key strings allocated before the measurement. The core of the benchmark:

```go
type entry struct {
 start string
 route *int
}

func less(a, b entry) bool { return a.start < b.start }

func floorSlice(s []entry, k entry) (entry, bool) {
 i, found := slices.BinarySearchFunc(s, k, func(a, b entry) int { return strings.Compare(a.start, b.start) })
 if found {
  return s[i], true
 }
 if i == 0 {
  return entry{}, false
 }
 return s[i-1], true
}

func floorGoogle(t *gbtree.BTreeG[entry], k entry) (out entry, ok bool) {
 t.DescendLessOrEqual(k, func(e entry) bool { out, ok = e, true; return false })
 return out, ok
}

func floorTidwall(t *tbtree.BTreeG[entry], k entry) (out entry, ok bool) {
 t.Descend(k, func(e entry) bool { out, ok = e, true; return false })
 return out, ok
}

// Publishing one change, per structure:
//   slice:   next := slices.Clone(sorted); next[i%n].route = &route
//   google:  next := cur.Clone(); next.ReplaceOrInsert(entry{start: sorted[i%n].start, route: &route})
//   tidwall: next := cur.Copy(); next.Set(entry{start: sorted[i%n].start, route: &route})
```

### Where sources disagree

- **Gossip at scale.** SWIM's analysis gives each member a constant load whatever the group size. HashiCorp's operational guidance bounds a pool at 5,000 agents, because the membership changes that churn produces still reach every member.

## What we could not establish

- etcd's throughput for lease renewals. The published benchmarks measure puts and ranges, not `LeaseKeepAlive`.
- The size of one million placements in etcd. Our figure of about 1 KB per shard with three replicas is an estimate from key sizes and etcd's per-key overhead, not a measurement.
- How SM's partition registry chooses a mini-SM for a partition. The paper names the component without its algorithm.

## What would change this answer

- A published system that runs a hundred million shards under one undivided control plane.
- An etcd release whose limits on request size, storage or watch memory rise by an order of magnitude.
- A lookup cost for B-trees above the cost of a network round trip, which would favour full copies in every client.

## Sources

| # | Source | What it is | Retrieved | What it supports |
|---|---|---|---|---|
| 1 | [Lee et al., "Shard Manager", SOSP '21](https://dl.acm.org/doi/10.1145/3477132.3483546), §6.1 and §8.1 | Paper by the system's authors | 2026-09-27 | Mini-SMs, partitions, the partition registry, scale figures |
| 2 | [Guo and Kooburat, "Scaling services with Shard Manager", 2020](https://engineering.fb.com/2020/08/24/production-engineering/scaling-services-with-shard-manager/) | Maintainers' post | 2026-09-27 | Internally sharded scheduler, partitioning direction |
| 3 | [etcd system limits](https://etcd.io/docs/v3.6/dev-guide/limit/) | Maintainers' documentation | 2026-09-27 | Request size and storage limits |
| 4 | [etcd performance](https://etcd.io/docs/v3.6/op-guide/performance/) | Maintainers' documentation | 2026-09-27 | Throughput and benchmark figures |
| 5 | [etcd watch memory usage benchmark](https://etcd.io/docs/v3.7/benchmarks/etcd-3-watch-memory-benchmark/) | Maintainers' benchmark, published 2026-04-13 | 2026-09-27 | Watch goals and memory per connection, stream and watching |
| 6 | [etcd v3.7.2 server defaults](https://github.com/etcd-io/etcd/blob/v3.7.2/server/embed/config.go) | Source | 2026-09-27 | Operations per transaction |
| 7 | [Kubernetes KEP-589](https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/589-efficient-node-heartbeats/README.md) | Enhancement proposal | 2026-09-27 | Heartbeats and etcd size |
| 8 | [HashiCorp, operating Consul at scale](https://docs.hashicorp.com/consul/docs/manage/scale) | Maintainers' documentation | 2026-09-27 | Gossip pool size and churn |
| 9 | [Das et al., "SWIM", DSN 2002](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) and [Dadgar et al., "Lifeguard"](https://arxiv.org/abs/1707.00788) | Papers | 2026-09-27 | Gossip load and false positives |
| 10 | [Chang et al., "Bigtable", OSDI '06](https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf), §5.1 | Paper | 2026-09-27 | Three-level location hierarchy, client caching, prefetch |
| 11 | [CockroachDB distribution layer](https://www.cockroachlabs.com/docs/v26.2/architecture/distribution-layer) | Maintainers' documentation | 2026-09-27 | Meta ranges, range cache, leaseholder redirects |
| 12 | [google/btree v1.1.3](https://github.com/google/btree) and [tidwall/btree v1.8.1](https://github.com/tidwall/btree) | Source | 2026-09-27 | APIs measured |
| 13 | Our benchmark, reproduced under Method | Our measurement | 2026-09-27 | Route cache costs |

## What we searched

| Search | Tool | Date | Useful |
|---|---|---|---|
| Shard Manager's scale-out: "mini-SM", "partition registry", "scale out" in the SOSP '21 paper and the Meta post | Exa search | 2026-09-27 | Yes |
| etcd limits, performance, benchmarks and watch memory | Exa fetch of the etcd documentation | 2026-09-27 | Yes |
| Node heartbeats and etcd load in Kubernetes | Exa search | 2026-09-27 | Yes, KEP-589 |
| Gossip pool size and churn for Consul, Serf and memberlist | Exa search | 2026-09-27 | Yes |
| Bigtable tablet location; CockroachDB meta ranges and range cache | Exa fetch and search | 2026-09-27 | Yes |
| Latest releases of google/btree, tidwall/btree, memberlist, etcd's client and xxhash | Go module proxy | 2026-09-27 | Yes |
| B-tree dependencies in the thesmos repositories | `go.mod` files under `~/Projects/thesmos` | 2026-09-27 | None found |

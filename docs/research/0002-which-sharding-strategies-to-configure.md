---
research: 0002
title: Along which axes do sharding systems differ, and how do configurable systems select and validate strategies?
author: Claude, for Roy Klopper
status: Answered
created: 2026-09-27
updated: 2026-09-28
freshest-source: 2026-09-23
supersedes: none
superseded-by: none
---

# Research-0002: Along which axes do sharding systems differ, and how do configurable systems select and validate strategies?

Note: this research of 2026-09-27 predates the design, and its conclusions are historical. The strategies of the design, and the subset that the first release supports, are in [the RFCs](../rfc/README.md).

## The question

Which design choices differ between published sharding systems, which options does each choice offer, and how do the systems that make behaviour pluggable select, parse and validate a strategy?

## The answer

Published systems differ on nine axes: the key-to-shard mapping, shard-to-node placement, the replication model, ownership semantics, the handoff protocol, membership and failure detection, the store of the assignment, the topology of the controller, and routing. Shard Manager also caps the replicas that planned maintenance takes down at once. Each system fixes most axes and exposes one or two as strategies. Kubernetes, Kafka, Akka, Pekko, Orleans and Ignite expose placement, Hazelcast exposes the grouping of members for backups, and Infinispan exposes the key-to-segment mapping. Each of them keeps its correctness mechanisms, such as fencing, handoff ordering and the budget, outside the strategy interface.

Every framework we examined that selects strategies from configuration uses the same mechanism:

- A strategy registers under a name.
- The configuration refers to the strategy by that name.
- The strategy parses its own typed configuration.
- The whole configuration is validated before it takes effect.

The frameworks disagree on these details:

- whether an unknown field is an error
- what a duplicate registration does
- whether a rejected update keeps the last good configuration or stops the process

## Findings

Each subsection covers one axis. Each table row gives what one source states, with the version read. A paragraph that opens with "We conclude" gives our own conclusion.

### Key-to-shard mapping

| System | Mapping | Configurable, with defaults | Keeps key order | Resharding |
|---|---|---|---|---|
| Redis Cluster 8.10.2 | CRC16 of the key, modulo 16384 slots | Hash tags. The slot count is a compile-time constant | No | Slots move between nodes. The slot count never changes |
| Elasticsearch 9.5.4 | Murmur3 of the routing value, modulo the primary shard count, for indices created on 9.4.0 or later | `number_of_shards` (1), `routing_partition_size` (1), `_routing` (default `_id`) | No | `_split` to a multiple, `_shrink` to a factor, or reindex |
| MongoDB 8.3 | Shard-key ranges called chunks, or hashed keys | Ranged keys by default, hashed keys, a range size of 128 MB, zones | Ranged keys only | Balancer migrations and `reshardCollection` |
| Vitess v23 | A vindex maps a column to a keyspace ID, which falls in a shard's range | The vindex type per table: hash, xxhash, lookup, consistent lookup, region | Depends on the vindex | An operator-driven workflow with a few seconds of read-only time |
| CockroachDB v26.2 | Contiguous ranges of one sorted key space | `range_max_bytes` 512 MiB, `range_min_bytes` 128 MiB, load-based splitting on | Yes | Automatic splits and merges |
| TiKV and PD v8.5 | Contiguous key ranges called regions | `region-split-size` 256 MiB, `max-merge-region-size` 54 MiB | Yes | Automatic splits and merges |
| Cassandra 5.0.9 | A Murmur3 token ring | `num_tokens` 16 in the release's `cassandra.yaml`, `allocate_tokens_for_local_replication_factor` 3 | No, with the default partitioner | Range streaming when nodes join or leave |
| Dynamo, 2007 | An MD5 ring with virtual nodes | Tokens per node, fixed partitions, and (N,R,W), commonly (3,2,2) | No | Three strategies compared. Fixed, equal-sized partitions balanced best |
| Slicer, 2016 | Ranges of a 63-bit hash of the key | The framework splits and merges | No | Automatic, by weighted moves |
| Shard Manager, 2021 | Shards that the application defines | The application's choice | The application's choice | Driven by the application |
| Akkio, 2018 | Micro-shards that the application defines, located through a directory | Migration policy | Groups related data | Per-micro-shard migration |
| Akka 2.10.22 and Pekko 1.7.0 | A message extractor maps entity IDs to one of `number-of-shards` shards | `number-of-shards` (1000). Akka advises ten times the planned maximum node count | No | The shard count cannot change without stopping every node |

Shard Manager's authors found static sharding "≈3x more popular than consistent hashing" across Facebook's applications, and one of their applications serves prefix scans as 9% of its queries.

We conclude that a generic library needs hashed and ordered mappings, and fixed shard counts as well as ranges that split. Each row of the preceding table has users for whom the other choices fail.

### Hash functions for placement

| Function | Movement when one node joins or leaves | Balance, as measured by its authors or others | Restriction |
|---|---|---|---|
| Consistent hashing (Karger et al., 1997) | Only the minimum fraction needed to rebalance | Standard consistent hashing put 53% more load than the average on some servers of Facebook's TAO cache | Needs virtual points per bucket to balance |
| Rendezvous hashing (Thaler and Ravishankar, 1998) | Only the objects of the removed server | Weights per server can be scaled for unequal capacity | A lookup costs time proportional to the number of servers |
| Jump consistent hash (Lamping and Veach, 2014) | 1/(n+1) of the keys when a bucket is added, all to the new bucket | "almost perfectly" divides the key space, by its authors | "the buckets must be numbered sequentially" |
| Bounded loads (Mirrokni et al., 2016) | O(m/(nε²)) balls per bin change, for capacity factor c = 1+ε | No bin above ⌈cm/n⌉ | Vimeo used c = 1.25 in production |
| Maglev (Eisenbud et al., 2016) | About twice as many keys as ring hash, by Envoy's simulations | A 65537-entry table, sized above 100 × N for 1% balance | The table is rebuilt on every change |

### Shard-to-node placement

| System | Strategy interface | Built-in strategies | Default | Limits on rebalancing |
|---|---|---|---|---|
| Kubernetes scheduler v1.37.1 | Filter and score plugins at extension points | 18 default plugins, including resource fit, topology spread and affinity | One profile. Resource fit scores least-allocated nodes highest | The scheduler never moves running pods. The separate descheduler caps evictions only when configured |
| Kafka consumer client 4.3.1 | `ConsumerPartitionAssignor.assign` | Range, round-robin, sticky, cooperative-sticky | Range | A 3-second delay before a new group's first rebalance |
| Kafka broker, KIP-848 | `ConsumerGroupPartitionAssignor.assign` | Uniform and range, both sticky | Uniform | At most one assignor run per group per second |
| Akka and Pekko cluster sharding | `ShardAllocationStrategy` with `allocateShard` and `rebalance` | Least-shard, external | Least-shard, with the older algorithm | A check every 10 seconds. The newer algorithm moves at most 10% of shards per round and is off by default |
| Orleans 10.3.1 | `IPlacementDirector.OnAddActivation` | Random, prefer-local, hash-based, activation-count, resource-optimised, silo-role, stateless-worker | Resource-optimised since 9.2 | The experimental rebalancer has no migration cap by default |
| Apache Ignite 2.18.0 | `AffinityFunction` | Rendezvous | 1024 partitions | Rebalance throttling off by default |
| Infinispan 16.2.3 | `KeyPartitioner` is public. Consistent-hash factories are no longer configurable | Sync and topology-aware variants | 256 segments, 2 owners | Segment count fixed until a cluster restart |
| Hazelcast 5.7.0 | Partition groups only | Per-member, zone-aware, node-aware, placement-aware, custom, SPI | 271 partitions, per-member groups | 10 parallel migrations per member |
| Dapr 1.18.4 | None found | A consistent-hash ring per actor type | 100 virtual nodes per host | One lock, update and unlock round per table change |
| Shard Manager, 2021 | A constraint solver with prioritised goals | Region preference, spread, maintenance, utilisation, global and regional balance, parallel failover | Local search | Caps on concurrent moves per server and per application |
| Service Fabric, 2018 | Metrics, capacities, constraints and an energy function | Simulated annealing | Balance on replica counts when no metric is defined | 1000 movements per 600 seconds by default |

Mitzenmacher showed that a system in which each item chooses among a small number of random candidates "can perform almost as well as a perfect load balancing system with global load knowledge". Orleans's activation-count placement samples two silos by default. In the preceding table, Kubernetes, Kafka, Akka, Pekko, Orleans and Ignite expose a placement interface, and Shard Manager and Service Fabric expose goals and constraints instead.

We conclude that sampling candidates is a sound default for a placement search. A generic library needs a placement interface as well as configurable goals and constraints.

### Replication models

- Shard Manager supports three replication strategies: primary-only, secondary-only and primary-secondary. In August 2020, 67% of its applications were primary-only and they ran on 17% of its servers.
- Shard Manager's maintainers added `update_membership` for Paxos applications, which must apply replica-set changes themselves.
- Helix lets each application declare a state model with per-state bounds, such as at most one master, and requires a path to its `DROPPED` state from every state. Its built-in models are master-slave, leader-standby and online-offline.
- Service Fabric defines primary, active secondary and idle secondary roles, and only active secondaries count toward the write quorum.

### Ownership and fencing

| System | Guarantee | Mechanism |
|---|---|---|
| Dicer, open-source release | Affinity only | Committing a Strong-mode assignment throws `NotImplementedError` |
| Slicer, consistent mode | At most one owner per key | Three Chubby locks per job. "not yet deployed by customers in production" in 2016 |
| Splitter v1.5.0 | Exclusive in steady state, optional overlap during moves | 40-second leases on grants, with loaded and unloaded signals |
| Temporal v1.32.0 | One writer per history shard | A range ID incremented on acquisition, and every write conditioned on it |
| Kafka, KIP-848 | Stale members fenced from offset commits | A member epoch checked by the coordinator |
| Helix | One master per partition in the controller's plan | No fencing of a participant that loses ZooKeeper. Issue 691 reports two masters |
| Service Fabric | One primary per epoch | Epochs in the replicator, and a write quorum |

Kleppmann argues that the storage itself must reject writes whose token went backwards. The authors of WriteGuards state that "Ownership at the caching layer is not enough".

We conclude that ownership is a choice between affinity and exclusivity. Exclusivity needs a fence in the application's storage as well as a lease.

### Handoff protocols

| System | Steps | What clients see | Guarantee |
|---|---|---|---|
| Redis Cluster, legacy migration | Importing on the destination, migrating on the source, key-by-key migration, then the new owner is set on the destination first | `ASK` for keys already moved, `TRYAGAIN` for split multi-key commands, `MOVED` after completion | A key exists on one node at a time. Write safety is best-effort |
| Redis 8.4 atomic slot migration | Snapshot and live delta to the destination, a write pause on the source, one ownership handoff | Writes block during the handoff, then `MOVED` | One atomic handoff. Writes can be lost if the pause timeout is too short |
| Pekko cluster sharding 1.7.0 | Every region stops routing and buffers, the old region stops its entities, the shard moves | Buffered messages, at most 100,000 per region. The old region drops what it buffered to avoid reordering | At-most-once delivery. Entity state does not move |
| Orleans, since 7.2 | The grain dehydrates on the source and rehydrates on the target | Requests move with the grain | Advisory. State is lost if the source fails before the transfer completes |
| Kafka, KIP-429 | Revoke only the moving partitions, then assign them in a second rebalance | Revocation and assignment callbacks | A partition is revoked before it is reassigned |
| Shard Manager, 2021 | Prepare the new primary, make the old one forward, promote, publish, drop | No dropped requests | No failure handling described |

### Membership and failure detection

| Detector | Defaults | Detection time or measured behaviour |
|---|---|---|
| Phi accrual in Pekko 1.7.0 | Threshold 8, heartbeat 1 s, acceptable pause 3 s | About 5.5 s, by Pekko's documentation |
| Phi accrual in Cassandra 5.0.9 | Threshold 8, gossip every 1 s | About 18 times the mean interval, by our reading of its exponential model |
| SWIM, 2002 | Protocol period, indirect probes, suspicion timeout | Suspicion "reduces (but does not eliminate)" false detections |
| Lifeguard, 2017 | Local health awareness on top of SWIM | More than 50 times fewer false positives, with the median detection time up by 0.1 s |
| hashicorp/memberlist v0.7.0 | Probe every 1 s, suspicion multiplier 4 | A suspicion timeout between 8 s and 48 s at 100 nodes |
| Kubernetes v1.37.1 | Node lease renewed every 10 s, grace period 50 s | Eviction starts 5 minutes after a node is marked unknown |
| Kafka 4.3.1 | Session 45 s, heartbeat 3 s | The session timeout |
| Dicer | Kubernetes termination grace period of 30 s | A crashed pod detected in roughly 30 s |

### Store, distribution and topology

- Shard Manager stores the orchestrator's state in ZooKeeper and detects failures through ZooKeeper ephemeral nodes. Application servers read their assignment at start-up without the control plane.
- Slicer writes assignments under generation numbers to optimistically consistent storage and distributes them through a tier of distributors. Clients route on cached assignments when every component of Slicer's service fails. Subscribers received 95% of assignments within 2 seconds.
- Dicer's open-source Assigner keeps assignments in memory, where one "may be lost forever" if the Assigner crashes first. Slicelets relay assignments to clients.
- Splitter stores its state in Raft inside the service. Cadence's shard-manager stores its state in etcd.
- etcd v3.7.2 allows 128 operations per transaction and 1.5 MiB per request by default, and suggests at most 8 GiB of data.
- Dapr's placement service builds a hash table per actor type and sends it to every sidecar, which computes owners locally.

### Routing

- Shard Manager, Slicer and Dicer route in a client library linked into each client.
- Redis Cluster redirects with `MOVED` and `ASK`, and CockroachDB returns "an error with a pointer to which node it believes is the leaseholder".
- Vitess routes through a proxy tier.
- Redis's atomic slot migration generates up to 116 times fewer redirects than its legacy migration, by Redis's own benchmark.

### Disruption budgets

- Shard Manager's TaskController approves a subset of pending container operations under a global cap and a per-shard cap that count replicas already down. Planned container stops were about 1000 times more frequent than unplanned failures at Facebook.
- Most Shard Manager applications drain their primaries before a restart and leave their secondaries in place.
- At Databricks, planned restarts are roughly 99.9% of all restarts.

### Configuration mechanisms

| Project | Registration | Typed configuration | Validation | Invalid or unknown configuration |
|---|---|---|---|---|
| grpc-go v1.84.0 | `balancer.Register` from `init`. The last registration of a name replaces the earlier ones | `ParseConfig(json.RawMessage)` per policy | Inside `ParseConfig` | An invalid service config is ignored whole and the previous one kept. Unknown policy names in the list are skipped |
| Caddy v2.11.4 | `RegisterModule` from `init`. A duplicate panics | Strict JSON into the module struct | `Provision`, then `Validate` | The reload fails and the old configuration is rolled back |
| OpenTelemetry Collector v0.161.0 | Factories passed as maps. A duplicate is an error | YAML decoded strictly over `CreateDefaultConfig()` | Recursive `Validate`, with all errors joined | Startup fails. A failed reload stops the collector |
| Kubernetes scheduler v1.37.1 | `app.WithPlugin` in the operator's own binary. A duplicate is an error | Versioned, strictly decoded configuration | All errors aggregated | Startup fails |
| database/sql, Go 1.27.1 | `sql.Register` from `init`. A duplicate panics | A driver-specific string | The driver's own | `Open` returns "unknown driver" |
| hashicorp/go-plugin v1.8.0 | A plugin set on both sides. No global registry | Calls over gRPC | A handshake of protocol versions | Incompatible versions are refused. A plugin crash does not crash the host |
| Kubernetes CRD CEL rules | None. Rules are part of the schema | Type-checked CEL | Estimated cost on write, a runtime budget of 1,000,000 per call | The write is rejected. Transition rules compare the new object with the old |
| Envoy xDS v1.39.1 | Factories looked up by type URL | A protobuf `Any` | Per-resource validation | A NACK contains the error, and the last valid configuration remains in effect |

Go's `plugin` package warns that runtime crashes are likely unless the application and its plugins are built with exactly the same toolchain, build tags and flags. A loaded plugin cannot be closed, and plugins work only on Linux, FreeBSD and macOS. go-plugin's documentation names performance as the advantage of shared libraries over its RPC design.

We conclude that strategies should compile into the application and register in an explicit registry. A controller should validate the whole configuration and keep the last good configuration when it rejects a new one.

### Where sources disagree

- **Registration of a duplicate name.** gRPC keeps the last registration. Caddy and `database/sql` panic. The Kubernetes scheduler and the OpenTelemetry Collector return an error.
- **Unknown configuration fields.** gRPC ignores them, for forward compatibility. Caddy, the OpenTelemetry Collector and the Kubernetes scheduler reject them.
- **A rejected update.** Caddy rolls back, gRPC and Envoy keep the last valid configuration, and the OpenTelemetry Collector shuts down. A request to change the collector's behaviour was closed as not planned.
- **Fencing tokens.** Kleppmann requires a monotonic token that storage checks. antirez calls the token "unrealistic" and proposes a random token with check-and-set. Hochstein's 2025 model check found that tokens alone do not stop a lost update when the write with the lower token arrives at the storage first.
- **Jump consistent hash for storage.** Its authors call it suitable for data storage. Appleton and O'Reilly call it "not generally applicable", because it cannot lose an arbitrary node.
- **Kafka's default assignor.** KIP-429 planned cooperative-sticky as the default of Kafka 3.0. Kafka 4.3.1 still selects the range assignor first.
- **Cassandra's token count.** Cassandra 5.0's documentation still states a default of 256 virtual nodes. The `cassandra.yaml` in the release sets 16, and the code falls back to 1 when the key is unset.
- **Elasticsearch routing.** The live documentation and the 9.4.0 source route new indices by hash modulo the shard count. The documentation files at tag v9.5.4 still show only the older routing-shards formula.
- **Pekko's buffered messages.** The concepts page states that buffered messages go to the new location after a handoff. The source drops the messages that the old region buffered.

## What we could not establish

- The performance of the frameworks' placement strategies at our scale. Each source measures one framework under its own workload, and we did not find a comparison under a common one.
- Whether Dicer's production algorithm matches its open-source one. Its source states that it does not, and the production algorithm is not public.
- Exact formulas in the SWIM and rendezvous papers, which text extraction lost. The findings quote sentences from those papers, not formulas.
- Whether Hazelcast or Dapr offer a placement hook that we did not find. A search can show only that we found none.

## What would change this answer

- A system that makes all nine axes configurable, which would be prior art for the whole design instead of for single axes.
- A published measurement showing that one placement strategy serves the use cases in the placement table, which would weaken the case for pluggable placement.
- Dicer releasing its Strong mode, which would move it from the affinity row of the ownership table.

## Sources

| # | Source | What it is | Retrieved | What it supports |
|---|---|---|---|---|
| 1 | [Lee et al., "Shard Manager", SOSP '21](https://dl.acm.org/doi/10.1145/3477132.3483546) | Paper by the system's authors | 2026-09-27 | Replication strategies, placement goals, TaskController, graceful migration, static sharding share |
| 2 | [Guo and Kooburat, "Scaling services with Shard Manager", 2020](https://engineering.fb.com/2020/08/24/production-engineering/scaling-services-with-shard-manager/) | Maintainers' post | 2026-09-27 | Primary-only share, `update_membership` |
| 3 | [Adya et al., "Slicer", OSDI '16](https://www.usenix.org/system/files/conference/osdi16/osdi16-adya.pdf) | Paper | 2026-09-27 | Hash ranges, consistent mode, distribution, control-plane failure |
| 4 | [databricks/dicer at 747e89acb3b7](https://github.com/databricks/dicer/tree/747e89acb3b7b28fb52a8dc02933e162b86f29b8) and its [FAQ](https://github.com/databricks/dicer/blob/747e89acb3b7b28fb52a8dc02933e162b86f29b8/docs/FAQ.md) | Source and documentation | 2026-09-27 | Affinity only, in-memory store, crash detection |
| 5 | [Databricks, "Open sourcing Dicer", 2026-01-13](https://www.databricks.com/blog/open-sourcing-dicer-databricks-auto-sharder) | Maintainers' post | 2026-09-27 | Planned restarts as 99.9% of restarts |
| 6 | [atoms-co/splitter v1.5.0](https://github.com/atoms-co/splitter/tree/1cb229a9b3429db424618d652e129b0459ed7ef0) | Source and documentation | 2026-09-27 | Leases, grants, UUID ranges, Raft |
| 7 | [Redis Cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/) and [atomic slot migration](https://redis.io/blog/atomic-slot-migration/) | Maintainers' documentation | 2026-09-27 | Slots, hash tags, redirects, migration |
| 8 | [Elasticsearch routing field](https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/mapping-routing-field) and [split API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-split) | Maintainers' documentation | 2026-09-27 | Routing formula, split rules |
| 9 | [MongoDB sharding documentation](https://www.mongodb.com/docs/manual/core/sharding-data-partitioning/) | Maintainers' documentation | 2026-09-27 | Chunks, range size, balancer |
| 10 | [Vitess vindexes](https://vitess.io/docs/reference/features/vindexes/) and [VTGate](https://vitess.io/docs/concepts/vtgate/) | Maintainers' documentation | 2026-09-27 | Vindex types, proxy routing |
| 11 | [CockroachDB distribution layer](https://www.cockroachlabs.com/docs/v26.2/architecture/distribution-layer) and [replication layer](https://www.cockroachlabs.com/docs/stable/architecture/replication-layer) | Maintainers' documentation | 2026-09-27 | Range sizes, splits, leaseholder errors |
| 12 | [TiKV configuration v8.5](https://docs.pingcap.com/tidb/v8.5/tikv-configuration-file) | Maintainers' documentation | 2026-09-27 | Region split and merge sizes |
| 13 | [Cassandra 5.0 architecture](https://cassandra.apache.org/doc/5.0/cassandra/architecture/dynamo.html) | Maintainers' documentation | 2026-09-27 | Token ring, virtual nodes |
| 14 | [DeCandia et al., "Dynamo", SOSP '07](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) | Paper | 2026-09-27 | Partitioning strategies |
| 15 | [Annamalai et al., "Akkio", OSDI '18](https://www.usenix.org/system/files/osdi18-annamalai.pdf) | Paper | 2026-09-27 | Micro-shards and directory |
| 16 | [Karger et al., "Consistent Hashing and Random Trees", STOC '97](https://www.cs.princeton.edu/courses/archive/fall09/cos518/papers/chash.pdf) | Paper | 2026-09-27 | Consistent hashing |
| 17 | [Thaler and Ravishankar, HRW, ToN 1998](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/02/HRW98.pdf) | Paper | 2026-09-27 | Rendezvous hashing |
| 18 | [Lamping and Veach, jump consistent hash](https://arxiv.org/abs/1406.2294) | Paper | 2026-09-27 | Jump hash and its restriction |
| 19 | [Mirrokni et al., bounded loads](https://arxiv.org/abs/1608.01350) | Paper | 2026-09-27 | Capacity bound, Vimeo's factor |
| 20 | [Eisenbud et al., "Maglev", NSDI '16](https://www.usenix.org/system/files/conference/nsdi16/nsdi16-paper-eisenbud.pdf) | Paper | 2026-09-27 | Table size and balance |
| 21 | [Huang et al., HotNets '14](http://conferences.sigcomm.org/hotnets/2014/papers/hotnets-XIII-final128.pdf) | Measurement paper | 2026-09-27 | 53% imbalance of consistent hashing |
| 22 | [Envoy v1.39.1 load balancers](https://github.com/envoyproxy/envoy/blob/v1.39.1/docs/root/intro/arch_overview/upstream/load_balancing/load_balancers.rst) | Maintainers' documentation | 2026-09-27 | Maglev movement against ring hash |
| 23 | [Kubernetes v1.37.1 scheduler source](https://github.com/kubernetes/kubernetes/tree/v1.37.1/pkg/scheduler) and [scheduling framework](https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/) | Source and documentation | 2026-09-27 | Plugins, weights, score range, nodes scored |
| 24 | [Apache Kafka 4.3.1 source](https://github.com/apache/kafka/tree/4.3.1) and [KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol) | Source and specification | 2026-09-27 | Assignors, defaults, epochs |
| 25 | [Apache Pekko 1.7.0 source](https://github.com/apache/pekko/tree/v1.7.0) and [Akka cluster sharding](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html) | Source and documentation | 2026-09-27 | Allocation strategies, handoff, buffering |
| 26 | [Orleans grain placement](https://learn.microsoft.com/en-us/dotnet/orleans/grains/grain-placement) | Maintainers' documentation | 2026-09-27 | Placement strategies and defaults |
| 27 | [Apache Ignite 2.18.0 source](https://github.com/apache/ignite/tree/2.18.0) | Source | 2026-09-27 | Affinity function, partitions |
| 28 | [Infinispan configuration](https://infinispan.org/docs/stable/titles/configuring/configuring.html) | Maintainers' documentation | 2026-09-27 | Segments, owners, key partitioner |
| 29 | [Hazelcast 5.7 partition groups](https://docs.hazelcast.com/hazelcast/5.7/clusters/partition-group-configuration) | Maintainers' documentation | 2026-09-27 | Partition count and groups |
| 30 | [Dapr v1.18.4 placement options](https://github.com/dapr/dapr/blob/v1.18.4/cmd/placement/options/options.go) | Source | 2026-09-27 | Virtual nodes |
| 31 | [Mitzenmacher, TPDS 2001](https://www.eecs.harvard.edu/~michaelm/postscripts/tpds2001.pdf) | Paper | 2026-09-27 | The power of two choices |
| 32 | [Kakivaya et al., "Service Fabric", EuroSys '18](https://dprg.cs.uiuc.edu/data/files/2018/ServiceFabricEuroSys2018.pdf) | Paper | 2026-09-27 | Annealing, epochs |
| 33 | [Apache Helix 2.0.1 documentation](https://helix.apache.org/2.0.1-docs/tutorial_state.html) and [issue 691](https://github.com/apache/helix/issues/691) | Documentation and issue | 2026-09-27 | State models, missing participant fencing |
| 34 | [temporalio/temporal v1.32.0](https://github.com/temporalio/temporal/tree/d94e34a1ebba5410a2e7d07119a76896909591aa) | Source | 2026-09-27 | Range ID fencing |
| 35 | [Kleppmann, 2016](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html), [antirez, 2016](https://antirez.com/news/101) and [Hochstein, 2025](https://surfingcomplexity.blog/2025/03/03/locks-leases-fencing-tokens-fizzbee/) | Essays | 2026-09-27 | The fencing disagreement |
| 36 | [Mao et al., "WriteGuards", OSDI '26](https://www.usenix.org/system/files/osdi26-mao-ziming-writeguards.pdf) | Paper | 2026-09-27 | Ownership in the storage layer |
| 37 | [Das et al., "SWIM", DSN 2002](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) and [Dadgar et al., "Lifeguard"](https://arxiv.org/abs/1707.00788) | Papers | 2026-09-27 | Gossip failure detection |
| 38 | [hashicorp/memberlist v0.7.0](https://github.com/hashicorp/memberlist/tree/v0.7.0) | Source | 2026-09-27 | Suspicion timeouts |
| 39 | [Cassandra 5.0.9 source](https://github.com/apache/cassandra/tree/cassandra-5.0.9) | Source | 2026-09-27 | Phi convict threshold |
| 40 | [etcd v3.7.2 source](https://github.com/etcd-io/etcd/tree/v3.7.2) | Source | 2026-09-27 | Transaction and request limits, election recipe |
| 41 | [grpc-go v1.84.0](https://github.com/grpc/grpc-go/tree/v1.84.0) and [gRPC proposal A21](https://github.com/grpc/proposal/blob/master/A21-service-config-error-handling.md) | Source and specification | 2026-09-27 | Policy registration and config errors |
| 42 | [Caddy v2.11.4](https://github.com/caddyserver/caddy/tree/v2.11.4) and [Caddy API documentation](https://caddyserver.com/docs/api) | Source and documentation | 2026-09-27 | Modules, validation, rollback |
| 43 | [OpenTelemetry Collector v0.161.0](https://github.com/open-telemetry/opentelemetry-collector/tree/v0.161.0) | Source | 2026-09-27 | Factories, strict decoding, validation |
| 44 | [Go 1.27.1 source: database/sql and plugin](https://github.com/golang/go/tree/go1.27.1/src) | Source | 2026-09-27 | Driver registry, plugin warnings |
| 45 | [hashicorp/go-plugin v1.8.0](https://github.com/hashicorp/go-plugin/tree/v1.8.0) | Source and documentation | 2026-09-27 | Process plugins and their cost |
| 46 | [Kubernetes CRD validation rules](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#validation-rules) | Documentation | 2026-09-27 | CEL rules and budgets |
| 47 | [Envoy v1.39.1 xDS protocol](https://github.com/envoyproxy/envoy/blob/v1.39.1/docs/root/api-docs/xds_protocol.rst) | Maintainers' documentation | 2026-09-27 | ACK, NACK, last valid configuration |

We used four search agents to read these sources at the tags and dates shown. We re-opened every quotation in this document, and every number that the RFCs rely on, at its source on 2026-09-27. The remaining defaults in the tables come from the agents' reading at the cited tags, and every link was fetched.

## What we searched

| Search | Tool | Date | Useful |
|---|---|---|---|
| The fifteen partitioning systems and papers by name, then "key to partition mapping", "resharding without downtime online shard split", "consistent hashing load imbalance", "criticism or measured drawbacks of jump consistent hash and rendezvous hashing", "Cassandra availability with virtual nodes", "Redis cluster why 16384 slots", "Envoy Maglev versus ring hash disruption" | Web search, Exa search, arXiv search | 2026-09-27 | Yes |
| Code searches for Elasticsearch resharding metadata, CockroachDB range and merge settings, TiKV region ratios, Redis slot hashing, MongoDB initial chunk policy | GitHub code search | 2026-09-27 | Yes, except one MongoDB query that returned nothing |
| The nine placement frameworks by name, then "pluggable shard allocation strategy", "partition assignment strategy configurable", "placement strategy actor framework", and complaints about Akka rebalancing, Kafka range imbalance, Orleans placement, Dapr dissemination, Hazelcast's partition count and the Kubernetes descheduler | Web search, Exa search, GitHub API | 2026-09-27 | Yes |
| Redis resharding and atomic slot migration, Pekko handoff, Orleans migration, Kafka rebalancing, CockroachDB leader leases, phi accrual, SWIM, Lifeguard, memberlist and Kubernetes node heartbeats, then "shard handoff protocol buffering", "slot migration redirect", "failure detector false positives", and reported problems for each | Web search, Exa search, GitHub search, Apache Jira, Crossref | 2026-09-27 | Yes |
| grpc-go, Caddy, the OpenTelemetry Collector, the Kubernetes scheduler, database/sql, go-plugin, the plugin package, CRD CEL rules, Envoy xDS and functional options by name, then "go plugin registry pattern", "typed config per plugin go", "validate configuration before apply", "config NACK", and criticism of each | Web search, Exa search | 2026-09-27 | Yes, except the three problem searches in quotes, which found nothing relevant |

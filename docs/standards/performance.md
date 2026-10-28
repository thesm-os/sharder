<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# Performance

These rules cover the hot paths: the calls in the cost table of an RFC,
such as the append, the reads, verification and integration. Every other
path keeps the simplest code that meets its contract.

## Principles

The ledger applies the principles of the LMAX Disruptor where they apply
in Go:

| Principle | Rule in the ledger |
|---|---|
| Single writer | Each ledger has one writer, fenced by a leader epoch. One executor admits and applies the ledger's commands in order, and readers read committed state without the writer's lock |
| Pre-allocation | A hot path allocates nothing in steady state. Its buffers, batches and call states come from pools, and a pooled value is built in its slot |
| Batching | A stage that hands work to a slower stage, such as a commit to a device, takes everything that is queued at once. No timer delays a batch |
| Sequences instead of polling | A waiter waits for a sequence number, such as a position or a durable count, on core's `epoch.EventCount`. Nothing polls or sleeps |
| No false sharing | A field that one goroutine writes often and another reads has a cache line of its own. Counters use core's `telemetry.ShardedCounter`, whose cells take 128 bytes each |
| Mechanical sympathy | The engine writes sequentially, with aligned buffers for direct I/O. The append path hashes each payload byte once and each header once |

## Hand-offs between goroutines

- A ring buffer in the Disruptor's form belongs where a hot path hands
  work from one goroutine to another: the durable engine's commit queue,
  and the replication of a cluster's shard. The design of such a
  component measures a channel, a ring and an `epoch.EventCount` in Go
  before it chooses one.
- The append path above the engine runs in the caller's goroutine and
  hands nothing to another goroutine.
- A wait blocks by default. A goroutine that spins occupies one of the
  `GOMAXPROCS` processors, and Go cannot pin it to a core. A spinning
  wait needs a measurement that shows its gain on spare cores.
- Every operation of `sync/atomic` is sequentially consistent, so each
  store of a cursor is a full barrier. Go has no release-only store such
  as the `lazySet` of the Java Disruptor.
- The Disruptor's published latency of 52 ns per hop is a Java
  measurement. A Go design states its own numbers.

## Allocations

- A ceiling above zero states its cause in the allocation contract of
  the docblock. The cause is a cost that Go imposes, such as a channel
  per call that honours a context.
- A profile precedes every ceiling. `go test -bench <name>
  -memprofile <file> -memprofilerate=1` with `go tool pprof
  -sample_index=alloc_objects` lists the sites, and the ceiling equals
  the measured count.
- A pointer that a call passes to an interface method escapes to the
  heap. The interface standard states where such a request comes from.
- A function that returns bytes appends them to the caller's `dst`, as
  `AppendBinary` does.

## Measurement

- Every hot path has a benchmark that runs at 1 and 4 cores with
  `-cpu=1,4`, and an allocation test that checks its ceiling in an
  ordinary test run. dokimi generates both from the budget of a callable of
  a suite. A hot path without a suite, such as a method of a type whose
  codec kanon generates, keeps its hand-written `Benchmark<Unit>` under
  `bench.Start(b).MaxAllocs(n)` and its `Test<Unit>Allocs`. So does a pass
  of the planner over 1,000,000 shards.
- Every RFC states the costs and the targets of its hot paths. A slice is
  done when its benchmarks meet the targets.
- A comparison of two versions runs both sides in alternating rounds and
  compares them with `benchstat`. A change states a gain only after such
  a comparison.
- A change that slows a hot path by more than 10%, at p below 0.05, needs
  a stated reason, and the reviewer accepts or refuses it.

## Complexity

- An optimization starts from a profile and ends with an interleaved
  comparison of before and after.
- An optimization outside a hot path needs a benchmark that shows its
  gain. Without one, the simpler code remains.

## References

| What | Where |
|---|---|
| Thompson, Farley, Barker, Gee and Stewart, Disruptor: High performance alternative to bounded queues for exchanging data between concurrent threads, 2011 | <https://lmax-exchange.github.io/disruptor/disruptor.html> |
| Fowler, The LMAX Architecture, 2011 | <https://martinfowler.com/articles/lmax.html> |
| The Go Memory Model, atomic values | <https://go.dev/ref/mem> |

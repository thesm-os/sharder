<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# Packages and modules

The repository has five Go modules. The root module is the library, and the other four contain the etcd adapter, the HTTP transport, `sharderctl` and the examples.

## Modules

| Module | Directory | Contains | Requires |
|---|---|---|---|
| `go.thesmos.sh/sharder` | `.` | The library | `go.thesmos.sh/core`, `go.thesmos.sh/kanon` and `github.com/cespare/xxhash/v2`. Its tests and its conformance kits also require `go.dokimi.dev/assert` |
| `go.thesmos.sh/sharder/etcd` | `etcd/` | `store.Store` and `membership.Membership` over etcd | The root module and the etcd client |
| `go.thesmos.sh/sharder/httptransport` | `httptransport/` | The clients of the protocol, and an HTTP/2 handler that calls its servers | The root module |
| `go.thesmos.sh/sharder/cmd` | `cmd/` | `sharderctl` | The root module, `etcd` and `httptransport` |
| `go.thesmos.sh/sharder/examples` | `examples/` | One example per preset of the support matrix, and an example of two processes over etcd | The root module, `etcd` and `httptransport` |

Each backend is a module of its own, so the module graph of an application contains only the backends that it imports. The tree does not contain a `grpc` module, a `sharderd` program or a `membership/static` package, because the support matrix does not contain the gRPC transport, the `service` topology or a static membership.

## Packages of the root module

A contract is an interface together with the package that declares it. Each built-in implementation of a contract is a subpackage of the contract's package. Each contract with more than one implementation has a conformance kit in the subpackage that adds `test` to the package's name, such as `store/storetest`.

A package may import the packages in its row, and every package that those packages import.

### Shared types and configuration

| Package | Contains | Imports from sharder |
|---|---|---|
| `shard` | The value types that every package shares, such as scopes, nodes, placements and the cell map | Nothing |
| `proposal` | The proposals of operators, their kinds and their IDs | `shard` |
| `config` | Specs, the strategy registry, the support matrix type, presets, `Resolve` and `Change` | `shard`, `proposal` |
| `builtin` | `Support`, `Register`, `Presets`, a constructor of the factory of each built-in strategy that the support matrix contains, and the names of `exclusive`, `embedded`, `watch` and the four presets | `config` and the built-in strategies |

### Strategies, fencing and budgets

| Package | Contains | Imports from sharder |
|---|---|---|
| `partition` | The `Partitioner` and `Hash` contracts, and layouts with their lookup | `shard` |
| `partition/fixedslots` | The partitioner `fixed-slots` | `partition` |
| `partition/singleton` | The partitioner `singleton` | `partition` |
| `partition/xxhash64` | The hash `xxhash64`, which is XXH64 with seed 0 | `partition` |
| `partition/partitiontest` | The conformance kit of partitioners and hashes | `partition` |
| `placement` | The `Placer`, `Constraint` and `Goal` contracts, the planner's `State`, moves, plans and `Validate` | `shard`, `partition`, `replication` |
| `placement/planner` | The planner, with repair, its two modes and its local search | `placement` |
| `placement/constraint` | The six built-in constraints | `placement` |
| `placement/goal` | The seven built-in goals | `placement` |
| `placement/placementtest` | The conformance kit of placers, constraints and goals | `placement` |
| `replication` | Replica state models, their compilation into transition tables, and the built-in model `primary-only` | `shard` |
| `ownership` | The configuration of exclusive ownership and its lease | `shard` |
| `handoff` | The `Protocol` contract, scripts, transfers and the lifecycle of a transition | `shard` |
| `handoff/breakbeforemake` | The protocol `break-before-make` | `handoff` |
| `fence` | Fencing tokens, `fence.Retired`, and the interfaces of fenced storage that an application implements | `shard` |
| `budget` | Disruptions, decisions, reservations, and the `Budget` interface that admits disruptions | `shard`, `replication` |

### Process wiring

| Package | Contains | Imports from sharder |
|---|---|---|
| `store` | The `Reader`, `Store` and `Writer` contracts, the records of a scope, and the errors of a commit | `shard`, `budget`, `proposal` |
| `store/memory` | The in-memory store and membership, with fault hooks for tests and simulations | `store`, `membership`, `config` |
| `store/storetest` | The conformance kit of stores | `store`, `internal/streamtest` |
| `membership` | The `Membership` contract, views and sessions | `shard`, `ownership` |
| `membership/membershiptest` | The conformance kit of memberships | `membership`, `store`, `internal/streamtest` |
| `protocol` | Messages, metadata, the server and client contracts, and `Dispatch` | `shard`, `proposal`, `budget` |
| `protocol/local` | The in-process transport, `Network` and `Local` | `protocol` |
| `protocol/protocoltest` | The conformance kit of transports | `protocol` |

### Components

| Package | Contains | Imports from sharder |
|---|---|---|
| `controller` | `Controller` with its `Config`, and the errors of a controller process | `shard`, `config`, `proposal`, `budget`, `store`, `membership`, `protocol` and its internal packages |
| `replica` | `Agent` with its `Config`, `Handler` with its requests and results, `LoadReporter`, and the errors of an agent | `shard`, `config`, `fence`, `store`, `membership`, `protocol` and its internal packages |
| `router` | `Router` with its requests and results, `Watcher` with its `Config`, `Do`, `NotOwnerError`, and the errors of a router | `shard`, `config`, `store`, `membership` and its internal packages |
| `sim` | The deterministic simulator, with its scheduler, faults, safety properties and crash points | Every package of the root module |

## Internal packages

Each controller, agent and router is a state machine. A machine takes one event at a time and returns the effects of the event, such as calls, commits and timers. `internal/driver` runs the machines in production, and `sim` runs every machine of a namespace on one goroutine. The machines are not part of the public API, so they are internal packages of the root module.

Each internal package has at least two callers, so the logic that two components share exists once. `internal/control` is the only caller of `internal/engine` and `internal/scope`. It runs them for the root and for each cell.

| Package | Contains | Callers |
|---|---|---|
| `internal/machine` | The events that a machine takes and the effects that it returns | The four machines, `internal/driver` and `sim` |
| `internal/machine/root` | The machine of a namespace's root | `controller`, `sim` |
| `internal/machine/cell` | The machine of a cell's controller | `controller`, `sim` |
| `internal/machine/agent` | The machine of an agent | `replica`, `sim` |
| `internal/machine/routes` | The machine of a router's cell map and route caches | `router`, `sim` |
| `internal/control` | The controller of one scope, with the acquisition of the scope, its assignment, the views of its membership, its planning passes, its budget, its proposals and its instruments | `internal/machine/root`, `internal/machine/cell` |
| `internal/engine` | The transition engine, with its records, steps, abort paths, retries and batches of commits, and the bounds on the transitions in flight and on the callbacks to each node | `internal/control` |
| `internal/scope` | The acquisition of a scope, commits with receipts, and the settlement of unknown outcomes | `internal/control` |
| `internal/follow` | The paged snapshots and the watch of a scope, which it passes to its machine under the store's rules for readers | `internal/machine/cell`, `internal/machine/agent`, `internal/machine/routes` |
| `internal/driver` | The production driver, which performs the effects of a machine under core's `task` | `controller`, `replica`, `router` |
| `internal/streamtest` | The receive of the items of iterators and channels with a bound on each wait, for the tests and the conformance kits | `store/storetest`, `membership/membershiptest`, and the tests of `store/memory` |

## Adapters, the command and the examples

- `etcd/` contains package `etcd`. Its `internal/keys` declares the key layout, and its `internal/txn` builds each transaction and computes `Fits`. Its tests run real etcd servers through etcd's `embed` package, with a proxy that drops a connection before or after a commit applies.
- `httptransport/` contains package `httptransport`, with the framing of each family of calls in a file of its own.
- `cmd/sharderctl/` is a package `main`. `cmd/internal/ctl` contains the commands, so that tests run them in-process.
- `examples/` contains one directory per preset of the support matrix: `exclusiveworker/`, `consumergroup/`, `actorentities/` and `leaderelection/`. `examples/twoprocess/` runs two processes over etcd, and `examples/internal/fencedstore` contains the fenced storage that the examples share.

## Import rules

1. The root module imports only the standard library, `go.thesmos.sh/core`, `go.thesmos.sh/kanon`, `github.com/cespare/xxhash/v2` and its own packages. Its tests and its conformance kits also import `go.dokimi.dev/assert`.
2. `shard` does not import another package of sharder.
3. A subpackage may import the package above it. A package never imports a package below it.
4. The strategy packages and their subpackages do not import `config`, `builtin`, `controller`, `replica`, `router`, `sim` or an internal package. The strategy packages are `partition`, `placement`, `replication`, `ownership`, `handoff`, `fence` and `budget`.
5. The modules `etcd`, `httptransport`, `cmd` and `examples` do not import a package below `go.thesmos.sh/sharder/internal/`. Go's own rule for `internal` allows these imports, because the go command compares import paths in module mode. The import paths of these modules start with `go.thesmos.sh/sharder`.

## Tests and generated files

- Every hand-written source file except `doc.go` has a test file beside it, in the package `<name>_test`.
- Every implementation of a contract runs the conformance kit of the contract in its tests. Every fenced writer runs core's `coretest/epochtest`.
- These hot paths have a benchmark and a `Test<Unit>Allocs` test: `shard.CellMap.Locate`, `partition.Layout.Locate`, `Position` of each partitioner, `Sum64` of `xxhash64`, `Replicas`, `Utilisation` and `PeakCounts` of `placement.State`, `Check` of each built-in constraint, `Delta` of each built-in goal, `router.Watcher.Route`, `protocol.Metadata.Check`, and `Key` and `Digest` of `protocol.Request`.
- kanon generates `<file>.kanon.go` and `<file>.kanon_test.go` beside each source file whose types have codecs. The codec tests keep their golden files in `testdata/golden/<package>.<Type>.kanon.golden`. The root module lists kanon in the `tool` directive of its `go.mod`.
- stringer generates `<file>.string_gen.go` for the integer enumerations of a source file.

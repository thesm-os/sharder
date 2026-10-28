<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# Packages and modules

The repository has two Go modules. The root module is the library, and the examples module contains the examples.

## Modules

| Module | Directory | Contains | Requires |
|---|---|---|---|
| `go.thesmos.sh/sharder` | `.` | The library | `go.thesmos.sh/core`, `go.thesmos.sh/kanon` and `github.com/cespare/xxhash/v2`. Its tests and its kits also require `go.dokimi.dev/assert`, `go.dokimi.dev/conform` and `go.dokimi.dev/stub` |
| `go.thesmos.sh/sharder/examples` | `examples/` | One example per preset and per topology of the support matrix | The root module and `go.thesmos.sh/core`. Its tests also require `go.dokimi.dev/assert` |

The root module contains the in-memory store and membership of `store/memory`, and the in-process transport of `protocol/local`. It contains no adapter of a production store and no network transport.

## Packages of the root module

A contract is an interface together with the package that declares it. Each built-in implementation of a contract is a subpackage of the contract's package. Each contract with more than one implementation has a conformance kit in the subpackage that adds `test` to the package's name, such as `store/storetest`. The same subpackage contains the generated doubles of the interfaces of the package and the generated builders of its structs, also for a package without a conformance kit, such as `config/configtest`.

A package may import the packages in its row, and every package that those packages import.

### Shared types and configuration

| Package | Contains | Imports from sharder |
|---|---|---|
| `shard` | The value types that every package shares, such as scopes, nodes, placements and the cell map | Nothing |
| `shard/shardtest` | The suite of the cell map, and the builders of `Placement` and `Node` | `shard` |
| `proposal` | The proposals of operators, their kinds and their IDs | `shard` |
| `config` | Specs, the strategy registry, the support matrix type, presets, `Resolve` and `Change` | `shard`, `proposal` |
| `config/configtest` | The doubles of `Parsed`, `Factory` and `Provider`, and the builders of `Spec` and `Selection` | `shard`, `config` |
| `builtin` | `Support`, `Register`, `Presets`, a constructor of the factory of each built-in strategy that the support matrix contains, and the names of `exclusive`, `embedded`, `watch` and the four presets | `config` and the built-in strategies |

### Strategies, fencing and budgets

| Package | Contains | Imports from sharder |
|---|---|---|
| `partition` | The `Partitioner` and `Hash` contracts, and layouts with their lookup | `shard` |
| `partition/fixedslots` | The partitioner `fixed-slots` | `partition` |
| `partition/singleton` | The partitioner `singleton` | `partition` |
| `partition/xxhash64` | The hash `xxhash64`, which is XXH64 with seed 0 | `partition` |
| `partition/partitiontest` | The conformance suites of partitioners, hashes and layouts, and the doubles of partitioners and hashes | `shard`, `partition`, `partition/fixedslots`, `partition/xxhash64` |
| `placement` | The `Placer`, `Constraint` and `Goal` contracts, the planner's `State`, moves, plans and `Validate` | `shard`, `partition`, `replication` |
| `placement/planner` | The planner, with repair, its two modes and its local search | `placement` |
| `placement/constraint` | The six built-in constraints | `placement` |
| `placement/goal` | The seven built-in goals | `placement` |
| `placement/placementtest` | The conformance suites and the doubles of placers, constraints and goals, the suite and the double of `State`, and the cells that their checks run on | `shard`, `partition`, `replication`, `placement`, `placement/constraint`, `placement/goal`, `placement/planner` |
| `replication` | Replica state models, their compilation into transition tables, and the built-in model `primary-only` | `shard` |
| `ownership` | The configuration of exclusive ownership and its lease | `shard` |
| `handoff` | The `Protocol` contract, scripts, transfers and the lifecycle of a transition | `shard` |
| `handoff/breakbeforemake` | The protocol `break-before-make` | `handoff` |
| `handoff/handofftest` | The double of `Protocol` | `shard`, `handoff` |
| `fence` | Fencing tokens, `fence.Retired`, and the interfaces of fenced storage that an application implements | `shard` |
| `fence/fencetest` | The double of `Acquirer` | `fence` |
| `budget` | Disruptions, decisions, reservations, and the `Budget` interface that admits disruptions | `shard`, `replication` |

### Process wiring

| Package | Contains | Imports from sharder |
|---|---|---|
| `store` | The `Reader`, `Store` and `Writer` contracts, the records of a scope, and the errors of a commit | `shard`, `budget`, `proposal` |
| `store/memory` | The in-memory store and membership, with fault hooks for tests and simulations | `store`, `membership`, `config` |
| `store/storetest` | The conformance suite of stores with the operations of the kit that its checks call, the doubles of the interfaces of `store`, and the builder of `Mutation` | `shard`, `budget`, `proposal`, `store`, `store/memory`, `internal/streamtest` |
| `membership` | The `Membership` contract, views and sessions | `shard`, `ownership` |
| `membership/membershiptest` | The conformance suite of memberships with the operations of the kit that its checks call, and the doubles of the interfaces of `membership` | `shard`, `ownership`, `membership`, `store`, `store/memory`, `internal/streamtest` |
| `protocol` | Messages, metadata, the server and client contracts, and `Dispatch` | `shard`, `proposal`, `budget` |
| `protocol/local` | The in-process transport, `Network` and `Local` | `protocol` |
| `protocol/protocoltest` | The conformance suite of transports through `Connector`, the suite of `Metadata`, and the doubles of the clients of the protocol | `shard`, `budget`, `proposal`, `protocol`, `protocol/local` |

### Components

| Package | Contains | Imports from sharder |
|---|---|---|
| `controller` | `Controller` with its `Config`, and the errors of a controller process | `shard`, `config`, `proposal`, `budget`, `store`, `membership`, `protocol` and its internal packages |
| `replica` | `Agent` with its `Config`, `Handler` with its requests and results, `Revoker`, `Runner` with its `Opener`, `LoadReporter`, and the errors of an agent | `shard`, `config`, `fence`, `store`, `membership`, `protocol` and its internal packages |
| `replica/replicatest` | The doubles of `Handler`, `Revoker`, `LoadReporter` and `Opener` | `shard`, `replica` |
| `router` | `Router` with its requests and results, `Watcher` with its `Config`, `Do`, `NotOwnerError`, and the errors of a router | `shard`, `config`, `store`, `membership` and its internal packages |
| `router/routertest` | The suite of `Watcher` and the double of `Router` | `router` |
| `embedded` | `Process` with its `Config` and its `Namespace`, the `Transport` of a process, and the errors of a process | `shard`, `config`, `fence`, `store`, `membership`, `protocol`, `controller`, `replica` and `router` |

## Internal packages

Each controller, agent and router is a state machine. A machine takes one event at a time and returns the effects of the event, such as calls, commits and timers. `internal/driver` runs the machines in production, and the tests of each machine step it on one goroutine. The machines are not part of the public API, so they are internal packages of the root module.

The internal packages contain the machines and the logic that two components share, so that logic exists once. `internal/control` is the only caller of `internal/engine` and `internal/scope`. It runs them for the root and for each cell.

| Package | Contains | Callers |
|---|---|---|
| `internal/machine` | The events that a machine takes and the effects that it returns | Every machine and each of its parts, `internal/driver`, `controller`, `replica` and `router` |
| `internal/machine/machinetest` | The double of `Machine` | The tests of `internal/driver` |
| `internal/machine/root` | The machine of a namespace's root | `controller` |
| `internal/machine/cell` | The machine of a cell's controller | `controller` |
| `internal/machine/agent` | The machine of an agent | `replica`, and `controller` for the agent of the root that hosts the cells of a controller process |
| `internal/machine/routes` | The machine of a router's cell map and route caches | `router` |
| `internal/machine/routes/routestest` | The suite and the double of the machine of `internal/machine/routes`, and the router of a memory store that its checks run on | The tests of the kit |
| `internal/control` | The controller of one scope, with the acquisition of the scope, its assignment, the views of its membership, its planning passes, its budget, its proposals and its instruments | `internal/machine/root`, `internal/machine/cell` |
| `internal/engine` | The transition engine, with its records, steps, abort paths, retries and batches of commits, and the bounds on the transitions in flight and on the callbacks to each node | `internal/control` |
| `internal/scope` | The acquisition of a scope, commits with receipts, and the settlement of unknown outcomes | `internal/control` |
| `internal/follow` | The paged snapshots and the watch of a scope, which it passes to its machine under the store's rules for readers | `internal/machine/cell`, `internal/machine/agent`, `internal/machine/routes` |
| `internal/driver` | The production driver, which performs the effects of a machine under core's `task` | `controller`, `replica`, `router` |
| `internal/driver/drivertest` | The doubles of `Handler`, `Revoker` and `LoadReporter` | The tests of `internal/driver` |
| `internal/streamtest` | The receive of the items of iterators and channels with a bound on each wait, for the tests and the conformance kits | `store/storetest`, `membership/membershiptest`, and the tests of `store/memory` |

## The examples

`examples/` contains one directory per example. The key of each directory is the kind of the example and the registered name that it shows: `preset-actor-entities/`, `preset-consumer-group/`, `preset-exclusive-worker/`, `preset-leader-election/` and `topology-embedded/`. Each directory contains the program in `main.go` and its test in `main_test.go`. The program contains the application of the example. `examples/internal/fencedstore` is the fenced storage that the examples share.

## Import rules

1. The root module imports only the standard library, `go.thesmos.sh/core`, `go.thesmos.sh/kanon`, `github.com/cespare/xxhash/v2` and its own packages. Its tests and its kits also import `go.dokimi.dev/assert`, `go.dokimi.dev/conform` and `go.dokimi.dev/stub`.
2. `shard` does not import another package of sharder.
3. A subpackage may import the package above it. A package never imports a package below it.
4. The strategy packages and their subpackages do not import `config`, `builtin`, `controller`, `replica`, `router` or an internal package. The strategy packages are `partition`, `placement`, `replication`, `ownership`, `handoff`, `fence` and `budget`.
5. The examples module does not import a package below `go.thesmos.sh/sharder/internal/`. Go's own rule for `internal` allows these imports, because the go command compares import paths in module mode. The import paths of the examples start with `go.thesmos.sh/sharder`.

## Tests and generated files

- Every hand-written source file except `doc.go` has a test file beside it, in the package `<name>_test`.
- Every implementation of a contract runs the conformance kit of the contract in its tests. Every fenced writer runs core's `coretest/epochtest`.
- These hot paths have a benchmark and an allocation test. dokimi generates both from the budget of a callable of a suite, and a spec writes them by hand for a path without a suite: `shard.CellMap.Locate`, `partition.Layout.Locate`, `Position` of each partitioner, `Sum64` of `xxhash64`, the readers and `Apply` of `placement.State`, `Check` of each built-in constraint, `Delta` of each built-in goal, `router.Watcher.Route`, a membership view of `internal/machine/routes`, `protocol.Metadata.Check`, and `Key` and `Digest` of `protocol.Request`.
- kanon generates `<file>.kanon.go` and `<file>.kanon_test.go` beside each source file whose types have codecs. The codec tests keep their golden files in `testdata/golden/<package>.<Type>.kanon.golden`. The root module lists kanon in the `tool` directive of its `go.mod`.
- dokimi generates `<file>_enum.go` for the enumerations of a source file, with `<file>_enum_test.go` for an exported enumeration, and `errors_sentinel_test.go` for the error values of a package.
- dokimi generates `<file>_suite.go` with its spec in the kit of each declaration under `suite`, and `<file>_stub.go` with its spec in the kit of each declaration under `stub`. It writes `<file>_suite_test.go` with the tests of each subject of a suite in the external test package of the subject, or of the kit for a struct whose suite is in that kit.

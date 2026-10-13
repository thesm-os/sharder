<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# sharder

[![CI](https://github.com/thesm-os/sharder/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/thesm-os/sharder/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/go.thesmos.sh/sharder.svg)](https://pkg.go.dev/go.thesmos.sh/sharder)
[![Go version](https://img.shields.io/github/go-mod/go-version/thesm-os/sharder)](go.mod)
[![License](https://img.shields.io/badge/license-BUSL--1.1-blue.svg)](LICENSE)

sharder is a Go library that a sharded application embeds to map keys to shards, place the shards
on the processes of the application and move them between processes. A fixed core runs every
move. It gives each writer of a shard a new epoch, and a fence in the storage of the application
refuses the writes of an older epoch once a newer writer has acquired the shard.

An application describes each namespace of shards in a spec. The spec selects a preset, and it can
select another strategy on each axis where the preset does not fit: partitioning, placement,
replication, ownership, handoff, topology and routing. `config.Resolve` returns
`config.ErrUnsupported` for a spec outside the support matrix of the release, so such a spec does
not take effect.

## Install

```sh
go get go.thesmos.sh/sharder@latest
```

sharder requires Go 1.27.2 or later.

## Quick start

Each example runs every process of an application in one OS process. The processes share the
in-memory store of `store/memory` and the in-process transport of `protocol/local`. To run the
example of the preset `exclusive-worker`, clone the repository and run the program in the module
`examples`:

```sh
git clone https://github.com/thesm-os/sharder.git
cd sharder/examples
go run ./preset-exclusive-worker
```

The program starts worker-a, and the controller places the 8 shards of the namespace `jobs` on it.
worker-b then starts, and the controller moves half of the shards to it. Once worker-b closes, its
shards move back to worker-a. Each line of the output lists the worker that ran each job of one
tenant:

```text
tenant-06: 1:worker-a 2:worker-a 3:worker-b 4:worker-b 5:worker-b 6:worker-a 7:worker-a 8:worker-a
```

The jobs that run on worker-b differ from run to run. The [catalogue of the
examples](examples/README.md) lists an example for each preset and for the topology `embedded`.

## Embed sharder

Each process of the application runs one `embedded.Process`. The process runs the controller of
the process, the agent of each namespace, and the router of each namespace that sets `Route`. The
application supplies these parts:

| Part | Interface | Implementation in this module |
|---|---|---|
| The store of the scopes of each namespace | `store.Store` | `store/memory` |
| The sessions of the processes | `membership.Membership` | `store/memory` |
| The calls between the processes | `embedded.Transport` | `protocol/local` |
| The fence of each tenure, in the storage of the application | `fence.Acquirer` | none |
| The work of each tenure | `replica.Opener`, which `replica.NewRunner` runs | none |

`store/memory` keeps every scope in the memory of one OS process, and `protocol/local` connects
the processes of one OS process by direct calls. The tests and the examples run on both.

These calls build, serve and start a process over `store/memory` and `protocol/local`. `clk` is a
clock of core and `logger` is a `*slog.Logger`. `opener` and `acquirer` are the application's
implementations of `replica.Opener` and `fence.Acquirer`:

```go
reg := config.NewRegistry(builtin.Support())
if err := builtin.Register(reg); err != nil {
	return err
}
st := memory.New(clk)
net := local.NewNetwork()
node := shard.Node{ID: "worker-a", Address: "worker-a:7000", Identity: "worker-a"}
p, err := embedded.New(&embedded.Config{
	Namespaces: []embedded.Namespace{{
		Spec:    config.Spec{Namespace: "jobs", Preset: builtin.ExclusiveWorker},
		Handler: replica.NewRunner(opener),
		Guard:   acquirer,
	}},
	Store:      st,
	Membership: st.Membership(),
	Transport:  net.Local(protocol.Metadata{Caller: node.Identity, Version: protocol.Current}),
	Clock:      clk,
	Rand:       crypto.New(),
	Reporter:   noop.New(),
	Registry:   reg,
	Logger:     logger,
	Node:       node,
	Volatile:   true,
})
if err != nil {
	return err
}
net.AddAgent("jobs", node.ID, p.AgentServer())
net.AddController(node.Address, p.ControllerServer())
if err := p.Start(ctx); err != nil {
	return err
}
```

- `AddAgent` and `AddController` of `protocol/local` deliver the calls of the other processes to
  `Process.AgentServer` and `Process.ControllerServer`. A transport adapter does the same.
- `store/memory` does not provide `config.Durable`, so the process runs on it only with
  `Volatile`.
- `Process.Router` returns the router of a namespace that sets `Route`. `router.Do` calls a
  function with the route of a key and routes the key again after a `*router.NotOwnerError`.
- `Process.Close` moves the replicas of the node to the other processes before the process stops.
  Its context needs a deadline. When the deadline passes first, the replicas fail over as after a
  crash.

## Packages

Each package states its contract, its errors and its rules for concurrent use in its package
documentation.

| Area | Packages | Provides |
|---|---|---|
| Processes | [`embedded`](embedded/), [`controller`](controller/), [`replica`](replica/), [`router`](router/) | An application process of the topology `embedded`, a controller process with the root of each namespace and the controllers of its cells, the agent that hosts the replicas of a node, and the router that maps the key of a request to the replicas of its shard |
| Specs | [`config`](config/), [`builtin`](builtin/) | Namespace specs, the registry of strategies and presets, the support matrix, and the built-in strategies and presets |
| Partitioning | [`partition`](partition/), [`partition/fixedslots`](partition/fixedslots/), [`partition/singleton`](partition/singleton/), [`partition/xxhash64`](partition/xxhash64/) | The map from keys to positions, the division of a cell into shards, the partitioners `fixed-slots` and `singleton`, and the hash `xxhash64` |
| Placement | [`placement`](placement/), [`placement/planner`](placement/planner/), [`placement/constraint`](placement/constraint/), [`placement/goal`](placement/goal/) | The placement of the replicas of a cell on its nodes, the planner, and its six constraints and seven goals |
| Replicas and moves | [`replication`](replication/), [`ownership`](ownership/), [`handoff`](handoff/), [`handoff/breakbeforemake`](handoff/breakbeforemake/) | Replica state models, exclusive ownership with a lease, and the protocol `break-before-make`, which turns a move into steps with an abort path for each step |
| Safety | [`fence`](fence/), [`budget`](budget/), [`proposal`](proposal/) | Fencing tokens and the interfaces of fenced storage, the disruption budget of a cell, and the changes that operators propose |
| State | [`store`](store/), [`store/memory`](store/memory/), [`membership`](membership/) | The store of the scopes of the namespaces, the live nodes of each scope, and an in-memory store and membership with hooks that inject faults |
| Protocol | [`protocol`](protocol/), [`protocol/local`](protocol/local/) | The calls between controllers and agents, the calls of operators and cluster managers, and an in-process transport |
| Values | [`shard`](shard/) | The value types that every package shares |
| Conformance | [`partition/partitiontest`](partition/partitiontest/), [`placement/placementtest`](placement/placementtest/), [`membership/membershiptest`](membership/membershiptest/), [`store/storetest`](store/storetest/), [`protocol/protocoltest`](protocol/protocoltest/) | Suites that check an implementation of each interface against the laws of its package |

## Support matrix

A spec selects only the strategies and the presets in this table. `builtin.Support` returns the
same matrix, and a test of package `builtin` fails when the two differ.

| Axis | Supported |
|---|---|
| `partitioning` | `fixed-slots`, `singleton` |
| `placement` | `planner` |
| `replication` | `primary-only` |
| `ownership` | `exclusive` |
| `handoff` | `break-before-make` |
| `topology` | `embedded` |
| `routing` | `watch` |
| `constraint` | Every registered name |
| `goal` | Every registered name |
| `hash` | Every registered name |
| Presets | `exclusive-worker`, `consumer-group`, `actor-entities`, `leader-election` |
| Required capabilities | `fenced` |

The ownership configuration of a namespace sets `"fenced": true`, because only a fenced
`exclusive` provides the capability `fenced`.

## Design

- Controllers and agents are state machines. Each takes one event at a time and returns its
  effects. The reply to a callback and the end of a timer are events, and a commit and a callback
  are effects. A runtime performs the effects.
- The controller commits the record of a move before the first callback of the move and commits
  each step before the next callback. A controller that takes over continues each move from its
  record.
- Each callback contains the epochs of its move and the incarnation of its node. An agent refuses
  a callback of an older controller and a callback to an ended incarnation.
- The disruption budget admits a negotiable disruption of a node only after a drain has moved
  every replica off the node. `maxUnavailable` bounds the moves that make a replica unavailable.
- The store, the membership, the transport, the partitioners and the placers each have a
  conformance suite, so an adapter proves its contract with one test.
- The production packages import the standard library, `go.thesmos.sh/core`, the runtime of
  `go.thesmos.sh/kanon` and `github.com/cespare/xxhash/v2`.
- The [testing standard](docs/standards/testing.md) requires tests that cover every statement of
  each package and detect every mutant of its code. Property tests drive the state machines
  through random sequences of events and faults.
- CI runs the linters, the tests and the race detector on Linux, macOS and Windows. A nightly job
  runs the fuzz targets, the benchmarks and mutation testing.

## Documentation

- The [RFCs](docs/rfc/README.md) propose each part of the design and record the alternatives that
  each rejected. The [ADRs](docs/adr/README.md) record the decisions that followed.
- The [architecture](docs/architecture/README.md) describes the packages and their imports, and
  the [C4 model](docs/c4/README.md) shows the system, its containers, its components and its
  deployment in each topology.
- The [roadmap](docs/roadmap/README.md) orders the milestones.
- Each [research](docs/research/README.md) file records the answer to one question about other
  sharding systems and lists its sources.
- The [standards](docs/standards/README.md) state the rules for interfaces, performance and tests.

## Versioning

Until v1.0.0, a minor release of sharder can change an exported API. Semantic versioning allows
such a change for major version zero. Each change that a release contains has a changeset under
`.changeset/`, and the release adds the changeset to the changelog of each package that it names.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before you open a pull request. The Makefile runs every
check through [ergon](https://github.com/dokimasia/ergon):

```sh
make check   # the gate: lint, test, race and audit in every module
make fmt     # format the sources with the formatters of .golangci.yml
make help    # list the targets
```

## Security

Report a vulnerability privately through the channels of [SECURITY.md](SECURITY.md). Do not open
a public issue for it.

## License

sharder is licensed under the Business Source License 1.1. The license permits copying,
modification and non-production use, and production use needs a commercial license from
ThesmOS B.V. Each version becomes available under the Apache License 2.0 four years after its
first publication. See [LICENSE](LICENSE).

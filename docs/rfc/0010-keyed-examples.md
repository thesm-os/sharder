---
rfc: 0010
title: A catalogue of examples keyed by registered names
author: Roy Klopper
status: Accepted
created: 2026-10-13
updated: 2026-10-13
discussion: none
supersedes: none
superseded-by: none
produces-adr: tbd
research: none
related: RFC-0001, RFC-0002, RFC-0006, RFC-0008, RFC-0009
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# RFC-0010: A catalogue of examples keyed by registered names

## Summary

The module `go.thesmos.sh/sharder/examples` contains one program per example. The name of the directory of each example is its key. The key joins the kind of the example and the registered name that the example shows with a hyphen, as in `preset-exclusive-worker`. The kinds are the terms of a spec: `preset` for an example of a preset, and `topology` for an example of a topology. The catalogue has one example per preset of the support matrix and one example of the topology `embedded`. The examples share the package `internal/fencedstore`. A test derives the catalogue from `builtin`.

## Motivation

The directories of the examples in the accepted layout are `exclusiveworker/`, `consumergroup/`, `actorentities/`, `leaderelection/` and `twoprocess/`. These names have three defects:

- The words of each name run together. A reader has to parse `actorentities` before the preset `actor-entities` is recognisable.
- A name does not show the kind of its example. The tree sorts `twoprocess` among the presets, and nothing in the name shows that the example is about a topology.
- The spelling of a name differs from the registered name that the example shows, such as `exclusiveworker` against `exclusive-worker`. A program cannot derive one from the other, so no test can check that the examples cover the support matrix.

## Detailed design

### Keys

- The key of an example is its kind, a hyphen and the registered name that the example shows. The key is the name of the directory of the example.
- The kinds are the terms of a spec: the field `preset`, and the axis `topology`.
- The tree sorts the keys of one kind together, and the registered names order the examples within a kind.

| Kind | Key | Registered name |
|---|---|---|
| A preset of the support matrix | `preset-<name>` | A `config.PresetName` that `builtin.Presets` returns |
| A topology of the support matrix | `topology-<name>` | A `shard.StrategyName` that `builtin.Support` lists on `config.Topology` |

### The catalogue

| Directory | Application | Shows |
|---|---|---|
| `preset-actor-entities` | Entities that receive their requests through a router | `router.New`, `router.Do`, and the redirect of a `NotOwnerError` after a move |
| `preset-consumer-group` | Consumers of the fixed partitions of a queue | One slot per partition, and a new consumer of a partition that starts at the offset that the old consumer committed through the fence |
| `preset-exclusive-worker` | Workers that each run the jobs of the tenants of their shards | `controller.New`, `replica.Start`, a `Handler`, `Agent.Token` and the writes of a tenure through its fence |
| `preset-leader-election` | One active instance of a periodic task | The partitioner `singleton`, and a new leader after the old leader stops |
| `topology-embedded` | Two application processes of one namespace | Two initial specs and one committed spec, the cell hosted through an `add` of the root, a new host of the cell, a new leader of the root, and a `Close` of every component |

The examples of the kind `preset` cover each preset of the support matrix once. `topology-embedded` uses the preset `exclusive-worker`, because `Resolve` rejects a spec without a preset. The example shows the controllers of the topology, not the preset.

### Layout

```text
examples/
  go.mod                     module go.thesmos.sh/sharder/examples
  README.md                  the catalogue
  preset-actor-entities/     package main
  preset-consumer-group/     package main
  preset-exclusive-worker/   package main
  preset-leader-election/    package main
  topology-embedded/         package main
  internal/fencedstore/      the fenced storage of the examples
```

- Each example is a program that uses only the exported API of the root module. Its test runs the program in the process of the test.
- The examples run every node in one OS process, over `store/memory` with `controller.Config.Volatile` and over `protocol/local`. The store of production is undecided, so no example starts a second OS process.
- `internal/fencedstore` records the key of each callback in the transaction of the effect of the callback. It refuses a write whose token is older than the fence of its shard. Its replicas apply the writes of the store with a delay, so a test can pause a replica before it applies the last write of the old writer.
- The tests of each example of a preset replay every callback of a move. They also check that the handler does not read its shard from a paused replica, as milestone 0001 requires.
- `README.md` lists the directory, the application and the API of each example.
- A test of the module derives the expected directories from `builtin.Presets` and `builtin.Support`. It fails when a supported preset or topology has no example, when a directory has a key that no supported name has, and when the table of `README.md` differs from the directories.
- The examples skip mutation testing. Their tests cover 100% of their statements.

### Verdicts on other examples

| Candidate | Verdict | Reason | Evidence that would reverse it |
|---|---|---|---|
| A quick start before the presets | Out | It would show a preset a second time, and `preset-exclusive-worker` is the smallest embedding of the library | A preset example that a new user cannot follow without a smaller one |
| One example per failure, such as an outage of the store | Out | The simulator checks every failure trace of milestone 0001 for every preset | A failure that an application has to handle in its own code |
| One example per action of an operator, such as a drain | Out | `sharderctl` performs the actions of an operator | An action of an operator that `sharderctl` does not have |
| An example of a preset or a topology outside the support matrix | Out | RFC-0001 allows no example with a strategy outside the matrix | A change of the support matrix |
| A README per example | Out | The package comment of each program describes the example, and `README.md` lists the catalogue | A program whose package comment cannot describe its setup |

## Alternatives considered

### Numbered keys

Keys such as `10-exclusive-worker/` order the examples. The Rust book numbers its listings within each chapter this way.

Rejected because a number has no meaning. A reader has to look up what each example shows. A program cannot derive the number of an example from the support matrix either.

### Names without a kind

`exclusive-worker/` and `embedded/` read better than the accepted names, and a program can derive them from `builtin`. Temporal's samples for Go list 83 such names in one directory.

Rejected because the tree mixes the kinds, so a reader cannot tell whether a directory shows a preset or a topology.

### Prefixes of their own

Prefixes such as `usecase-` and `composition-` group the examples. Akka prefixes its samples with `akka-sample-`, and nats.go prefixes its examples with `nats-`.

Rejected because a spec has its own terms for the two kinds: the field `preset` and the axis `topology`. A prefix of its own would be a second vocabulary. No program could check it against `builtin`.

## Drawbacks

- Each key repeats its kind, such as `preset-` in four directories.
- An example that does not show a registered name has no key. Every example of the catalogue shows one.

## Open questions

None.

## Unresolved and future work

- A form of `topology-embedded` with two OS processes needs a store that two processes share. It waits for the decision on the store of production.

## References

| What | Where |
|---|---|
| The rule that the examples cover each supported preset once | RFC-0001, The support matrix |
| The presets of the support matrix and their use cases | RFC-0001, Presets |
| The field `preset` and the axes of a spec | RFC-0002 |
| The handler that records its keys in the transactions of its storage | RFC-0006, The callback contract |
| The examples of the first milestone | `docs/roadmap/0001-exclusive-fenced-shards.md`, The boundary and The public composition |
| The layout of the examples module | `docs/architecture/packages.md`, Adapters, the command and the examples |
| The listings of the Rust book | <https://github.com/rust-lang/book/tree/main/listings> |
| The samples of Akka | <https://github.com/akka/akka-samples> |
| The examples of nats.go | <https://github.com/nats-io/nats.go/tree/main/examples> |
| Temporal's samples for Go | <https://github.com/temporalio/samples-go> |

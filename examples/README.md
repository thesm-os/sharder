<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# Examples

Each example is a program that embeds sharder. It runs every node in one OS process, over the in-memory store of `store/memory` and the in-process transport of `protocol/local`. Each node is an `embedded.Process`, and a `replica.Runner` runs the tenures of its shards. Run an example from this directory with `go run ./<directory>`.

The name of each directory is the kind of the example and the registered name that the example shows. The kinds are the terms of a spec: `preset` and `topology`.

| Directory | Application | Shows |
|---|---|---|
| `preset-actor-entities` | Accounts whose deposits arrive through a router | A router of each process, `router.Do`, the activation of the accounts of each tenure, and the redirect of a `NotOwnerError` after a move |
| `preset-consumer-group` | Consumers of the fixed partitions of a queue | One slot per partition, and a new consumer of a partition that starts at the offset that the old consumer committed through the fence |
| `preset-exclusive-worker` | Workers that each run the jobs of the tenants of their shards | `embedded.New`, `replica.Runner`, an `Opener`, and the writes of a tenure through its fence |
| `preset-leader-election` | One active instance of a periodic task | The partitioner `singleton`, and a new leader after the old leader stops |
| `topology-embedded` | Two application processes of one namespace | Two initial specs and one committed spec, the cell hosted through an `add` of the root, a new host of the cell, a new leader of the root, and the `Close` of a process |

The `main.go` of each example contains the application, the scenario and `main`. `internal/fencedstore` is the fenced storage that the examples share. The tests of package `examples` check this table and the directories against the support matrix.

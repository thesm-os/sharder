---
adr: 0003
title: Built-in implementations are subpackages of their contracts
status: Accepted
date: 2026-10-10
supersedes: none
superseded-by: none
rfc: RFC-0001, RFC-0002, RFC-0003, RFC-0004, RFC-0005, RFC-0006, RFC-0007, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0003: Built-in implementations are subpackages of their contracts

## Status

Accepted

## Context

A contract is an interface together with the package that declares it, such as `partition.Partitioner` in `partition`. In the package layout of the design, a contract package also contains its built-in implementations:

- `partition` contains the partitioners `fixed-slots` and `singleton`, and the hash `xxhash64`.
- `placement` contains the planner with its constraints and goals, and the function placers.
- `handoff` contains `break-before-make` and the other handoff protocols.
- `protocol` contains the in-process transport, which consists of `Network` and `Local`.

A contract and its implementations change together:

- A fix to the planner changes package `placement`. Every application that writes a constraint or a goal imports that package.
- An application that registers its own hash imports `partition`, and with it the code of every built-in partitioner.
- A new built-in adds files to the package of its contract.

In core, the subpackages `clock/hlc` and `clock/fake` implement `clock.Clock`, and `rand/pcg` implements `rand.Rand`. The design already puts the in-memory store in the subpackage `store/memory`.

## Decision

We will put each built-in implementation of a contract in a subpackage of the contract's package, because a change to one implementation then leaves the contract and the other implementations unchanged.

| Contract | Subpackages |
|---|---|
| `partition.Partitioner` | `fixedslots`, `singleton` |
| `partition.Hash` | `xxhash64` |
| `placement.Placer` | `planner` |
| `placement.Constraint` | `constraint`, with the six built-in constraints |
| `placement.Goal` | `goal`, with the seven built-in goals |
| `handoff.Protocol` | `breakbeforemake` |
| `store.Store` | `memory`, which also implements `membership.Membership` |
| `protocol.AgentClient`, `protocol.ControllerClient` | `local`, with `Network` and `Local` |

The table lists the built-ins that the support matrix contains. A contract package does not import its subpackages. `builtin` imports the built-in strategies to register their factories, and an application that uses a built-in without a registry imports its subpackage.

The rule applies to code. The package that declares the type of a built-in value also declares the value. The state models of `replication`, the modes of `ownership`, the topologies of `controller` and the routing sources of `router` are such values. `controller`, `replica`, `router` and their internal packages contain the code that runs those values. These components have no subpackages.

## Alternatives Considered

### Built-ins in the packages of their contracts

In the package table of the design, each contract package contains its built-in implementations. An application then finds a contract and its built-ins in one package. The root module also has eight fewer packages.

Rejected because a fix to one built-in changes the package of its contract, and an application that implements a contract imports every built-in of that contract.

## Consequences

**Positive:**

- A fix to a built-in changes only its subpackage.
- An application that implements a contract imports the contract without its built-ins.
- A new built-in adds a package and leaves its contract unchanged.

**Negative:**

- The root module has eight more packages: `fixedslots`, `singleton`, `xxhash64`, `planner`, `constraint`, `goal`, `breakbeforemake` and `local`.
- The import path of a built-in starts with the import path of its contract, such as `go.thesmos.sh/sharder/placement/planner`.
- Code that two built-ins share needs a package of its own, because one built-in cannot use the unexported code of another.

**Neutral:**

- A contract and its built-ins are in one module, so they still change in one release.

## References

| What | Where |
|---|---|
| go.thesmos.sh/core v0.8.0, `clock/doc.go` and `rand/doc.go`: the time and randomness seams and their implementations | https://github.com/thesm-os/core/tree/v0.8.0 |

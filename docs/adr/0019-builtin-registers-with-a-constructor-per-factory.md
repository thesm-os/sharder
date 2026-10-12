---
adr: 0019
title: Builtin registers the support matrix with Register and one constructor per factory
status: Accepted
date: 2026-10-11
supersedes: none
superseded-by: none
rfc: RFC-0001, RFC-0002, RFC-0004, RFC-0006
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0019: Builtin registers the support matrix with Register and one constructor per factory

## Status

Accepted

## Context

In the design, `builtin.Registry()` returns a new registry with the support matrix, every built-in strategy of the matrix and the presets of the matrix. The factories are unexported, and the package declares the names of the 30 built-in strategies of the design.

These facts of the repository conflict with that design:

- `config.Register` and `config.RegisterPreset` return an error. `Registry()` cannot return it, and no input makes its check fail.
- With unexported factories, an application cannot create a registry with a support matrix of its own, with some built-ins, or with a built-in under another name.
- The configuration of the planner contains the configurations of its constraints and goals as JSON. `Resolve` parses the selection of each axis of a spec, and it does not parse a configuration inside another configuration. The root stores a spec with the defaults of every configuration.
- The defaults of exclusive ownership depend on other fields. The renewal interval of a lease is a third of its TTL. The lock delay and the action at the loss of a lease depend on whether the configuration sets `fenced`.
- The support matrix has one topology and one routing source. The design declares the types `controller.Topology` and `router.Source` for them.

An emergency pass of the planner over a cell of 1,000,000 shards places the replicas of one failed node. With the default of 200,000 evaluations in the design, the pass placed 41 of the 200 replicas and returned an incomplete plan. With 1,000,000 evaluations, it placed all 200 in 0.54 to 0.56 s. A periodic pass stops at `maxSourceShards` first, after at most 80,000 evaluations.

The preset `exclusive-worker` of the design selects the goals `count` and `spread`. With one replica per shard, the cost of `spread` is always 0.

## Decision

We will build `builtin` with `Register`, `Presets` and one exported constructor per factory, because an application then composes a registry of its own from the built-ins, and `Register` returns the error of every registration.

The API:

- `Support()` returns the support matrix. `Register(r)` registers the 22 factories of the matrix and the four presets in `r`, and it joins the error of each registration that fails.
- `Presets()` returns the four presets, so an application can register some of them with `config.RegisterPreset`.
- One constructor per factory returns a `config.Factory`, such as `FixedSlots(r)`, `Planner(r)`, `Capacity()`, `Exclusive()` and `Embedded()`. `FixedSlots` and `Planner` take the registry with which they build their hash functions, constraints and goals.
- The package declares the names of `exclusive`, `embedded` and `watch`, which have no package of their own, and the names of the four presets. It does not declare the 22 other names of the design.

The factories:

- One generic factory type serves the 22 strategies. Its `Parse` decodes the configuration over the defaults, and it refuses an unknown field and data after the JSON value. The seven strategies without a configuration share one empty configuration, which encodes as `{}`.
- `embedded` and `watch` build `struct{}`.
- `exclusive` builds an `ownership.Config`. Its `Parse` decodes twice, so the defaults of the renewal interval, the lock delay and the action follow the TTL and the fence that the configuration sets.
- A configuration of `exclusive` without `"fenced": true` resolves, and the support matrix refuses the namespace as unsupported.

| Factory | Provides | Requires |
|---|---|---|
| `primary-only` | `single-writer` | Nothing |
| `exclusive` | `fenced`, when the configuration sets `"fenced": true` | `single-writer` from the replica state model, `exclusive-safe` from the handoff protocol and `leases` from the membership |
| `break-before-make` | `exclusive-safe` | Nothing |
| `embedded` | `controller` | Nothing |
| `planner` and `watch` | Nothing | `controller` from the topology |
| Every other factory | Nothing | Nothing |

The defaults:

- `maxEvaluations` of the planner is 1,000,000.
- The other defaults follow the design:
  - `fixed-slots` has 1,024 slots, the hash `xxhash64` and no hash tags.
  - The search of the planner has `maxMoves` 500, `maxSourceShards` 5,000, `sample` 16 and a timeout of 60 s.
  - The churn of the planner is 0.09, 2 and 1. Its triggers are 30 s, 1.1, 0.25 and a pass at a change of the membership. The planner has no constraints or goals by default.
  - `isolation` has `max` 1, and `utilisation` has `above` 0.9.
  - A lease has a TTL of 10 s, a renewal interval of a third of the TTL and a drift rate of 0.01.
  - `break-before-make` has timeouts of 30 s for an add and 10 s for a drop.

The selections inside a configuration:

- `config` adds `ParseNamed(r, axis, name, raw)`, which parses a configuration with the factory of a name.
- The `Parse` of the planner parses the configuration of each constraint and goal with `ParseNamed`, and replaces each configuration with its encoding, which contains every default. The `Parse` of `fixed-slots` checks its hash the same way.
- `Resolve` reports a problem of a constraint or a goal with its place in the configuration, such as `goals[1]`.

The presets:

| Preset | Partitioning | Goals of the planner |
|---|---|---|
| `exclusive-worker` | `fixed-slots` with 1,024 slots | `count` |
| `consumer-group` | `fixed-slots` with the default slots | `count` in tier 0 and `stickiness` in tier 1 |
| `actor-entities` | `fixed-slots` with the default slots | `count` |
| `leader-election` | `singleton` | None |

- All four presets select `primary-only`, `exclusive` with `"fenced": true`, `break-before-make`, `embedded`, `watch` and a budget with planned interruption.
- The descriptions of `consumer-group` and `actor-entities` tell an application to set one slot per partition, or about 10 slots per expected node.
- `hold`, `scale` and a replica count are unknown fields, so `Parse` refuses them with an error of class `errs.Invalid`.
- The README contains a table of the support matrix, and a test of `builtin` fails when the table and `Support()` differ.

## Alternatives Considered

### Registry with unexported factories

`Registry()` returns a registry with every built-in, as in the design, and the factories are unexported.

Rejected because `Registry()` cannot return the error of a registration. An application also cannot create a registry with a matrix of its own, with some built-ins, or with a built-in under another name.

### Register with unexported factories

`Register(r)` registers every built-in in a registry of the caller, and the factories are unexported.

Rejected because `Register` is then the only way to put a built-in into a registry. It registers every factory and every preset, so a registry with some built-ins or with a built-in under another name is still impossible.

### A default of 200,000 evaluations

The planner keeps the default of `maxEvaluations` of the design.

Rejected because an emergency pass at the bound of a cell placed 41 of the 200 replicas of a failed node under it.

### errs.Unsupported for hold, scale and replica counts

The factories refuse `hold`, `scale` and a replica count other than one with an error of class `errs.Unsupported`.

Rejected because the refusal of a field that a configuration does not declare needs code and tests of its own.

### The goal spread in exclusive-worker

`exclusive-worker` selects `count` and `spread`, as in the design.

Rejected because the cost of `spread` is always 0 with one replica per shard.

### Types of the topology and the routing source

`embedded` builds a `controller.Topology`, and `watch` builds a `router.Source`.

Rejected because the support matrix has one topology and one routing source, so neither type has a second implementation.

## Consequences

**Positive:**

- An application creates a registry with a matrix of its own, with some built-ins, or with a built-in under another name.
- `Register` returns the error of every registration, and a test makes each check fail.
- The stored spec contains the defaults of every constraint and goal, and `Resolve` reports a problem of a constraint or a goal with its place.
- At the bound of a cell, an emergency pass with the default limit places every replica of a failed node.

**Negative:**

- `builtin` exports 22 constructors beside `Support`, `Register` and `Presets`. The docblock of each constructor states its defaults and its capabilities.
- A caller makes two calls, `config.NewRegistry(builtin.Support())` and `builtin.Register(r)`, where the design has one.
- The `Parse` of every built-in decodes its configuration twice, also where no default depends on another field.
- `ParseNamed` and `BuildNamed` do not read the support matrix. A registry whose matrix closes the constraint, goal or hash axis does not refuse a name outside the matrix inside the configuration of the planner or of `fixed-slots`.
- `config` exports one more function.
- An unknown field and a field of a capability outside the matrix fail with the same class, so a caller cannot tell `hold` from a misspelt field by the class of the error.

**Neutral:**

- The 22 other names of the design are constants of a test of `builtin`.

## References

| What | Where |
|---|---|
| The emergency pass at the bound of a cell: one pass over the cell `failed-node` of `BenchmarkPlanner` with 1,000,000 shards, three times with each limit | `placement/planner/planner_test.go` |

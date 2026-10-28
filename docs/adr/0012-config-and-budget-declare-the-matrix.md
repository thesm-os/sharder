---
adr: 0012
title: Config and budget declare only the capabilities of the support matrix
status: Accepted
date: 2026-10-11
supersedes: none
superseded-by: ADR-0045 in part
rfc: RFC-0002, RFC-0005, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0012: Config and budget declare only the capabilities of the support matrix

## Status

Accepted. [ADR-0045](0045-dokimi-generates-the-test-code.md) supersedes it in part.

## Context

The design of `config` and `budget` declares capabilities outside the support matrix, which the processes of the matrix refuse:

- `config` declares 13 capabilities, preferences with warnings, an approvals section with a trust anchor, and the interruption `none`. The matrix uses six capabilities, and it does not use preferences or approvals. Its only handoff, `break-before-make`, leaves a writer gap in every move, so a namespace of the matrix cannot select `none`.
- Axes, capabilities, interruptions, impacts and registered names are strings, and `config` declares a `Duration` of its own.

The design also predates four facts of the repository:

- Roles and statuses are `uint8` enumerations, and the registered name of a strategy is a `shard.StrategyName`.
- `shard.Duration` encodes a duration in JSON.
- gocritic's hugeParam refuses a parameter of 80 bytes or more. A `Spec` is 440 bytes, a `Preset` 472 and a `Disruption` 184.
- Core has no JSON package, so no dependency provides the JSON Merge Patch of RFC 7396.

## Decision

We will build `config` and `budget` with only the declarations that the support matrix uses and with the types of `shard` and of core, because the processes of the matrix do not call any other declaration.

- `Axis`, `Capability`, `Interruption` and `budget.Impact` are `uint8` enumerations with `Valid` and a `String` from stringer. `Interruption` and `shard.Role` have text methods, because a spec writes them in JSON.
- `Capability` declares `controller`, `single-writer`, `exclusive-safe`, `fenced`, `leases` and `durable`. `Capabilities` has no preferences, and `Resolved` has no warnings.
- `Interruption` declares `planned` alone, and `Resolve` refuses a resolved spec without it. A nil `maxUnavailable` resolves to 1, the default of `primary-only` under planned interruption.
- An empty `drainRoles` resolves to the writer roles of the model. `Resolve` builds the replication strategy as a `*replication.Compiled`, as it builds the partitioner for the mapping.
- `Selection.Name`, `Factory.Name`, `Support.Strategies` and `BuildNamed` use `shard.StrategyName`. `Spec.Preset` and `Preset.Name` use `config.PresetName`, and `LoadModel.Window` is a `shard.Duration`.
- `config` does not declare an approvals section, `Support.Approvals` or a trust anchor.
- `config` merges a selection over its preset with a JSON Merge Patch of its own. Its tests are the examples of RFC 7396.
- `Resolve` and `RegisterPreset` take pointers, and `Budget.Admit` takes a `*Disruption`. `Change` takes no registry, because no rule of the matrix reads one.
- `budget.ID` embeds core's `id.ID`, as `proposal.ID` does, and `Done` takes it.
- `ErrUnknown` reports an unknown preset as well as an unknown strategy. `ErrSpec`, `ErrRegistration` and `ErrChange` classify the other refusals as `errs.Invalid`.

## Alternatives Considered

### The declarations of the design

Each package declares the capabilities of the design, and the processes of the matrix refuse the ones outside it.

Rejected because every refused declaration needs code and tests of its refusal, and strings give up the check of the compiler and of the `exhaustive` linter.

### A fixed drainRoles of primary

`config` fills an empty `drainRoles` with `shard.RolePrimary`, without the model.

Rejected because a second model with another writer role would get a drain of a role that it does not have.

### Value parameters, as in the design

`Resolve`, `RegisterPreset` and `Admit` take their structs by value.

Rejected because hugeParam refuses each of them, and each call copies hundreds of bytes.

## Consequences

**Positive:**

- The compiler refuses an axis, a capability, an interruption or an impact that the packages do not declare.
- Each package contains only code that the processes of the matrix run.
- `Resolve` reads the writer roles from the model of the spec, so a second model does not need a change of `config` for its drain roles.

**Negative:**

- A design that adds a capability, an interruption, preferences or approvals declares them first.
- A second model whose default `maxUnavailable` differs from 1 needs a rule that reads the default from the model.
- `config` imports `partition` and `replication`, and a factory on the replication axis builds a `*replication.Compiled`.
- A caller of `Resolve` or `RegisterPreset` passes a pointer, and `RegisterPreset` keeps the slices and the pointers of the preset.

**Neutral:**

- `budget` keeps the semantics of the design, and changes only its types.

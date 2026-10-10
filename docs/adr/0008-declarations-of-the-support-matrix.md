---
adr: 0008
title: Replication, ownership, fence and proposal declare only the capabilities of the support matrix
status: Accepted
date: 2026-10-11
supersedes: ADR-0004
superseded-by: none
rfc: RFC-0002, RFC-0005, RFC-0006
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0008: Replication, ownership, fence and proposal declare only the capabilities of the support matrix

## Status

Accepted

## Context

The design of the four packages declares capabilities outside the support matrix, which the processes of the matrix refuse:

- `replication` declares three built-in models, a bound that follows the replica count of a shard, and four availability rules. The matrix contains `primary-only`, whose one role has a bound of one and counts a shard as available while its writer serves.
- `ownership` declares the modes `affinity`, `exclusive`, `overlapping` and `delegated`. The matrix contains `exclusive`.
- `fence.Token` lists the parent shards that a split or a merge retires. Resharding is outside the matrix.
- `proposal` declares signed proposals, approvals sections and trust anchors, and the operation kinds `reshard` and `cell-split`. Approvals, resharding and cell splits are outside the matrix.

The design also predates three decisions about types:

- `shard.Role` is a `uint8` enumeration with a `String` method, which is the state constraint of core's `fsm`.
- The registered name of a strategy is a `shard.StrategyName`.
- `config.Duration` encodes a duration in JSON, and a strategy package must not import `config`.

## Decision

We will build `replication`, `ownership`, `fence` and `proposal` with only the declarations that the support matrix uses, in the types of `shard` and of core, because every other declaration has no caller that the matrix runs.

- `replication.Model` has a `shard.StrategyName`, serving roles with a count as their bound, and transitions. The transition table of a compiled model uses `shard.Role` as its state and its event, so the model does not need an index type. The package declares one built-in model, `PrimaryOnly`, and no availability rule.
- `ownership` configures exclusive ownership alone, so it does not declare a mode. `OnLeaseLoss` is a `uint8` enumeration with text methods, and `Lease` writes its durations as `shard.Duration`, which replaces `config.Duration`.
- `fence.Token` has no parent shards, and `Acquire` advances one fence key.
- Package `proposal` declares `OperationKind`, `ID` and `Proposal`. `OperationKind` is a `uint8` enumeration of `spec`, `move`, `drain`, `undrain` and `explain`, and `Proposal.ID` embeds core's `id.ID`.
- `proposal` does not declare a signed proposal, an approvals section or a trust anchor. `config` imports `proposal`, `protocol` imports `proposal` in place of `config`, and `Submit` takes a `proposal.Proposal`.

## Alternatives Considered

### The declarations of the design

Each package declares the capabilities of the design. The processes of the matrix refuse the ones outside it with an error of class `errs.Unsupported`. `Resolve` refuses a spec with an approvals section, `controller.New` refuses a trust anchor, and `Submit` refuses the kinds `reshard` and `cell-split`.

Rejected because every refused declaration needs code and tests of its refusal, and the designs that would use the declarations are not complete:

- Approvals need a decision about rollback by a writer to the store.
- `secondary-only` needs an allowance above its bound during a move.
- Consensus models need an availability rule that counts voters.

## Consequences

**Positive:**

- Each of the packages contains only code that the processes of the matrix run.
- A design that adds a mode, a model, approvals or resharding declares the types that it needs.

**Negative:**

- Approvals change the parameter of `Submit` from a `proposal.Proposal` to a signed proposal, which breaks every transport.
- A spec with an approvals section, and a proposal of a kind outside the five, fail as invalid input of class `errs.Invalid` instead of as unsupported input.
- A component cannot branch on the ownership mode until a second mode exists.

**Neutral:**

- `protocol` and the transport adapters still do not import `config`.

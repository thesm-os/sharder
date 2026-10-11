---
adr: 0013
title: Store, membership and protocol declare only the capabilities of the support matrix
status: Accepted
date: 2026-10-11
supersedes: none
superseded-by: none
rfc: RFC-0007, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0013: Store, membership and protocol declare only the capabilities of the support matrix

## Status

Accepted

## Context

The design of `store`, `membership` and `protocol` declares capabilities outside the support matrix, which the processes of the matrix never use:

- `store` records the version of a layout and approvals in each state and mutation. A mutation can delete a scope, and a read can start at a position. `Campaign` takes the name of an election and a candidate, for the per-shard elections of the leaderless topology.
- `protocol` declares twelve callbacks as strings, the fields of the callbacks of other replica state models, the capabilities of an agent in each report, and the lookups of a router.

The design also predates facts of the repository:

- Roles and phases are `uint8` enumerations, and the registered name of a strategy is a `shard.StrategyName`. The IDs of disruptions and proposals embed core's `id.ID`.
- gocritic's hugeParam refuses a parameter of 80 bytes or more. A `Mutation` is 336 bytes, a `Request` 248, a `shard.Cell` 112, and a `shard.Node` and a `DoneArgs` 104 each.
- One Go type cannot declare two methods named `Watch` with different signatures, and `store.Reader` and `membership.Membership` each declare one.
- The conformance kits run their laws inside property cases, on goroutines of the cases, where a `*testing.T` must not stop the test.

## Decision

We will build `store`, `membership` and `protocol` with only the declarations that the support matrix uses, with the types of `shard`, `budget` and `proposal`, and with pointer parameters, because the processes of the matrix do not call any other declaration and the linters refuse the rest.

- `store.State` and `store.Mutation` have no layout version and no approvals, a `Mutation` has no deletion, and `Query` selects a node and no start position. `Campaign` takes a scope, and its context bounds the term that it grants.
- `Drain`, `DrainKey`, `Mutation.Release` and `DoneArgs` use `budget.ID`. `State.Applied` and `Mutation.Forget` use `proposal.ID`, an `Applied` is a proposal with its caller, and `Transition.Protocol` is a `shard.StrategyName`. `shard.Phase` is a `uint8` enumeration of running, aborting, done and aborted.
- `Commit` and `Fits` take a `*Mutation`, `Join` takes a `*shard.Node`, and the servers and the clients of `protocol` take pointers to their messages, `DoneArgs` included.
- `Mutation.Digest` returns the SHA-256 digest of the encoding of a mutation, and `Commit` returns its error, before it sends anything, for a mutation that does not encode. `Request.Digest` returns an error for a request that does not encode as well.
- `protocol.Call` is a `uint8` enumeration of `add` and `drop`, and `protocol.Path` one of forward, abort and cleanup. A `Request` has no current role, peer, members or superseded shards, a `Report` has no capabilities, and `protocol` has no lookups.
- `protocol.ErrUnreachable` reports a call that the transport does not deliver to a server, and classifies as `errs.Transient`. `CallError` is an error whose `Class` method returns its field `ErrorClass`, because a field and a method cannot share the name `Class`.
- `Network` and `Local` form package `protocol/local`, whose `New` replaces `NewLocal`.
- `membership.View.Node` finds a node by its ID with a binary search over the nodes of the view, which are in the order of their IDs.
- `store/memory` serves the membership through a second type, `memory.Membership`, which `Store.Membership` returns. A page token of the memory store is text: the incarnation in hexadecimal, the generation in decimal and the start in base64url, separated by periods.
- A watch of a node yields, for each generation, the ID of its commit, the placements that name the node or named it before, the transitions and the ended transitions with an endpoint on the node, and the controller record. `Removed` lists the shards whose placements stop naming the node. A generation without such a change is an empty event.
- `storetest` adds `WithCompact`, and its callbacks and those of `membershiptest` take an `assert.TB`. `membershiptest` runs the leases on a fake clock, and adds `WithLostRenewals`. `protocoltest` checks a transport against servers that record each call.

## Alternatives Considered

### The declarations of the design

Each package declares the capabilities of the design, and the processes of the matrix refuse the ones outside it.

Rejected because every refused declaration needs code and tests of its refusal, and strings give up the check of the compiler and of the `exhaustive` linter.

### Value parameters, as in the design

`Commit`, `Fits`, `Join` and the methods of the servers and the clients take their structs by value.

Rejected because hugeParam refuses each of them, and each call copies hundreds of bytes.

### Callbacks of the kits that take a *testing.T, as in the design

The kits call `newStore` and the functions of their options with the `*testing.T` of the law.

Rejected because a property case runs its body on a goroutine of its own, and a `*testing.T` that fails there stops the wrong goroutine.

## Consequences

**Positive:**

- The compiler refuses a callback, a path, a phase or an ID of the wrong kind.
- Each package contains only code that the processes of the matrix run.
- The kits check every store, membership and transport against the same laws, inside property cases that shrink a failure to its smallest case.

**Negative:**

- A design that adds a replica state model adds its callbacks to `Call` and its fields to `Request`, and raises the major version of the protocol.
- The leaderless topology needs the name of an election in `Campaign` again.
- A router of the matrix reads the placements from the store, because `protocol` has no lookups.
- A caller of `memory` gets the membership from `Store.Membership`, not from the store itself.
- An adapter of the store whose membership runs its leases on the clock of its backend cannot run the laws of `membershiptest` on a fake clock.

**Neutral:**

- `store`, `membership` and `protocol` keep the semantics of the design, and change only their declarations and their types.

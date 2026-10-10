---
adr: 0001
title: Conformance kits for every contract with more than one implementation
status: Accepted
date: 2026-10-10
supersedes: none
superseded-by: none
rfc: RFC-0001, RFC-0004, RFC-0007, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0001: Conformance kits for every contract with more than one implementation

## Status

Accepted

## Context

These contracts of sharder have more than one implementation, or an application implements them:

- `store.Store`, implemented by the in-memory store and the etcd adapter.
- `membership.Membership`, implemented by the in-memory store and the etcd adapter.
- The clients and servers of `protocol`, implemented by the in-process transport and `httptransport`.
- `partition.Partitioner` and `partition.Hash`, implemented by the built-ins and by an application's hash.
- `placement.Constraint`, `placement.Goal` and `placement.Placer`, implemented by the built-ins and by an application's rules.

Each implementation must keep the laws of its contract. Two of them are the order of the checks of `Commit` and the refusal of an unknown protocol version before any effect. The package layout of the design has a package of laws for the store alone, `store/storetest`. Core already checks every fenced writer with one suite, `coretest/epochtest`.

## Decision

We will give each of these contracts a conformance kit in a subpackage named after the contract with the suffix `test`, because the laws of a contract then exist once and every implementation runs the same laws.

| Kit | Checks | Run by |
|---|---|---|
| `store/storetest` | The laws of `store.Store` | The in-memory store and the etcd adapter |
| `membership/membershiptest` | The laws of `membership.Membership` | The in-memory store and the etcd adapter |
| `protocol/protocoltest` | The checks of a call's metadata before any effect, the error class of a reply, and the round trip of each message | The in-process transport and `httptransport` |
| `partition/partitiontest` | One position per key in every process, a layout that covers its cell, and the fixed vectors of a hash | `fixed-slots`, `singleton`, `xxhash64` and an application's hash |
| `placement/placementtest` | The checks of a constraint, a goal's `Delta` against its change of `Cost`, and one plan per input | The built-in constraints and goals, the planner and an application's rules |

Fenced writers run core's `coretest/epochtest`, so sharder does not have a kit for fences. The kits do not contain implementations that break a law on purpose.

## Alternatives Considered

### Only store/storetest

Each other implementation would check its contract in its own tests, and the store alone would have a kit.

Rejected because the two transports would each copy the tests of the version check, and an application's hash or rule would have no laws to run at all.

### Kits with implementations that break each law

Each kit would contain, in an internal package, one implementation per law that breaks that law. The kit's own tests would then show that each law can fail.

Rejected because a generator of conformance suites is to produce such implementations, and implementations written by hand would duplicate it.

## Consequences

**Positive:**

- Every implementation of a contract runs the same laws, an application's hash, constraint and goal included.
- A new implementation, such as another store adapter, shows its conformance by running the kit.

**Negative:**

- The kits other than `store/storetest` are public packages that the design's package layout does not list, and applications depend on their API.
- A change to the laws of a contract is a change to its kit, and every implementation must pass the change.
- The tests of a kit do not show that each of its laws can fail, so nobody notices a check that never fails.

**Neutral:**

- Core checks its fenced writers the same way, with `coretest/epochtest`.

## References

| What | Where |
|---|---|
| go.thesmos.sh/core v0.8.0, `coretest/epochtest` | https://github.com/thesm-os/core/tree/v0.8.0/coretest/epochtest |

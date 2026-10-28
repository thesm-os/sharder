---
adr: 0045
title: dokimi generates the enumerations, the error checks, the conformance suites, the doubles and the builders
status: Accepted
date: 2026-10-24
supersedes: ADR-0001 in part, ADR-0006 in part, ADR-0012 in part, ADR-0013 in part, ADR-0015 in part
superseded-by: none
rfc: RFC-0016
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0045: dokimi generates the enumerations, the error checks, the conformance suites, the doubles and the builders

## Status

Accepted

## Context

sharder wrote this test code by hand:

- the text methods of 34 enumerations, through stringer and hand-written methods
- the checks of the messages of the error values of 33 packages
- five conformance kits, whose 93 laws had no planted defect
- the allocation tests and the benchmarks of the hot paths, which stated each ceiling twice
- the test doubles of 26 interfaces
- the literals of five structs that the specs of five to ten packages build field by field

ADR-0001 rejected planted defects in the kits until a generator of conformance suites existed. dokimi generates each of these kinds of code from directives in the doc comments of the declarations. [RFC-0016](../rfc/0016-test-code-that-dokimi-generates.md) designs its use and lists the rules of the standards that change with it.

## Decision

We will generate the test code of sharder with five generators of dokimi, as RFC-0016 designs it, because a generated check has a planted defect that proves that it can fail, and a directive states each fact once.

- `//dokimi:enum text=comment` writes the members of each enumeration, with `IsValid` in place of `Valid`. Each package declares the sentinel `ErrUnknown<T>` of each exported enumeration in `errors.go`.
- `//dokimi:sentinel` checks the error values of each package with an `errors.go`.
- `//dokimi:suite` writes the conformance suites of the contracts of the five kits and of the hot paths that `docs/architecture/packages.md` lists. A type that core or kanon tests does not get a suite. Each law of a kit is a check under `//dokimi:check` with a planted defect over a verified fake of its kit. Each implementation applies its suite with `//dokimi:subject`, whose param `options` passes `conform.Using` where the calls of the suite need values of the kit.
- `//dokimi:budget` states the allocation ceilings of the callables of a suite. A ceiling of a steady state or of each slot is a counted check of the consumer, and the escape of a request in the frame of its caller remains a hand-written allocation spec. The allocation tests and the benchmarks outside the suites remain as they are.
- `//dokimi:stub` writes the double of each interface of sharder that a spec replaces, and of each declaration of a suite whose planted defects build on its double.
- `//dokimi:builder` writes the builders of `shard.Placement`, `shard.Node`, `store.Mutation`, `config.Spec` and `config.Selection`.
- The generate step of the gate runs dokimi and then `go generate`, which runs kanon, through the key `go.generate.commands` of ergon 0.14.2.

This decision supersedes these parts of earlier decisions:

| Record | Part |
|---|---|
| ADR-0001 | "The kits do not contain implementations that break a law on purpose." |
| ADR-0006 | "a `String` method from stringer and a `Valid` method" |
| ADR-0012 | "`Valid` and a `String` from stringer" |
| ADR-0013 | "`storetest` adds `WithCompact`, and its callbacks and those of `membershiptest` take an `assert.TB`. `membershiptest` runs the leases on a fake clock, and adds `WithLostRenewals`." |
| ADR-0015 | "the kit measures `Check` and `Delta` with `MaxAllocs`" |

## Alternatives Considered

### Hand-written planted defects in `<pkg>test/mutant`

Each kit writes one implementation per law that breaks the law, as the testing standard stated before RFC-0016.

Rejected because a planted defect is the double of its declaration with the calls of one law replaced, and dokimi generates that double.

### A suite on every struct with an allocation test

Every struct with a hand-written allocation test gets a suite, `shard.ID` and `shard.Range` included, so a budget states its ceilings.

Rejected because a suite is the conformance suite of a contract or of a hot path, and an allocation test makes a struct neither. core tests the identifiers of `shard.ID`, and kanon tests the codec of `shard.Range`.

## Consequences

**Positive:**

- Every law of a kit and every ceiling of a budget has a planted defect, and the generated test of its suite proves that it fails on that defect.
- A directive states each enumeration, error check, ceiling, double and builder once.

**Negative:**

- The kits change their API. `Run<X>Suite` and the interfaces of the operations of a kit replace `Assert<X>` and its options.
- The enumerations change their API. `IsValid` replaces `Valid`, `ErrUnknownOperationKind` replaces `ErrUnknownKind`, and the text methods return errors that wrap `ErrUnknown<T>`.
- New kits serve the suites of the hot paths, the doubles and the builders.

## References

| What | Where |
|---|---|
| RFC-0016, the test code that dokimi generates | [0016-test-code-that-dokimi-generates.md](../rfc/0016-test-code-that-dokimi-generates.md) |
| The guide of dokimi for a Go package | <https://github.com/dokimasia/dokimi/blob/main/docs/guides/go.md> |
| conform, the runtime of the generated suites | <https://github.com/dokimasia/dokimi-go/tree/main/conform> |
| dokimi ADR-0006, a check that counts allocations runs outside the parallel tests | <https://github.com/dokimasia/dokimi/blob/main/docs/adr/0006-a-check-that-counts-allocations-runs-outside-the-parallel-tests.md> |

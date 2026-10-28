---
adr: 0046
title: The specs build the structs with generated builders through the fixtures of their package
status: Accepted
date: 2026-10-28
supersedes: RFC-0016 in part
superseded-by: none
rfc: RFC-0016
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0046: The specs build the structs with generated builders through the fixtures of their package

## Status

Accepted

## Context

RFC-0016 gives `shard.Placement`, `shard.Node`, `store.Mutation`, `config.Spec` and `config.Selection` builders that dokimi generates into the kits of their packages. It states that a spec builds such a struct with `New<X>Builder` and the setters of the fields that its case sets.

The specs build these structs in 418 literals in 79 files. A rewrite of the 410 literals with keyed fields into chains of setters added 585 lines. Each chain repeated the values that the specs of its package share, such as the namespace, the preset and the shard IDs of the tests. Helpers of the specs, such as `placement(n)` in five packages and `spec(ns)` in `controller`, already build these structs with such values.

## Decision

We will build these structs in the specs through fixtures. A fixture sets the shared values of a package once, so a case sets only the values that it changes.

- A fixture is a function of the specs of one package. It starts from `New<X>Builder` and sets the values that the specs of the package share. It then applies its parameters with setters and returns the value. A fixture of `store.Mutation` returns a pointer, because `Commit` and `Fits` take one.
- A helper of the specs that returns one of these structs is a fixture.
- A package declares a fixture where two or more sites of its specs build a struct with the same shared values, or from the same nested values. Each such site calls the fixture.
- A literal remains where a fixture would only rename it. Such a literal is a value that one case states, such as an expected result or a row of a table, a value that the specs declare once by name, such as a node of the tests, or a literal whose fields all come from its case.
- The kits do not declare fixtures, because the specs of each package use their own namespace, nodes and shard IDs.

This decision supersedes this part of RFC-0016:

| Record | Part |
|---|---|
| RFC-0016 | "A spec builds such a struct with `New<X>Builder` and the setters of the fields that its case sets." |

## Alternatives Considered

### A chain of setters at each site

Each site builds its struct with `New<X>Builder` and the setters of its fields, as RFC-0016 states.

Rejected because each chain repeats the values that the specs of its package share, and the specs grow by 585 lines.

### Fixtures in the kits

Each kit exports fixtures of its structs, such as `shardtest.Placement(n)`.

Rejected because the specs of each package use their own namespace, nodes and shard IDs. The specs of every package would then override the values of a shared fixture.

### Defaults of the builders

A `default` tag on a field, or the param `defaults` of the directive `builder`, gives `New<X>Builder` a starting value.

Rejected because the struct and its package declare these defaults, so values of the tests would enter the production code. The defaults would also apply in the specs of every package.

## Consequences

**Positive:**

- A case sets only the values that it changes, and each fixture sets the shared values of its package once.
- A helper and a case that build a struct with the same fields call one fixture.

**Negative:**

- A reader of a case reads the fixture for the values that the case does not set.
- The specs of a package build some structs through fixtures and others through literals.

## References

| What | Where |
|---|---|
| RFC-0016, the test code that dokimi generates | [0016-test-code-that-dokimi-generates.md](../rfc/0016-test-code-that-dokimi-generates.md) |
| ADR-0045, dokimi generates the test code | [0045-dokimi-generates-the-test-code.md](0045-dokimi-generates-the-test-code.md) |
| The guide of dokimi for a Go package | <https://github.com/dokimasia/dokimi/blob/main/docs/guides/go.md> |

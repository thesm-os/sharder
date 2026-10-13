---
adr: 0038
title: An example keeps its program in main.go and its application in a package under internal
status: Superseded
date: 2026-10-13
supersedes: none
superseded-by: ADR-0039
rfc: RFC-0010
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0038: An example keeps its program in main.go and its application in a package under internal

## Status

Superseded by [ADR-0039](0039-example-applications-are-in-main.md)

## Context

RFC-0010 gives each example a directory of package `main`, and the examples share the package `internal/fencedstore`. An example has an application of its own besides its program. The application of `preset-exclusive-worker` is a handler of the agent that runs jobs, and a process that embeds a controller and an agent.

A directory of a program contains one file of package `main`. No package imports a package `main`, so a spec outside package `main` can call the application only when package `main` exports it.

## Decision

We will split each example into its program and its application:

- The directory of an example contains `main.go` and its spec `main_test.go`. `main.go` contains the package comment of the program, the scenario of the example and `main`.
- The application of an example is a package under `examples/internal`, named after its concept, such as `internal/jobs` for `preset-exclusive-worker`. Its specs are black-box.
- `main_test.go` declares package `main`, because only a test of package `main` calls `main`. It runs `main` as a process of the test binary, and runs the scenario in the process of the test.

## Alternatives Considered

### The application in package main

The handler and the process are files of package `main`, which exports them to its specs.

Rejected because the directory of a program then contains more than one file of package `main`.

### The application in a package under the directory of the example

The application is a package such as `preset-exclusive-worker/jobs`.

Rejected because the directory of a program contains the program alone, as the directory of a command does.

## Consequences

**Positive:**

- The specs of each application are black-box.
- `main.go` shows the scenario of its example in one file.

**Negative:**

- `examples/internal` has one package for each example besides `fencedstore`.

**Neutral:**

- RFC-0010 keeps its catalogue, its keys and its tests. This ADR adds the packages of the applications to its layout.

## References

| What | Where |
|---|---|
| The catalogue and the layout of the examples | RFC-0010 |
| The process that embeds a controller and an agent | ADR-0036, ADR-0037 |

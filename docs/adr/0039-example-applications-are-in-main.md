---
adr: 0039
title: An example keeps its application in main.go, and examples/internal keeps the storage that the examples share
status: Accepted
date: 2026-10-13
supersedes: ADR-0038
superseded-by: none
rfc: RFC-0010, RFC-0011
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0039: An example keeps its application in main.go, and examples/internal keeps the storage that the examples share

## Status

Accepted

## Context

ADR-0038 puts the application of each example in a package under `examples/internal`. At that time the application of `preset-exclusive-worker` composed a process of a controller and an agent, and kept a goroutine for each tenure.

RFC-0011 moved both into the library. `embedded.Process` composes the process, and `replica.Runner` runs the work of each tenure. The application of each example is now an `Opener` and the keys of its storage. The `Opener` is the part of the library that an application implements, so a reader of an example looks for it beside the scenario.

## Decision

We will keep the application of each example in its program:

- The directory of an example contains `main.go` and its spec `main_test.go`, as before.
- `main.go` contains the package comment, the application of the example, the scenario and `main`. The application is the `Opener` of the example and the keys of its storage. The application of `preset-actor-entities` also contains the server of the deposits.
- `main_test.go` declares package `main`. It contains the specs of the application and of the scenario, and runs `main` as a process of the test binary.
- `examples/internal` contains `fencedstore` alone, the fenced storage that every example uses. A package `main` cannot share code with another program.

## Alternatives Considered

### The application in a package under examples/internal

ADR-0038 keeps each application in a package of its own, with black-box specs.

Rejected because the application of an example is now one `Opener`. A reader then opens a second package to see what the example implements.

### Every package of the examples in main.go, fencedstore included

Each program contains a copy of the fenced storage.

Rejected because five programs would contain five copies of the same storage and its specs.

## Consequences

**Positive:**

- One file shows the whole of each example: the application, the composition of its processes and its scenario.
- `examples/internal` contains one package.

**Negative:**

- `main.go` has from 392 to 722 lines, and `main_test.go` from 408 to 737 lines.
- The specs of each application are white-box specs of package `main`.

**Neutral:**

- RFC-0010 keeps its catalogue, its keys and its tests.

## References

| What | Where |
|---|---|
| The catalogue and the layout of the examples | RFC-0010 |
| The embedded process and the runner of tenures | RFC-0011 |
| The application of each example in a package under internal | ADR-0038 |

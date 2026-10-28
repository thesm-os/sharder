---
adr: 0047
title: The planner applies the suite of Placer through the directive subject
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

# ADR-0047: The planner applies the suite of Placer through the directive subject

## Status

Accepted

## Context

RFC-0016 applies the suite of `placement.Placer` to the planner by hand, in `TestPlannerConformance`. A generated call of `Place` draws each field of an `Input` at random, so the planner refuses the input. The verdict of RFC-0016 reverses with a sample of the input of `Place` in its generated calls (dokimi RFC-0012).

dokimi writes the sample of a parameter into the file of the suite, so the sample must be a value that `placementtest` can spell from a declaration that `placement` exports. A valid input of a pass needs a clock, a source of randomness, a model, a layout and nodes. No declaration of `placement` builds such an input, and a declaration that did would put a cell of the tests into the production package.

The param `options` of the directive `subject` passes the options of an application to every test that dokimi generates for a subject (dokimi RFC-0008). `conform.Using` states the generator of a type for the property runs, the fuzz targets and the benchmarks of an application. The built-in constraints and goals already draw their states from the cells of `placementtest.Inputs` this way, through `ConstraintCells` and `GoalCells`.

## Decision

We will apply the suite of `placement.Placer` to the planner through `//dokimi:subject placement.Placer new=newConstrained options=cells`. The options give the generated calls of `Place` inputs that the planner plans.

- `placementtest` adds `PlacerCells`. It states with `conform.Using` that each generated call of `Place` takes an input of the cells of `Inputs`.
- The generated `TestPlannerConformsToPlacer` replaces `TestPlannerConformance`.
- The generated `BenchmarkPlannerConformsToPlacer` measures `Place` on those cells. `BenchmarkPlanner` measures passes at the stated sizes as before.
- The parameter of `Place` takes no sample.

This decision supersedes this part of RFC-0016:

| Record | Part |
|---|---|
| RFC-0016 | "The planner applies the suite of `Placer` by hand, in `TestPlannerConformance` alone. A generated call of `Place` draws each field of an `Input` at random, so the planner refuses the input, and `BenchmarkPlanner` measures its passes." |

## Alternatives Considered

### A sample of the input of Place

`//dokimi:sample` on the parameter `in` of `Place` fixes the input of every generated call, as RFC-0016 expected.

Rejected because the sample must be an exported declaration of `placement`, which would then contain a cell of the tests. One fixed input also covers fewer cells than the generator of `Inputs`.

### The application by hand

`TestPlannerConformance` keeps applying the suite with `RunPlacerSuiteFromHandle`.

Rejected because the application by hand runs the checks alone. The generated tests also give the planner the benchmark and the fuzz targets of the suite.

## Consequences

**Positive:**

- The planner has the generated conformance test, benchmark and fuzz targets that every other subject of a kit has.
- The generated calls of `Place` plan cells. A run of 100 calls of the generated benchmark does not report an error.

**Negative:**

- The generated benchmark measures small cells alone. `BenchmarkPlanner` remains the measurement at the stated sizes.

## References

| What | Where |
|---|---|
| RFC-0016, the test code that dokimi generates | [0016-test-code-that-dokimi-generates.md](../rfc/0016-test-code-that-dokimi-generates.md) |
| ADR-0045, dokimi generates the test code | [0045-dokimi-generates-the-test-code.md](0045-dokimi-generates-the-test-code.md) |
| dokimi RFC-0008, the doors of a subject and of a planted defect | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0008-the-doors-of-a-subject-and-of-a-planted-defect.md> |
| dokimi RFC-0012, the sample of a parameter | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0012-the-sample-of-a-parameter.md> |

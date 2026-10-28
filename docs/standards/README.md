<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# Standards

A standard states a rule that every package of the ledger follows. An
RFC proposes one design and argues for it. A standard lists the rules
that apply to every change. It changes only through an RFC that names
the rule it amends.

A change brings each file that it touches up to these standards.

| Standard | Covers |
|---|---|
| [Interfaces](interfaces.md) | The requests and results of the calls that cross a package, their growth without a break, and their allocations |
| [Performance](performance.md) | The hot paths: the principles of the LMAX Disruptor in Go, hand-offs between goroutines, allocations and measurement |
| [Testing](testing.md) | Black-box specs, names, conformance suites, determinism, coverage, mutation, allocation ceilings, fuzzing and portability |
| [Using assert](assert.md) | The part of `go.dokimi.dev/assert` that states each contract, stateful machines, histories and properties |

## Enforcement

A tool checks a rule wherever one can. Review checks every other rule.

| Rule | Checked by |
|---|---|
| A caller writes keyed struct literals | vet's `composites` analyzer, through `make lint` |
| A request or a result cannot be compared with `==` | the compiler, through the field `_ incomparable` |
| A test file declares the package `x_test` | `testpackage`, through `make lint` |
| A test and its subtests run in parallel | `paralleltest` and `tparallel`, through `make lint` |
| A test takes its context from the test | `usetesting`, through `make lint` |
| A call does not allocate in steady state | the budget of a callable of a suite, through the allocation test that dokimi generates, and the `Allocs` test of any other call |
| The request of a call does not escape the frame of its caller | the hand-written allocation spec of the call |
| Each law of a kit can fail | the test of its suite that dokimi generates |
| The generated files match their directives | `make verify-generate-go` |
| Every package covers 100% of its statements | `go test -cover`, before review |
| Every mutant is detected | `dokimi-mutate-go`, before review |
| Every other rule of these standards | review |

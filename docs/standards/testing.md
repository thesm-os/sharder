<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# Testing

A test specifies behaviour that a caller can observe. Every rule in this
document applies to every package, the internal ones included. Every
test asserts through `go.dokimi.dev/assert`. The assert standard states
which part of the library states which contract.

## Files

- Every test file declares the package `x_test` for its package `x`, so
  a test calls only what the package exports. No test file declares the
  package `x`, and no package has an `export_test.go`.
- A source file `x.go` has its spec `x_test.go` beside it. The spec tests
  the behaviour that `x.go` implements. `doc.go` has no spec, and a
  generated file has one that pins what it generates.
- A test file without a source file of its name does not exist, except
  `helpers_test.go`. It contains the helpers and fixtures that two or
  more specs of the package use.
- A helper calls `t.Helper()` first. A helper exists only for logic of two
  or more steps that two or more cases call.

## Names

- A spec declares `Test<Subject>`, named after the concept of its source
  file. A subtest of the first level is named after the exported function
  or method under test. A method of another type is nested under that
  type's level, as `TestRecord/Head/Valid`.
- Each level is a `t.Run` of its own. No name joins two levels with a dot.
- A case's name is a sentence that states one behaviour: a verb in the
  third person, the result and the condition, as `returns ErrCAS when the
  head moved`. It uses the code's words, such as `returns ErrNotFound` or
  `reports false`, and contains no `and`, no `or` and no `should`.
- A table of cases is a slice named `tests`, each row is `tt`, its `name`
  is the whole case sentence, and its other fields start with `give` and
  `want`.
- `Test<Subject>Allocs` checks the allocation ceilings of the subject,
  `Benchmark<Subject>` measures them, and `Fuzz<Function>` fuzzes one
  entry point.

## Conformance suites

- The contract of an interface has a suite `<pkg>test`, such as
  `storagetest` or `ledgertest`, that every implementation runs.
- Each case of a suite is named as a law in kebab case, such as
  `batches-are-atomic`. It checks the law through assertions only, so a
  failure has a record.
- Each case has one mutant in `<pkg>test/mutant`: an implementation that
  breaks the law and nothing else. The suite's own test runs the case
  against its mutant with `assert.Rejects`, and checks that the record's
  contract is the law that the mutant breaks.
- A stateful contract has a case `histories-are-linearizable`: a
  `stateful` machine with concurrent clients, checked against the
  sequential specification of the contract.

## Determinism and isolation

- Every test and subtest calls `t.Parallel()`. The exceptions are
  `Test<Subject>Allocs` and a test that reads state of the whole process,
  whose doc comment states which state.
- A test takes its context from `t.Context()`, and never from
  `context.Background()` or `context.TODO()`.
- A test waits on a channel or a context, never on `time.Sleep`. Code
  under test reads time through core's `clock`. A test that advances time
  uses core's `clock/fake` inside a `testing/synctest` bubble, and waits
  with `synctest.Wait`.
- Randomness comes from core's `rand/seeded` or `rand/constant`, so a run
  is the same on every machine. An assertion never reads the wall clock
  and never depends on the local time zone.
- A test writes files under `t.TempDir()`. A network test dials an
  in-process listener or a `net.Pipe`.

## Coverage and mutation

- The tests of each package cover 100% of its statements, measured with
  `go test -cover`. Code that no black-box test can execute is dead code
  or a missing test.
- `dokimi-mutate-go` mutates the code, with `-diff` for the lines of a
  change. Every package requires a score of 100%.
- A mutant under which every test passes is a missing assertion, or code
  whose behaviour no caller can observe. Add the assertion or delete the
  code.
- A mutant that cannot change behaviour gets an annotation on its line:
  `//dokimi:mutate-skip <kinds>: <reason>`.
- Nobody mutates code by hand to test a check. `assert.Rejects` proves
  that a check fails against an implementation that breaks it.

## Allocation ceilings and benchmarks

- Every exported function on a hot or warm path has a benchmark: one
  `b.Run` per method under `bench.Start(b).MaxAllocs(n)`, with an
  assertion on the result after the loop. A cold path, such as a
  registration that runs once, has none.
- `Test<Subject>Allocs` checks the same ceilings with `assert.MaxAllocs`
  or `expect.MaxAllocs`, so `go test -count=1` enforces them without
  `-bench`.
- A ceiling equals the count that a profile shows. A run over a corpus
  that allocates millions of times has a budget about 5% above one
  measurement.
- A test of a path through a `sync.Pool` calls `runtime.GC()` twice before
  it measures, so the count does not depend on what earlier tests left in
  the pool.

## Fuzzing

- A function that decodes bytes from outside the program has a
  `Fuzz<Function>` target with `prop.Fuzz`, which `go test -fuzz` runs.
- The same property runs with `prop.ForAll` in a test, so plain `go test`
  checks it on generated inputs.

## Test data

- Golden files are under `testdata/golden` and compared with
  `golden.MatchAt`. `go test -update` rewrites them.
- `prop` stores each smallest failing case under `testdata/prop`. A case
  that a real defect produced is committed as a regression case. A case
  that a mistake in a test produced is deleted by hand.

## Portability

CI runs the tests on Linux, macOS and Windows.

- A path is built with `filepath.Join`, or with `filepath.FromSlash` from
  a slash-separated constant.
- A file mode after an operation is compared with the mode before it,
  never with a literal, because Windows reports other modes.
- A test waits for every process that it starts, so Windows can delete
  the test's files.
- A test orders two events with an atomic counter, never with two clock
  readings, because the Windows clock advances in steps of about 16 ms.

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
  `helpers_test.go` and the specs that dokimi writes: `x_enum_test.go`,
  `errors_sentinel_test.go` and `x_suite_test.go`. `helpers_test.go`
  contains the helpers and fixtures that two or more specs of the package
  use.
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
- dokimi names the tests of a subject of a suite
  `Test<Subject>ConformsTo<Declaration>`,
  `Test<Subject>ConformsTo<Declaration>Allocs`,
  `Benchmark<Subject>ConformsTo<Declaration>` and
  `Fuzz<Subject>ConformsTo<Declaration><Callable>`. A spec does not
  declare a test of these names.
- A hand-written `Fuzz<Function>` fuzzes one entry point that no suite
  covers. A hand-written `Test<Subject>Allocs` and `Benchmark<Subject>`
  check and measure the ceilings of a struct or a function that has no
  suite, or apply the budgets of a suite that its subject applies by hand.

## Conformance suites

- dokimi generates the suite of the contract of an interface into
  `<pkg>test` from the directives `suite`, `stub` and `out path=<pkg>test/`
  on the interface. Every implementation applies the suite with the
  directive `subject`.
- The param `options` of the directive `subject` passes the options of the
  application, such as `conform.Using` and `conform.Restartable`. The suite
  starts a struct without exported fields from its zero value, so a subject
  whose calls need other values of such a struct passes `conform.Using`
  there.
- A spec applies a suite by hand where a generated test cannot apply it,
  such as to a subject whose generated calls draw inputs that it refuses,
  or to a type that the spec declares: `Test<Subject>Conformance` calls
  `Run<X>Suite`, `Test<Subject>Allocs` calls `Run<X>Budgets`, and
  `Benchmark<Subject>` calls `Bench<X>Suite`.
- A struct of a hot path that `docs/architecture/packages.md` lists has a
  suite of its own, unless core or kanon tests its type. Its factory is a
  function of the external test package of its kit.
- Each law is a check of the kit: an exported function under
  `//dokimi:check <Declaration>.<Method> defect=<function>`, whose doc
  comment opens with "<Name> checks that". dokimi names the check after
  the function in kebab case, such as `batches-are-atomic`. The check
  states the law through assertions only, and its decisive assertion
  passes `h.Statement()`.
- The planted defect of each check is the generated double over a
  verified fake of the kit, with the calls that break the law replaced.
  The generated test of the suite proves that the check fails on it at
  its statement, and that every other check passes on it.
- A law that needs an operation outside its contract reads the operation
  from an interface of the kit that the subject implements. An
  implementation without the operation excuses the law with
  `//dokimi:excuse` and a reason.
- A stateful contract has a check `histories-are-linearizable`: a
  `stateful` machine with concurrent clients, checked against the
  sequential specification of the contract.

## Determinism and isolation

- Every test and subtest calls `t.Parallel()`. The exceptions are the
  allocation tests, generated or hand-written, and a test that reads state
  of the whole process, whose doc comment states which state.
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

- Every callable of a suite on a hot or warm path has a `//dokimi:budget`
  with the ceiling of one call. dokimi generates its allocation test, its
  benchmark and a planted defect of each ceiling, so `go test -count=1`
  enforces the ceiling without `-bench`. A cold path, such as a
  registration that runs once, has none.
- A budget applies to every input that the call generator generates. A
  ceiling of a part of the domain, such as a call into a dst with room, is
  a budget over the whole domain and a check of the part.
- A ceiling of a steady state or of each element of an input is a check of
  the consumer under `//dokimi:check` with `counted=true`. It counts with
  `assert.MaxAllocs` in the allocation tests, which do not run in parallel.
- Every other exported function on a hot or warm path has a hand-written
  benchmark: one `b.Run` per method under `bench.Start(b).MaxAllocs(n)`,
  with an assertion on the result after the loop. `Test<Subject>Allocs`
  checks the same ceilings with `assert.MaxAllocs` or `expect.MaxAllocs`.
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

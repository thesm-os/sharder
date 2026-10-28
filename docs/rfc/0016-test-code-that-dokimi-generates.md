---
rfc: 0016
title: Enumerations, error checks, conformance suites, doubles and builders that dokimi generates
author: Roy Klopper
status: Accepted
created: 2026-10-23
updated: 2026-10-25
discussion: none
supersedes: RFC-0007 in part
superseded-by: ADR-0046 in part
produces-adr: ADR-0045
research: none
related: RFC-0001, RFC-0004, RFC-0007, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# RFC-0016: Enumerations, error checks, conformance suites, doubles and builders that dokimi generates

## Summary

dokimi reads directives in the doc comments of Go code and writes test code beside that code. sharder uses five of its generators:

- `enum` writes the text methods of 34 enumerations.
- `sentinel` writes the checks of the error values of 34 packages.
- `suite` writes the conformance suites of the five kits and of the hot paths.
- `stub` writes the test doubles of the 26 interfaces that specs replace, and the doubles that the planted defects of the kits build on.
- `builder` writes the builders of the five structs that the specs of five to ten packages build field by field.

This RFC proposes that dokimi generates this code in place of the hand-written forms. Each law of a kit becomes a check with a planted defect, so a test proves that every law can fail. A suite also generates the fuzz targets, the benchmarks and the budgets of its callables. The RFC amends the rules of the testing, performance and interfaces standards that name the hand-written forms.

## Motivation

### Hand-written test code

| Kind | Hand-written form | Count |
|---|---|---|
| Enumerations | A `String` method from stringer, and hand-written `Valid`, `MarshalText` and `UnmarshalText` methods with a spec of each text | 34 enumerations in 26 files |
| Errors | A level of `errors_test.go` for each error value, with its class and the prefix of its message | 33 packages |
| Conformance kits | An `Assert<X>` function that runs each law as a subtest | 5 kits with 93 laws |
| Allocation ceilings | `Test<Unit>Allocs` with `MaxAllocs`, and `Benchmark<Unit>` with the same ceiling under `bench.Start(b).MaxAllocs(n)` | 46 tests and 35 benchmarks |
| Test doubles | A type of a spec with the methods of the interface that it replaces | 26 interfaces |
| Fixtures | A literal of a struct with each field that a case sets | 230 literals of `shard.Placement` in 7 folders, 188 of `store.Mutation` in 5, 102 of `shard.Node` in 10, 82 of `config.Selection` in 7, and 38 of `config.Spec` in 7 |

The ceilings state each number twice: once in the test and once in the benchmark. Every form restates facts that dokimi derives from the declaration.

### Laws without a planted defect

ADR-0001 gave every contract with more than one implementation a conformance kit. It rejected an implementation that breaks each law "because a generator of conformance suites is to produce such implementations". Its negative consequences state the cost: "The tests of a kit do not show that each of its laws can fail, so nobody notices a check that never fails."

The testing standard puts one planted defect for each law into `<pkg>test/mutant`. No kit has that directory. None of the 93 laws of the five kits has a planted defect.

## Detailed design

### Components

| Directive | Placed on | dokimi writes | Replaces |
|---|---|---|---|
| `enum text=comment` | Each enumeration | `<file>_enum.go`, and `<file>_enum_test.go` for an exported enumeration | stringer, and the hand-written text methods with their specs |
| `sentinel` | The package clause of each package with an `errors.go` | `errors_sentinel_test.go` | The checks of the messages in `errors_test.go` |
| `suite` and `out path=<pkg>test/` | Each contract of a kit, and each hot path that `docs/architecture/packages.md` lists, unless core or kanon tests its type | `<file>_suite.go` in the kit, with its spec | `Assert<X>`, and the allocation tests and the benchmarks of the hot paths |
| `check <Declaration>.<Method> defect=<function>` | The function of each law and of each allocation check of a suite, in the kit | The check in the suite, and its falsification | The subtest of the law |
| `subject <Declaration> new=<factory> options=<function>` | Each implementation of a contract, and each struct of a hot path. The function of `options` returns the options of the application, such as `conform.Using` and `conform.Restartable` | `<file>_suite_test.go` with the conformance test, the allocation test, the benchmarks and the fuzz targets | `Test<Unit>Conformance`, and the allocation test and the benchmark of the subject |
| `budget allocs=<n>` | A callable of a suite, or a callable of a subject | The check of each ceiling, with its planted defect | `AssertConstraintAllocs`, `AssertGoalAllocs`, and the ceilings of the allocation tests that the suite replaces |
| `stub` and `out path=<pkg>test/` | Each interface of sharder that a spec replaces, and each declaration of a suite whose planted defects build on its double | `<X>Stub` in the kit of its package, with its spec | The hand-written doubles |
| `builder` and `out path=<pkg>test/` | `shard.Placement`, `shard.Node`, `store.Mutation`, `config.Spec` and `config.Selection` | `New<X>Builder` in the kit of its package, with its spec | The literals of these structs that the specs repeat |

### The generate step

- The step `generate` of `.ergon.yaml` lists two commands under the key `go.generate.commands` of ergon 0.14.2: `go run go.dokimi.dev/dokimi/cmd/dokimi@main run`, and `go generate`, which runs kanon. `check-go` runs `verify-generate-go`, which fails when a run changes a tracked file or writes an untracked one.
- `.dokimi.yaml` sets `version: 1`, `strict: true` and the policy `golang.width: "120"`, the width of golines. A strict run fails on every warning.
- A run writes its state to `.dokimi/`, which `.gitignore` ignores.
- sharder runs dokimi at the head of its branch main, because dokimi changes daily. Each run of the gate takes the head that the module proxy returns.

### Enumerations

- Each of the 34 enumerations has `//dokimi:enum text=comment`. The trailing comment of each constant is its text.
- dokimi writes `String`, `Parse<T>`, `<T>Values`, `IsValid`, `MarshalText` and `UnmarshalText`. It writes only the members that the package does not declare.
- An exported enumeration gets the spec `Test<T>` in `<file>_enum_test.go`. Its source file has no hand-written spec of its texts.
- An unexported enumeration, such as the state of a machine, gets unexported members and no spec (dokimi ADR-0004).
- Each package declares `ErrUnknown<T>` in `errors.go` with the class `errs.Invalid`. `Parse<T>` and `UnmarshalText` return an error that wraps it. `MarshalText` returns such an error for a value that `IsValid` refuses (dokimi ADR-0003).
- The codecs of kanon validate an enumeration through `IsValid`.

### Error checks

- The package comment of each of the 34 packages with an `errors.go` ends with `//dokimi:sentinel`.
- `errors_sentinel_test.go` checks that each error value is set and that its message opens with the prefix of the package. It also checks that no two messages are the same and that no error value matches another. It checks each error type the same way.
- dokimi does not check the prefix of an error value that another package declares, such as a re-export of `shard.ErrIncarnation`.
- `protocol.CallError` contains the message of an error of another process, so the package comment of `protocol` ends with `//dokimi:sentinel remote=[CallError]` (dokimi ADR-0005).
- `errors_test.go` checks the class of each error value alone.

### Conformance kits

| Kit | Declarations under `suite` | Verified fake of the planted defects | Subjects |
|---|---|---|---|
| `partition/partitiontest` | `partition.Partitioner` and `partition.Hash` | `fixedslots.Partitioner` and `xxhash64.Hash` | `fixedslots`, `singleton` and `xxhash64` |
| `placement/placementtest` | `placement.Constraint`, `placement.Goal` and `placement.Placer` | `constraint.Drain`, `goal.Stickiness` and the planner | The six built-in constraints, the seven built-in goals and the planner |
| `membership/membershiptest` | `membership.Membership` | `store/memory` | `store/memory` |
| `store/storetest` | `store.Store` | `store/memory` | `store/memory` |
| `protocol/protocoltest` | `protocoltest.Connector`, an interface of the kit | `protocol/local` | `protocol/local` |

Each law becomes a check:

- A law is an exported function of the kit with `//dokimi:check <Declaration>.<Method> defect=<function>`. Its doc comment opens with "<Name> checks that". dokimi names the check after the function in kebab case, such as `Store.Campaign/grants-each-term-to-one-candidate`.
- A check states its law through assertions alone, and its decisive assertion passes `h.Statement()` as its message.
- A law over generated inputs is a property check, which takes the case of the property engine beside the handle. The machines of `stateful` and the histories of `history` run inside such a check. A check that requires a share of the outcomes of its cases, such as the machine of commits of `storetest`, states the shares through the param `options` of its directive `check`.
- The planted defect of a law is the generated double over the verified fake of its kit, with the calls that break the law replaced.
- The generated test of each suite proves that each check fails on its own defect at its statement, and that every other check passes on that defect.
- core's `coretest/epochtest` checks every fenced writer. The spec of `store/memory` runs it beside the generated suite.

The transport laws choose the metadata of each call before the transport connects, such as a call of another major version. The suite of the transports is on an interface of the kit whose method takes that metadata:

```go
// Connector connects the clients of a transport to the servers of e. Every call of the
// clients sends md.
//
//dokimi:suite
//dokimi:stub
type Connector interface {
	Connect(md protocol.Metadata, e *Endpoints) (protocol.AgentClient, protocol.ControllerClient)
}
```

### Operations outside a contract

Some laws need an operation that the contract does not declare, such as the compaction of a history. A law reads such an operation from an interface of its kit that the subject implements:

| Kit | Interface | Methods | Laws that call it |
|---|---|---|---|
| `storetest` | `Faulty` | `LoseCommit(l Loss)` | A retry and `Outcome` settle a commit that a fault interrupts |
| `storetest` | `Deleter` | `Delete(sc shard.Scope)` | The reads, page tokens, watches and outcomes of a scope that another incarnation creates again |
| `storetest` | `Compactor` | `Compact()` | `Read` and `Watch` return `ErrCompacted` for a discarded generation |
| `membershiptest` | `Backed` | `Store() store.Store` and `Advance(d time.Duration)` | Every law, because a law creates its scope through the store and expires leases on the clock |
| `membershiptest` | `Faulty` | `LoseRenewals(l Loss)` | The `Done` of a session whose renewals a fault interrupts |
| `placementtest` | `Constrained` | `Constraints() []placement.Constraint` | `Place` returns a plan that `placement.Validate` accepts with the constraints of the placer |

- The factory of an implementation returns a type of its external test package. The type embeds the implementation and implements the interface of each operation that the implementation has.
- Every planted defect of a kit implements every interface of the kit, through the verified fake.
- A check whose subject does not implement its interface ends with a fault, so the check has no verdict on a defect that dokimi derives. An implementation without the operation excuses the check with `//dokimi:excuse` and a reason.
- A law that opens a store again after a restart calls `h.Restart`. A store supplies the restart through `conform.Restartable` in the options of its directive `subject`, and the planted defect of the law supplies its own restart through the param `restart` of its directive `check`. The memory store does not keep anything beyond its process, so it excuses the law.
- `Campaign` of `store.Store` waits until the leader of its scope resigns, so it carries `//dokimi:mixin waits`. No generated benchmark repeats it on one subject.
- The suite starts a struct without exported fields from its zero value, such as the `placement.State` of `Check`, `Cost` and `Delta` (dokimi ADR-0012). An implementation whose calls need other values passes `conform.Using` through the param `options` of its directive `subject`. Each built-in constraint and goal passes `placementtest.ConstraintCells` or `placementtest.GoalCells` this way.
- The planner applies the suite of `Placer` by hand, in `TestPlannerConformance` alone. A generated call of `Place` draws each field of an `Input` at random, so the planner refuses the input, and `BenchmarkPlanner` measures its passes. A spec of a kit applies its suite by hand to the types that the spec declares.
- The subjects read core's clock, and the clock of the handle is the clock of the assert library. A law that advances time calls `Advance` of the subject.

The API of the kits changes:

- `Run<X>Suite`, `Run<X>SuiteFromHandle` and the interfaces of the operations replace `Assert<X>`, `Option` and the `With<X>` options.
- The budgets of `Check` and `Delta` replace `AssertConstraintAllocs` and `AssertGoalAllocs`.
- The sums of the vectors of a hash are known answers of one implementation, so the spec of `xxhash64` checks them. `partitiontest.Vector` is removed.
- `placementtest.Inputs`, `Moves`, `Replicated` and the names of the metrics and the labels remain. The kit adds `States`, `CellMoves`, and the options `ConstraintCells` and `GoalCells` of `conform.Using`, which draw the states and the moves of the calls of `Check`, `Cost` and `Delta` from the cells of `Inputs`.
- `storetest.Loss` and `membershiptest.Loss` remain.
- `internal/streamtest` contains helpers of the kits and is not a kit.

### Hot paths

A suite standardizes the testing of a hot path with its derived checks, its fuzz targets, its benchmarks and its budgets. Each struct of a hot path that `docs/architecture/packages.md` lists gets a suite: `shard.CellMap`, `partition.Layout`, `placement.State`, `protocol.Metadata`, `router.Watcher` and the machine of `internal/machine/routes`. The partitioners, the hash, the constraints and the goals of the hot paths are subjects of the suites of their kits.

- A struct of a hot path has `//dokimi:suite`, the directive `out` into the kit of its package, and `//dokimi:subject <T> new=<factory>`. dokimi writes the test of the subject into the external test package of the kit, so the factory is a function of that package. The suites add the kits `shard/shardtest`, `router/routertest` and `internal/machine/routes/routestest`.
- A struct of a hot path has `//dokimi:stub` only where the planted defect of a check of its suite builds on its double.
- core tests its identifiers, so a type that embeds one, such as `shard.ID`, does not get a suite. kanon tests the codecs that it generates, so a type with a codec of kanon, such as `protocol.Request`, does not get a suite either.
- A factory whose subject runs goroutines, such as a started `router.Watcher`, stops the subject through `h.Cleanup`. conform v0.4.0 registers that function on every seat of a check.

### Budgets and benchmarks

A budget states the ceilings of a callable of a suite. Its allocation ceiling is a property check. Each case generates a call, and `assert.MaxAllocs` counts that call on a fresh subject. dokimi falsifies each ceiling with a planted defect that allocates once more than the ceiling admits. For each subject it generates `Test<Subject>ConformsTo<Declaration>Allocs` and `Benchmark<Subject>ConformsTo<Declaration>`.

- A budget is on the declaration when the contract of the declaration states the ceiling for every implementation. `placement.Constraint.Check`, `placement.Goal.Delta` and `partition.Hash.Sum64` state that they do not allocate, so each has `//dokimi:budget allocs=0`.
- A budget is on the subject when the ceiling belongs to one implementation, as `//dokimi:budget Partitioner.Position allocs=1` on `fixedslots.Partitioner`.
- A budget applies to every input that the call generator generates. A ceiling of a part of the domain becomes a budget over the whole domain and a check of the part. `Partitioner.Position` does not allocate when dst has room. Its budget admits the one allocation of the growth of dst, and a check of `partitiontest` states that `Position` appends into the room of dst.
- A profile over the inputs of the call generator precedes each ceiling, as the performance standard requires.

The 46 allocation tests and the 35 benchmarks map to these verdicts:

| Callables | Verdict |
|---|---|
| `Check` of the six built-in constraints, `Delta` of the seven built-in goals, and `Sum64` of `xxhash64` | Budgets on the declarations of their contracts |
| `Position` of `fixedslots` and `singleton` | Budgets on the subjects of `partition.Partitioner` |
| `Locate` of `shard.CellMap` and of `partition.Layout`, the readers of `placement.State`, `Check` of `protocol.Metadata`, `Route` of `router.Watcher`, and `Route` of the machine of `internal/machine/routes` | Budgets in the suites of these hot paths |
| `Apply` of `placement.State`, and the view of a cell in `internal/machine/routes` | Counted checks of the consumer in the suites of these hot paths, which count a sequence of calls or a ceiling for each slot under `assert.MaxAllocs`. A budget states the ceiling of one call (dokimasia/dokimi#20 and dokimasia/dokimi#21) |
| `Observed` and `Token` of `replica.Agent`, and `Route` of `router.Watcher` | Hand-written allocation specs, because a budget does not count the escape of a request in the frame of its caller (dokimasia/dokimi#22) |
| `Key` and `Digest` of `protocol.Request` | Hand-written allocation specs. kanon tests the codec of `Request`, so it gets no suite |
| A pass of the planner over 1,000 to 1,000,000 shards | The hand-written `BenchmarkPlanner`. The generated benchmark of a placer measures the small cells that its generator returns (dokimasia/dokimi#21) |
| Every other allocation test and benchmark, such as those of `shard.ID`, `shard.Range`, `placement.NewState`, `handoff.Script`, the machine of `internal/machine/agent`, `router.Do` and the driver of `internal/driver` | Unchanged |

`assert.MaxAllocs` counts the allocations of the whole process, so a check that counts allocations takes the param `counted=true` of the directive `check`. conform runs such a check in `Run<X>Budgets` and `FalsifyAllocs`, whose tests do not run in parallel. The change that adds each counted check removes its part of `TestStateAllocs` and `TestCellAllocs`.

### Doubles

- Each interface of sharder that a spec replaces has `//dokimi:stub` and the directive `out` into the kit of its package. A spec builds its double with `New<X>Stub` and the options `With<X><Method>`.
- A spec arms an error of a method with the helpers that `//dokimi:fault errors=[...]` on the method gives the double.
- A double that changes one call of a real implementation, such as a store whose watches wait, delegates to the implementation with `<X>StubDelegateTo` and replaces that call.
- These 26 interfaces have hand-written doubles, and their generated doubles replace them:

| Package | Interfaces |
|---|---|
| `config` | `Parsed`, `Factory` and `Provider` |
| `fence` | `Acquirer` |
| `handoff` | `Protocol` |
| `internal/driver` | `Handler`, `Revoker` and `LoadReporter` |
| `internal/machine` | `Machine` |
| `membership` | `Membership` and `Session` |
| `partition` | `Hash` |
| `placement` | `Constraint`, `Goal` and `Placer` |
| `protocol` | `AgentClient` and `ControllerClient` |
| `replica` | `Handler`, `Revoker`, `LoadReporter` and `Opener` |
| `router` | `Router` |
| `store` | `Store`, `Reader`, `Writer` and `Leadership` |

- A double of an interface of core, such as `clock.Timer` or `telemetry.Reporter`, remains hand-written, because its directive belongs on its declaration in core.
- The kit of an internal package is internal too, such as `internal/driver/drivertest`.

### Builders

- `shard.Placement`, `shard.Node`, `store.Mutation`, `config.Spec` and `config.Selection` have `//dokimi:builder` and the directive `out` into the kit of their package. The specs of five to ten packages build each of them field by field.
- A spec builds such a struct with `New<X>Builder` and the setters of the fields that its case sets. The builder writes the fields directly and does not check the invariants of a constructor.
- The builders of `config` add the kit `config/configtest`, which also contains the doubles of `config`.
- Other structs do not get builders, because no struct of sharder declares defaults and no other struct repeats across the specs of five packages or more.

### The standards

| Standard | Rule | Amended rule |
|---|---|---|
| Testing, Files | "A test file without a source file of its name does not exist, except `helpers_test.go`." | A test file without a source file of its name does not exist, except `helpers_test.go` and the specs that dokimi writes: `<file>_enum_test.go`, `errors_sentinel_test.go` and `<file>_suite_test.go`. |
| Testing, Names | "`Test<Subject>Allocs` checks the allocation ceilings of the subject, `Benchmark<Subject>` measures them, and `Fuzz<Function>` fuzzes one entry point." | dokimi names the tests of a subject `Test<Subject>ConformsTo<Declaration>`, `Test<Subject>ConformsTo<Declaration>Allocs`, `Benchmark<Subject>ConformsTo<Declaration>` and `Fuzz<Subject>ConformsTo<Declaration><Callable>`. A hand-written `Fuzz<Function>` fuzzes one entry point that no suite covers. A hand-written `Test<Subject>Allocs` and `Benchmark<Subject>` check and measure the ceilings of a struct or a function that has no suite, or apply the budgets of a suite that its subject applies by hand. |
| Testing, Conformance suites | The suite `<pkg>test`, the laws in kebab case, and a mutant for each law in `<pkg>test/mutant` | dokimi generates the suite of each contract into `<pkg>test`. Each law is a check of the kit with `//dokimi:check`, and dokimi names it in kebab case. The planted defect of each check is the generated double over a verified fake of the kit, and the generated test of the suite proves that the check fails on it. An implementation applies the suite with `//dokimi:subject`, and excuses a check only with `//dokimi:excuse` and a reason. The param `options` of the directive passes the options of the application, such as `conform.Using` and `conform.Restartable`. A spec applies a suite by hand where a generated test cannot apply it, such as to a subject whose generated calls draw inputs that it refuses, or to a type that the spec declares: `Test<Subject>Conformance` calls `Run<X>Suite`, `Test<Subject>Allocs` calls `Run<X>Budgets`, and `Benchmark<Subject>` calls `Bench<X>Suite`. A struct of a hot path that `docs/architecture/packages.md` lists has a suite of its own, unless core or kanon tests its type. Its factory is a function of the external test package of its kit. |
| Testing, Determinism and isolation | "The exceptions are `Test<Subject>Allocs` and a test that reads state of the whole process" | The exceptions are the allocation tests, generated or hand-written, and a test that reads state of the whole process. |
| Testing, Allocation ceilings and benchmarks | A benchmark under `bench.Start(b).MaxAllocs(n)` for every exported function on a hot or warm path, and `Test<Subject>Allocs` with the same ceilings | Every callable of a suite on a hot or warm path has a `//dokimi:budget` with the ceiling of one call. dokimi generates its allocation test, its benchmark and a planted defect of each ceiling. A budget applies to every generated input, and a ceiling of a part of the domain is a budget over the whole domain and a check of the part. A ceiling of a steady state or of each element of an input is a counted check of the consumer, which counts with `assert.MaxAllocs` outside the parallel tests. Every other exported function on a hot or warm path keeps its hand-written benchmark under `bench.Start(b).MaxAllocs(n)` and its `Test<Subject>Allocs`. |
| Performance, Measurement | "Every hot path has a benchmark that runs at 1 and 4 cores with `-cpu=1,4`, under `bench.Start(b).MaxAllocs(n)`. Its `Test<Unit>Allocs` checks the same ceiling in an ordinary test run." | Every hot path has a benchmark that runs at 1 and 4 cores with `-cpu=1,4`, and an allocation test that checks its ceiling in an ordinary test run. dokimi generates both from the budget of a callable of a suite. A hot path without a suite, such as a method of a type whose codec kanon generates, keeps its hand-written `Benchmark<Unit>` under `bench.Start(b).MaxAllocs(n)` and its `Test<Unit>Allocs`. So does a pass of the planner over 1,000,000 shards. |
| Interfaces, Allocations | "The `Allocs` test of each call measures it with the request in a local variable, as a host writes it." | A hand-written allocation spec of each call measures the call with the request in a local variable, as a host writes it. The budget of the call does not count that escape. |
| README, Enforcement | "A call does not allocate in steady state: the `Allocs` test of the call" | A call does not allocate in steady state: the budget of a callable of a suite, through the allocation test that dokimi generates, and the `Allocs` test of any other call. The request of a call does not escape the frame of its caller: the hand-written allocation spec of the call. Each law of a kit can fail: the test of its suite that dokimi generates. The generated files match their directives: `make verify-generate-go`. |

### Superseded decisions

The ADR of the decision of this RFC supersedes these parts, and each record links to the other:

| Record | Part | Replaced by |
|---|---|---|
| RFC-0007 | The API of `storetest`: `AssertStore`, `Option`, `WithReopen`, `WithLostCommits` and `WithDelete` | The generated suite of `store.Store`, `conform.Restartable`, and the interfaces of the operations of the kit |
| ADR-0001 | "The kits do not contain implementations that break a law on purpose." | A planted defect for each law |
| ADR-0006 | "a `String` method from stringer and a `Valid` method" | The members that dokimi generates, with `IsValid` |
| ADR-0012 | "`Valid` and a `String` from stringer" | The members that dokimi generates, with `IsValid` |
| ADR-0013 | "`storetest` adds `WithCompact`, and its callbacks and those of `membershiptest` take an `assert.TB`. `membershiptest` runs the leases on a fake clock, and adds `WithLostRenewals`." | The generated suites of `store.Store` and `membership.Membership`, and the interfaces of the operations of their kits |
| ADR-0015 | "the kit measures `Check` and `Delta` with `MaxAllocs`" | The budgets of `Check` and `Delta` |
| `docs/architecture/packages.md` | The outputs of stringer, and the `Test<Unit>Allocs` tests of the hot paths | The files that dokimi writes, and the budgets of the suites |

### Breaking changes

- `IsValid` replaces `Valid` on the 18 exported enumerations.
- `proposal.ErrUnknownOperationKind` replaces `ErrUnknownKind`, because dokimi names the error after the type `OperationKind`.
- `UnmarshalText` of an enumeration returns an error that wraps `ErrUnknown<T>`. `MarshalText` returns such an error for a value that `IsValid` refuses.
- The generated suites replace `Assert<X>` and the options of each kit. An implementation outside sharder applies a suite through `//dokimi:subject` or `Run<X>Suite`.
- `partitiontest.Vector` and the known answers of `AssertHash` are removed.

### Invariants

| Invariant | Enforced by |
|---|---|
| Every law of a kit has a planted defect, and its check fails on that defect | The generated test of each suite |
| A planted defect breaks one law | The generated test of each suite, which runs every other check on each defect |
| Every allocation ceiling fails on a defect that allocates once more | `Test<Declaration>SuiteAllocs`, which dokimi generates |
| A ceiling applies to every generated input | The property check of each ceiling |
| The generated files match their directives | `make verify-generate-go` in `check-go` |
| Every implementation of a contract runs the suite of the contract | `//dokimi:subject` on each implementation, or `Run<X>Suite` in its spec |

### Failure handling

| Failure | Result | Recovery |
|---|---|---|
| A subject fails a derived check, such as `cancel` on a method that does not read its context | The conformance test of the subject fails | Fix the method, or excuse the check with its reason |
| A check of a kit passes on its planted defect | The generated test of the suite fails | Fix the check or the defect |
| A planted defect breaks a second law | The generated test of the suite fails and lists the other checks | Narrow the defect to one law |
| A budget fails on a generated input | The allocation test fails with the smallest such input | Remove the allocation, or state the domain with a tag of `prop` or a registration |
| A run does not count allocations, such as a run under the race detector | Each allocation ceiling skips with the reason of the assert library | `go test -count=1` without the race detector |
| A head of dokimi changes a generated file | `verify-generate-go` fails | Run `make generate-go`, and review the change |

### Verdicts

| Capability | Verdict | Reason | Evidence that would reverse it |
|---|---|---|---|
| dokimi's text methods for every enumeration | In | dokimi writes each member and its spec from the declaration | None |
| `IsValid` as the name of the validity method | In | It is the default of dokimi, so no enumeration needs an option | None |
| The sentinel checks of every package with errors | In | dokimi writes the checks of the messages from the declarations | None |
| Generated suites for the five kits | In | Each law gets a planted defect, and the falsification proves that each check can fail | None. ADR-0001 waited for this generator |
| The options of an application in the directive `subject` | In | The param `options` passes `conform.Using` and `conform.Restartable` to every test that dokimi writes for a subject (dokimi RFC-0008) | None |
| Applications by hand of the suites whose generated calls a subject refuses | In | The generated benchmark of the planner would measure the refusals of random inputs | A sample of the input of `Place` in its generated calls (dokimi RFC-0012) |
| Operations outside a contract as interfaces of the kits | In | The operations differ per contract, and each implementation implements them on its own types | A door of conform that each kit can declare |
| `protocoltest.Connector` as the declaration of the transport suite | In | The laws choose the metadata of each connection | A transport whose client takes its metadata on each call |
| The known answers of a hash in the spec of its implementation | In | The vectors belong to one implementation | None |
| Budgets on the declarations of `Check`, `Delta` and `Sum64` | In | Their contracts state the ceiling for every implementation | None |
| A budget over the whole domain beside a check of the room of dst | In | A budget applies to every generated input, and a parameter of a slice takes no tag | A budget that states the domain of one parameter |
| Suites of the hot paths that `docs/architecture/packages.md` lists | In | A suite standardizes the testing of a hot path with its derived checks, fuzz targets, benchmarks and budgets | None |
| No suite on a type that core or kanon tests | In | core tests its identifiers, and kanon tests the codecs that it generates | None |
| Checks of the consumer for a steady state and for a ceiling for each slot | In | A budget states the ceiling of one call, and dokimi keeps that meaning (dokimasia/dokimi#20 and dokimasia/dokimi#21) | A second consumer with ceilings for each element, which reverses the verdict of dokimasia/dokimi#21 |
| A hand-written allocation spec of the escape of a request | In | A generated call does not measure the frame of its caller for every kind of argument (dokimasia/dokimi#22) | None |
| The other allocation tests and benchmarks unchanged | In | They test structs and functions that no suite covers | A suite that covers one of them |
| The hand-written benchmark of the planner at stated sizes | In | The generated benchmark of a placer measures 100 generated calls of small cells | A benchmark of dokimi at a stated size of its input |
| Builders of `shard.Placement`, `shard.Node`, `store.Mutation`, `config.Spec` and `config.Selection` | In | The specs of several packages build each of them field by field | None |
| Builders of other structs | Out | No struct of sharder declares defaults, and no other struct repeats across the specs of several packages | A struct with declared defaults, or one that the specs of several packages build field by field |
| Generated doubles of the interfaces of sharder | In | A stub records each call and arms each declared fault, which the hand-written doubles repeat | None |
| Generated doubles of the interfaces of core | Out | A directive belongs on its declaration, in core | A release of core with the stubs of its interfaces |
| Latency ceilings | Out | The testing standard does not use latency ceilings, because latency differs per machine | A class of runners that repeats each latency within its ceiling |
| kanon in the generate step | In | ergon 0.14.2 runs a list of commands, so `go generate` follows dokimi | None |
| A pinned release of dokimi | Out | dokimi changes daily | A release cadence of dokimi |

## Alternatives considered

### Hand-written planted defects in `<pkg>test/mutant`

Each kit writes one implementation per law that breaks the law, as the testing standard states.

Rejected because dokimi generates the double of each declaration. A planted defect is that double with the calls of one law replaced. ADR-0001 rejected the hand-written form for the same reason.

### A door of conform for each operation

conform takes a function of the consumer for each operation, as the options of `Assert<X>` do.

Rejected because the doors of conform are machineries that every suite shares, such as the restart and the clock. The compaction of a history belongs to one contract, so the interface of its kit states that operation once with its doc comment.

### A suite on every struct with an allocation test

Every struct with a hand-written allocation test gets a suite, `shard.ID` and `shard.Range` included, so a budget states its ceilings.

Rejected because a suite is the conformance suite of a contract or of a hot path, and an allocation test makes a struct neither. core tests the identifiers of `shard.ID`, and kanon tests the codec of `shard.Range`.

### A registration of `[]byte` with room

`TestMain` registers a generator of `[]byte` whose slices have room. A budget of 0 then applies to `Position` and `AppendBinary`.

Rejected because a registration applies to the whole test binary. Every property of the binary would then generate slices with room.

## Drawbacks

- A struct of a hot path gets a suite with derived checks of every exported method.
- The generated code grows by one call generator, one fuzz target and one level of a benchmark for each callable of a suite.
- The suites of the hot paths add the kits `shard/shardtest`, `router/routertest` and `internal/machine/routes/routestest`. The doubles and the builders add kits such as `config/configtest`, `fence/fencetest`, `replica/replicatest` and `internal/driver/drivertest`.
- The kits change their API, so an application that applies a kit changes its tests.
- A property check runs 100 cases, with a fresh subject for each case.
- The allocation tests and the benchmarks outside the suites remain hand-written, so their ceilings have no planted defect.
- A run at the head of dokimi can change generated files without a change of sharder.

## Open questions

None.

## References

| What | Where |
|---|---|
| The guide of dokimi for a Go package | <https://github.com/dokimasia/dokimi/blob/main/docs/guides/go.md> |
| dokimi RFC-0003, the suite generator | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0003-the-suite-generator.md> |
| dokimi RFC-0004, doubles, builders, enumerations and errors | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0004-doubles-builders-enumerations-and-errors.md> |
| dokimi RFC-0006, generated inputs, fuzz targets, benchmarks and budgets | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0006-generated-inputs-fuzz-targets-benchmarks-and-budgets.md> |
| dokimi ADR-0001, every generated test is a black-box test | <https://github.com/dokimasia/dokimi/blob/main/docs/adr/0001-every-generated-test-is-a-black-box-test.md> |
| dokimi ADR-0002, a struct is the subject of each declaration that it names | <https://github.com/dokimasia/dokimi/blob/main/docs/adr/0002-a-struct-is-the-subject-of-each-declaration-it-names.md> |
| dokimi ADR-0003, the encode of an enumeration refuses a value outside the variants | <https://github.com/dokimasia/dokimi/blob/main/docs/adr/0003-the-encode-of-an-enumeration-refuses-a-value-outside-the-variants.md> |
| dokimi ADR-0004, an unexported enumeration has unexported members and no checks | <https://github.com/dokimasia/dokimi/blob/main/docs/adr/0004-an-unexported-enumeration-has-unexported-members-and-no-checks.md> |
| dokimi ADR-0005, a remote error type has the message of another process | <https://github.com/dokimasia/dokimi/blob/main/docs/adr/0005-a-remote-error-type-has-the-message-of-another-process.md> |
| conform, the runtime of the generated suites | <https://github.com/dokimasia/dokimi-go/tree/main/conform> |
| A budget of a sequence of calls, which dokimi does not plan | <https://github.com/dokimasia/dokimi/issues/20> |
| A ceiling that grows with the size of an input, which dokimi does not plan | <https://github.com/dokimasia/dokimi/issues/21> |
| A budget that counts the escape of a request, which dokimi does not plan | <https://github.com/dokimasia/dokimi/issues/22> |
| dokimi ADR-0006, a check that counts allocations runs outside the parallel tests | <https://github.com/dokimasia/dokimi/blob/main/docs/adr/0006-a-check-that-counts-allocations-runs-outside-the-parallel-tests.md> |
| dokimi ADR-0007, a suite does not ask for the double of its declaration | <https://github.com/dokimasia/dokimi/blob/main/docs/adr/0007-a-suite-does-not-ask-for-the-double-of-its-declaration.md> |
| dokimi ADR-0008, a generator checks its references against the package of its routed file | <https://github.com/dokimasia/dokimi/blob/main/docs/adr/0008-a-generator-checks-its-references-against-the-package-of-its-routed-file.md> |
| dokimi ADR-0012, a struct without exported fields takes its zero value and an application adds generators | <https://github.com/dokimasia/dokimi/blob/main/docs/adr/0012-a-struct-without-exported-fields-takes-its-zero-value-and-an-application-adds-generators.md> |
| dokimi RFC-0008, the doors of a subject and of a planted defect | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0008-the-doors-of-a-subject-and-of-a-planted-defect.md> |
| dokimi RFC-0009, the options of the property run of a check | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0009-the-options-of-the-property-run-of-a-check.md> |
| dokimi RFC-0010, the benchmarks of a callable that waits for another call | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0010-the-benchmarks-of-a-callable-that-waits.md> |
| ergon, a generate step of more than one command | <https://github.com/dokimasia/ergon/issues/53> |
| core's suite of fenced writers | <https://github.com/thesm-os/core/tree/main/coretest/epochtest> |
| The conformance kits | ADR-0001 |
| The enumerations | ADR-0006, ADR-0012 |
| The allocations of constraints and goals | ADR-0015 |
| The API of `storetest` | RFC-0007, Laws and conformance |
| The testing, performance and interfaces standards | `docs/standards/` |

---
rfc: 0016
title: Enumerations, error checks, conformance kits, budgets and doubles that dokimi generates
author: Roy Klopper
status: Draft
created: 2026-10-23
updated: 2026-10-23
discussion: none
supersedes: RFC-0007 in part
superseded-by: none
produces-adr: tbd
research: none
related: RFC-0001, RFC-0004, RFC-0007, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# RFC-0016: Enumerations, error checks, conformance kits, budgets and doubles that dokimi generates

## Summary

dokimi reads directives in the doc comments of Go code and writes test code beside that code. sharder writes five kinds of such code by hand:

- the text methods of 34 enumerations, through stringer and hand-written methods
- the checks of the error values of 33 packages
- five conformance kits with 93 laws
- 46 allocation tests and 35 benchmarks
- the test doubles of 26 interfaces

This RFC proposes that dokimi generates all five. Each law of a kit becomes a check with a planted defect, so a test proves that every law can fail. Each allocation ceiling becomes a budget of its callable, and dokimi generates the allocation test and the benchmark of the budget. The RFC amends the rules of the testing, performance and interfaces standards that name the hand-written forms.

## Motivation

### Hand-written test code

| Kind | Hand-written form | Count |
|---|---|---|
| Enumerations | A `String` method from stringer, and hand-written `Valid`, `MarshalText` and `UnmarshalText` methods with a spec of each text | 34 enumerations in 26 files |
| Errors | A level of `errors_test.go` for each error value, with its class and the prefix of its message | 33 packages |
| Conformance kits | An `Assert<X>` function that runs each law as a subtest | 5 kits with 93 laws |
| Allocation ceilings | `Test<Unit>Allocs` with `MaxAllocs`, and `Benchmark<Unit>` with the same ceiling under `bench.Start(b).MaxAllocs(n)` | 46 tests and 35 benchmarks |
| Test doubles | A type of a spec with the methods of the interface that it replaces | 26 interfaces |

The ceilings state each number twice: once in the test and once in the benchmark. Every form restates facts that dokimi derives from the declaration.

### Laws without a planted defect

ADR-0001 gave every contract with more than one implementation a conformance kit. It rejected an implementation that breaks each law "because a generator of conformance suites is to produce such implementations". Its negative consequences state the cost: "The tests of a kit do not show that each of its laws can fail, so nobody notices a check that never fails."

The testing standard puts one planted defect for each law into `<pkg>test/mutant`. No kit has that directory. None of the 93 laws of the five kits has a planted defect.

### Ceilings without a call of sharder

Eight of the 46 allocation tests, and 10 subtests of 5 others, count the allocations of a composite literal, a comparison, a map lookup or the conversion of a value to an interface. These tests do not call a function or a method of sharder.

## Detailed design

### Components

| Directive | Placed on | dokimi writes | Replaces |
|---|---|---|---|
| `enum text=comment` | Each enumeration | `<file>_enum.go`, and `<file>_enum_test.go` for an exported enumeration | stringer, and the hand-written text methods with their specs |
| `sentinel` | The package clause of each package with an `errors.go` | `errors_sentinel_test.go` | The checks of the messages in `errors_test.go` |
| `suite`, `stub` and `out path=<pkg>test/` | Each contract of a kit, and each concrete type with a budget | `<file>_suite.go` and `<file>_stub.go` in the kit, with their specs | `Assert<X>` |
| `check <Declaration>.<Method> defect=<function>` | The function of each law, in the kit | The check in the suite, and its falsification | The subtest of the law |
| `subject <Declaration> new=<factory>` | Each implementation, and each concrete type with a budget | `<file>_suite_test.go` with the conformance test, the allocation test, the benchmarks and the fuzz targets | `Test<Unit>Conformance`, `Test<Unit>Allocs` and `Benchmark<Unit>` |
| `budget allocs=<n>` | A method of a declaration under `suite`, or a callable of a subject | The check of each ceiling, with its planted defect | The ceilings of `Test<Unit>Allocs` and `Benchmark<Unit>` |
| `stub` and `out path=<pkg>test/` | Each interface of sharder that a spec replaces | `<X>Stub` in the kit of its package | The hand-written doubles |

### The generate step

- The step `generate` of `.ergon.yaml` runs `go run go.dokimi.dev/dokimi/cmd/dokimi@main run` in each module. `check-go` runs `verify-generate-go`, which fails when a run changes a tracked file or writes an untracked one.
- `.dokimi.yaml` sets `version: 1`, `strict: true` and the policy `golang.width: "120"`, the width of golines. A strict run fails on every warning.
- A run writes its state to `.dokimi/`, which `.gitignore` ignores.
- The step runs one command, so the gate does not regenerate the codecs of kanon until dokimasia/ergon#53 is resolved. A change of a type with a codec runs `go generate` for kanon by hand.
- sharder runs dokimi at the head of its branch main, because dokimi changes daily. Each run of the gate takes the head that the module proxy returns.

### Enumerations

- Each of the 34 enumerations has `//dokimi:enum text=comment`. The trailing comment of each constant is its text.
- dokimi writes `String`, `Parse<T>`, `<T>Values`, `IsValid`, `MarshalText` and `UnmarshalText`. It writes only the members that the package does not declare.
- An exported enumeration gets the spec `Test<T>` in `<file>_enum_test.go`. Its source file has no hand-written spec of its texts.
- An unexported enumeration, such as the state of a machine, gets unexported members and no spec (dokimi ADR-0004).
- Each package declares `ErrUnknown<T>` in `errors.go` with the class `errs.Invalid`. `Parse<T>` and `UnmarshalText` return an error that wraps it. `MarshalText` returns such an error for a value that `IsValid` refuses (dokimi ADR-0003).
- The codecs of kanon validate an enumeration through `IsValid`.

### Error checks

- The package comment of each of the 33 packages with an `errors.go` ends with `//dokimi:sentinel`.
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
- A law over generated inputs is a property check, which takes the case of the property engine beside the handle. The machines of `stateful` and the histories of `history` run inside such a check.
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
- A law that opens a store again after a restart calls `h.Restart`, the door that `conform.Restartable` supplies. A store whose directive cannot state that option calls `RunStoreSuiteFromHandle` in a test of its own.
- The subjects read core's clock, and the clock of the handle is the clock of the assert library. A law that advances time calls `Advance` of the subject.

The API of the kits changes:

- `Run<X>Suite`, `Run<X>SuiteFromHandle` and the interfaces of the operations replace `Assert<X>`, `Option` and the `With<X>` options.
- The budgets of `Check` and `Delta` replace `AssertConstraintAllocs` and `AssertGoalAllocs`.
- The sums of the vectors of a hash are known answers of one implementation, so the spec of `xxhash64` checks them. `partitiontest.Vector` is removed.
- `placementtest.Inputs`, `Moves`, `Replicated` and the names of the metrics and the labels remain. The kit registers generators of `*placement.State` and `placement.Move` in an `init` function, so the calls of its budgets and its property checks take the states of the cells that `Inputs` generates.
- `storetest.Loss` and `membershiptest.Loss` remain.
- `internal/streamtest` contains helpers of the kits and is not a kit.

### Budgets and benchmarks

A budget states the ceilings of a callable on its declaration. Its allocation ceiling is a property check. Each case generates a call, and `assert.MaxAllocs` counts that call on a fresh subject. dokimi falsifies each ceiling with a planted defect that allocates once more than the ceiling admits. For each subject it generates `Test<Subject>ConformsTo<Declaration>Allocs` and `Benchmark<Subject>ConformsTo<Declaration>`.

- A budget is on the declaration when the contract of the declaration states the ceiling for every implementation. `placement.Constraint.Check`, `placement.Goal.Delta` and `partition.Hash.Sum64` state that they do not allocate, so each has `//dokimi:budget allocs=0`.
- A budget is on the subject when the ceiling belongs to one implementation, as `//dokimi:budget Partitioner.Position allocs=1` on `fixedslots.Partitioner`.
- A budget applies to every input that the call generator generates. A ceiling of a part of the domain becomes a budget over the whole domain and a check of the part. `Partitioner.Position` does not allocate when dst has room. Its budget admits the one allocation of the growth of dst, and a check of `partitiontest` states that `Position` appends into the room of dst. The same pair replaces the ceilings of `AppendBinary` and `AppendKanon` of `shard.ID` and of `AppendBinary` of `shard.NamespaceIncarnation`.
- A ceiling that depends on the receiver is a second application of the budgets. `MarshalBinary` of `shard.ID` allocates the returned slice, and nothing for the zero ID. The subject `shard.ID` states `//dokimi:budget ID.MarshalBinary allocs=1`, and a spec applies `RunIDBudgets` to the zero ID with `conform.Ceilings` of 0 allocations.
- A concrete type with a budget has `//dokimi:suite`, `//dokimi:subject <T> new=<factory>` and the directive `out` into the kit of its package. The kits `shard/shardtest`, `handoff/handofftest`, `internal/machine/agent/agenttest` and `internal/machine/routes/routestest` are new.
- A profile over the inputs of the call generator precedes each ceiling, as the performance standard requires.

The 46 allocation tests and the 35 benchmarks map to these verdicts:

| Callables | Verdict |
|---|---|
| `Check` of the six built-in constraints, `Delta` of the seven built-in goals, and `Sum64` of `xxhash64` | Budgets on the declarations |
| `Position` of `fixedslots` and `singleton`, `Locate` of `partition.Layout` and `shard.CellMap`, `Contains` of `shard.Range`, `IsZero`, `Compare`, `SizeKanon`, `AppendBinary`, `AppendKanon`, `MarshalBinary` and `UnmarshalBinary` of `shard.ID`, `AppendBinary` of `shard.NamespaceIncarnation`, `Check` of `protocol.Metadata`, `Key` and `Digest` of `protocol.Request`, `Node` of `handoff.Transfer`, `Validate` of `handoff.Script`, `Script` of `breakbeforemake.Protocol`, the readers of `placement.State`, `Observed` and `Token` of the machine of `internal/machine/agent`, and `Route` of the machine of `internal/machine/routes` | Budgets on the subjects |
| `Apply` of `placement.State`, and the view of a cell in `internal/machine/routes` from the third view of the cell on | Budgets once dokimi counts a sequence of calls, dokimasia/dokimi#20 |
| `placement.NewState`, the view of a cell for each slot that it replaces, and a pass of the planner over 1,000 to 1,000,000 shards | Budgets and a benchmark once a ceiling can grow with the size of an input, dokimasia/dokimi#21 |
| `Observed` and `Token` of `replica.Agent`, `Route` of `router.Watcher`, `router.Do`, and `Call` of the driver of `internal/driver` | Budgets once a factory can stop a subject that runs goroutines, dokimasia/dokimi-go#8, and a budget counts the escape of a request, dokimasia/dokimi#22 |
| The constructions, comparisons, map lookups and conversions of `shard`, `partition` and `internal/machine` | Removed. A budget states the ceiling of a callable, and these tests do not call one |

The hand-written test of a callable that waits for a request remains until dokimi releases the request. The change that adds the budget of the callable removes the test.

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

### The standards

| Standard | Rule | Amended rule |
|---|---|---|
| Testing, Files | "A test file without a source file of its name does not exist, except `helpers_test.go`." | A test file without a source file of its name does not exist, except `helpers_test.go` and the specs that dokimi writes: `<file>_enum_test.go`, `errors_sentinel_test.go` and `<file>_suite_test.go`. |
| Testing, Names | "`Test<Subject>Allocs` checks the allocation ceilings of the subject, `Benchmark<Subject>` measures them, and `Fuzz<Function>` fuzzes one entry point." | dokimi names the tests of a subject `Test<Subject>ConformsTo<Declaration>`, `Test<Subject>ConformsTo<Declaration>Allocs`, `Benchmark<Subject>ConformsTo<Declaration>` and `Fuzz<Subject>ConformsTo<Declaration><Callable>`. A hand-written `Fuzz<Function>` fuzzes one entry point that no suite covers. |
| Testing, Conformance suites | The suite `<pkg>test`, the laws in kebab case, and a mutant for each law in `<pkg>test/mutant` | dokimi generates the suite of each contract into `<pkg>test`. Each law is a check of the kit with `//dokimi:check`, and dokimi names it in kebab case. The planted defect of each check is the generated double over a verified fake of the kit, and the generated test of the suite proves that the check fails on it. An implementation applies the suite with `//dokimi:subject`, and excuses a check only with `//dokimi:excuse` and a reason. |
| Testing, Determinism and isolation | "The exceptions are `Test<Subject>Allocs` and a test that reads state of the whole process" | The exceptions are the allocation tests that dokimi generates and a test that reads state of the whole process. |
| Testing, Allocation ceilings and benchmarks | A benchmark under `bench.Start(b).MaxAllocs(n)` for every exported function on a hot or warm path, and `Test<Subject>Allocs` with the same ceilings | Every exported callable on a hot or warm path has a `//dokimi:budget`. dokimi generates its allocation test, its benchmark and a planted defect of each ceiling. A budget applies to every generated input, and a ceiling of a part of the domain is a budget over the whole domain and a check of the part. |
| Performance, Measurement | "Every hot path has a benchmark that runs at 1 and 4 cores with `-cpu=1,4`, under `bench.Start(b).MaxAllocs(n)`. Its `Test<Unit>Allocs` checks the same ceiling in an ordinary test run." | Every hot path has a budget. dokimi generates its benchmark, which runs at 1 and 4 cores with `-cpu=1,4`, and its allocation test, which checks the ceiling in an ordinary test run. |
| Interfaces, Allocations | "The `Allocs` test of each call measures it with the request in a local variable, as a host writes it." | The allocation ceiling of each call counts a request in a local variable of the caller, as a host writes it. |
| README, Enforcement | "A call does not allocate in steady state: the `Allocs` test of the call" | A call does not allocate in steady state: the budget of the call, through the allocation test that dokimi generates. Each law of a kit can fail: the test of its suite that dokimi generates. The generated files match their directives: `make verify-generate-go`. |

### Superseded decisions

The ADR of the decision of this RFC supersedes these parts, and each record links to the other:

| Record | Part | Replaced by |
|---|---|---|
| RFC-0007 | The API of `storetest`: `AssertStore`, `Option`, `WithReopen`, `WithLostCommits` and `WithDelete` | The generated suite of `store.Store`, `conform.Restartable`, and the interfaces of the operations of the kit |
| ADR-0001 | "The kits do not contain implementations that break a law on purpose." | A planted defect for each law |
| ADR-0006 | "a `String` method from stringer and a `Valid` method" | The members that dokimi generates, with `IsValid` |
| ADR-0012 | "`Valid` and a `String` from stringer" | The members that dokimi generates, with `IsValid` |
| ADR-0015 | "the kit measures `Check` and `Delta` with `MaxAllocs`" | The budgets of `Check` and `Delta` |
| `docs/architecture/packages.md` | The hot paths with a `Test<Unit>Allocs` test, and the outputs of stringer | The budgets, and the files that dokimi writes |

ADR-0020 cites `TestMachineAllocs` as the measurement of its negative consequence that an event or an effect that escapes costs one allocation. The consequence is a property of the conversion of a value to an interface in Go, and remains true without the test.

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
| Every implementation of a contract runs the suite of the contract | `//dokimi:subject` on each implementation |

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
| Operations outside a contract as interfaces of the kits | In | The operations differ per contract, and each implementation implements them on its own types | A door of conform that each kit can declare |
| `protocoltest.Connector` as the declaration of the transport suite | In | The laws choose the metadata of each connection | A transport whose client takes its metadata on each call |
| The known answers of a hash in the spec of its implementation | In | The vectors belong to one implementation | None |
| Budgets on the declarations of `Check`, `Delta` and `Sum64` | In | Their contracts state the ceiling for every implementation | None |
| A budget over the whole domain beside a check of the room of dst | In | A budget applies to every generated input, and a parameter of a slice takes no tag | A budget that states the domain of one parameter |
| The removal of the ceilings of constructions, comparisons, lookups and conversions | In | They do not call code of sharder | A type of sharder whose construction or comparison calls code of sharder |
| Builders | Out | No struct of sharder declares defaults, and each constructor validates its config | A struct with declared defaults that specs build field by field |
| Generated doubles of the interfaces of sharder | In | A stub records each call and arms each declared fault, which the hand-written doubles repeat | None |
| Generated doubles of the interfaces of core | Out | A directive belongs on its declaration, in core | A release of core with the stubs of its interfaces |
| Latency ceilings | Out | The testing standard does not use latency ceilings, because latency differs per machine | A class of runners that repeats each latency within its ceiling |
| kanon in the generate step | Out | The step runs one command | dokimasia/ergon#53 |
| A pinned release of dokimi | Out | dokimi changes daily | A release cadence of dokimi |

## Alternatives considered

### Hand-written planted defects in `<pkg>test/mutant`

Each kit writes one implementation per law that breaks the law, as the testing standard states.

Rejected because dokimi generates the double of each declaration. A planted defect is that double with the calls of one law replaced. ADR-0001 rejected the hand-written form for the same reason.

### A door of conform for each operation

conform takes a function of the consumer for each operation, as the options of `Assert<X>` do.

Rejected because the doors of conform are machineries that every suite shares, such as the restart and the clock. The compaction of a history belongs to one contract, so the interface of its kit states that operation once with its doc comment.

### Hand-written allocation tests beside the budgets

The concrete types keep `Test<Unit>Allocs`. The contracts of the kits get budgets.

Rejected because one rule would then have two mechanisms. A budget proves with a planted defect that its ceiling can fail. A hand-written test does not.

### A registration of `[]byte` with room

`TestMain` registers a generator of `[]byte` whose slices have room. A budget of 0 then applies to `Position` and `AppendBinary`.

Rejected because a registration applies to the whole test binary. Every property of the binary would then generate slices with room.

## Drawbacks

- A concrete type with a budget gets a suite with derived checks of every exported method.
- The generated code grows by one call generator, one fuzz target and one level of a benchmark for each callable.
- The kits `shard/shardtest`, `handoff/handofftest`, `internal/machine/agent/agenttest` and `internal/machine/routes/routestest` serve concrete types alone. The kits of the doubles add packages such as `config/configtest` and `replica/replicatest`.
- The kits change their API, so an application that applies a kit changes its tests.
- A property check runs 100 cases, with a fresh subject for each case.
- The callables of dokimasia/dokimi#20, #21 and #22 and of dokimasia/dokimi-go#8 keep a hand-written ceiling until dokimi releases each request.
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
| A budget of a sequence of calls | <https://github.com/dokimasia/dokimi/issues/20> |
| A ceiling that grows with the size of an input | <https://github.com/dokimasia/dokimi/issues/21> |
| A budget that counts the escape of a request | <https://github.com/dokimasia/dokimi/issues/22> |
| A factory that stops a subject with goroutines | <https://github.com/dokimasia/dokimi-go/issues/8> |
| ergon, a generate step of more than one command | <https://github.com/dokimasia/ergon/issues/53> |
| core's suite of fenced writers | <https://github.com/thesm-os/core/tree/main/coretest/epochtest> |
| The conformance kits | ADR-0001 |
| The enumerations | ADR-0006, ADR-0012 |
| The allocations of constraints and goals | ADR-0015 |
| The API of `storetest` | RFC-0007, Laws and conformance |
| The testing, performance and interfaces standards | `docs/standards/` |

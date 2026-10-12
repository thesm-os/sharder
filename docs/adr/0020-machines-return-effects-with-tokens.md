---
adr: 0020
title: Machines take events and return effects through a closed vocabulary with tokens
status: Accepted
date: 2026-10-11
supersedes: none
superseded-by: none
rfc: RFC-0001, RFC-0006, RFC-0007, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0020: Machines take events and return effects through a closed vocabulary with tokens

## Status

Accepted

## Context

The root of each namespace, the controller of each cell, each agent and each router is a state machine. A simulation runs every machine of a namespace on one goroutine, and a seed chooses the order of the events that are due at one time. In production, a runtime performs the same calls on goroutines. Both run the same machines, so the events and the effects between a machine and its runtime form one contract. No document of the design defines that contract.

The contract depends on these facts of the design and of the finished packages:

- A machine calls methods of store.Store, membership.Membership, protocol.AgentClient, protocol.ControllerClient, fence.Acquirer and placement.Placer, and the AddShard and DropShard of the handler of the application. The support matrix fixes the set of calls.
- A lease deadline is a timer of the monotonic clock. The production clock of core returns the wall time of a hybrid logical clock, and that wall time stops while the system clock steps back. A deadline between two readings of the clock outlasts its duration by the size of the step.
- A server and the application call a machine and wait for its reply. Some replies wait for a commit, such as the commit of a deferral before the reply to a disruption.
- BenchmarkPlanner measured one pass over a cell of 1,000,000 shards at 0.27 to 0.41 s, and placement.Validate builds a second State of the same size. A pass inside a step stops the steps of the machine for that time.
- The acquisition of a scope, the state of a scope and the transition engine each serve more than one machine.

## Decision

We will connect each machine to its runtime through one Step method and a closed set of events and effects in `internal/machine`, with a token for each effect and each inbound call, because the production runtime and the simulation then perform the same effects with the same results.

The rules of the contract:

- A machine takes one event in `Step` and appends its effects to a slice. It does not start a goroutine or perform I/O. It reads its clock only for timestamps and for the windows of budgets.
- An effect is one call of an interface of a finished package, a timer, a planning pass or a reply. Each effect has a token, and the events of its result have the same token. The set is closed for the support matrix, so a machine of the matrix does not add an event or an effect.
- A timer is an effect with a duration. The runtime passes `Elapsed` when the duration has passed. A timer cannot be stopped, so the machine ignores an `Elapsed` that it no longer waits for.
- An inbound call is an event with a reply token. The servers call `Do`, `Report`, `Submit`, `Disrupt` and `Done`, the application calls `ReportFault`, `Recovered`, `Close` and `Refresh`, and the constructor of a component calls `Started`. The machine replies with a `Reply` once the effects that the reply depends on have completed. When the caller stops waiting, the runtime replies to the caller itself and passes `Cancelled`.
- A planning pass is an effect. The machine passes a copy of its assignment, and the runtime runs `Place` and then `Validate` on goroutines of its own.
- The parts of a machine are types with methods, not machines. Each part takes tokens from the counter of its machine, and the machine passes each result to the part that appended its effect.
- Telemetry and logs are not effects. A machine reports and logs during `Step`.
- Other goroutines read values that the machine publishes through atomic pointers, such as the fencing tokens of an agent and the routes of a router.
- The runtime can perform the effects of one step in any order and concurrently. A machine appends an effect that depends on another effect after the result of the other effect has arrived.
- A call to another process or to the application has a `Timeout` that the machine sets. The calls of the store and of the membership have none.

The vocabulary:

| Effect | Call of the runtime | Result |
|---|---|---|
| `Head` | `store.Reader.Head` | `HeadRead` |
| `Read` | `store.Reader.Read`, for one page | `PageRead`, with the token of the next page |
| `Watch` | `store.Reader.Watch` | One `Watched` per item |
| `Commit` | `store.Writer.Commit` | `Committed` |
| `Outcome` | `store.Store.Outcome` | `OutcomeRead` |
| `Campaign` | `store.Store.Campaign` | `Campaigned`, and `TermEnded` when a granted term ends |
| `Resign` | `store.Leadership.Resign` for the term of a `Campaign` | The `TermEnded` of the `Campaign` |
| `Join` | `membership.Membership.Join` | `Joined`, and `SessionEnded` when the session ends |
| `Leave` | `membership.Session.Leave` for the session of a `Join` | The `SessionEnded` of the `Join` |
| `WatchViews` | `membership.Membership.Watch` | One `Viewed` per item |
| `Unwatch` | The end of a `Watch` or a `WatchViews` | None |
| `Callback` | `protocol.AgentClient.Do` | `Called` |
| `SendReport` | `protocol.ControllerClient.Report` | `Reported` |
| `SendDisrupt` | `protocol.ControllerClient.Disrupt` | `Decided` |
| `SendDone` | `protocol.ControllerClient.Done` | `Ended` |
| `Handle` | `AddShard` or `DropShard` of the handler | `Handled` |
| `Acquire` | `fence.Acquirer.Acquire` | `Acquired` |
| `Plan` | `placement.Placer.Place`, then `placement.Validate` | `Planned` |
| `Timer` | A timer of the clock | `Elapsed` |
| `Reply` | The reply to an inbound call | None |

A runtime passes `Started` first, and `Stopped` when the context of the run ends. `Reply` is generic over the value of the call. A call that returns only an error gets a `Reply[struct{}]`. `Report` gets a `protocol.Ack`, `Submit` gets a `protocol.Receipt`, and `Disrupt` gets a `budget.Decision`.

## Alternatives Considered

### The Ready and Advance of etcd's raft library

raft returns its outputs in batches through `Ready`, and its rules order the outputs. A caller sends the messages of a batch only after the latest state and the entries of every earlier batch have persisted. The caller then calls `Advance` for the next batch.

Rejected because an effect that depends on another effect already waits for the result of the other effect, so a batch adds a second mechanism of order.

### The next deadline of quinn-proto

The `Connection` of quinn-proto returns its next deadline from `poll_timeout` as an instant, and the runtime calls `handle_timeout` with the current instant.

Rejected because a lease deadline is a timer of the monotonic clock. An instant of the clock of a machine is a reading of the wall time of the hybrid logical clock.

### Goroutines under testing/synctest

The components run as goroutines, and a test fakes their time with `testing/synctest`.

Rejected because `testing/synctest` has no way to choose the order of runnable goroutines, so a seed cannot select an interleaving and reproduce it.

## Consequences

**Positive:**

- The simulation and the production runtime perform the same effects by the same rules, so a simulation tests the logic that runs in production.
- `internal/machine` is complete before its readers exist, and the machines of the support matrix do not add an event or an effect.
- A runtime is one type switch over a closed set of effects, and each effect corresponds to one method that the runtime calls.
- A machine sets every timeout of a call to another process or to the application, so a simulation applies the same timeouts.

**Negative:**

- An `Event` or an `Effect` that escapes costs one allocation for its value, except a `Stopped`. `TestMachineAllocs` measures one allocation for a `Commit` in the slice of a step.
- A machine keeps the token of every effect that it waits for. It ignores the late events of a token that it no longer waits for, such as an `Elapsed` of a replaced timer or a `Watched` after an `Unwatch`.
- A timer cannot be stopped, so a runtime keeps each timer until its duration has passed.
- A planning pass reads a copy of the assignment of the cell. The cost of the copy at the bound of a cell has not been measured.
- The compiler checks that the value of a `Reply` has one of the four result types, and does not check that the type matches the call. A runtime matches the two.
- The vocabulary has no event or effect for delegated ownership, the lookup source or the gRPC transport, which are outside the support matrix. Adding one changes `internal/machine` and every runtime.

**Neutral:**

- Telemetry and logs are not effects, so a simulation does not order them.

## References

| What | Where |
|---|---|
| etcd-io/raft, the README: `Ready`, `Advance` and the order of a batch | <https://github.com/etcd-io/raft/blob/main/README.md> |
| quinn-proto, `Connection`: `poll_timeout` and `handle_timeout` | <https://github.com/quinn-rs/quinn/blob/main/quinn-proto/src/connection/mod.rs> |
| Go, the package `testing/synctest` | <https://pkg.go.dev/testing/synctest> |
| One pass of the planner over a cell of 1,000,000 shards: the cells `exclusive-worker` and `failed-node` of `BenchmarkPlanner`, measured on 2026-10-11 | `placement/planner/planner_test.go` |
| The allocation of an effect in the slice of a step | `internal/machine/machine_test.go`, `TestMachineAllocs` |

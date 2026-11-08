---
adr: 0048
title: dokimi generates the simulation of the machines of a namespace
status: Accepted
date: 2026-11-07
supersedes: RFC-0001 in part
superseded-by: none
rfc: RFC-0017
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0048: dokimi generates the simulation of the machines of a namespace

## Status

Accepted

## Context

Milestone 0001 accepts the first release on 40 failure traces in a deterministic simulator. RFC-0001 designs that simulator as package `sim`, with a scheduler of its own. ADR-0020 has since made each component a sans-IO machine. The specs of the four machines and `internal/driver` contain five runtimes of those machines, and each of them runs one machine.

The generator `sim` of dokimi writes the simulation of a system of sans-IO machines from directives on their declarations. Its module `go.dokimi.dev/sim` runs every machine of a case on the goroutine of the case. It strikes the faults of processes, messages, clocks and dependencies, and checks five rules of a runtime on every step. [RFC-0017](../rfc/0017-simulation-that-dokimi-generates.md) designs its use. sim v0.10.1 and core v0.42.0 provide the mechanisms of fourteen requests of sharder:

- dokimasia/dokimi#63 runs the machines of one process on a host that a crash, a pause and a restart strike as a whole.
- dokimasia/dokimi#64 gives the store the time of the run.
- dokimasia/dokimi#65 strikes the faults of a dependency at an effect that a function performs.
- dokimasia/dokimi#66 lets the system take its inputs from the case.
- dokimasia/dokimi#69 ends a granted term and a live session at an outage of their dependency, and cancels what the function of a wait left in the dependency when the world drops or ends the wait.
- dokimasia/dokimi#70 marks an effect that ends the wait of an earlier effect, so the rule `fresh-tokens` does not count its token as a reuse.
- dokimasia/dokimi#71 takes the effects of a machine without the types of test files, so a test type that embeds an effect does not stop the generation.
- dokimasia/dokimi#72 ends the other machines of a host at the error of one of them, as a process of sharder ends. A restart then starts the host again.
- dokimasia/dokimi#73 lets a simulation set the steps of its cases.
- dokimasia/dokimi#75 gives the clock of a host a monotonic time, which the jumps of the clock do not move.
- dokimasia/dokimi#76 keeps a delivery and the advance of time in every case, so the cells serve while the faults of a case strike.
- dokimasia/dokimi#77 runs the final phase of a case until the bound of its convergence has passed.
- dokimasia/dokimi#78 applies the launch and the stop of the outcome of an effect at a loss after the effect, as a host applies an add or a drop whose reply is lost.
- thesm-os/core#130 gives core a clock over a physical clock of the caller, so the replay of a case matches the case.

The design also needs a mechanism that sharder requests from dokimi: dokimasia/dokimi#80 starts a host again once it exits in the final phase of a case, as a supervisor starts a process that has exited.

## Decision

We will simulate the machines of a namespace with the generator `sim` of dokimi and with `go.dokimi.dev/sim`, as RFC-0017 designs it. Every choice of a run is then a choice of a property case, so prop shrinks each failing run to its smallest case and replays it.

- `machine.Machine` takes the directive `sim` with the steps of a case, its token types take `token` and `token reply`, `machine.Reply` takes `reply`, and each of the 21 effect types takes `perform`. The directive of an effect of the store, the membership or the application also sets the interface of its dependency and the event of its failure. `Unwatch`, `Resign` and `Leave` name the effects whose waits they end, and `Campaign` and `Join` name the end event of their waits.
- The methods of those interfaces declare the errors of a lost call with the directive `fault`: `version.ErrOutcomeUnknown` for the store and the membership, and `context.DeadlineExceeded` for the application.
- The simulation is in `internal/machine/machinetest`, with the system, the performers, the actions, the reference application, the invariants, the convergence, the scenarios and the traces of sharder.
- The specs of the simulation set `maxDriftRate` of the lease to 0.5, and each case chooses the mode of a loss of the lease of each namespace.
- `store/memory` gains a manual mode for a runtime on one goroutine.
- The machines of each host read the time through `hlc.NewOver` of core over the clock of the host.
- The proof of the invariant of the reported violations runs a scenario of a spec edit, because its random cases seldom reach a pass that keeps a replica against a new constraint.
- The convergence of a case in which a recreation of a namespace stops the agents of the old incarnation during the final phase waits for a release of dokimasia/dokimi#80.
- The spec of the traces skips in short mode, and check-go runs `go test` with `-short`. A job of the nightly workflow runs the spec through the nightly step of ergon that dokimasia/ergon#78 requests.

This decision supersedes these parts of RFC-0001:

| Record | Part |
|---|---|
| RFC-0001 | The row of package `sim` in the packages of the root module: "The deterministic simulator" |
| RFC-0001 | Verification: "The scheduler keeps one queue of events", the `rand/pcg` source of a run, "Faults enter at three boundaries", and "A failing run prints its seed and its trace" |
| RFC-0001 | The API of package `sim`: `Faults`, `Scenario`, `Run` and `CrashPoints` |

## Alternatives Considered

### A simulator that sharder writes

`machinetest` contains a runtime of the machines that sharder writes, as RFC-0001 designs it.

Rejected because the runtime does not depend on sharder, and the repository already repeats the rules of a runtime in five places.

### One machine for each process of the world

Each machine of sharder is a process of the world, as `go.dokimi.dev/sim` v0.5.0 runs it.

Rejected because a crash then ends the agent of a root while its cell machines keep running. A deployment of sharder never produces that state.

## Consequences

**Positive:**

- A failing run shrinks to its smallest case, and prop stores that case as a regression case.
- The world checks the rules of a runtime on every step of every run.

**Negative:**

- The convergence of a case whose recreation of a namespace takes effect in its final phase waits for a release of dokimasia/dokimi#80.
- The traces run in the nightly workflow alone, so a defect that only a trace finds fails the night after its push. Their job waits for a release of dokimasia/ergon#78.
- The doubles of the store, the membership, the fence and the handler gain a helper for each error of a lost call.
- `store/memory` gains two types, an option and ten methods.
- The specs of the simulation run with a `maxDriftRate` of 0.5, and the default specs of the presets have 0.01.

## References

| What | Where |
|---|---|
| RFC-0017, the simulation that dokimi generates | [0017-simulation-that-dokimi-generates.md](../rfc/0017-simulation-that-dokimi-generates.md) |
| The simulation of sans-IO machines in dokimi | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0025-a-simulation-of-sans-io-machines.md> |
| The components and the hosts of a simulation in dokimi | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0036-components-that-run-goroutines.md> |
| The requests of the mechanisms of sim v0.10.1 | <https://github.com/dokimasia/dokimi/issues/63>, <https://github.com/dokimasia/dokimi/issues/64>, <https://github.com/dokimasia/dokimi/issues/65>, <https://github.com/dokimasia/dokimi/issues/66>, <https://github.com/dokimasia/dokimi/issues/69>, <https://github.com/dokimasia/dokimi/issues/70>, <https://github.com/dokimasia/dokimi/issues/71>, <https://github.com/dokimasia/dokimi/issues/72>, <https://github.com/dokimasia/dokimi/issues/73>, <https://github.com/dokimasia/dokimi/issues/75>, <https://github.com/dokimasia/dokimi/issues/76>, <https://github.com/dokimasia/dokimi/issues/77> and <https://github.com/dokimasia/dokimi/issues/78> |
| The request of the clock of core v0.42.0 | <https://github.com/thesm-os/core/issues/130> |
| The request of a final phase that starts a host again once it exits | <https://github.com/dokimasia/dokimi/issues/80> |
| The request of a nightly step of ergon that runs the long tests | <https://github.com/dokimasia/ergon/issues/78> |

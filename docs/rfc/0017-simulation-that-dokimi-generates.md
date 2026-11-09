---
rfc: 0017
title: A deterministic simulation of a namespace that dokimi generates
author: Roy Klopper
status: Accepted
created: 2026-10-28
updated: 2026-11-09
discussion: none
supersedes: RFC-0001 in part
superseded-by: RFC-0018 in part, ADR-0050 in part, ADR-0051 in part
produces-adr: ADR-0048
research: none
related: RFC-0001, RFC-0004, RFC-0006, RFC-0007, RFC-0008, RFC-0014, RFC-0015, RFC-0016
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# RFC-0017: A deterministic simulation of a namespace that dokimi generates

## Summary

Milestone 0001 accepts the first release on 40 failure traces in a deterministic simulator. CI runs each trace over every supported preset. RFC-0001 designs the simulator as package `sim`, with a scheduler of its own and a `rand/pcg` seed that orders the events of a run. ADR-0020 has since made each component a sans-IO machine.

We propose that the generator `sim` of dokimi writes the simulation of the machines of a namespace from directives on `internal/machine`. The module `go.dokimi.dev/sim` runs it: its world performs the effects, injects the faults and checks the rules of a runtime. The simulation is in `internal/machine/machinetest`, the package that the directive `out` of `machine.Machine` names. sharder writes the parts that need its own logic there:

- The system of hosts, with the machines of each host, the store and a reference application over a fenced store.
- The performers of the 21 effects.
- The actions of operators, applications and the environment.
- The safety properties and the oracles of the traces, each with a planted defect.
- The predicate of convergence and its bound.
- The scenarios of the crash points, and a simulation for each trace.

Each run of the simulation is a stateful machine of assert, so prop shrinks a failing run to its smallest case and stores the case as a regression case. `store/memory` gains a manual mode for a runtime on one goroutine. sim v0.10.2 provides these mechanisms of a world:

- The machines of a process run on one host, and a shared dependency reads the time of the run.
- The clock of a host has a monotonic time, which the jumps and the strobe of the clock do not move.
- The faults of a dependency strike an effect that a function performs, and the system takes its inputs from its case.
- An effect, an outage and the stop of a process end a wait, and the world calls the cancel of the outcome of the wait.
- The error of a machine ends its host, and a restart starts the host again.
- A simulation sets the mean and the most steps of the active phase of a case. Each case keeps a delivery and the advance of time.
- The final phase runs until the bound of the convergence has passed. A performer makes the world poll a wait at a time on the clock of its host.
- The final phase starts a host that exits again, as a supervisor starts a process that has exited. The first restart is at once, and the delay of each later one doubles from 10 s up to 5 minutes.
- A loss after an effect applies the launch, the stop and the message of its outcome, and loses the result alone.

Core's `hlc.NewOver` runs a hybrid logical clock over a physical clock that the caller supplies.

This RFC supersedes the simulator of RFC-0001 and amends the testing standard.

## Motivation

### The traces of milestone 0001

The section "Failure traces in the simulator" of milestone 0001 lists 40 traces. CI runs each trace over every supported preset at `GOMAXPROCS` 1 and 4, and fails when the two traces differ. After every event the simulator checks a predicate of each safety property of RFC-0001. It checks convergence within a bound that it derives from the spec, and counts the bound from the end of the last fault. The crash points of a scenario crash the process of each event before and after the event.

### The runtimes in the repository

| Part | Where | What it runs |
|---|---|---|
| A runtime of the root machine | `internal/machine/root/helpers_test.go`, 429 lines | One root machine against a memory store. It grants each campaign at once and passes each view by hand |
| A runtime of the cell machine | `internal/machine/cell/helpers_test.go`, 516 lines | One cell machine. It performs the pending effects oldest first, and polls each watch with a context that has ended |
| A runtime of the agent | `internal/machine/agent/helpers_test.go`, 816 lines | One agent machine |
| A runtime of the router machine | `internal/machine/routes/helpers_test.go`, 652 lines | One router machine |
| The production runtime | `internal/driver`, 1,150 lines | One machine, with its effects on goroutines under core's `task` |
| A property of two controller processes | `controller/controller_test.go` | The drivers of two processes inside `testing/synctest`, with stops, outages of the store, lost replies of an add, spec proposals and disruptions |

Each harness runs one machine. The property of `controller` runs the drivers of a namespace on goroutines, in an order that the Go scheduler chooses. The traces need every machine of a namespace, in an order that the case chooses.

### Changes since RFC-0001

- ADR-0020 makes each machine return effects with tokens. A runtime performs 21 kinds of effects and routes each result by its token. RFC-0001 states that a machine returns "the calls it makes".
- ADR-0032 makes a controller process host each cell through an agent of the root. The add of a cell starts a cell machine, and its reply waits for the reply of that machine to `Started`.
- ADR-0013 moves `Network` and `Local` into `protocol/local`. They call a server at once and do not inject faults, so the faults of messages belong to the simulation.
- `store/memory` blocks in `Campaign`, `Watch` and `Membership.Watch`, and starts a goroutine that applies the events of the leases. It renews each session on its own clock while the context of the join has not ended, so a paused process keeps its sessions.
- The API of RFC-0001 lacks the inputs of most traces: the calls of an operator, two namespaces, the recreation of a namespace, a compaction, a spec edit, or the renewals of one node.
- The testing standard requires `testing/synctest` for a test that advances time. ADR-0020 rejects `synctest` for the simulation because it cannot choose the order of runnable goroutines.
- dokimi has a generator `sim` and a runtime `go.dokimi.dev/sim` for a system of sans-IO machines on hosts (dokimi RFC-0025, RFC-0036 and RFC-0037). A world of the runtime runs every machine on the goroutine of a case, and every choice of a run is a choice of the case.

## Detailed design

### Components

| Component | Where | Written by | Responsibility |
|---|---|---|---|
| Directives | `internal/machine` | sharder | The machine interface, its tokens, its lifecycle events, its reply, and the performer of each effect |
| The world and the run | `go.dokimi.dev/sim` | dokimi | The processes and their clocks, the queue of events, the routing of each result by its token, the faults of processes, messages, clocks and dependencies, the strategy of scheduling, the five rules of a runtime, the log and its checksum, the crash points and the falsification |
| The simulation | `internal/machine/machinetest/machine_sim.go`, generated | dokimi | The anatomy of `machine.Machine`, the performer over the 21 effects, and `MachineSimulation` with the actions, the invariants, the convergence and the scenarios of sharder |
| The tests | `internal/machine/machinetest/machine_sim_test.go`, generated | dokimi | `TestSimulation`, `TestSimulationFalsify` and `FuzzSimulation` |
| System | `internal/machine/machinetest` | sharder | The hosts of a namespace and their machines, the store and the reference application |
| Performers | `internal/machine/machinetest` | sharder | One function for each effect |
| Actions | `internal/machine/machinetest` | sharder | The calls of operators and applications, the changes of membership and of the spec, and the recreation of a namespace |
| Reference application | `internal/machine/machinetest` | sharder | A handler, a fence and a revoker over a fenced store that records each write with its tenure and each callback key |
| Invariants | `internal/machine/machinetest` | sharder | The six safety properties and the oracles of the traces, each with a planted defect |
| Convergence | `internal/machine/machinetest` | sharder | The predicate of convergence and its bound |
| Scenarios | `internal/machine/machinetest` | sharder | The runs that the crash points cover |
| Traces | `internal/machine/machinetest` | sharder | The simulation of each trace of milestone 0001 over each preset, and its test |
| Manual store | `store/memory` | sharder | A mode of the store for a runtime on one goroutine |

### The directives

| Directive | On | States |
|---|---|---|
| `//dokimi:sim start=Started stop=Stopped cancel=Cancelled strategy=pct:3`, with `mean-steps`, `max-steps`, `tolerate` and `require` | `machine.Machine` | The step method, the error method, the closed sets of events and effects, the events of the lifecycle, the steps of the active phase of a case, the faults in force at once and the share of the cases in which each fault strikes |
| `//dokimi:token` | `machine.Token` | That a field of this type in an effect links the effect to the event of its result |
| `//dokimi:token reply` | `machine.ReplyToken` | That a field of this type in an event links an inbound call to the reply to it |
| `//dokimi:reply` | `machine.Reply` | The fields `Value`, `Err` and `To`. The union of `Result` gives each of its four types a case of the anatomy |
| `//dokimi:perform func=<function>`, with `dependency=<interface> result=<event>` for an effect whose dependency the world strikes, `end=<event>` for a wait that the end event of its dependency ends after its first event, and `timeout=Timeout` for an effect with a timeout | Each of the 21 effect types | The function of `machinetest` that performs the effect, the interface of its dependency, the event of its failure, the end event of its wait, and the field that bounds its call |
| `//dokimi:perform ends=[<effect>, ...]`, with `func=<function>` for an effect that calls its dependency | `Unwatch`, `Resign` and `Leave` | The effects whose waits the effect ends, and the function that performs it |
| `//dokimi:fault errors=[...]` | The methods of the interfaces of the dependencies | The errors with which the world fails a lost effect of the dependency |
| `//dokimi:system` | A function of `machinetest` | The hosts of the case and their dependencies |
| `//dokimi:action` | Each change of `machinetest` | A change outside the machines, with inputs that the case chooses |
| `//dokimi:action call` | Each call of `machinetest` | An inbound call of an operator or of the application. The history records it |
| `//dokimi:action call final` | A call of `machinetest` | A call that the run makes once the faults have ended |
| `//dokimi:invariant defect=<function>` | Each predicate of `machinetest` | A property that the run checks after every step, and the system of its planted defect |
| `//dokimi:converges within=<function>` | The predicate of convergence | The check once the faults end, and its bound |
| `//dokimi:scenario` | Each scenario of `machinetest` | A run whose events the crash points cover |

The params `mean-steps` and `max-steps` of `sim` give the active phase of a case a mean of 1,000 steps and at most 2,000. sim gives a case 82 choices for each step of `max-steps`, so a case of 2,000 steps has room for 164,000 choices. The param `tolerate` names each fault of the world with a bound of 1: `crash`, `restart`, `pause`, `partition`, `drop`, `delay`, `duplicate`, `clock-jump`, `clock-strobe`, `lose-before`, `lose-after`, `outage` and `slow`. The param `require` sets the share of the cases that each fault strikes: 0.23 for `crash`, 0.24 for `partition`, 0.24 for `outage` and 0.17 for `lose-after`. Each share is about half of the share that 1,000 cases of the simulation measured at these steps: 46.3% for `crash`, 48.8% for `partition`, 48.4% for `outage` and 35.9% for `lose-after`. The swarm of a case keeps each kind of fault in about half of the cases. A loss after an effect strikes less often because the world arms it on one of five dependencies, and a loss on the fence or the revoker waits for a move of a shard. The directive `reply` has no param `unknown`, because the simulation has no spec of the calls. `machine.Machine` keeps its directives `stub` and `out path=machinetest/`.

### The world

The world of `go.dokimi.dev/sim` runs the machines of a case on the goroutine of the case:

- The case chooses the drift of the clock of each host from −500 to +1,000 per mille. The machines of a host share its clock. Each machine reads it through a hybrid logical clock of core that `hlc.NewOver` runs over the clock of the host. Its wall time is `sim.Clock.Now`, and its `Elapsed` is the monotonic time of the host, `sim.Clock.Monotonic`. Each incarnation of a host builds a new hybrid logical clock, as a process of sharder builds its clock when it starts. The world hashes each effect with every pointer followed, and the effect of a planning pass contains the clock of its controller. A clock that reads the system time, as `hlc.NewWithSource` does for `Elapsed`, makes each replay of a case diverge.
- A jump and a strobe move the time that a clock returns, and they do not move its timers or its monotonic time. A machine measures every wait with a `Timer`, so these faults change its timestamps alone. The transition engine moves the window of its retry budget by `Elapsed`, so the faults of a clock do not move that window either.
- The store reads the time of the run through `hlc.NewOver`. Its physical clock returns `World.Now` as the wall time and the time of the run since `sim.Start` as `Elapsed`. A pause, a jump and a strobe of a host do not change it.
- A physical clock of a world reports a call of `NewTimer` as a fault of the case. A machine measures each wait with a `Timer` effect, and the store of a world runs in its manual mode, so no code of a world waits on the channel of a timer. The clock reports the fault through a function, because the world hashes a function by whether it is nil, and the hash of a planning pass then does not read the case.
- The strategy `pct:3` chooses the process that steps next. The case chooses the priority of each process and the change points.
- The world passes the result of an effect in a later step, and routes it to its machine by its token. It drops an event of an earlier epoch of a process before its delivery.
- The rules of a runtime check every step of every run: `replies-once`, `replies-before-stop`, `quiet-after-stop`, `quiet-after-error` and `fresh-tokens`. Together they check the rules that `internal/machine/doc.go` states for a machine.
- The world records each step in its log with the time of the clock and the faults in force, and hashes the log into a checksum.

The specs of the simulation set `maxDriftRate` of the lease to 0.5. A node acts as a writer for `TTL·(1 − maxDriftRate)` on its clock after an accepted renewal. On the slowest clock of a world, at half the rate of the run, that validity ends at the TTL of the store. So for every drift that the case chooses, the deadline of each node passes before the store expires its lease. A faster clock ends the validity sooner.

### Effects and their performers

The faults of a dependency are `outage`, `lose-before`, `lose-after` and `slow`. The directive `perform` of an effect names the interface of its dependency, and the world strikes the faults of that dependency at the effect:

- `store.Store` is the dependency of `Head`, `Read`, `Watch`, `Commit`, `Outcome` and `Campaign`.
- `membership.Membership` is the dependency of `Join` and `WatchViews`.
- `fence.Acquirer` is the dependency of `Acquire`. `Handler` and `Revoker` of `internal/driver` are the dependencies of `Handle` and `Revoke`. Package `machine` cannot import `internal/driver`, so their directives name them by the import path of the package.

The world fails a lost effect with an error that the directive `fault` declares on the methods of the interface. An error without a class, such as `sim.ErrLost`, stops the machine that takes it, because no machine retries it:

- The methods of `store.Store` and `membership.Membership` declare `version.ErrOutcomeUnknown`, because a commit that a fault loses after it applies has an unknown outcome. The machines retry a read or a watch that fails with it.
- The methods of the three interfaces of the application declare `context.DeadlineExceeded`. The world draws one error for a loss before and after the call. A loss after the call must leave its outcome unknown, as an attempt past its deadline does, because a handler that has served returns no error.

`Unwatch`, `Resign` and `Leave` end the wait of an earlier effect, and the param `ends` of their directives names that effect. A runtime ends the wait whatever the call returns, so these three effects take no fault of a dependency. The world performs the function of such an effect and then calls the `Cancel` of the outcome of the wait. The rule `fresh-tokens` judges only the effects that start a call or a wait, so it does not count the tokens of these three effects as reuses.

The faults of the store and the membership strike `Campaign` and `Join`. Their directives set the end event of their waits with the param `end`, so an outage ends a granted term with `TermEnded` and a live session with `SessionEnded`. An outage before the first event ends the wait with the event of its failure.

| Effect | Performer | Result | Faults of the run |
|---|---|---|---|
| `Head` | `memory.Store.Head` | `HeadRead` | The faults of the store |
| `Read` | `memory.Store.Read`, for one page | `PageRead` | The faults of the store, and the action Compact |
| `Watch` | A wait over `memory.Store.Watch` under a context that has ended, from the generation after the last event of the watch | One `Watched` per generation | The faults of the store, the action Compact, and an end with `store.ErrCompacted` as a full buffer ends a watch |
| `Unwatch` | The cancel of the wait of its token, which the world calls | None | None |
| `Commit` | `memory.Store.Writer(...).Commit` | `Committed` | The faults of the store |
| `Outcome` | `memory.Store.Outcome` | `OutcomeRead` | The faults of the store |
| `Campaign` | A wait over `memory.Store.Candidate` | `Campaigned`, then `TermEnded` | The faults of the store, and the end of the terms of a host that crashes |
| `Resign` | `store.Leadership.Resign` of a term that the store granted, then the cancel of the wait of the campaign | The `TermEnded` of a granted term, and a `Campaigned` with an error before the grant | None |
| `Join` | A wait over `memory.Membership.Join`, with a renewal through `Membership.Renew` at each `Renew` of the lease on the clock of the host | `Joined`, then `SessionEnded` | The faults of the membership, the action that loses a renewal, and a pause of the host |
| `Leave` | `membership.Session.Leave`, then the cancel of the wait of the join | `Left`, then the `SessionEnded` of the join | None |
| `WatchViews` | A wait over `memory.Membership.Views` | One `Viewed` per view | The faults of the membership |
| `Callback` | A message to the agent of the node and the scope of the request | `Called` | `drop`, `delay`, `duplicate`, `partition`, and the action Forge |
| `SendReport` | A message to the cell machine at the address of the record of the cell | `Reported` | `drop`, `delay`, `duplicate` and `partition` |
| `SendDisrupt` | A message to the cell machine at that address | `Decided` | `drop`, `delay`, `duplicate` and `partition` |
| `SendDone` | A message to the cell machine at that address | `Ended` | `drop`, `delay`, `duplicate` and `partition` |
| `Handle` | At the agent of the root, the start or the stop of a cell machine on its host. At the agent of a cell, the handler of the reference application | `Handled` | The faults of `driver.Handler`: `slow` for a handler that returns after the timeout of its step, and a loss before or after the call with `context.DeadlineExceeded` |
| `Acquire` | The fence of the reference application | `Acquired` | The faults of `fence.Acquirer` |
| `Revoke` | The revoker of the reference application | `Revoked` | The faults of `driver.Revoker` |
| `Plan` | `placement.Placer.Place`, then `placement.Validate` | `Planned` | None |
| `Timer` | `World.After` on the clock of the host | `Elapsed` | The drift of the clock, and a pause of the host |
| `Reply` | The world, through the directive `reply` | None | The faults of the message of the call |

The performers of the waits `Campaign`, `Join`, `Watch` and `WatchViews` return `Next` and `Cancel`, and the performers of `Callback`, `SendReport`, `SendDisrupt` and `SendDone` return `Message`. `Commit` gets its writer from `store.Store.Writer`, `Read` reads its page from a cursor, and the handler effects call the application. The parameters of `store.Reader.Head` and `store.Store.Outcome` do not have the names of the fields of `Head` and `Outcome`. So no effect binds through `perform call=`, and each performer is a function of `machinetest` that follows these rules:

- A callback, a report, a disruption and a done are messages between hosts. Each performer checks the metadata of its call with `protocol.Metadata.Check`, as `protocol/local` does. A call whose check fails returns its result with the error of the check, and does not send a message.
- The target of a callback is the agent of the node and the scope of its request. The target of each other message is the cell machine of its scope at the address of the record of the cell, as ADR-0036 routes such a call.
- The result of a message to a crashed or a stopped process returns `protocol.ErrUnreachable` in place of `sim.ErrCrashed` and `sim.ErrRunEnded`, as a transport reports a process that does not reply. A message without a reply before its `Timeout` returns the error of its context, and the world passes `Cancelled` to its target.
- `Handle` at the agent of the root follows ADR-0032 on the host of the agent. An add starts the cell machine at the epoch of the request through `Outcome.Launch`, and returns once that machine replies to `Started`. A drop stops the cell machine through `Outcome.Stop`, and returns once the machine has stopped. A repeated add of a cell machine of the same cell and term that still runs waits for the reply of that machine to `Started`, as `controller` waits for the acquisition of a running controller.
- The host records the reply of a cell machine to `Started` itself, and does not pass the machine the cancel of that `Started`, as the driver of a controller process passes `Started` and waits for its reply. So the end of the wait of an add and a loss after its effect leave the start of the machine alone, and a later add finds the reply.
- `Plan` runs on the goroutine of the case, with a parallelism of 1.
- The `Next` of a wait reads the manual store without blocking: a candidacy, the end of a term or of a session, a change of a watched scope and the views of a scope. The `Next` of a join also renews its session at each `Renew` of its lease on the clock of its host. It ends the session with `Membership.Lose` once the validity of the lease after its last accepted renewal has passed on the monotonic time of that clock, as the membership client of a node does. A timer of `World.AfterFunc` at each renewal, and at the local deadline after a renewal that fails, makes the world poll the wait at those times without an event to a machine. The jumps and the strobe of the clock do not move that time. A paused host does not advance its clock, so it does not renew.
- The performer of `Campaign` joins the node of its host in a scope of the namespace that only the campaigns use, with the lease of the ownership of the namespace, and the candidacy ends with that session. The `Next` of the campaign renews the session as the `Next` of a join does, as the client of a store renews the lease of its election keys. A candidacy whose session ends before the grant ends with an error that wraps `version.ErrOutcomeUnknown`, the error that the directive `fault` of `Campaign` declares.
- The `Cancel` of a campaign ends its candidacy, and the `Cancel` of a join ends the renewals of its session. The `Cancel` of a watch of views closes its reader. The world calls the `Cancel` of a wait at a loss after the effect, at an outage, at an effect that ends the wait, and at the stop of the process. A crash does not call it, so the sessions and the terms of a crashed host end at their expiry.
- A performer of an effect of 80 bytes or more takes a pointer to the effect, as gocritic's `hugeParam` requires of a file that is no test file.

### The hosts of a namespace

The support matrix contains the topology `embedded`, in which every application process is also a controller process. Each such process is one host in the world, which `World.AddHost` adds. `Host.Spawn` adds each machine that runs for the whole run of the process. A host has a node, a clock, a source of randomness and the reference application. It runs these machines for each namespace of the case:

- The root machine.
- The agent of the root. It starts and stops the cell machines that the root places on the host.
- The cell machine of each hosted cell.
- The agent of the cell, with the handler, the fence and the revoker of the reference application.
- The router machine.

The system takes its inputs from the case:

- The case chooses from 1 to 5 hosts.
- It chooses 1 or 2 namespaces.
- It chooses the slot count of `fixed-slots` of each namespace from 1 to 16. The preset `leader-election` keeps the one shard of `singleton`.
- It chooses the mode of a loss of the lease of each namespace, `continue-fenced` or `stop`.
- It chooses the seed of `rand/seeded` of each host.
- It chooses the zone of the node of each host, `east` or `west`, as the label `zone`.

Each spec adds the hard constraint `labels` to the planner of its preset, with a rule that refuses each node of the zone `west`. A node of the zone `west` then takes no replica, and the action Change can invalidate an assignment, as the trace at line 84 of milestone 0001 needs.

Each host builds the config of each machine as `controller`, `replica` and `router` build it. The machines of a host read its clock, and each pass has a parallelism of 1. An incarnation of a host builds a `telemetrytest.Recorder` of core when it builds its clock. The machines of the incarnation report their instruments to that recorder, which the oracles read. The system keeps each machine that it builds in the state of its host. Each process of the host has that state as a dependency, so a predicate reads it through `sim.Dependency`.

A host has three faults:

- A crash ends every machine of the host at once, without `Stopped`. The world drops the results of the effects of the host. The sessions of the host end at their expiry, and its terms end at the expiry of the sessions of its campaigns. The data of the reference application remain.
- A restart starts the root machines, the agents and the routers of the host again, with the same node ID and new machines. Its agents join with a new node incarnation. A restart does not start a cell machine, because the root places each cell again.
- A pause stops the delivery of events to the host while the time of the run advances. The host does not renew its sessions during the pause, so a pause longer than the TTL ends them and the terms of the host. When the pause ends, the host takes the events that became due during the pause, in an order that the case chooses.

The error of a machine ends its host, because `controller.Controller.Run` and `embedded.Process` end at the error of a part. The world stops every other machine of the host, and the host exits. A restart starts an exited host again, as a supervisor starts a process that has exited, and the recovery of `Settle` restarts every exited host. A recreation of a namespace stops the agents of the old incarnation this way.

### The actions of sharder

| Action | Directive | Call or change | Lines of the traces in milestone 0001 |
|---|---|---|---|
| Submit | `action call` | `Submit` to a root by an operator or by another identity: a spec edit, a drain or an undrain | 72, 74, 80, 84, 85, 95 |
| Disrupt | `action call` | `Disrupt` of a planned disruption to a cell | 67, 68, 75, 76, 80, 81, 95 |
| Done | `action call` | `Done` of a reservation | 68, 80, 95 |
| Close | `action call` | `Close` of an agent with a deadline | 81 |
| Fault | `action call` | `ReportFault` and `Recovered` of the application | 70 |
| Refresh | `action call` | `Refresh` by a client of a router | 94 |
| Forge | `action call` | A callback of an earlier node incarnation, of a caller that is not the controller, or with a key that another payload used | 64, 65, 80, 89 |
| Read the routes | `action call final` | A `Refresh` of each router to the last generation of each cell | 78, 90 |
| Drain the first node | None | A `Submit` of an operator that drains the node of the first host from the cell of a namespace | 71 |
| Write | `action` | A write transaction of the writer of a tenure in the fenced store | 58, 63, 66 |
| Join | `action` | The start of a host with a new node | 71, 72 |
| Change | `action` | The restart of a host with other labels or another capacity | 84 |
| Recreate | `action` | The deletion of the scopes of a namespace, and its creation under the same name | 79, 88, 94 |
| Upgrade | `action` | A commit of a newer release that raises the schema of a scope, through `memory.Store.Upgrade` | 86 |
| Compact | `action` | A compaction of a scope of the store | 62, 87, 91, 92, 93 |
| Lose a renewal | `action` | A fault of `PointRenewApply` or `PointRenewAck` of the store, armed before the next renewal | 96 |

The calls of the actions under `action call` go through `World.Call`, so the history records each call and the world completes it with its reply. The history has no spec, so the run checks no linearization of the calls. The rules of a runtime check every reply. A spec edit of Submit selects the placement of the simulation, with the constraint of the zones. The trace at line 71 takes the action that drains the first node. `MachineSimulation` does not take it.

### The traces

Each trace of milestone 0001 is `MachineSimulation` with these changes:

- The system of one preset, with the inputs that the text of the trace states.
- The actions of the trace, which the table of the actions lists by the line of the trace.
- The faults that the text of the trace names in `Tolerate`, each with the bound of 1.
- In `Require`, the share of `MachineSimulation` for each of those faults that `require` names.
- The bound of the convergence of the inputs of the trace.

Only the trace at line 58 runs the scenarios and their crash points. The trace at line 82 is out, because `router.Do` is not a machine. Line 97 states the checks of the simulator, and every trace runs them: each invariant after every step, the convergence from the end of the last fault, and a pause that lasts into `Settle`. That leaves 38 traces. `machinetest.Traces` returns them, and `Trace.Simulation` returns the simulation of a trace over a preset.

| Line | Actions | Faults | Inputs of the system |
|---|---|---|---|
| 58 | Write | `crash`, `restart` | None |
| 59 | None | `crash`, `restart`, `lose-after` | None |
| 60 | None | `crash`, `restart` | None |
| 61 | None | `crash`, `restart`, `lose-before`, `lose-after` | None |
| 62 | Compact | `lose-before`, `lose-after` | None |
| 63 | Write | `lose-after` | None |
| 64 | Forge | `crash`, `restart`, `delay` | None |
| 65 | Forge | `lose-after` | None |
| 66 | Write | `pause` | None |
| 67 | Disrupt | `crash`, `restart` | None |
| 68 | Disrupt, Done | `crash`, `restart` | None |
| 69 | None | `crash`, `restart`, `pause`, `lose-after` | None |
| 70 | Fault | None | None |
| 71 | Join, Drain the first node | None | Bounds of one, and one host of the zone `east` with 16 shards of each namespace |
| 72 | Submit, Join | None | Bounds of one |
| 73 | None | `lose-after` | Bounds of one |
| 74 | Submit | None | A `maxSourceShards` of 1 |
| 75 | Disrupt | `crash` | None |
| 76 | Disrupt | None | Bounds of one |
| 77 | None | None | None |
| 78 | Read the routes | `delay`, `duplicate` | None |
| 79 | Recreate | `delay` | Two namespaces |
| 80 | Submit, Disrupt, Done, Forge | None | None |
| 81 | Close, Disrupt | None | None |
| 83 | None | `crash`, `pause`, `partition`, `outage` | None |
| 84 | Submit, Change | `crash`, `restart` | None |
| 85 | Submit | None | The first version of each spec without the constraint of the zones |
| 86 | Upgrade | None | None |
| 87 | Compact | `crash`, `lose-after` | None |
| 88 | Recreate | `delay` | None |
| 89 | Forge | `crash`, `delay` | None |
| 90 | Read the routes | `delay`, `lose-after` | None |
| 91 | Compact | `lose-before`, `lose-after` | None |
| 92 | Compact | `delay` | None |
| 93 | Compact | None | None |
| 94 | Recreate, Refresh | None | None |
| 95 | Submit, Disrupt, Done | `crash` | None |
| 96 | Lose a renewal | None | The mode `stop` |

The inputs of a trace replace the choices of its case:

- Bounds of one set `maxTransitions`, `maxNodeTransitions` and `nodeCallbacks` of each spec to 1, as the specs of the planted defect of the bounds have them.
- One host of the zone `east` with 16 shards starts the system with one host, and each namespace with the most slots. The first placement then puts every shard of a namespace on the node of that host. The action that drains the first node drains that node as an operator, and Join adds the nodes that take its shards.
- The first version of each spec has no constraint. The first spec edit of Submit then adds the constraint of the zones. The invariant of constraints checks each replica against the constraint of the spec version that planned its move.

The bound of the convergence of a trace counts the budget and the planner of its specs. Bounds of one make it 54m40s. A `maxSourceShards` of 1 lets a pass examine one shard as the source of a move. A recovery of 16 shards then counts 17 passes in place of two, and the bound is 24m40s.

The spec of `Traces` runs `sim.Test` on the simulation of each trace over each preset: 152 simulations. Without their convergence, one run of the 152 simulations took 992 s of CPU time and 4m20s on four cores. A simulation that passed took 1.7 to 20.8 s, with a median of 4.2 s. The job check-go took 10 to 24 minutes on its three systems in the last runs on main, and its limit is 30 minutes. Its step race also runs every test under the race detector. So the spec skips in short mode, as the Go project skips its long tests in all.bash and runs them on its longtest builders. check-go runs `go test` with `-short`. A job of the nightly workflow runs the spec without the flag. dokimasia/ergon#78 requests the nightly step of ergon that this job needs.

### The reference application

A fenced store keeps the data of the shards of each namespace:

- Each shard has a fence key with a watermark. `Acquire` raises the watermark to the epoch of the fencing token, and returns an error that wraps `epoch.ErrFenced` for an epoch below it. An acquisition waits until each write transaction that checked the fence before it has ended.
- A write transaction of the writer of a tenure checks the fence when it starts, and commits after a delay that the case chooses. A commit of a tenure below the watermark fails with `epoch.ErrFenced`.
- The store records each committed write with its tenure, and each callback key with the effect of its handler.

The handler starts and ends tenures with `AddShard` and `DropShard`. It records the key of each callback in the transaction of its effect, as milestone 0001 requires of each example. The revoker ends the writes of a tenure. The store runs core's `coretest/epochtest`, as every fenced writer does.

### Invariants

The run checks each invariant after every step. A predicate reads the store, the reference application, the log and the history of the world, and the values that the machines publish, such as the fencing tokens of an agent and the routes of a router. It takes the facts that a compaction discards from the log of the world, such as the epochs that each commit reserved.

| Property of RFC-0001 | The predicate checks | Planted defect |
|---|---|---|
| Epoch uniqueness | No commit reserves an epoch of a shard twice, and a commit reserves each epoch before a callback names it | A cell machine whose commits reserve the epoch of the placement again, through `machinetest.MachineStub` |
| Writer safety | Each tenure starts after an acquisition of the fence of its shard at its epoch, and no write of a tenure commits after a newer tenure of the shard has acquired the fence | An agent whose `Acquire` returns success without a call of the fence |
| Control safety | Each commit is in the namespace incarnation of its scope, names the generation that it replaces, and has an epoch at or above the watermark of its scope | A store writer that admits each commit at the generation and the watermark of its scope, and applies a retry of a commit again, through `storetest.WriterStub` |
| Role safety | The replicas of each role, with the started transitions that add a replica to the role, do not exceed the bound of the role | A cell machine whose commit of the new writer of a move keeps the old writer in the placement, as an add before the drop of the old writer does |
| Admission | Each admitted move and disruption keeps the unavailable replicas and the reservations within the budget | A cell machine that admits every disruption |
| Constraints | Each replica that a commit adds or moves satisfies every hard constraint of its spec version | A cell machine whose commit of a move places the replica on a node that a constraint refuses, through `machinetest.MachineStub` |

The oracles of the traces check what the six properties do not cover:

| Oracle | Lines of the traces in milestone 0001 | The predicate checks | Planted defect |
|---|---|---|---|
| Each callback key has one effect in the fenced store. A resent key returns the kept result without a second call of the handler | 58, 65, 91 | No key has a second effect, and an agent calls no handler with a key whose result it keeps | An agent that takes each call of the handler that succeeded as one that ran past its deadline |
| A callback of an earlier node incarnation, of a caller that is not the controller, or with a key that another payload used, does not call a handler | 64, 65, 80, 89 | The step at which an agent takes a callback of the action Forge has no effect other than a reply with an error | An agent of a cell and of the root that replies with success to a callback that it refuses |
| No abort path starts while the outcome of a commit is unknown | 62, 87 | A machine whose commit has an unknown outcome commits no other mutation of the term of that commit that starts or ends an abort path until it settles the commit. A root that takes a new term reads the outcome from the state of its scope | A cell machine that commits the abort path of each running transition of a commit whose outcome is unknown |
| The view of a controller and the routes of a router do not return to an older generation | 78, 90 | The sequence and the generation that a controller acks to a node incarnation do not fall, nor do the generation and the epoch of the routes of each shard of a router | A router that takes the first generation of a placement again after a newer one |
| No commit, callback, session or route of one namespace incarnation changes another incarnation | 79, 88, 94 | The transitions and the reservations of each commit, the request and the fencing token of each call of the handler, each session and each route have the incarnation of their scope | A cell machine whose commits have the transitions of an incarnation that no namespace has |
| Writers and routers serve during an outage as the outage table of RFC-0001 states | 83 | An agent ends a tenure only at a step that drops it, takes the result of its add, stops the agent, belongs to a Close, or under `stop` takes the end of its session. A router changes its routes only at a step that takes a page, a generation or a view, or that stops it | A router that stops at an error of the store or of the membership |
| A session under `stop` ends at the deadline of its last accepted renewal | 96 | The local deadline of each session follows its last accepted renewal, and an agent under `stop` has no fencing token from the step that takes the end of its session | A membership client that moves the deadline of a session at a renewal that failed |
| The transitions in flight remain within `maxTransitions` and `maxNodeTransitions`, and the callbacks in flight to a node within `nodeCallbacks` | 71, 72, 76 | The transitions of each cell and of each of its nodes, and the callbacks in flight from a controller to each incarnation of a node, remain within the budget of their namespace | Specs with bounds of one, and a cell machine that reads them without the bounds |
| A controller refuses a scope of a newer schema before its first commit | 86 | No commit applies to a scope whose header has a newer schema | A store whose header of a scope hides a newer schema |
| The instruments report the transitions in flight and the shards without a serving writer as separate counts | 73 | Once a cell machine has committed the last generation of its scope, its gauges count the transitions of the scope and the shards of its layout without a writer | A cell machine whose gauge of the unplaced shards also sets a series of the gauge of the transitions in flight |
| No tenure starts without a comparison that covers it, and a cleanup applies once under its key | 91, 92, 93 | An agent acquires the fence of a tenure only while the placement at its last applied generation lists the tenure, and the cleanups of a tenure take effect under one key | An agent that calls the handler with each cleanup again under a second key |
| A replica that a planning pass keeps against a constraint is reported as a violation of the pass | 84, 85 | Each planning pass of the controller of a cell reports as a violation each replica that its plan keeps on a node that the constraint of its spec version refuses | A runtime of the planning pass that drops the violations, over one host of the zone `west` with the most slots, specs whose first version has no constraint, and a planner that examines one source shard in a pass |

Each oracle is an invariant with a planted defect of the same form: a system with a double of one machine, of the reporter of a machine, of the store, of the membership client of a node or of the runtime of a planning pass that breaks the oracle. Each defect is a function with the signature of the system, and `TestSimulationFalsify` proves each invariant on it. A proof runs the random cases of the defect first. It then runs each scenario with the simplest choices of the system of the defect. The planted defect of the reported violations fails in the scenario of the spec edit. There the first pass under the constraint examines one of the 16 shards on the host of the zone `west` and keeps the other 15 against the constraint.

### Convergence

`Settle` ends every fault, restarts each crashed host, runs each final call, and runs the simulation until the bound has passed. The predicate checks six facts:

- Each transition has ended in `Done` or `Aborted`.
- One controller serves each cell, at the address that the record of the cell states.
- Each shard has a writer, or no node of the view of its cell that is not drained and that the constraint of its spec admits.
- The writer of each shard is live and on such a node, unless the view of its cell has none. Live means that the view has the node of the writer at the incarnation of its placement. A drain has then completed, and repair has moved each replica that a changed constraint or a changed node refuses.
- The agent of the node of each writer alone may have a tenure of its shard, at the epoch of its placement, and the agent of a live writer has it. A writer that is not live remains only on a shard that no node may take. It keeps its tenure while its process runs on under `ContinueFenced`, and a node that restarts restores no writer.
- The router of each host routes a key of each shard to the writer of its placement at its epoch while the view lists the node of the writer, because a route takes the address of each replica from the view. It has no route of a shard without a writer or whose writer has left the view.

The function under `within` derives the bound from the specs of the presets. A recovery adds the TTL of the lease, the longest backoff of each machine and of the moves of a shard, the periodic passes of the placer, and the longest move of a protocol for the cell and for each round of the moves of its shards. The recovery counts one pass for each `maxSourceShards` shards of the layout, and one more. A round moves as many shards as the bounds on the transitions in flight admit. The longest move adds the longest backoff of a step, the timeouts of the steps of the script up to a failed step and of the abort path of that step, and the lock delay of a drop. The bound doubles the longest recovery, because a timer on the slowest clock of a world takes twice its duration in the time of the run. It is 10m0s for the presets of the simulation. sim gives the final phase at most 1,000 steps for each second of the bound, 600,000 steps for 10m0s. The run counts the bound from the end of the last fault, so a pause that lasts into `Settle` does not shorten it. A run that does not converge within the bound fails with the statement of the convergence, apart from a violation of an invariant.

A world in which the action Upgrade has raised the schema of a scope has no settled state, because each host that runs a machine of the scope exits at the error of that machine. The predicate checks none of its facts in such a world.

### Crash points

The scenarios are the first move of a shard, the abort path after a lost reply of the add of the new writer, the settlement of the commit that starts the first move, whose reply is lost after it applies, and a spec edit that the controller of the cell takes in a planning pass. In the simplest case, the abort path of the first placement commits the placement without a writer, and the controller of the cell settles the lost commit by a retry with the same commit ID. The scenario of the spec edit submits the edit as an operator once the first move has ended. It ends once the controller of the cell has taken a pass under the edit, or once a crash of the root before its commit has ended the call. Each scenario runs over each preset. The crash points run a scenario once, and then twice for each event of that run: once with the process of the event crashed just before the event, and once just after it. That crash ends every machine of the host of the process. Each run checks every invariant and the rules of a runtime after every step. A crash of the controller of the cell at one of these events is a takeover, so the scenarios cover a takeover between the effect of a callback and the commit of its step, on the forward and on the abort path.

### Determinism

- The world runs every machine on the goroutine of the case, and the manual store does not start a goroutine of its own.
- Each pass runs with a parallelism of 1, so the planner evaluates its candidates on the goroutine of the case. RFC-0004 states that the plan does not depend on the parallelism.
- The manual store records the end of the sessions of a deleted scope, and of the sessions that the first commit of another incarnation ends, as one revision of the views. A view of each removal would follow the order of a Go map.
- The world passes the checksum of its log to `Case.Observe` after each step. `sim.Test` replays every failing case and every 16th passing case with the same choices. A replay whose checksum differs fails the test at the first step at which the two runs differ.
- prop stores each smallest failing case under `testdata/prop`. A case of a real defect is committed as its regression case, and the run tries it first.

### The manual mode of `store/memory`

A performer of the world must not block or start a goroutine, and the world applies the events of the leases through the calls of its performers. `store/memory` gains a mode for such a runtime. Its other calls keep their behaviour:

```go
// WithManualTime makes the caller apply the events of the leases of the store, and run the
// node side of each session. The store does not start a goroutine, renew a session or end a
// session at its local deadline. The caller applies the due expiries of the leases with Pump,
// renews each session with Membership.Renew, and ends each session at its local deadline on
// the clock of its node with Membership.Lose.
func WithManualTime() Option

// Pump applies every event of the leases that is due on the clock of the store.
func (s *Store) Pump()

// Candidate enters the election of the scope id without waiting. The election grants
// the candidacy a term in the order of the calls, as it grants a term to Campaign, and
// ends the term when ctx ends.
//
// Error modes:
//   - an error that wraps ErrClosed after Close.
//   - an error that wraps ErrOutage during an outage.
func (s *Store) Candidate(ctx context.Context, id shard.Scope) (*Candidacy, error)

// Candidacy is one candidate of an election that waits for a term.
type Candidacy struct{ /* unexported fields */ }

// Granted returns the channel that the election closes when it grants the term.
func (c *Candidacy) Granted() <-chan struct{}

// Lead returns the term that the election granted, and false before the grant.
func (c *Candidacy) Lead() (store.Leadership, bool)

// Upgrade applies an empty commit at the next generation of the scope id with the
// schema schema, as a controller of a newer release does.
func (s *Store) Upgrade(id shard.Scope, schema uint32)

// Renew renews the lease of ss now, as the store renews a session in its own loop.
// FailAt of PointRenewApply and PointRenewAck applies to the renewal.
//
// Error modes:
//   - an error that wraps ErrClosed after Close.
//   - an error that wraps ErrOutage during an outage.
//   - an error that wraps ErrFault at a fault of PointRenewApply or PointRenewAck.
func (m *Membership) Renew(ss membership.Session) error

// Lose ends the session ss at its node, as the local deadline of its lease ends it: Done
// closes, and the session renews its lease no more. The views list the node until the store
// expires the lease.
//
// Error modes:
//   - an error of class errs.Invalid for a session that another store created.
func (m *Membership) Lose(ss membership.Session) error

// Views returns a reader of the views of the scope id in the incarnation inc. The
// store keeps each view until the reader has read it or has closed.
func (m *Membership) Views(id shard.Scope, inc shard.NamespaceIncarnation) *ViewReader

// ViewReader reads the views of one scope without waiting.
type ViewReader struct{ /* unexported fields */ }

// Next returns the views that the reader has not read, in the order of their
// revisions, and the channel that the next change of the scope closes.
//
// Error modes:
//   - an error that wraps ErrClosed after Close.
//   - an error that wraps ErrOutage during an outage.
//   - shard.ErrIncarnation when a commit of another incarnation than inc created the
//     scope.
func (r *ViewReader) Next() ([]membership.View, <-chan struct{}, error)

// Close ends the reader.
func (r *ViewReader) Close()
```

`Campaign` waits on a `Candidacy`, and `Membership.Watch` reads a `ViewReader`, so both modes share one election and one reader of views. The store of a simulation reads the time of the run through `hlc.NewOver` of core. The action that loses a renewal arms its fault of `FailAt` right before the renewal that it interrupts. One goroutine makes the calls, so each armed fault interrupts that call.

### The standards

| Standard | Rule | Amended rule |
|---|---|---|
| Testing, Files | "A source file `x.go` has its spec `x_test.go` beside it. The spec tests the behaviour that `x.go` implements. `doc.go` has no spec, and a generated file has one that pins what it generates." | A source file `x.go` has its spec `x_test.go` beside it. The spec tests the behaviour that `x.go` implements. `doc.go` has no spec, and a generated file has one that pins what it generates. The generated tests of a simulation are the spec of each source file that declares a function under a directive of `sim`. |
| Testing, Determinism and isolation | "A test that advances time uses core's `clock/fake` inside a `testing/synctest` bubble, and waits with `synctest.Wait`." | A test that advances time uses core's `clock/fake` inside a `testing/synctest` bubble, and waits with `synctest.Wait`. The simulation of a namespace advances the time of its world on the goroutine of the case, outside a bubble. |
| Packages, Internal packages | "`internal/machine/machinetest` \| The double of `Machine` \| The tests of `internal/driver`" | `internal/machine/machinetest` \| The double of `Machine`, and the simulation of a namespace with its system, performers, actions, reference application, invariants, convergence, scenarios and traces \| The tests of `internal/driver`, and its own tests |
| Packages, Import rules | "Its tests and its kits also import `go.dokimi.dev/assert`, `go.dokimi.dev/conform` and `go.dokimi.dev/stub`." | Its tests and its kits also import `go.dokimi.dev/assert`, `go.dokimi.dev/conform` and `go.dokimi.dev/stub`, and `internal/machine/machinetest` imports `go.dokimi.dev/sim`. |

### Superseded decisions

The ADR of the decision of this RFC supersedes these parts, and each record links to the other:

| Record | Part | Replaced by |
|---|---|---|
| RFC-0001 | The row `sim` of the packages of the root module | The simulation that dokimi generates in `internal/machine/machinetest` |
| RFC-0001 | Verification: the scheduler with its queue, the `rand/pcg` seed of a run, the three boundaries of the faults, and the seed that a failing run prints | The world of `go.dokimi.dev/sim`, the choices of a property case, the faults of hosts, messages, clocks and dependencies, and the case that prop stores |
| RFC-0001 | The API of package `sim`: `Faults`, `Scenario`, `Run` and `CrashPoints` | The run, the scenarios and the crash points of `go.dokimi.dev/sim`, and the actions and the traces of `machinetest` |

The change that accepts this RFC also updates three items of milestone 0001:

| Item | Text | New text |
|---|---|---|
| Failure traces in the simulator, the introduction | "CI runs each trace over every supported preset from fixed seeds, at `GOMAXPROCS` 1 and 4, and fails when the two event traces differ." | CI runs the simulation of each trace and its stored cases over every supported preset, and fails when the replay of a case gives another log. |
| The first trace | "each `sim.CrashPoints` scenario of a preset" | each scenario of the crash points of a preset |
| The check of the simulator | "A pause that lasts into the last quarter of a run does not cause a false convergence failure. When a pause ends, the process handles the timers that expired during the pause." | A pause that lasts into `Settle` does not shorten the bound of convergence. When a pause ends, the process takes the events that became due during the pause. |

### Bounds

| Quantity | Bound |
|---|---|
| Hosts of a case | 1 to 5 |
| Namespaces of a case | 1 or 2 |
| Shards of a namespace | 1 to 16 |
| Faults of each kind in force at once | 1 |
| Steps of the active phase of a case | A mean of 1,000, and at most 2,000 |
| Choices of a case | 82 for each step of `max-steps`, 164,000 |
| Steps of the final phase of a case | 1,000 for each second of the bound of the convergence, 600,000 |
| Cases of a simulation | 100, the default of prop |
| Simulations of the traces | 38 traces over 4 presets, 152 |
| Runs of the crash points of one scenario | 1 plus 2 for each event of its first run |

These times were measured on four cores. sim v0.10.2 changes the final phase alone, so the times without the convergence are those of sim v0.10.1:

- The falsification proved its 23 checks in 1.6 s with sim v0.10.2.
- The random cases of `MachineSimulation` without their convergence, and the crash points of the scenario of the spec edit, took 4.7 s.
- A simulation of a trace without its convergence took 1.7 to 20.8 s.

### Failure handling

| Failure | Result | Recovery |
|---|---|---|
| An invariant fails | The case fails with the statement of the invariant, the shrunk log and the history. prop stores the case under `testdata/prop` | Fix the defect, and commit the case as a regression case |
| A run does not converge within its bound | The case fails with the statement of the convergence and the log | Fix the defect of liveness, or derive a larger bound from the specs |
| A machine breaks a rule of a runtime | The case fails with the reason of the rule at the step | Fix the machine |
| A replay of a case gives another log | The test fails with the first step at which the two runs differ | Remove the source, such as an iteration over a map or a read of the wall clock |
| A planted defect passes its invariant | `TestSimulationFalsify` fails | Fix the predicate or the defect |
| A simulation of a trace seldom strikes its fault | `prop.Require` fails the simulation | Change the actions or the faults of the trace |
| A head of dokimi changes the generated simulation | `verify-generate-go` fails | Run `make generate-go`, and review the change |

### Verdicts

| Capability | Verdict | Reason | Evidence that would reverse it |
|---|---|---|---|
| A simulation that dokimi generates | In | The generator `sim` writes the anatomy, the performer and the simulation from the directives, and `go.dokimi.dev/sim` runs every part that does not depend on sharder | None |
| A simulator that sharder writes | Out | The world, its faults and its rules do not depend on sharder | None |
| The simulation in `internal/machine/machinetest` | In | The generator writes the simulation into the package that `out` of the machine interface names, and the functions of the simulation are in that package | A directive `out` for one generator |
| Hosts of machines | In | A crash, a pause and a restart end, stop and start the machines of a process together, and the agent of the root starts and stops its cell machines while the process runs | None |
| The time of the run for the store | In | The leases of every node run on the clock of the store, and `MaxDriftRate` bounds each node against it | None |
| The faults of the store, the membership and the application at the effects | In | The traces need commits that a fault loses before and after they apply, outages and slow handlers, and every effect has a function as its performer | None |
| A system that takes inputs from its case | In | The number of hosts, the slot counts and the seeds of the machines then shrink with the rest of a case | None |
| The errors of a lost effect from the directive `fault` on the interfaces of the dependencies | In | The world fails a lost effect with an error that the directive declares, and each machine stops at an error without a class | A param of the world for the errors of a dependency |
| `version.ErrOutcomeUnknown` for a lost effect of the store and the membership | In | The store returns it for each commit that may have applied, and the machines retry a read and a watch that fail with it | None |
| `context.DeadlineExceeded` alone for a lost call of the application | In | The world draws one error for a loss before and after the call. A permanent error after a call that served breaks the contract of the handler and of the fence | A world that draws the error of a loss after the call from the errors that leave its outcome unknown |
| The faults of a dependency at `Unwatch`, `Resign` and `Leave` | Out | A runtime ends the wait of their token whatever the call returns, and the world does not call the performer of a lost effect | A world that ends the wait of a lost effect |
| The faults of the store and the membership at `Campaign` and `Join` | In | The traces need campaigns and joins that a fault of the store or the membership ends | None |
| The `Cancel` of each wait that leaves a candidacy, the renewals of a session or a reader in the store | In | The world calls it where a runtime cancels the context of a call, so a lost, ended or stopped wait leaves no registration that the runtime of sharder ends | None |
| The start of a tenure in the predicate of writer safety | In | The reference application starts each tenure with a fenced commit, which raises the watermark. A write of an older tenure then fails after the start, also when the agent did not acquire the fence, so only the start shows the missing acquisition | A reference application whose start of a tenure does not commit |
| A planted writer that admits a commit of another generation and applies a retry again | In | A commit below the watermark needs a takeover while the replaced controller runs, and a commit after an upgrade needs a controller that has acquired the scope, which cases seldom reach. The retry of the commit whose reply the scenario of the lost commit reply loses names the generation before that commit | None |
| The planted defect of constraints in the commits of a cell machine | In | The controller admits each move against the constraints of the spec once more before it starts it, so a pass without `placement.Validate` breaks no constraint. In 1,000 cases such a pass broke the invariant in none | A controller that starts the moves of a plan without an admission |
| A zone of each host and a constraint of the zones in each spec | In | No preset has a hard constraint, so the invariant of constraints and its planted defect need one. A host of the zone `west` exists from the start, so a placer that breaks the constraint finds a node for it | None |
| A session of each campaign that bounds its term | In | The term of a root must end at the expiry of the lease of its host. The agent of the root joins the root scope only once the first term has created the namespace, so its session cannot bound that term | A store whose campaigns take a session of the membership |
| The end of the machines of a host at the error of one of them | In | A process of sharder ends every part at the error of one part, and a recreation of a namespace stops every agent of the old incarnation | A process of sharder whose parts end apart |
| The shares of `require` at about half of a measurement over 1,000 cases | In | A share that a run seldom reaches fails the simulation without a defect, and half of the measured share still detects a change that halves it | A change of the steps of a case, or of the deliveries of its active phase |
| A mean of 1,000 steps and at most 2,000 in the active phase of a case | In | The shares of `require` are measured at these steps, and a longer active phase strikes more faults in a case. The falsification proves its 23 checks at these steps in 1.6 s | A change of the deliveries or of the budget of choices of the active phase |
| A hybrid logical clock of core over the clock of its host, through `hlc.NewOver` | In | The world hashes each effect with every pointer followed, and the effect of a planning pass contains the clock of its controller | None |
| The monotonic time of the host as the `Elapsed` of its clock | In | The transition engine moves the window of its retry budget by `Elapsed`, and the timers of a host wait for its monotonic time. A reading of `Now` moves with each jump, and the time of the run has no drift | None |
| A new hybrid logical clock for each incarnation of a host | In | A process of sharder builds its clock when it starts, so a clock that a host keeps across a crash hides the instants that a restarted process issues below those of its last incarnation | None |
| One revision of the views for the sessions that a Delete or the first commit of another incarnation ends | In | Each such end is one change of the scope, and a view of each removal follows the order of a Go map, so the replay of a case diverges | None |
| A fault of the case at a call of `NewTimer` on a physical clock of a world | In | No machine and no store of a world waits on the channel of a timer, so such a call is a defect of the simulation | A machine or a mode of the store that waits on the channel of a timer |
| Performers of a pointer for the effects of 80 bytes or more | In | gocritic's `hugeParam` refuses a parameter of 80 bytes or more outside a test file, and 9 of the 21 effects have 80 to 128 bytes | None |
| `maxDriftRate` of 0.5 in the specs of the simulation | In | Every drift of a world from −500 to +1,000 per mille then keeps the deadline of a node before the expiry of its lease at the store | A bound of the drift that a simulation states |
| The default `maxDriftRate` of 0.01 in the simulation | Out | A drift of the world below −10 per mille ends the validity of a node after the store expires its lease. The lease of the default spec excludes that drift | A bound of the drift that a simulation states |
| A renaming of the parameters of `store.Reader.Head` and `store.Store.Outcome`, for `perform call=` | Out | It binds 2 of the 11 effects of the store and the membership, and the param `dependency` binds all of them | None |
| A spec of the calls of operators and applications | Out | The calls change the placements through the machines and the store, and the invariants check those effects. A sequential spec of their replies states none of the properties of RFC-0001 | A call whose reply alone states a contract |
| A check of isolation through `isolation` | Out | The calls of the clients are no transactions of list-append or register operations | None |
| A fingerprint of the state of the system | Out | The checksum of the log covers every result that a dependency returns to a machine | A state of a dependency that no step passes to a machine |
| The manual mode of `store/memory` | In | A performer must not block or start a goroutine, and the store must not apply its leases on a goroutine of its own | An accepted design that keeps the state of a scope in a package without locks or goroutines, such as the state of Draft RFC-0014 |
| Renewals that the performer of a join makes on the clock of its host | In | A paused or crashed host stops renewing its sessions | None |
| The local deadline of a session on the monotonic time of the clock of its host, which the performer applies with `Membership.Lose` | In | A node measures the validity of its lease on its own clock. A deadline on the time of the run ends each session of a host whose clock runs below two thirds of the rate of the run before its first renewal: in a case of 2026-11-08, one root agent joined 57 times. A deadline on the wall time of the clock moves with each jump of the clock | None |
| The mode of a loss of the lease of each namespace as an input of the case | In | The outage table states the writers under `stop` and under `continue-fenced`, the trace at line 96 runs under `stop`, and the agent revokes its tenures at the end of a session under `stop` alone | None |
| A recorder of the instruments for each incarnation of a host | In | A process builds its reporter when it starts. A recorder that a host keeps across a crash keeps the instruments that the machines of the crashed incarnation bound, so the gauges of a cell show two controllers | None |
| Bounds of one on the transitions in flight in the specs of `MachineSimulation` | Out | The bound of the convergence then counts a round of moves for each shard, 55m0s against 10m0s. Under the defaults of the presets no bound binds for 16 shards, so the planted defect of the bounds runs specs with bounds of one | A final phase whose steps do not grow with the bound |
| The convergence of a world in which the action Upgrade has raised the schema of a scope | Out | Each host that runs a machine of the scope exits at the error of that machine, and the machines of the other namespace of the host exit with it | A release whose machines serve a scope of a newer schema |
| A forged callback with the key of a served callback and another payload | In | An agent keeps the result of a callback that did not end with a transient error, so the forgery takes the key of a callback to which the controller had a reply without an error. A forgery of a key whose result the agent does not keep can take effect as the first callback of its key | None |
| A reference application over a fenced store in `machinetest` | In | Writer safety and exactly-once effects need the writes and the keys of an application | None |
| `examples/internal/fencedstore` as the reference application | Out | It is in the examples module, and the root module does not import that module | None |
| Two namespaces on the hosts of one case | In | The trace at line 79 of milestone 0001 runs two namespaces on one controller and one network | None |
| A simulation for each trace and preset | In | `prop.Require` fails a trace whose cases do not strike its faults, and each preset runs its own spec | None |
| The faults that the text of each trace names, with the shares of `MachineSimulation` | In | A trace strikes the faults that it describes. Each share is about half of the share that `MachineSimulation` measured at the same steps | A trace whose cases strike one of its faults below its share without a defect |
| The inputs of the system that the text of a trace states | In | The traces at lines 71, 72, 73 and 76 need bounds of one, the trace at line 74 a `maxSourceShards` of 1, and the trace at line 79 two namespaces. The trace at line 85 needs a spec edit that adds a constraint, and the trace at line 96 the mode `stop` | None |
| The scenarios of the crash points in the trace at line 58 alone | In | The text of that trace states the crash points, and `MachineSimulation` runs the same scenarios | A trace of another line that states a crash point |
| An action that drains the node of the first host | In | The trace at line 71 drains a node that holds every shard of its namespace. Submit draws the host, the kind and the caller of each call, so few of its calls drain that node as an operator | None |
| A spec edit of Submit that selects the placement of the simulation | In | The trace at line 85 adds the constraint of the zones with a spec edit. The specs of every other trace already have that placement, so their edits keep it | None |
| The spec of the traces in the nightly workflow, and its skip in short mode | In | Without their convergence, its 152 simulations took 992 s of CPU time in one run. The job check-go took 10 to 24 minutes of its limit of 30 minutes on its three systems, and its step race runs every test under the race detector. The Go project skips its long tests in short mode in all.bash, and runs them on its longtest builders | A limit of check-go that fits the spec with its convergence and under the race detector |
| The node of each writer in the convergence | In | The traces at lines 71, 72, 74 and 84 state that a drain completes, and the traces at lines 84 and 85 that repair moves a replica that a changed constraint or a changed node refuses. RFC-0004 moves such a replica to a feasible node. A writer that remains on a drained or a refused node passes the other five facts | None |
| An oracle of the violations that each planning pass reports | In | The trace at line 85 states that the core reports a replica that a transition places against a constraint. RFC-0004 reports each replica that a plan keeps and that a constraint rejects as a violation of the pass. The root places the cells under a spec of its own with the constraint of capacity alone, so the oracle checks the passes of the controllers of the cells | None |
| A scenario of a spec edit | In | The planted defect of the reported violations needs a pass that keeps a replica against a new constraint. In 400 random cases of that defect, an edit of an operator took effect in 9 cases, and no pass kept such a replica. A proof also runs each scenario with the simplest choices of its system, so the edit reaches that state in each proof. The crash points of the scenario cover the commit of the edit and the pass that applies it | A planted defect whose random cases keep a replica against a constraint |
| The callbacks in flight to each incarnation of a node in the oracle of the bounds | In | A view that ends the incarnation of a node cancels each callback to that incarnation and returns its permit, as ADR-0023 and ADR-0021 decide. In a case of 2026-11-08, a controller sent 16 adds to an incarnation that a view then ended, and 16 adds to the next incarnation | A controller that waits for the result of a callback to an ended incarnation |
| The reply of a cell machine to `Started`, which its host records | In | The driver of a controller process passes `Started` itself and waits for the reply, so the end of the wait of an add does not cancel the start. A loss after the effect of an add then leaves the started machine to serve | None |
| A final phase that starts a host again once it exits | In | A recreation of a namespace stops the agents of the old incarnation, and a supervisor starts their process again. A case whose recreation takes effect in its final phase cannot converge while the host remains down | None |
| A suite of `machine.Machine` | Out | The rules of a runtime check every step of every run, with the events that a namespace produces. A suite would generate events that the machines mostly refuse | A rule that no run of the simulation exercises |
| `router.Do` in the simulation | Out | `router.Do` is not a machine. The specs of `router` check the parts of the traces at lines 82 and 94 that concern its retries | A runtime of `router.Do` on one goroutine |
| The report of `sharderctl` in the simulation | Out | `sharderctl` is a command. Its tests check the part of the trace at line 87 that concerns the outcome of a proposal | None |
| A formal model | Out | As RFC-0001 states, the crash points cover a crash before and after every event of each scenario | An authority or migration protocol whose interleavings the traces leave uncovered, such as a cell split |

## Alternatives considered

### A simulator that sharder writes

`machinetest` contains a runtime of the machines, written by hand, as RFC-0001 designs it.

Rejected because the runtime does not depend on sharder once a generator reads the interface, the events and the effects of a machine. The harnesses and the driver repeat the rules of a runtime in five places. A sixth copy would add a scheduler, clocks, the routing of tokens, faults and traces.

### One machine for each process

Each machine of sharder is a process of the world, as the world of `go.dokimi.dev/sim` runs it today.

Rejected because a crash then ends the agent of a root while the cell machines that it hosts keep running, and a pause stops one machine of a process. A deployment of sharder never produces those states, so a failure in them is not a defect of sharder.

### A machine that multiplexes the machines of a process

Each host is one process of the world, whose machine routes each event to one of the machines of the host.

Rejected because each machine numbers its tokens from one, so the machine of the host would rewrite the tokens of every effect and every result. An inbound call also has no field for its target machine. That machine would be a runtime of its own.

### The processes of the production runtime as components on hosts

Each process runs `embedded.Process` with its drivers, as a component of dokimi RFC-0036.

Rejected because native mode replays the choices of a case and not the order of the goroutines, and the traces need orders of events that the case chooses (ADR-0020). Tasks mode needs code that blocks only through the host, and the drivers, the store and the transports block on channels and mutexes. Inside a bubble a livelock also stops virtual time (ADR-0040).

### The store as a component on a host

The memory store runs as a component, with its own goroutine on the clock of its host.

Rejected because a performer must not block, and a component takes calls only through `World.Invoke` from a client. The goroutine of the store also runs in an order that native mode does not replay.

### A seed of `rand/pcg`

The run selects each interleaving from a seed, as RFC-0001 designs it.

Rejected because a seed replays a failing run but does not shrink it. A property case records each choice, so prop shrinks a failure to its smallest case and stores that case.

### A conformance suite of a namespace

A suite on a declaration of the parts of a namespace checks the properties, and its planted defects replace one part each.

Rejected because a suite checks that each implementation keeps a contract, while a simulation runs a whole system in a simulated environment. The generated suites of the kits remain.

### A store of the simulation in `machinetest`

`machinetest` keeps the scopes, the elections and the leases of the simulation itself.

Rejected because it would repeat the generations, the receipts, the elections and the leases of `store/memory`. `store/storetest` and `membership/membershiptest` check those.

## Drawbacks

- A random case of the planted defect of the reported violations seldom keeps a replica against a constraint, so the proof of that oracle rests on the scenario of the spec edit.
- Each campaign of a root joins a session of its own, which a store of a deployment does not need. The term of a root ends at the expiry of that session, which the client of an etcd store gets from the lease of its election keys.
- The interfaces of the store, the membership, the fence and the handler declare the errors of a lost call, so each of their doubles gains a helper for each error.
- `store/memory` gains two types, an option and ten methods for a runtime on one goroutine.
- The performers of `machinetest` repeat the routing of the servers of `controller` and `replica`, and the rules of the host of ADR-0032. The production driver and the simulation can diverge there.
- Each host builds the configs of its machines apart from the public constructors.
- The specs of the simulation run with a `maxDriftRate` of 0.5, and the default specs of the presets have 0.01.
- A case runs a namespace of up to five hosts, and each trace runs over four presets.
- The spec of the traces runs in the nightly workflow alone, so a defect that only a trace finds fails the night after its push.
- The safety properties overlap, so a planted defect can break two of them.

## Open questions

None.

## References

| What | Where |
|---|---|
| The simulation of sans-IO machines in dokimi | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0025-a-simulation-of-sans-io-machines.md> |
| The components and the hosts of a simulation in dokimi | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0036-components-that-run-goroutines.md> |
| The runtime of the simulation | <https://github.com/dokimasia/dokimi-go/tree/main/sim> |
| The request of the generator | <https://github.com/dokimasia/dokimi/issues/43> |
| The request of hosts of machines | <https://github.com/dokimasia/dokimi/issues/63> |
| The request of the time of the run for a dependency | <https://github.com/dokimasia/dokimi/issues/64> |
| The request of the faults of a dependency at an effect that a function performs | <https://github.com/dokimasia/dokimi/issues/65> |
| The request of a system that takes inputs from its case | <https://github.com/dokimasia/dokimi/issues/66> |
| The request of the end of a wait at an outage and at a loss after the effect | <https://github.com/dokimasia/dokimi/issues/69> |
| The request of an effect that ends the wait of an earlier effect | <https://github.com/dokimasia/dokimi/issues/70> |
| The request of the effects of a machine without the types of test files | <https://github.com/dokimasia/dokimi/issues/71> |
| The request of the end of the machines of a host at the error of one of them | <https://github.com/dokimasia/dokimi/issues/72> |
| The request of the number of the steps of a case | <https://github.com/dokimasia/dokimi/issues/73> |
| The request of the monotonic time of the clock of a host | <https://github.com/dokimasia/dokimi/issues/75> |
| The request of an active phase that delivers enough events for the cells to serve | <https://github.com/dokimasia/dokimi/issues/76> |
| The request of a final phase that covers the bound of a convergence | <https://github.com/dokimasia/dokimi/issues/77> |
| The request of a loss after an effect that applies its launch and its stop | <https://github.com/dokimasia/dokimi/issues/78> |
| The request of a final phase that starts a host again once it exits | <https://github.com/dokimasia/dokimi/issues/80> |
| The deliveries and the choices of the active phase in dokimi | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0040-deliveries-and-choices-of-the-active-phase.md> |
| The final phase of a long convergence in dokimi | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0041-the-final-phase-of-a-long-convergence.md> |
| The outcome at a loss after the effect in dokimi | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0042-the-outcome-at-a-loss-after-the-effect.md> |
| The request of a clock over a source that reads every time from its sources | <https://github.com/thesm-os/core/issues/130> |
| The end of a wait in dokimi | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0038-the-end-of-a-wait.md> |
| Hosts that exit, the steps of a case and performers of a pointer in dokimi | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0039-hosts-that-exit-the-steps-of-a-case-and-performers-of-a-pointer.md> |
| The hosts, the started machines and the inputs of a system in dokimi | <https://github.com/dokimasia/dokimi/blob/main/docs/rfc/0037-hosts-started-machines-and-the-inputs-of-a-system.md> |
| The simulator, the properties and the outage table | RFC-0001, Verification and Properties |
| The determinism of the planner | RFC-0004 |
| The validity of a lease and its drift rate | RFC-0006 |
| The vocabulary of effects, and the rejection of `testing/synctest` | ADR-0020 |
| The host of the cells of a controller process | ADR-0032 |
| The routing of the calls of a controller by address | ADR-0036 |
| A livelock inside a bubble | ADR-0040 |
| The rules of a runtime | `internal/machine/doc.go` |
| The traces | Milestone 0001, Failure traces in the simulator |
| TigerBeetle, the VOPR | <https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md> |
| FoundationDB, simulation and testing | <https://apple.github.io/foundationdb/testing.html> |
| The flag `-short` of go test | <https://pkg.go.dev/cmd/go#hdr-Testing_flags> |
| The short mode and the long-test mode of `cmd/dist test` | <https://github.com/golang/go/blob/go1.27.3/src/cmd/dist/test.go> |
| The request of a nightly step of ergon that runs the long tests | <https://github.com/dokimasia/ergon/issues/78> |
| turmoil, a simulation of hosts and a network for Rust | <https://github.com/tokio-rs/turmoil> |
| madsim, a deterministic simulator of a Rust runtime | <https://github.com/madsim-rs/madsim> |
| The stateful machines of assert | <https://github.com/dokimasia/assert-go/tree/main/stateful> |

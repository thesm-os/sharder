---
rfc: 0012
title: A Disrupt that waits for its admission, and a Close that ends the root sessions of a controller
author: Roy Klopper
status: Accepted
created: 2026-10-13
updated: 2026-10-13
discussion: none
supersedes: none
superseded-by: none
produces-adr: tbd
research: none
related: RFC-0007, RFC-0008, RFC-0009, RFC-0011
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# RFC-0012: A Disrupt that waits for its admission, and a Close that ends the root sessions of a controller

## Summary

`embedded.Process.Close` returns about one second after the replicas of its node have moved, and the cells of its controller then have no controller for up to the TTL of a session. This RFC proposes two changes:

- A `Disrupt` call of a pending disruption waits at the controller until the budget admits the disruption, until its reservation ends, or until one second before the deadline of the call. The agent of a closing node then learns of the admission at the commit that admits it, in place of at its next call one second later.
- `controller.Controller.Close` ends the session of the agent of the root of each namespace, and then stops the run. The view without the process starts a pass of the root, and the root places the cells of the process on the other controller processes at once. `embedded.Process.Close` calls it after the drains of its agents.

## Motivation

### The admission of a drain waits for the next call

`Agent.Close` sends a negotiable disruption of its node to the controller of its cell. The controller commits a pending reservation and the drains of the node, and replies with a deferral whose `RetryAfter` is one second (`RetryAfter` in `internal/control/budget.go`). The drains move the replicas off the node. The controller admits the disruption only when the agent calls again, after `max(RetryAfter, RetryBase)` (`decided` in `internal/machine/agent/close.go`). So a Close returns one second after the moves, even when the moves take two milliseconds.

### The cells of a stopped controller wait for its session to expire

Every application process of the topology `embedded` is a controller process. The agent of the root of the process joins the root scope, and the root places the cells of the namespace on the processes of the root view (RFC-0009). `Process.Close` stops the controller without a step of the root:

- The contract of `store.Store.Campaign` ends the term of the root when the context of `Campaign` ends. The root of another process leads at once.
- The session of the agent of the root renews until its context ends or `Leave` is called (`membership.Membership.Join`). The stop ends the renewals, and the session expires a TTL after its last renewal. The root view lists the stopped process until then.
- The root places a cell again only after the view ends the incarnation of its process. Until then the cell has no controller, and its moves, its drain admissions and its repairs wait.

In RFC-0011 we decided against a move of the cells before the stop. We recorded this evidence for a reversal of that verdict: "A measured stall of a rolling restart while a cell has no controller".

### Measurements

A probe ran the scenario of `topology-embedded` in real time over `store/memory` and `protocol/local`. The namespace had the preset `exclusive-worker` and 4 slots. Its lease had the default TTL of 10 seconds and a renewal every 3.3 seconds. process-a led the root and hosted the cell. process-a had three replicas, and process-b had one. The probe then closed process-a. Each value is the time from the start of the Close:

| Stage | Close 30.0 s after the start of process-a | Close 5.0 s after the start of process-a |
|---|---|---|
| The replicas have left process-a | 2.1 ms | 2.2 ms |
| `Close` returns | 1.0011 s | 1.0011 s |
| The root leads on process-b | 1.0032 s | 1.0011 s |
| The cell has a controller on process-b | 9.9995 s | 8.3311 s |

Three earlier runs of the probe without a delay measured the first three stages within 3 ms of the first column, and the cell from 9.998 to 10.000 seconds.

- Close spends 999 ms of its 1.001 seconds after the last move.
- The term of the root moves within 2.2 ms of the stop.
- The cell has no controller for 9.0 seconds and for 7.3 seconds after the stop. The root session of process-a expired 10 seconds after its last renewal, which was at 30.0 and at 3.3 seconds after its start. With the default lease, the gap lasts from 6.7 to 10 seconds.

### Prior art

| System | Handover on a graceful stop | Wait for the admission or the handover |
|---|---|---|
| Apache Kafka 4.3.1, controlled shutdown | The broker sends `WantShutDown` in a heartbeat, and the controller moves every leader off the broker | The broker polls 50 ms after the first refusal and every 2 s after that, until the controller replies `ShouldShutDown` |
| Apache Kafka 4.3.1, KRaft | The leader resigns and sends `EndQuorumEpoch`, and the most up-to-date voter starts an election at once | None |
| etcd 3.7.2 | `Stop` moves leadership to the voter with the longest-active connection | `MoveLeader` checks the leader once per tick, up to `ReqTimeout`, 7 s by default |
| client-go 0.37.1, `ReleaseOnCancel` | The release writes a record without a holder | The next candidate takes the lease at its next retry, every 2 s by default |
| HashiCorp Serf 0.10.4 and memberlist 0.5.4 | `Leave` broadcasts a dead message from the node itself, and each peer marks the node left at once | A crash ends only at a suspicion timeout |
| HashiCorp Consul 2.0.4 | A leaving leader transfers leadership, and the server leaves its Serf pools | The server sleeps `leave_drain_time`, 5 s by default |
| Kubernetes 1.37.1, eviction, and kubectl 0.37.1, drain | The API server refuses an eviction that a disruption budget does not allow, with 429 | kubectl sleeps 5 s after each 429 |
| HashiCorp Consul, blocking queries | None | A read with an index and a `wait` returns at the next change or at the end of the wait, 5 minutes by default |

The design takes these rules from them:

- A graceful stop ends the membership of the process, so its peers act at once. Serf marks a node that leaves as left without a timeout. Consul and KRaft end the leadership of a stopping leader. sharder's store already ends the term of the root at the stop.
- A call that returns at the change replaces a poll at a fixed interval. Kafka, etcd, client-go and kubectl wait a fixed interval between checks, from 50 ms to 5 s. A Consul blocking query returns at the change, and a return without a change is a timeout that the client repeats.

## Detailed design

### Components

| Component | Package | Responsibility |
|---|---|---|
| The waiting `Disrupt` | `internal/control` | Replies to a deferred `Disrupt` once the budget admits the reservation, once the reservation ends, or `WaitMargin` before the deadline of the call |
| The deadline of an inbound call | `internal/machine`, `internal/driver` | Passes the deadline of the context of a `Disrupt` to the controller in `machine.Disrupt.Deadline` |
| `Controller.Close` | `controller` | Ends the session of the agent of the root of each namespace, and then stops the run |
| `Process.Close` | `embedded` | Closes the agents, then the controller, then the routers |

### The waiting Disrupt

```go
// Disrupt is a planned disruption that a controller receives through
// protocol.ControllerServer.Disrupt. The machine replies with a Reply[budget.Decision].
type Disrupt struct {
	// Disruption is a copy of the disruption.
	Disruption *budget.Disruption
	// Metadata is the metadata of the call.
	Metadata protocol.Metadata
	// Deadline is the deadline of the context of the call, and zero for a call without one.
	Deadline time.Time
	// Reply is the token of the call.
	Reply ReplyToken
}

// WaitMargin is the time before the deadline of a waiting Disrupt at which the controller
// replies with the deferral.
const WaitMargin = time.Second
```

The controller of the root and the controller of each cell take these steps:

- It decides a `Disrupt` as today. An admission, a refusal and a call whose deadline is less than `WaitMargin` away get their reply as today.
- It keeps the reply of every other deferral. The call then waits, and the controller arms a `Timer` at `WaitMargin` before the deadline.
- After each step that applies a commit, takes a report or takes a view, the controller evaluates each waiting reservation in the order of the IDs. It admits each one that the budget allows. One commit contains the admissions of the step, and the controller replies to their calls once that commit has applied.
- When a reservation ends, the controller replies to each of its calls with the deferral of the end.
- When the `Timer` of a call expires, the controller replies with the current deferral and a `RetryAfter` of zero. The agent sends the disruption again after `RetryBase`.
- When the controller stops, each waiting call gets `ErrNotServing`. A call that waits for a commit gets the same error today.

`protocol.ControllerServer.Disrupt` documents the wait: "A deferral of a pending disruption returns once the budget admits the disruption, once its reservation ends, or `WaitMargin` before the deadline of ctx. A call without a deadline returns at once." A transport passes the deadline of each call to the server. `protocol/local` passes the context of the caller. The framing of `httptransport` contains the deadline. Milestone 0001 already requires that framing to cancel a call through the context of the request.

### The close of a controller process

```go
// Close ends the session of the agent of the root of each namespace of the process, and then
// stops the run. The view of each root then ends the incarnation of the process, and the root
// places the cells of the process on the other controller processes. Close returns once Run
// has returned. ctx bounds the leaves.
//
// Error modes:
//   - an error that wraps the error of a Leave. Close stops the run in every case.
func (c *Controller) Close(ctx context.Context) error
```

- `Close` sends each agent of the root an event that ends its session without a disruption. The agent of the root leaves its session, stops its watches and stops. The machine of the agent already has this path for a Close whose context ends before the admission.
- A cell controller of the process runs until the run stops. The root places the cell on another process, and the acquisition of the cell scope by that process fences the old controller.
- A `Close` before `Run` makes a later `Run` return `ErrRun`. `Run` returns nil once `Close` has stopped it.

`embedded.Process.Close` then takes these steps:

1. It calls `Agent.Close` of every namespace at once. The controller of the cell can run on the process, and it admits the drains.
2. It calls `Controller.Close` with the context of `Close`.
3. It closes every router.

### Invariants

- A `Disrupt` waits only while its reservation is pending and its deadline is more than `WaitMargin` away.
- The controller admits the waiting reservations in the order of their IDs, under the conditions that admit a repeated call. `maxConcurrentNodes` then bounds the admissions of one step as it bounds the admissions of separate calls.
- The root places a cell on another process only after its view ends the incarnation of the process of the cell.
- The acquisition of a cell scope fences every older controller of the cell, so a commit of the old controller fails once the next controller has acquired the scope.

### Bounds

- The admission of a waiting disruption follows the commit that ends the last move off its nodes by one step and one commit.
- A waiting call returns at the latest `WaitMargin` before its deadline. The calls of the agent have a timeout of 10 seconds (`CallTimeout`), so a pending disruption of an agent costs one call per 9 seconds in place of one per second.
- The controller keeps one entry per waiting call. Each step that can change the budget evaluates the waiting reservations, which costs O(w × r) for w waiting reservations and r reservations.
- The cells of a closed process have no controller for one pass of the root, one add and one acquisition per cell. The probe measured from 7.3 to 9.0 seconds for the same stage today. We have not measured the design.

### Failure handling

| Failure | Behaviour |
|---|---|
| The controller stops while a call waits | The call gets `ErrNotServing`. The agent sends the disruption again after a backoff, and the controller of the next term decides it |
| The deadline of a waiting call comes first | The controller replies with the deferral `WaitMargin` before the deadline, and the agent sends the disruption again after `RetryBase` |
| A `Disrupt` arrives without a deadline | The controller replies at once, as today |
| The reservation of a waiting call ends | The controller replies with the deferral of the end |
| A `Leave` of `Controller.Close` fails | `Close` stops the run, and the session expires a TTL after its last renewal, as today |
| The context of `Controller.Close` ends before every `Leave` returns | `Close` stops the run, and each session that did not end expires a TTL after its last renewal |
| The closed process is the last controller process | No root runs until a controller process starts. That process leads the root and places the cells |

### Verdicts

| Capability | Verdict | Reason | Evidence that would reverse it |
|---|---|---|---|
| A `Disrupt` that waits for the admission of its reservation | In | Close spends 999 ms after the last move, and the wait removes that time with fewer calls than the poll | A transport whose idle timeout ends the waiting calls, measured as failed calls |
| A wait for a call without a deadline | Out | The controller bounds each wait by the deadline of its call | A transport that cannot pass a deadline |
| A shorter `RetryAfter` | Out | 100 ms leaves a gap of up to 100 ms and sends ten calls per second for each pending disruption | A controller that keeps a waiting call at a higher cost than ten deferrals |
| The admission pushed to the agent in a callback | Out | It needs a new call of the protocol and the address of the caller. The waiting call has the same latency over the existing call | A caller that cannot keep a call open for the time of a drain |
| `Controller.Close` that ends the root sessions of the process | In | The cell of the closed process had no controller for 9.0 seconds | None. The gap is a TTL that a graceful stop does not need |
| A Close that moves the cells before the controller stops | Out | A cell controller is not on the path of the requests of the application, and the replicas of the cell serve while the cell moves. The view without the process starts the pass that moves the cells | A measured gap in the control of a cell during a rolling restart that a move before the stop would close |
| A resign of the term of the root in `Close` | Out | The store ends the term when the context of `Campaign` ends. The root of process-b led within 2.2 ms of the stop | A store whose term outlives the context of `Campaign` |
| A `Leave` when the context of `Run` ends | Out | The end of the context of `Run` stops the process at once, as the end of the context of `Agent.Start` stops an agent. `Close` is the graceful path | A deployment that stops its processes by the end of the context of `Run` alone |

## Alternatives considered

### A shorter RetryAfter

The controller defers with a `RetryAfter` of 100 ms, as Kafka retries 50 ms after its first refusal.

Rejected because a disruption then waits up to 100 ms after its last move, and each pending disruption sends ten calls per second. A rolling restart that defers many nodes at `maxConcurrentNodes` multiplies the calls to the controller of the cell.

### The admission in the watch of the agent

The agent reads the admission of its reservation from the watch of the scope of its cell.

Rejected because the watch of an agent contains only the changes that concern its node: its placements and its transitions (`store.Event`). A reservation in that watch changes the contract of every store, and the etcd adapter keeps an index key per node for that filter.

### A callback that pushes the admission

The controller calls the agent of the node when it admits the disruption, with a new call kind of `AgentServer.Do`.

Rejected because a disruption of an operator has no agent to call, and the protocol needs a new call kind. The waiting `Disrupt` gives every caller the same latency over the existing call.

### A move of the cells before the stop

`Controller.Close` drains the agent of the root as `Agent.Close` drains a node. The root moves the cells, admits the disruption, and the agent then leaves.

Rejected because the move adds a round of the budget of the root. The next controller reads every transition of the cell from its scope, as it does after a crash. The `Leave` moves the cells at the next pass, and a Close of the last controller process does not wait for a move that cannot happen.

### A flag of the caller for the wait

The caller sets a flag in the metadata of a call to accept a waiting reply.

Rejected because the deadline of the call already bounds the wait. The agent sets a deadline on each call, and a caller that wants an immediate decision calls without a deadline.

## Drawbacks

- A waiting call keeps a call of the transport open until the admission or until `WaitMargin` before its deadline.
- A transport must pass the deadline of each call to its server.
- The controller evaluates the waiting reservations after each step that can change the budget.
- A cell has no controller between the stop of its controller and the acquisition by the next one. The gap lasts one pass of the root in place of a TTL.

## Open questions

None.

## Unresolved and future work

None.

## References

| What | Where |
|---|---|
| The budget, its decisions and `RetryAfter` | RFC-0008, Disruption budgets |
| The root and the controller processes | RFC-0009, The root |
| The embedded process and its `Close` | RFC-0011, The embedded process |
| The contract of `Campaign` | `store/store.go` |
| The contract of `Join` | `membership/membership.go` |
| The default lease | `builtin/ownership.go` |
| The events that start a pass | `internal/control/doc.go` |
| Apache Kafka 4.3.1, graceful shutdown | <https://kafka.apache.org/43/operations/basic-kafka-operations/> |
| Apache Kafka 4.3.1, the heartbeats of a controlled shutdown | <https://github.com/apache/kafka/blob/4.3.1/server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java> |
| Apache Kafka, KIP-631 | <https://cwiki.apache.org/confluence/display/KAFKA/KIP-631%3A+The+Quorum-based+Kafka+Controller> |
| Apache Kafka 4.3.1, the resignation of a KRaft leader | <https://github.com/apache/kafka/blob/4.3.1/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java> |
| etcd 3.7.2, the leadership transfer on stop | <https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/server.go> |
| client-go 0.37.1, `ReleaseOnCancel` | <https://github.com/kubernetes/client-go/blob/v0.37.1/tools/leaderelection/leaderelection.go> |
| memberlist 0.5.4, the dead message of a leaving node | <https://github.com/hashicorp/memberlist/blob/v0.5.4/state.go> |
| Serf 0.10.4, `Leave` | <https://github.com/hashicorp/serf/blob/v0.10.4/serf/serf.go> |
| Consul 2.0.4, the leave of a server | <https://github.com/hashicorp/consul/blob/v2.0.4/agent/consul/server.go> |
| Kubernetes, API-initiated eviction | <https://kubernetes.io/docs/concepts/scheduling-eviction/api-eviction/> |
| kubectl 0.37.1, drain | <https://github.com/kubernetes/kubectl/blob/v0.37.1/pkg/drain/drain.go> |
| Consul, blocking queries | <https://developer.hashicorp.com/consul/api-docs/features/blocking> |

---
rfc: 0011
title: An embedded process, a runner of tenures and a pass after a refused move
author: Roy Klopper
status: Accepted
created: 2026-10-13
updated: 2026-10-13
discussion: none
supersedes: none
superseded-by: none
produces-adr: tbd
research: none
related: RFC-0001, RFC-0006, RFC-0008, RFC-0009, RFC-0010
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# RFC-0011: An embedded process, a runner of tenures and a pass after a refused move

## Summary

An application process of the topology `embedded` runs a controller, an agent for each namespace and a router for each namespace that it routes. Each embedder composes these components today, and each handler keeps the goroutine of each tenure by hand. This RFC proposes three changes:

- Package `embedded`. Its `New` takes the node and the dependencies of a process once, with the spec and the handler of each namespace. Its `Process` starts and closes every component in order, and gives the embedder one server for the callbacks of the process.
- `replica.Runner`, a `Handler` that calls the application's `Opener` once per tenure and runs the work that `Open` returns until the drop of the tenure.
- A bound of the transition engine: a move of a shard with a transition in flight counts as a move that the bounds refuse. The end of a transition then starts the next pass, in place of the periodic pass 30 seconds later.

## Motivation

### The composition of a process

The example `preset-exclusive-worker` composes one application process in `examples/internal/jobs/process.go`, in 114 lines of code:

- It resolves the spec to build the partitioner, and builds a controller and an agent from the same ten dependencies and the same node.
- It wraps both in `protocol.Dispatch` and registers them with the transport. It starts them in the order of ADR-0037: `Controller.Run`, then `Agent.Start`.
- Its `Stop` ends both runs and waits for them. It then removes `context.Canceled` from their two errors.

These lines are the same in every application. The topology `embedded` is the only topology of the support matrix, so every application writes them. A wrong order breaks the process. Before ADR-0037, a callback that arrived before the agent joined `Dispatch` made the controller of the cell abort the move.

### The goroutines of the tenures

Every preset of the support matrix is exclusive and fenced. An application of `exclusive-worker`, `consumer-group` or `leader-election` runs work for each tenure of a shard, such as the jobs of its tenants. The worker of the example, `examples/internal/jobs/worker.go`, keeps one goroutine per tenure:

- A replayed add of the same tenure keeps the goroutine, and an add of a newer tenure replaces it.
- A drop cancels the goroutine and waits for it to return. The end of the agent stops every goroutine.
- The worker takes the token of the tenure from `Agent.Token`, so it needs the agent inside its own handler. The example sets the agent into the worker after `replica.New` returns.

Only the load of the state and the body of the loop are application code. The authors of Shard Manager attribute its adoption to "a small, straightforward interface consisting of `add_shard` and `drop_shard` primitives" (RFC-0008, Motivation). A handler that also manages a goroutine per tenure no longer has that interface.

### A refused move waits for the periodic pass

A pass reads the committed state of its scope, so a pass that replays a recorded input computes the same plan. The engine also has the transitions that it has started and whose first record has not committed yet. A pass in that window plans a move for such a shard, and `Engine.Start` refuses the move with `ErrMove`. `Controller.moves` logs the refusal and skips the move without marking the plan as refused (`internal/control/plan.go`). The end of the transition does not start a pass, so the cell waits for the periodic pass. That pass comes every 30 seconds by default:

- In a run of the example in real time, worker-b joined while the 8 first adds to worker-a were in flight. The pass of the join planned 8 moves, and the engine refused all 8. worker-b did not receive a shard before the run ended 1.6 seconds later.
- In a test, worker-b closed while two moves to it were in flight. Those moves completed during the close, and the repair moves of the drain met their transitions. The drain ended after 31 seconds of virtual time, at the periodic pass.

A rolling restart that closes a node while moves are in flight waits up to the same 30 seconds.

### Prior art

Each of these systems except etcd takes the handlers of the application before the process joins its cluster, and one call starts the membership:

| System | Entry point | Work for a shard of the process |
|---|---|---|
| Apache Helix 1.4.3 | `HelixManagerFactory.getZKHelixManager`, `registerStateModelFactory`, then `connect` | A `StateModel` per partition. The participant calls its transition methods one at a time, and a transition to the current state succeeds without a call |
| Akka Cluster Sharding | `ClusterSharding(system).init(Entity(typeKey)(...))` on each node, once per entity type | An entity per key. A rebalance stops the entities of the shard before the coordinator hands the shard to another node |
| Apache Kafka 4.3.1 | `new KafkaConsumer`, `subscribe(topics, listener)`, then `poll` in a loop of the application | The loop of the application. The old member normally completes `onPartitionsRevoked` before the new member receives the partition |
| Temporal Go SDK 1.49.0 | `worker.New`, then `Run`, or `Start` and `Stop` | An activity `func(ctx, ...)`. `Stop` cancels its context after `WorkerStopTimeout` |
| client-go 0.37.1 | `leaderelection.RunOrDie(ctx, config)` | `OnStartedLeading(ctx)` in a goroutine of its own. Its context ends when the renewal fails, and `Run` does not wait for it to return |
| etcd 3.7.2 | `concurrency.NewSession`, `NewElection`, then `Campaign` | Work of the application that watches `Session.Done`. `Election.Rev` can serve as a fencing token |

The design takes the following rules from these systems:

- The handlers are part of the construction of the process, and one call starts it. `embedded.New` takes the handler of each namespace, as Helix takes its factories before `connect`.
- Akka and Kafka hand a shard to the next owner only after the old owner has stopped its work. client-go does not wait, and its documentation warns: "you must ensure all code guarded by this lease has successfully completed prior to cancelling the context, or you may have two processes simultaneously acting on the critical path". A drop of a `Runner` returns once `Run` has returned.
- None of these work functions receives the epoch of its tenure. Slicer fences each result with the generation of a guard lease, and the creation revision of an etcd election can serve as a token. A crashed node does not run a drop, as Orleans and Temporal state for their own hooks, so only a fence orders the writes of two tenures. `OpenRequest.Token` gives the work its token.

## Detailed design

### Components

| Component | Package | Responsibility |
|---|---|---|
| `Process` | `embedded` | Starts and closes the controller of an application process and the agent and the router of each of its namespaces, and returns the servers that the embedder serves |
| `Runner` | `replica` | A `Handler` that opens each tenure through the application's `Opener` and runs its work until the drop of the tenure |
| `Opener` | `replica` | The part that the application implements: it opens a tenure and returns the work of the tenure |
| The bound of one transition per shard | `internal/engine` | Reports a move of a shard with a transition in flight as refused by the bounds, so the end of a transition starts the next pass |

### The embedded process

```go
// Package embedded runs an application process of the topology embedded: the controller of
// the process, and the agent and the router of each namespace that the process serves.
package embedded

// Transport is the client of a transport adapter. It sends the callbacks of the controller
// and the calls of the agents to the other processes.
type Transport interface {
	protocol.AgentClient
	protocol.ControllerClient
}

// Config is the configuration of a Process. New refuses a Config without one of its
// required fields.
type Config struct {
	// Namespaces lists the namespaces of the process. It requires at least one, and no two
	// with one name.
	Namespaces []Namespace
	// Store keeps the scopes of each namespace. It is required.
	Store store.Store
	// Membership keeps the sessions of the process and of its nodes. It is required.
	Membership membership.Membership
	// Transport sends the calls of the process. It is required.
	Transport Transport
	// Clock, Rand, Reporter, Registry and Logger are the dependencies of every component of
	// the process. Each is required.
	Clock    clock.Clock
	Rand     rand.Rand
	Reporter telemetry.Reporter
	Registry *config.Registry
	Logger   *slog.Logger
	// Operators maps each namespace to the identities that may submit a proposal, as in
	// controller.Config.
	Operators map[shard.Namespace][]shard.Identity
	// Node describes the process. The controller and every agent of the process share it. It
	// requires an ID and an address, and it has no cell.
	Node shard.Node
	// CellCapacity, Volatile and Unauthenticated configure the controller as in
	// controller.Config. Unauthenticated also applies to every agent.
	CellCapacity    int
	Volatile        bool
	Unauthenticated bool
}

// Namespace is one namespace of a process.
type Namespace struct {
	// Spec is the initial spec of the namespace.
	Spec config.Spec
	// Handler starts and stops the replicas of the node. It is required.
	Handler replica.Handler
	// Guard acquires the fence of each tenure of the node. It is required.
	Guard fence.Acquirer
	// Route makes the process run a router of the namespace.
	Route bool
}

// Process is one application process of the topology embedded. It runs once.
type Process struct{ /* unexported */ }

// New returns the process of cfg. It builds the controller and the agent of each namespace.
// It does not start a goroutine.
func New(cfg *Config) (*Process, error)

// AgentServer returns the server of the callbacks of the process: protocol.Dispatch over the
// controller and the agent of each namespace. The embedder serves it through its transport.
func (p *Process) AgentServer() protocol.AgentServer

// ControllerServer returns the controller of the process, which the embedder serves at the
// address of the node.
func (p *Process) ControllerServer() protocol.ControllerServer

// Start runs the controller, starts the agent of each namespace and then the router of each
// namespace whose Route is set. It returns once every agent serves and every router has read
// its namespace.
func (p *Process) Start(ctx context.Context) error

// Agent returns the agent of the namespace ns, and reports whether the process has one.
func (p *Process) Agent(ns shard.Namespace) (*replica.Agent, bool)

// Router returns the router of the namespace ns, and reports whether the process runs one.
func (p *Process) Router(ns shard.Namespace) (*router.Watcher, bool)

// Close moves the replicas of the node away, closes every router and stops the controller.
// ctx bounds the moves, and requires a deadline.
func (p *Process) Close(ctx context.Context) error

// Done is closed once the run of the process has ended and every goroutine of the process has
// returned.
func (p *Process) Done() <-chan struct{}

// Err returns the error that ended the run, and nil while the process runs and after a Close.
func (p *Process) Err() error

// The errors of a process.
var (
	// ErrConfig is the error of New for a Config that it refuses. It classifies as
	// errs.Invalid.
	ErrConfig = errs.WithClass(errors.New("embedded: invalid configuration"), errs.Invalid)
	// ErrStarted is the error of a second Start, and of a Start after Close. It classifies
	// as errs.Invalid.
	ErrStarted = errs.WithClass(errors.New("embedded: the process has started"), errs.Invalid)
)
```

The process has these rules:

- `New` returns an error that wraps `ErrConfig` when `cfg` is nil, has no namespace or has two namespaces with one name. Its error also wraps every error of `controller.New` and `replica.New`.
- `AgentServer` contains the agent of every namespace from `New` on. A callback that arrives before the transport serves the process fails with `protocol.ErrUnreachable`, which classifies as `errs.Transient`, and the controller sends it again. The order of ADR-0037 no longer depends on the embedder.
- `Start` runs `Controller.Run` with the spec of each namespace, then `Agent.Start` of each namespace, then `router.New` of each namespace whose `Route` is set. When `ctx` ends first, or a component fails to start, `Start` stops every component that it started. It returns the error once every goroutine that it started has returned.
- The process runs until `Close`, until the context of `Start` ends, or until a component stops itself, such as an agent after the root created its namespace again. When a component stops itself, the process stops the other components. `Err` then returns the error of the component, and the embedder can start a new process once `Done` is closed.
- `Close` calls `Agent.Close` of every namespace at once, and then closes every router and stops the controller. The controller stops last, because it can host the controller of the cell that admits the drains. When `ctx` ends first, `Close` stops every component, and the replicas of the node fail over as after a crash.
- A `Close` before `Start` stops nothing, and a later `Start` returns `ErrStarted`.

The program of the example then composes its process in about 20 lines:

```go
p, err := embedded.New(&embedded.Config{
	Namespaces: []embedded.Namespace{{Spec: spec, Handler: replica.NewRunner(worker), Guard: storage}},
	Store:      st,
	Membership: st.Membership(),
	Transport:  net.Local(protocol.Metadata{Caller: node.Identity, Version: protocol.Current}),
	Clock:      clk,
	Rand:       rnd,
	Reporter:   noop.New(),
	Registry:   reg,
	Logger:     logger,
	Node:       node,
	Volatile:   true,
})
if err != nil {
	return err
}
net.AddAgent(namespace, node.ID, p.AgentServer())
net.AddController(node.Address, p.ControllerServer())
if err := p.Start(ctx); err != nil {
	return err
}
```

### The runner of tenures

```go
// Opener opens the tenures of the shards of a node. The application implements it, and a
// Runner calls it.
type Opener interface {
	// Open starts serving the tenure of req. It reads the data of the shard only from a state
	// that contains the acquisition of the fence, as Handler.AddShard does, and records the
	// key of req.Request in the transaction of each effect that must not happen twice. It
	// returns once the replica serves, with the work of the tenure.
	Open(ctx context.Context, req *OpenRequest) (OpenResult, error)
}

// OpenRequest is the request of Opener.Open.
type OpenRequest struct {
	_ incomparable
	// Request is the add that starts the tenure.
	Request *protocol.Request
	// Token is the fencing token of the tenure.
	Token fence.Token
}

// OpenResult is the result of Opener.Open. The runner keeps its Run until the work of the
// tenure has ended.
type OpenResult struct {
	_ incomparable
	// Run runs the work of the tenure until its context ends. A nil Run means a tenure
	// without work of its own, such as a replica that serves the requests of a router.
	Run func(ctx context.Context) error
}

// Runner is a Handler that opens each tenure of the node through an Opener, and runs the work
// of the tenure until the drop of the tenure.
type Runner struct{ /* unexported */ }

var _ Handler = (*Runner)(nil)

// NewRunner returns the runner of o.
func NewRunner(o Opener) *Runner

// AddShardRequest gains the token of the tenure that the add starts.
type AddShardRequest struct {
	_ incomparable
	// Request is the callback that starts the replica.
	Request *protocol.Request
	// Token is the fencing token of the tenure that the add starts. The agent acquires the
	// fence with it before it calls AddShard.
	Token fence.Token
}
```

A `Runner` keeps the tenure of each shard of the node:

- An add of the tenure that runs returns nil, and the work of the tenure keeps running. An add that arrives again has no effect.
- An add of a newer tenure ends the work of the older tenure of the shard, waits for it, and then calls `Open`. An error of `Open` is the error of the add, and no work starts.
- `Run` runs in a goroutine of the agent. Its context ends at the drop of the tenure, at an add of a newer tenure, and at the end of the run of the agent. A drop returns once `Run` has returned.
- A `Run` that returns an error before its context ends makes the runner report a permanent fault of the replica through `Agent.ReportFault`, with the error as the cause. The controller then replaces the replica on another node.
- An error that wraps `epoch.ErrFenced` means that a newer tenure has acquired the fence, and the runner does not report it. A `Run` that returns nil leaves the tenure without work.
- `replica.New` binds a `Runner` to its agent, and refuses a `Runner` that another agent uses with `ErrConfig`. The agent ends the work of every tenure before it closes `Done`.

The application still records the key of the add with each effect of `Open`. A transient error of an add makes the controller send the add again with the same key, and the earlier attempt may have committed its effect.

`AddShardRequest.Token` follows the interfaces standard: a new input is a new field, and a handler that does not read it keeps its behaviour. A handler that is not a `Runner` uses it in place of `Agent.Token`.

### A pass after a refused move

- `Engine.Start` reports a move of a shard with a transition in flight as not started, without an error, as it reports a move above `maxTransitions`. `ErrMove` remains the error of a move that the protocol cannot script.
- `Controller.moves` then marks the plan as refused. A commit that ends a transition starts the next pass, by the rule that already covers a move that the bounds refuse.
- A move proposal of an operator for such a shard returns `ErrBusy`, of class `errs.Transient`, in place of `ErrStale`, of class `errs.Conflict`. The operator can submit the proposal again once the transition ends.
- The controller no longer logs a warning for such a move, because it is part of the planning cycle and not a failure.

### Invariants

- A process serves its callbacks through one server that contains every agent of the process from `New` on.
- A shard has at most one transition in flight. The engine enforces it as before.
- The input of a pass is the committed state of its scope, so a recorded input replays the same plan.
- A plan whose move the bounds refused gets a pass at the next commit that ends a transition.
- The work of a tenure ends before the drop of the tenure returns, and before the agent closes `Done`.

### Bounds

- A refused move waits for the next commit that ends a transition, in place of up to `triggers.interval`, 30 seconds by default.
- A refused move starts at most one pass per such commit. The passes of a scope run one at a time, and a trigger during a pass queues one more pass.
- A `Runner` runs one goroutine per tenure with work, and keeps one entry per shard of the node.

### Failure handling

| Failure | Behaviour |
|---|---|
| The context of `Start` ends before the agents serve | `Start` stops every component and returns the cause of the context |
| A component stops itself | The process stops every component. `Done` is closed and `Err` returns the error of the component |
| The context of `Close` ends before the drains are admitted | `Close` stops every component, and the replicas of the node fail over as after a crash |
| `Open` fails | The add fails with the error, and the controller retries the add or aborts the move |
| `Run` fails | The runner reports a permanent fault, and the controller replaces the replica on another node |
| A move meets a transition in flight | The engine refuses it as a bound, and the end of a transition starts the next pass |

### Verdicts

| Capability | Verdict | Reason | Evidence that would reverse it |
|---|---|---|---|
| Several namespaces in one process | In | A controller process joins the root of each of its namespaces (RFC-0009), and `Dispatch` serves the agents of several namespaces | A namespace whose agent needs a node description of its own, such as other labels |
| A router per namespace on request | In | A worker does not route, and a router watches every cell of its namespace | An application that routes for a namespace that it does not serve |
| A `Close` that moves the cells of the controller before it stops | Out | The root places the cells again once the session of the process ends, and the agents serve their replicas meanwhile | A measured stall of a rolling restart while a cell has no controller |
| A transient fault from `Run` | Out | The runner cannot tell a transient error from a permanent one. An application reports a transient fault itself through `Agent.ReportFault` | An application whose work fails transiently and recovers in place |
| A restart of a `Run` that failed | Out | The controller replaces the replica of a permanent fault | An application whose work fails and recovers without a move |
| A runner for the optional interfaces outside the support matrix | Out | The support matrix needs `Handler` alone (ADR-0034) | A change of the support matrix |

### The examples

The examples of RFC-0010 compose their processes through package `embedded`, and run their tenures through `replica.Runner`. ADR-0038 keeps its layout. The application of each example under `examples/internal` then contains its `Opener` and its storage alone.

## Alternatives considered

### The composition in each application

We document the order of the composition, and each application keeps its own composition.

Rejected because the composition is the same in every application of the support matrix. Its order is a correctness rule that the library can enforce.

### A transport hook in the Config

`Config` takes a function that serves the servers of the process. `Start` calls it before it starts the components.

Rejected because each transport registers its servers in its own form. `local.Network` registers an agent per namespace and node, and an HTTP adapter mounts a handler on the server of the embedder. The server of a process contains every agent from `New` on, so the order of the registration no longer matters.

### The composition in package controller

`controller` offers the process, because every process runs a controller.

Rejected because `controller` is also the component of the topology `service`, whose processes do not run an agent. The process would make `controller` import `replica` and `router`.

### One function with a ready signal in place of Open and Run

The application implements one function that runs the tenure. The function calls a function of its request once the replica serves. Client-go's leader election runs `OnStartedLeading` in this form, without a ready signal.

Rejected because the add must return once the replica serves. A ready signal that the work never calls leaves the add waiting until its timeout. `Open` returns once the replica serves, and its result contains the work.

### The token from Agent.Token in the runner

The runner calls `Agent.Token` in place of a new field of `AddShardRequest`.

Rejected because every handler then needs its agent, and `replica.New` returns the agent only after it took the handler. The agent acquires the fence before the add, so it has the token at the call.

### The uncommitted transitions in the input of a pass

The input of a pass lists the transitions that the engine has started and not committed. The planner skips their shards.

Rejected because the input of a pass would then contain state that no commit records, so a recorded input would not replay the plan. The end of those transitions would also need a pass for the shards that the plan skipped.

### ErrMove refusals marked in the controller

`Controller.moves` marks the plan as refused when `Engine.Start` returns `ErrMove` for a shard in flight.

Rejected because `ErrMove` also covers a move that the protocol cannot script, and no later pass fixes such a move. The controller would have to tell the two causes apart. The engine already states every other bound of a move.

## Drawbacks

- An application can compose a process through `embedded` or through the components themselves, and the documentation covers both.
- A `Runner` runs one goroutine per tenure with work, while a handler of its own can share one goroutine between tenures.
- A `Run` that fails becomes a permanent fault, so an application with transient failures handles them inside its work.
- `Close` stops the controller without moving its cells first. The cells of the process have no controller until the root places them again.
- A move proposal that meets a transition in flight returns a transient error in place of a conflict.

## Open questions

None.

## Unresolved and future work

- The topology `service`, with `sharderd`, is outside the support matrix. Its process composes package `controller` directly.

## References

| What | Where |
|---|---|
| The small interface of Shard Manager | RFC-0008, Motivation |
| The agent, its handler and its tokens | RFC-0008, The agent, and ADR-0034 |
| The order of the start of an agent | ADR-0037 |
| A controller process in the root of each namespace | RFC-0009, The root |
| The callback contract and the reads that contain the acquisition | RFC-0006, The callback contract |
| The constructors of the components | ADR-0031 |
| The layout of the examples | RFC-0010 and ADR-0038 |
| The pass at the end of the transition of a writer that is not live | ADR-0033 |
| The form of a call and of its growth | `docs/standards/interfaces.md` |
| Apache Helix 1.4.3, the participant | <https://github.com/apache/helix/blob/helix-1.4.3/website/1.4.2/src/site/markdown/tutorial_participant.md> |
| Apache Helix 1.4.3, the transition to the current state | <https://github.com/apache/helix/blob/helix-1.4.3/helix-core/src/main/java/org/apache/helix/messaging/handling/HelixStateTransitionHandler.java> |
| Akka, cluster sharding concepts, shard rebalancing | <https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding-concepts.html#shard-rebalancing> |
| Apache Kafka 4.3.1, `ConsumerRebalanceListener` | <https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.java> |
| Temporal Go SDK 1.49.0, the worker | <https://github.com/temporalio/sdk-go/blob/v1.49.0/worker/worker.go> |
| Temporal Go SDK 1.49.0, the stop of a worker | <https://github.com/temporalio/sdk-go/blob/v1.49.0/internal/internal_worker_base.go> |
| client-go 0.37.1, `tools/leaderelection` | <https://github.com/kubernetes/client-go/blob/v0.37.1/tools/leaderelection/leaderelection.go> |
| etcd 3.7.2, the election of `client/v3/concurrency` | <https://github.com/etcd-io/etcd/blob/v3.7.2/client/v3/concurrency/election.go> |
| Orleans, the grain lifecycle | <https://github.com/dotnet/docs/blob/3ad903a790ef39f8b58ca5fc59f5228454fc6e50/docs/orleans/grains/grain-lifecycle.md> |
| Adya et al., "Slicer: Auto-Sharding for Datacenter Applications", OSDI '16, the Slicelet and the guard lease | <https://www.usenix.org/system/files/conference/osdi16/osdi16-adya.pdf> |

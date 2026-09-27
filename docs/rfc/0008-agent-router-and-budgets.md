---
rfc: 0008
title: The replica agent, the router, disruption budgets and the protocol
author: Roy Klopper
status: Draft
created: 2026-09-27
updated: 2026-09-27
discussion: none
supersedes: none
superseded-by: none
produces-adr: tbd
research: Research-0001, Research-0002, Research-0003
related: RFC-0001, RFC-0003, RFC-0005, RFC-0006, RFC-0007, RFC-0009
---

# RFC-0008: The replica agent, the router, disruption budgets and the protocol

## Summary

An application uses the library through four components:

- The agent runs inside each application process and calls the application's `Handler` to add and drop replicas. Optional interfaces enable role changes, the graceful and staged handoffs, consensus membership changes and resharding, and the agent reports which ones the application implements.
- The router runs inside each client and maps a key to the replicas that serve it. By default it keeps every route of the cells it uses, from a watch of the store. In `lookup` mode it caches only the routes it uses and fetches missing routes from the cell's controller in coalesced batches. In a leaderless namespace it computes each route with the namespace's function placer. It corrects a route when a server replies that it no longer serves the shard.
- The disruption budget admits planned operations, such as a rolling restart, only when every shard keeps enough replicas.
- A protocol defines the calls between controllers, agents and routers, as Go types and interfaces, and its message types have binary codecs that kanon generates. Transport adapters in their own modules run the calls on the embedder's servers and credentials: HTTP/2 through `net/http` by default, or gRPC. The library checks each caller's identity against the state in the store.
- Operators and cluster managers call the controller that applies each change through the same protocol, which the transport adapter authenticates. A controller accepts an operation with its approvers' signatures, or from an identity in the process's own configuration.

The library is Go-only. `sharderctl`, a set of metrics reported through core's `telemetry.Reporter`, and logs through `log/slog` cover operations.

## Motivation

SM's authors attribute its adoption to a small application interface, "a small, straightforward interface consisting of `add_shard` and `drop_shard` primitives", and to the handling of planned events. Planned container stops were about 1000 times more frequent than unplanned failures at Facebook, and treating each one as a failure multiplies unavailability by the same factor. At Databricks, planned restarts make up roughly 99.9% of all restarts. A library that makes the application implement more than two calls, or that treats a deploy as a crash, gives up the two things that made SM worth adopting.

Clients need a defined way to learn that an assignment moved. Redis Cluster replies to a request for a moved slot with `MOVED` and the new owner. CockroachDB returns an error that points at the node it believes to be the leaseholder. Pekko's cluster sharding instead buffers messages during a handoff, and drops what it buffered in the region that hands off, to avoid reordering. Each choice is visible to the application, so the library has to make it explicit.

At 1,000,000 shards per cell and many cells, a full copy of the routes costs tens of megabytes per cell in every client process, and every client that watches the store costs the store memory, such as 17 KB per client connection in etcd. Systems at that scale cache locations lazily. Bigtable's client library caches tablet locations from a three-level hierarchy, needs three network round trips with an empty cache and up to six with a stale one, and prefetches the locations of neighbouring tablets. CockroachDB's nodes cache range descriptors from two levels of meta ranges and update the cache when a request arrives at the wrong replica.

Any process that can call an agent can take or disable any shard unless the agent checks who is calling. An `add` in the writer role at an epoch above every epoch that the node has seen makes the agent acquire the fence at that epoch, and every write of the real writer then fails with `epoch.ErrFenced`. An `add` whose `supersedes` list names arbitrary shards raises their fence watermarks to `fence.Retired`, which no epoch that the core issues can pass. Those shards then refuse every write until an operator repairs their fence records. The embedder's transport can prove who is calling, but only sharder's state tells whether that caller is the cell's current controller.

## Detailed design

### The agent

```go
// Package replica runs the agent that an application process embeds to host replicas.
package replica

import (
 "context"
 "errors"
 "log/slog"

 "go.thesmos.sh/core/clock"
 "go.thesmos.sh/core/epoch"
 "go.thesmos.sh/core/errs"
 "go.thesmos.sh/core/telemetry"
 "go.thesmos.sh/sharder/config"
 "go.thesmos.sh/sharder/fence"
 "go.thesmos.sh/sharder/membership"
 "go.thesmos.sh/sharder/protocol"
 "go.thesmos.sh/sharder/shard"
 "go.thesmos.sh/sharder/store"
)

// Handler is implemented by every application. The agent calls it to change the state
// of a local replica. Each request contains the shard's epoch and an idempotency key. A
// handler that receives a key it has completed returns the first result and repeats no
// side effect. The context is cancelled when the step's timeout passes.
type Handler interface {
 // AddShard starts serving r.Shard in r.Role. It returns when the replica serves.
 AddShard(ctx context.Context, r protocol.Request) error
 // DropShard stops serving r.Shard and releases its local resources.
 DropShard(ctx context.Context, r protocol.Request) error
}

// RoleChanger is implemented by applications whose state model has more than one
// serving role. ChangeRole moves a replica from r.FromRole to r.Role.
type RoleChanger interface {
 ChangeRole(ctx context.Context, r protocol.Request) error
}

// Preparer is implemented by applications that support the graceful handoff protocol.
type Preparer interface {
 // PrepareAddShard readies this node to take the writer role from r.Peer. Until
 // AddShard, the node accepts writes only when r.Peer forwards them.
 PrepareAddShard(ctx context.Context, r protocol.Request) error
 // PrepareDropShard makes this node forward every write for r.Shard to r.Peer.
 PrepareDropShard(ctx context.Context, r protocol.Request) error
 // CancelPrepare undoes either prepare call. On the old writer it resumes writing at
 // r.Epoch.
 CancelPrepare(ctx context.Context, r protocol.Request) error
}

// Stager is implemented by applications that support the staged handoff protocol, in
// which the old owner saves the shard's state and the new owner loads it.
type Stager interface {
 // UnloadState stops writing r.Shard and saves its state for the next owner. The
 // replica may keep serving reads.
 UnloadState(ctx context.Context, r protocol.Request) error
 // LoadState loads the state that the previous owner saved.
 LoadState(ctx context.Context, r protocol.Request) error
}

// MembershipUpdater is implemented by consensus applications that change their own
// replica sets. It is called on the writer with the new members in r.Members.
type MembershipUpdater interface {
 UpdateMembership(ctx context.Context, r protocol.Request) error
}

// Resharder is implemented by applications that re-key local data when shards split or
// merge. Without it, the agent adds each new shard and then drops each parent.
type Resharder interface {
 // Reshard replaces the local replicas in parents with those in children, which cover
 // the same keys. It returns when the children serve and the parents do not.
 Reshard(ctx context.Context, parents, children []protocol.Request) error
}

// LoadReporter is implemented by applications that report per-shard load.
type LoadReporter interface {
 ShardLoad(id shard.ID) shard.Load
}

// Elector is implemented by applications whose own authority, such as the leader
// election of a consensus protocol, elects the writers of their shards. The delegated
// ownership mode requires it on every node.
type Elector interface {
 // Elect asks the authority to make this node the writer of r.Shard. It returns the
 // epoch that the authority granted, which is above r.Epoch, once this node serves
 // the shard as its writer.
 Elect(ctx context.Context, r protocol.Request) (epoch.Epoch, error)
 // Abdicate asks the authority to move the writer role of r.Shard to another replica
 // of the authority's choice. It returns once this node has stopped writing the shard.
 Abdicate(ctx context.Context, r protocol.Request) error
}

// Config configures an agent.
type Config struct {
 Namespace shard.Namespace
 // Node describes this node. Node.Cell selects the cell that the node joins.
 Node       shard.Node
 Store      store.Store
 Membership membership.Membership
 // Guard, when set, acquires each shard's fence before this node starts to write it.
 Guard fence.Acquirer
 // Controller sends the agent's reports to the cell's controller, through a transport
 // adapter or, in one process, through protocol.Local.
 Controller protocol.ControllerClient
 // Trust anchors the approvals of the namespace's specs. With it, the agent refuses a
 // spec whose approvals do not verify from the anchor. Nil accepts every spec.
 Trust *config.Trust
 // Clock is the clock of the agent's lease timers and loops. Nil selects a
 // clock/hlc clock.
 Clock clock.Clock
 // Telemetry reports the agent's metrics. Nil selects telemetry/noop.
 Telemetry telemetry.Reporter
 // Logger receives the agent's logs. Nil selects slog.Default.
 Logger *slog.Logger
}

// Agent hosts the replicas that the cell's controller assigns to this node. The
// embedder serves it to controllers through a transport adapter.
type Agent struct {
 // unexported fields
}

var _ protocol.AgentServer = (*Agent)(nil)

// ErrNotWriter is returned by Token when this node is not the shard's current writer.
// It classifies as errs.Conflict.
var ErrNotWriter = errs.WithClass(errors.New("replica: not the shard's writer"), errs.Conflict)

// Start joins the node's cell and reads this node's placements from the cell's scope.
// It restores the node's non-writer replicas through h and runs the agent's loops until
// ctx ends.
func Start(ctx context.Context, cfg Config, h Handler) (*Agent, error)

// Token returns the fencing token for a shard this node writes. It returns
// ErrNotWriter when this node is not the shard's writer, and epoch.ErrFenced when this
// node's lease deadline has passed or a later epoch has been granted. In a delegated
// namespace, the lease deadline does not apply, and the application reports a later
// epoch through Deposed.
func (a *Agent) Token(id shard.ID) (fence.Token, error)

// Close calls Disrupt on the cell's controller for this node, which does not need
// approval.
// It waits until the controller has moved the node's replicas away under the
// namespace's handoff protocol, or until ctx ends, then calls Done and leaves the cell.
func (a *Agent) Close(ctx context.Context) error

// Do runs one callback of the cell's controller. It returns an error of class
// errs.Denied when caller is not the identity that the scope names as its controller,
// and of class errs.Conflict when r's shard epoch or controller epoch is below the
// highest this node has seen. A repeated r.ID returns the first result.
func (a *Agent) Do(ctx context.Context, caller shard.Identity, r protocol.Request) error

// Election is a writer tenure that a delegated namespace's authority granted to this
// node without a request from the core, such as after a failover or a split.
type Election struct {
 Shard shard.ID
 Epoch epoch.Epoch
 // Range and Parents describe a shard that a split or a merge by the authority
 // created. They are empty for any other shard.
 Range   shard.Range
 Parents []shard.ID
}

// Elected reports a tenure that the authority granted to this node. The agent reports
// the replica in the model's writer role at e.Epoch, and Token returns tokens at that
// epoch. The first token of a shard with parents lists them in Supersedes.
func (a *Agent) Elected(e Election)

// Deposed reports that the authority granted shard id to another writer at an epoch
// above e. From then on, Token returns epoch.ErrFenced for the shard, and the agent
// reports the replica out of the writer role.
func (a *Agent) Deposed(id shard.ID, e epoch.Epoch)

// Placed describes a replica that the application's authority placed on this node in a
// role other than the writer's, such as a voter that does not lead.
type Placed struct {
 Shard shard.ID
 Role  shard.Role
 // Range and Parents describe a shard that a split or a merge by the authority
 // created. They are empty for any other shard.
 Range   shard.Range
 Parents []shard.ID
}

// Hosted reports a replica that the authority placed on this node. The agent reports it
// in p.Role from then on. It returns an error of class errs.Invalid when p.Role is the
// model's writer role or not a role of the model, or when the namespace's ownership is
// not delegated.
func (a *Agent) Hosted(p Placed) error

// Released reports that the authority removed this node's replica of shard id. The agent
// stops reporting it. It returns an error of class errs.NotFound when this node has no
// replica of the shard.
func (a *Agent) Released(id shard.ID) error

// ReportFault reports a fault in this node's replica of shard id. The agent reports
// the replica as failed at once, with f and the first 1,024 bytes of cause's message.
// It returns an error when this node has no replica of the shard.
func (a *Agent) ReportFault(id shard.ID, f shard.Fault, cause error) error

// Recovered reports that this node's replica of shard id serves again after a
// transient fault. It returns an error when the replica has no transient fault.
func (a *Agent) Recovered(id shard.ID) error
```

`Start` detects which optional interfaces the handler implements, and the agent advertises them as capabilities in every report. The core refuses to start a transition that needs a capability the node lacks, and reports the node and the missing interface. A namespace whose protocol needs a capability that none of its nodes have fails the first planning pass with the same report.

| Interface | Needed by |
|---|---|
| `Handler` | Every namespace |
| `RoleChanger` | State models with more than one serving role |
| `Preparer` | The `graceful` handoff protocol |
| `Stager` | The `staged` handoff protocol |
| `MembershipUpdater` | State models that set `membershipUpdates` |
| `Resharder` | Nothing. Without it, resharding uses `AddShard` and `DropShard` |
| `LoadReporter` | Nothing. Without it, every shard reports only the `count` metric |
| `Elector` | The `delegated` ownership mode |

In a `delegated` namespace, the core does not place replicas, so the application reports every replica that its authority places on the node, through these calls:

- `Hosted` reports a replica in a role other than the writer's, and a later `Hosted` for the same shard changes that role, such as a learner that becomes a voter.
- `Elected` moves a hosted replica into the writer role, or adds one.
- `Deposed` returns the replica to the role that `Hosted` last reported, and releases a replica that `Hosted` never reported.
- `Released` removes the replica.

A ledger member calls `Hosted` for each shard in which it votes or learns, `Elected` and `Deposed` for each change that `Member.Leading` yields, and `Released` when it leaves a shard's voter set. Without `Hosted`, the core sees only the replicas that have led, and a plan chooses the writer among those alone.

An application reports a replica that cannot serve, such as one on a failed disk or one whose storage rejects every write, with `ReportFault`. Service Fabric offers the same call with the same two kinds of fault. From the report on, the replica counts as unavailable, for the budget and for the model's availability rule:

- The core moves the writer role off a faulted writer by break-before-make.
- A replica with a permanent fault is replaced on another node, as the replicas of a failed node are.
- A replica with a transient fault remains on its node. The core may give it the writer role again after `Recovered`.

The agent runs its loops, the lease renewal, the reports and the watch of its node's index, under `task.Run` from core, and renews with `task.Every`. `Close` returns only after every goroutine the agent started has returned.

An application that restarts for a deploy calls `Close` first, from its shutdown hook. `Close` moves the node's replicas away under the namespace's protocol before the process exits, so a deploy never looks like a crash. Kubernetes gives a pod 30 seconds between the termination signal and a forced kill by default, and Dicer moves its slices away within that period. A node whose `Close` does not finish in time is handled as a crash when its lease expires.

### The router

```go
// Package router maps keys to the replicas that serve them.
package router

import (
 "context"
 "fmt"

 "go.thesmos.sh/core/epoch"
 "go.thesmos.sh/core/errs"
 "go.thesmos.sh/sharder/shard"
)

// Endpoint is one replica a client can call.
type Endpoint struct {
 Node    shard.NodeID
 Address string
 Role    shard.Role
}

// Route lists the replicas of one shard at one epoch, the writer first.
type Route struct {
 Cell       shard.CellID
 Shard      shard.ID
 Range      shard.Range
 Epoch      epoch.Epoch
 Generation epoch.Epoch
 Replicas   []Endpoint
}

// Router resolves keys to routes. It is safe for concurrent use.
type Router interface {
 // Route returns the route of key. A cached route costs one position and one search
 // and does not allocate. An uncached route waits for a lookup, which ctx bounds.
 Route(ctx context.Context, key []byte) (Route, error)
 // Refresh returns when the router has observed generation min of cell or a later
 // one, or when ctx ends.
 Refresh(ctx context.Context, cell shard.CellID, min epoch.Epoch) error
}

// NotOwnerError is returned by a server for a request for a shard it does not serve at
// the request's epoch. Generation is the cell's generation that the server has
// observed, and Hint names the node the server believes serves the shard.
type NotOwnerError struct {
 Cell       shard.CellID
 Shard      shard.ID
 Epoch      epoch.Epoch
 Generation epoch.Epoch
 Hint       shard.NodeID
}

func (e *NotOwnerError) Error() string {
 return fmt.Sprintf("router: shard %s of cell %s is not served here at epoch %d (generation %d, hint %s)",
  e.Shard, e.Cell, e.Epoch, e.Generation, e.Hint)
}

// Class returns errs.Conflict: the route that the caller used has moved, and the
// caller routes again before it retries.
func (e *NotOwnerError) Class() errs.Class { return errs.Conflict }

// Source is how a router receives routes.
type Source string

const (
 // Watch keeps every route of each cell that the router uses, from a watch of the
 // cell's scope. It is the default in the embedded and service topologies.
 Watch Source = "watch"
 // Lookup caches the routes that the router uses, and fetches a missing route from
 // the cell's controller, or from the cell's scope when the controller does not
 // respond.
 Lookup Source = "lookup"
 // Compute computes each route with the namespace's function placer, from the
 // layout and the cell's membership, as the agents of a leaderless namespace do. It
 // is the only source of the leaderless topology.
 Compute Source = "compute"
)

// Do calls call with the route of key. After a *NotOwnerError, it refreshes the route
// and routes again. Do calls call at most attempts times and returns the last error
// when the attempts run out.
func Do(ctx context.Context, r Router, key []byte, attempts int, call func(Route) error) error
```

A router first maps a key to its position with the namespace's partitioner, then finds the position's cell in the cell map, which it watches in the root scope. The cell map is small: one entry per cell. It then finds the route in the cell's cache.

In `watch` mode, the cache is the cell's whole assignment, kept current by a watch of the cell's scope. In `lookup` mode, the cache contains only the routes that the router has used:

- A cache miss becomes a lookup at the cell's controller, through the router's `protocol.ControllerClient`, and the controller serves it from memory. Concurrent misses in one cell are coalesced into one call by a `batch.Loader` from core, which also merges concurrent misses of the same position. Each lookup returns the requested placements and the 16 placements after them, as Bigtable's client prefetches neighbouring locations.
- When the controller does not respond, the router reads the same placements from the cell's scope in the store.
- A `NotOwnerError` evicts the route it names. The next request for the shard looks the route up again, with its neighbours.
- The cache keeps at most `maxRoutes` routes, 1,000,000 by default, and evicts routes at random above that, so the lookup path records no access.

The cache structure depends on the partitioner. `fixed-slots` positions are slot numbers, so its cache is an array of atomic pointers indexed by slot. The other strategies cache ranges in a copy-on-write B-tree: one goroutine applies each batch of changes to a copy and publishes it through an atomic pointer, and readers search the published tree without a lock. We measured the candidates at 1,000,000 entries with 16-byte keys:

| Structure | Lookup | Publish one change | Memory per entry, keys excluded |
|---|---|---|---|
| Sorted slice | 241 to 245 ns | 12.7 ms, copying 24 MB | 24 bytes |
| `github.com/google/btree` v1.1.3 | 291 ns | 3.6 to 3.7 µs | 52 bytes |
| `github.com/tidwall/btree` v1.8.1 | 292 ns | 2.9 to 3.0 µs | 36 bytes |

A cache that changes one route at a time must publish cheaply, so the router uses core's copy-on-write B-tree. The measured B-trees give the figures it has to match: about 290 ns per lookup, about 3 µs per published change, and 36 bytes per entry. The router needs four operations from it: a clone in constant time, a seek to the largest key at or below a position, iteration in order from a key, and a lookup without an allocation. A sorted slice remains the right structure for data that is rebuilt whole, such as a layout.

In `compute` mode, the router watches the cell's membership and computes each shard's nodes with the namespace's function placer, the same function that the agents run. The function's writer can be ahead of the shard's election: the node that the function selects campaigns only after it sees the same membership change. A request that arrives before the node leads the election gets `NotOwnerError`, and `router.Do` routes again.

A server replies to a request for a shard it does not serve, or for a lower epoch than its own, with `NotOwnerError`. It classifies as `errs.Conflict`, the class whose remedy is to read the state again and retry. Redis Cluster's `MOVED` and CockroachDB's not-leaseholder error follow the same redirect model. In `watch` mode, the router waits for the server's generation of the cell and routes again, because one generation can move many shards, such as every shard of a failed node.

The router does not buffer requests by default. A namespace can set `hold` in its routing configuration, such as `{"hold": {"max": "2s", "requests": 1000}}`. With `hold`, a request for a shard in the middle of a transition waits until the transition publishes its generation, for at most `max`. A request fails once more than `requests` requests are waiting. Pekko buffers the same way and delivers at most once. The router keeps each held request's order, and it never delivers a held request after `max`.

### The protocol

Controllers, agents and routers in separate processes call each other through a transport adapter. The protocol itself is Go types and interfaces:

- The agent implements `AgentServer`, and the controller implements `ControllerServer`.
- A controller sends callbacks through an `AgentClient`. Agents and routers send reports and lookups through a `ControllerClient`, and operators and cluster managers send operations and disruptions through it.
- The message types have binary codecs that kanon generates beside their source file, in package `protocol`. The generated code imports only the standard library, and an embedder with a transport of its own sends these encodings over it.
- `Network` connects the servers of every process of one simulation or test by direct calls. Each process calls through its own `Local`, so every server receives the identity of the process that calls it, as over an authenticating transport. `NewLocal` gives one process a network of its own, through which a controller calls the agent of its own process.

A transport adapter implements both clients, and serves both servers on the embedder's server with the embedder's credentials. sharder includes two:

| Module | Wire | Dependencies |
|---|---|---|
| `go.thesmos.sh/sharder/httptransport` | One HTTP/2 POST per call through `net/http`, with the kanon encoding of the message as its body. It is the default, and `sharderd` uses it | The standard library |
| `go.thesmos.sh/sharder/grpc` | A protobuf service with one method per call | gRPC and protobuf |

`net/http` multiplexes calls over HTTP/2 on TLS connections, and on unencrypted connections when `http.Protocols` enables it. It also provides deadlines through the request's context, connection reuse and keepalives. The Kubernetes API serves binary Protobuf bodies over plain HTTP in the same way.

Every adapter numbers the fields of its encoding, as protobuf and kanon do, so processes of two releases decode each other's messages during a rolling upgrade.

```go
// Package protocol defines the requests, reports and lookups between controllers,
// agents and routers, the operations of operators and cluster managers, and the
// interfaces that transport adapters implement. Its message types have binary codecs
// that kanon generates.
package protocol

import (
 "context"

 "go.thesmos.sh/core/epoch"
 "go.thesmos.sh/sharder/budget"
 "go.thesmos.sh/sharder/config"
 "go.thesmos.sh/sharder/shard"
)

//go:generate go tool kanon -type=Request,Report,Ack,Located,Receipt

// Call names a replica callback.
type Call string

const (
 Add              Call = "add"
 Drop             Call = "drop"
 ChangeRole       Call = "change-role"
 PrepareAdd       Call = "prepare-add"
 PrepareDrop      Call = "prepare-drop"
 CancelPrepare    Call = "cancel-prepare"
 LoadState        Call = "load-state"
 UnloadState      Call = "unload-state"
 UpdateMembership Call = "update-membership"
 Reshard          Call = "reshard"
 Elect            Call = "elect"
 Abdicate         Call = "abdicate"
)

// Request is one callback for one replica.
type Request struct {
 Call      Call
 Namespace shard.Namespace
 Cell      shard.CellID
 Shard     shard.ID
 Range     shard.Range
 Role      shard.Role
 FromRole  shard.Role      // the current role, for ChangeRole
 Peer      shard.NodeID    // the current writer for PrepareAdd, the next for PrepareDrop
 Members   []shard.Replica // the new replica set, for UpdateMembership
 // Supersedes lists the parent shards that this shard's first writer retires.
 Supersedes []shard.ID
 Epoch      epoch.Epoch
 Generation epoch.Epoch
 // ControllerEpoch is the fence epoch of the cell's controller that sends the
 // request. A node refuses a request below the highest controller epoch it has seen.
 ControllerEpoch epoch.Epoch
 // ID is the idempotency key. A node that completed ID returns the first result.
 ID string
}

// Report is what an agent sends its cell's controller.
type Report struct {
 Node         shard.NodeID
 Incarnation  epoch.Epoch
 Cell         shard.CellID
 Capabilities []Call
 Replicas     []shard.Observed
}

// Ack is the controller's reply to a report.
type Ack struct {
 // Generation is the latest generation of the cell that the controller has committed.
 Generation epoch.Epoch
 // ControllerEpoch is the controller's fence epoch. The agent refuses callbacks with
 // a lower one.
 ControllerEpoch epoch.Epoch
}

// Located is one placement with the addresses of its replicas' nodes.
type Located struct {
 Placement  shard.Placement
 Generation epoch.Epoch
 // Addresses lists one address per replica, in the order of Placement.Replicas.
 Addresses []string
}

// AgentServer receives the callbacks of a cell's controller. Its caller parameter is
// the identity that the transport adapter authenticated. An adapter that authenticates
// no caller passes an empty identity.
type AgentServer interface {
 Do(ctx context.Context, caller shard.Identity, r Request) error
}

// ControllerServer receives the reports of agents, the lookups of routers, and the
// operations and disruptions of operators and cluster managers.
type ControllerServer interface {
 // Report accepts one report of an agent.
 Report(ctx context.Context, caller shard.Identity, r Report) (Ack, error)
 // Lookup returns the placements of cell that contain positions, and up to prefetch
 // placements after the last of them, in position order.
 Lookup(ctx context.Context, caller shard.Identity, cell shard.CellID, positions [][]byte, prefetch int) ([]Located, error)
 // Submit applies a signed proposal of a kind that this controller applies, as the
 // table of proposal kinds assigns it, and records caller with it.
 Submit(ctx context.Context, caller shard.Identity, s config.Signed) (Receipt, error)
 // Disrupt admits a planned disruption under the budget of its nodes' cell. Every
 // node of d is in one cell.
 Disrupt(ctx context.Context, caller shard.Identity, d budget.Disruption) (budget.Decision, error)
 // Done ends the planned disruption id, whose nodes serve again.
 Done(ctx context.Context, caller shard.Identity, id string) error
}

// Receipt is a controller's reply to a proposal that it accepted.
type Receipt struct {
 // Applied reports whether the controller committed the proposal. An explain
 // proposal commits nothing.
 Applied bool
 // Result is JSON: the warnings of Resolve for a spec, the budget's decision for a
 // drain, or the constraints and goal costs for an explain proposal.
 Result []byte
}

// AgentClient sends callbacks to agents in other processes. Transport adapters
// implement it.
type AgentClient interface {
 Do(ctx context.Context, node shard.Node, r Request) error
}

// ControllerClient sends reports, lookups, operations and disruptions to the controller
// of a cell, or to the root when the cell's ID is empty. The root's cell is the root
// leader's record in the root scope. Transport adapters implement it, and check that
// the controller presents cell's ControllerIdentity.
type ControllerClient interface {
 Report(ctx context.Context, cell shard.Cell, r Report) (Ack, error)
 Lookup(ctx context.Context, cell shard.Cell, positions [][]byte, prefetch int) ([]Located, error)
 Submit(ctx context.Context, cell shard.Cell, s config.Signed) (Receipt, error)
 Disrupt(ctx context.Context, cell shard.Cell, d budget.Disruption) (budget.Decision, error)
 Done(ctx context.Context, cell shard.Cell, id string) error
}

// Network connects the servers of the processes of one simulation or test by direct
// calls, without an encoding. It is safe for concurrent use.
type Network struct {
 // unexported fields
}

// NewNetwork returns a network without servers.
func NewNetwork() *Network

// AddAgent makes the agent of node n reachable through every Local of the network.
func (n *Network) AddAgent(node shard.NodeID, s AgentServer)

// AddController makes the controller of cell c, or the root when c is empty,
// reachable through every Local of the network.
func (n *Network) AddController(c shard.CellID, s ControllerServer)

// Local returns the client of the process with identity self. Every call through it
// passes self as the caller.
func (n *Network) Local(self shard.Identity) *Local

// Local is one process's client of a Network. It implements AgentClient and
// ControllerClient, and passes the process's identity as the caller of every call.
type Local struct {
 // unexported fields
}

// NewLocal returns the client of a single process with identity self, over a network
// of its own. The process registers its own servers on l.Network(), so that a
// controller calls the agent of its own process without an encoding.
func NewLocal(self shard.Identity) *Local

// Network returns the network that l calls through.
func (l *Local) Network() *Network

// Do passes r to the agent that the network registered for node.ID, with the
// process's identity as the caller. Without such an agent, it returns an error of
// class errs.Transient, as a transport does for an unreachable node.
func (l *Local) Do(ctx context.Context, node shard.Node, r Request) error

// Report passes r to the controller that the network registered for cell.ID, with
// the process's identity as the caller. Without such a controller, it returns an error
// of class errs.Transient.
func (l *Local) Report(ctx context.Context, cell shard.Cell, r Report) (Ack, error)

// Lookup passes the lookup to the controller that the network registered for
// cell.ID, with the process's identity as the caller. Without such a controller, it
// returns an error of class errs.Transient.
func (l *Local) Lookup(ctx context.Context, cell shard.Cell, positions [][]byte, prefetch int) ([]Located, error)

// Submit passes s to the controller that the network registered for cell.ID, with the
// process's identity as the caller. Without such a controller, it returns an error of
// class errs.Transient.
func (l *Local) Submit(ctx context.Context, cell shard.Cell, s config.Signed) (Receipt, error)

// Disrupt passes d to the controller that the network registered for cell.ID, with the
// process's identity as the caller. Without such a controller, it returns an error of
// class errs.Transient.
func (l *Local) Disrupt(ctx context.Context, cell shard.Cell, d budget.Disruption) (budget.Decision, error)

// Done passes id to the controller that the network registered for cell.ID, with the
// process's identity as the caller. Without such a controller, it returns an error of
// class errs.Transient.
func (l *Local) Done(ctx context.Context, cell shard.Cell, id string) error

var (
 _ AgentClient      = (*Local)(nil)
 _ ControllerClient = (*Local)(nil)
)
```

| Condition | Error class at the caller |
|---|---|
| The request's shard epoch or controller epoch is lower than the node's | `errs.Conflict` |
| The caller's identity is not the identity that the authorisation rules require | `errs.Denied` |
| The node does not implement the call | `errs.Unsupported` |
| The step's timeout passed | `errs.Transient` |
| The handler returned an error | The handler error's class |

A transport adapter keeps each error's class across the wire. `httptransport` returns the class with the error's message in the response body, and the gRPC adapter maps each class to a gRPC status. The transition engine retries only `errs.Transient`, so a stale epoch ends the step at once, for every call. A handler error without a class is `errs.Unspecified`, which core's `errs.Retryable` reports as not retryable. A handler whose failure a retry can fix classifies its error as `errs.Transient`.

### Transport security

The embedder is responsible for transport security, and the root module contains no transport:

- A transport adapter serves on the embedder's server and dials with the embedder's credentials: mutual TLS, a service mesh, or whatever else the adapter supports. The library does not set a TLS version, a cipher or a certificate policy.
- The adapter authenticates each caller and passes the caller's identity to `AgentServer` and `ControllerServer`.
- The store and membership adapters take clients that the embedder built.
- `sharderd`, the program of the `service` topology, is its own embedder, and its `main` package chooses its adapters and configures its credentials.

Whether an authenticated caller may make a call depends on sharder's state: which identity is a cell's current controller, and which identity is node N. The transport knows neither. In the `embedded` topology, every application process can run a controller and must be able to call every agent. Without a check against that state, one compromised process can fence any writer with a forged `add` at a high epoch or retire any shard with a forged `supersedes` list. The core issues epochs one at a time from 1, so both are permanent in practice.

The library checks each call against its own state, with the identity that the transport adapter passes:

1. Each node registers its identity with its membership session, in `shard.Node.Identity`. Each cell's controller writes its identity to the cell's record in its first commit, and the root's leader campaigns under its identity.
2. An agent accepts `Do` only from the identity that its scope names as its controller: the cell's record in a cell's scope, and the root's leader in the root scope. It also refuses a controller epoch below the highest it has seen.
3. A cell's controller accepts a `Report` for node N only from N's registered identity.
4. A controller accepts `Submit` in a namespace with approvals from any caller, because the signatures authorise the proposal, and records the caller's identity with it. It accepts `Submit` in a namespace without approvals, and every `Disrupt` and `Done`, only from an identity that `controller.WithOperators` lists. The one exception is a disruption of node N alone, which `Agent.Close` sends, and which the controller also accepts from N's registered identity.

An adapter that authenticates no caller passes an empty identity, which turns off rules 2 and 3. Any process that can call an agent can then take or retire any shard, and a deployment relies on a network policy to limit which processes can call agents.

`Lookup` returns placements and addresses without changing any state, so the embedder authorises its callers, in the transport adapter or with a mesh policy. The shard and controller epoch checks apply whatever the transport.

Identities, cell records and membership sessions are in the store, so the store's access control is the root of trust for rules 2 and 3. A deployment gives each role its own store credentials: controllers write the root and cell scopes, nodes write their own membership keys, and routers only read. Operators and cluster managers do not need store credentials. The identities of rule 4 come from the process's own configuration, never from the store, so a writer to the store cannot grant itself operator rights.

### Disruption budgets

```go
// Package budget admits planned disruptions under each namespace's caps.
package budget

import (
 "context"
 "time"

 "go.thesmos.sh/sharder/shard"
)

// Impact is what a disruption does to the replicas on its nodes.
type Impact string

const (
 // NetworkLoss makes replicas unreachable and keeps their memory and disk.
 NetworkLoss Impact = "network-loss"
 // ProcessRestart loses the replicas' memory and keeps their disk.
 ProcessRestart Impact = "process-restart"
 // FullLoss loses the replicas' memory and disk.
 FullLoss Impact = "full-loss"
)

// Disruption is a planned operation on nodes.
type Disruption struct {
 ID     string
 Nodes  []shard.NodeID
 Impact Impact
 Start  time.Time
 End    time.Time
 // Negotiable is false for operations that start at Start whatever the budget decides,
 // such as hardware maintenance. The core then moves replicas away before Start.
 Negotiable bool
}

// Decision reports whether a negotiable disruption may start now.
type Decision struct {
 Allowed    bool
 Reason     string
 RetryAfter time.Duration
}

// Budget admits disruptions. Admit is idempotent per Disruption.ID.
type Budget interface {
 Admit(ctx context.Context, d Disruption) (Decision, error)
 // Done reports that a disruption ended and its nodes serve again.
 Done(ctx context.Context, id string) error
}
```

The budget admits a negotiable disruption when these conditions remain true after it starts:

- No shard has more unavailable replicas than the spec's `maxUnavailable`, counting replicas on failed nodes, in transitions, and on nodes of admitted disruptions.
- At most `maxConcurrentNodes` nodes are disrupted at once.

Before it admits a disruption, the core moves the replicas in `drainRoles` off the disrupted nodes through the handoff protocol. Most SM applications drain their primaries and leave their secondaries in place, and the default `drainRoles` is the model's writer role for the same reason. In a `delegated` namespace, the core runs `abdicate` for each writer on the disrupted nodes, and a writer that the authority does not move keeps the disruption deferred.

SM's TaskController works the same way: it approves a subset of pending container operations under a global cap and a per-shard cap that count replicas already down. A non-negotiable disruption is SM's non-negotiable event, which arrives with notice and an impact class. The budget cannot delay it, so the core uses the notice to move replicas away first.

Every replica of a shard is on a node of the shard's cell, so each cell's controller enforces the budget of its own cell. A cluster manager calls `Disrupt` on the controller of the disrupted nodes' cell, and `Done` when the disruption ends. A process's shutdown hook does the same for its own node through `Agent.Close`. `Disrupt` returns the budget's `Decision`. A cluster manager whose disruption is deferred calls `Disrupt` again after `RetryAfter`, as Twine notifies SM's TaskController of its pending operations again at each period and delays the ones that the TaskController has not approved.

### Operations

A command that changes a namespace sends a proposal to the controller that applies it, through `Submit`, and prints the `Receipt` or the error that `Submit` returns. `sharderctl` finds the controller in the controller's record: the cell's record for a cell, and the root leader's record for the root. `status` and `history` read the store and do not need a controller. `why` submits an `explain` proposal, and the cell's controller returns the explanation without committing anything. A proposal whose controller fails over before it replies returns `errs.Transient`, and `sharderctl` sends it again to the successor. The successor reports an ID that its scope has already applied instead of applying it twice.

| Command | Does |
|---|---|
| `sharderctl status <ns>` | Prints the cells, the generation of each, the shards per node, the transitions in flight, and the unplaced shards |
| `sharderctl apply <spec>` | Proposes a new spec version and prints the root's reply: the acceptance with the warnings of `Resolve`, or the reasons for rejecting it |
| `sharderctl drain <node>` / `undrain` | Moves every replica off a node under the budget, or releases it, and prints the budget's decision |
| `sharderctl move <shard> <node>` | Moves one shard's writer to a node of its cell through the handoff protocol |
| `sharderctl reshard <ns> --factor <k>` | Multiplies or divides a `fixed-slots` namespace's slot count |
| `sharderctl cell split <ns> <cell>` | Divides a cell at the shard boundary nearest its median, and prints the nodes to relabel |
| `sharderctl why <shard>` | Prints the constraints and goal costs that placed a shard where it is |
| `sharderctl history <ns> <cell>` | Lists recent generations of a cell and what changed in each |
| `sharderctl sign <proposal>` | Signs a proposal with the operator's key and prints the signature |
| `sharderctl submit <proposal> <signature>...` | Sends a signed proposal to the controller that applies it, and prints the controller's reply |
| `sharderctl anchor <proposal>` | Prints the digest of a namespace's first spec proposal, which each process's `config.Trust` names as its anchor |

The core validates each operation as it validates a placer's plan, before the operation's controller commits anything. A manual move that breaks a hard constraint or a role bound is refused. In a namespace with approvals, `apply`, `drain`, `undrain`, `move`, `reshard` and `cell split` print a proposal instead of sending it. `submit` sends the proposal once enough approvers have signed.

The library reports these instruments through core's `telemetry.Reporter`. Each binds its attributes, such as the namespace and the cell, once with `With`, so that every emission is free of allocations, as core's telemetry contract requires.

| Instrument | Kind | Unit | Meaning |
|---|---|---|---|
| `sharder.generation.lag` | Gauge | `{generation}` | Generations between the store and each agent or router |
| `sharder.transitions.in_flight` | Gauge | `{transition}` | Transitions started and not finished, per protocol and phase |
| `sharder.shards.unplaced` | Gauge | `{shard}` | Shards the placer could not place |
| `sharder.planner.pass.duration` | Histogram | `s` | Duration of each planning pass, per mode |
| `sharder.moves` | Counter | `{move}` | Replicas moved, per reason: failure, rebalance, drain, reshard |
| `sharder.lease.renewal.duration` | Histogram | `s` | Latency of lease renewals per node |
| `sharder.budget.deferred` | Counter | `{disruption}` | Disruptions and transitions the budget deferred |
| `sharder.fence.refusals` | Counter | `{acquisition}` | Fence acquisitions that returned `epoch.ErrFenced` |
| `sharder.router.lookups` | Counter | `{lookup}` | Route lookups, per result: hit, miss, redirect |
| `sharder.router.cache.routes` | Gauge | `{route}` | Routes in a router's cache, per cell |

The library logs through `log/slog`, as core does.

## Alternatives considered

### Applications implement the RPC service directly

Define only the wire protocol, as SM does, and let each application implement it without a Go library. There is then nothing to embed.

**Why not:** the agent's work is more than the calls. It keeps the highest epoch per shard, deduplicates repeated idempotency keys, runs the lease timer, acquires fences, restores replicas at start-up and moves them at shutdown. Each application would re-implement that work, and the library is Go-only.

### A proxy in front of the servers

Route requests through a proxy tier that knows the assignment, as Vitess routes through its gateway. Clients then run without a router, and a proxy can buffer during moves.

**Why not:** a proxy adds a network hop and a tier to scale and operate for every request. SM, Slicer and Dicer all route in a client library for the same reason. An application that wants a proxy can build one on the router.

### A full copy of every route in every router

Keep the whole assignment of every cell in every router, as the `watch` mode does for the cells a router uses.

**Why not as the only mode:** at 1,000,000 shards per cell, a sorted index costs about 40 MB per cell in every client process, and every client adds a watch connection to the store, at 17 KB of memory each in etcd. Bigtable and CockroachDB cache locations lazily for the same reason. `watch` is the default for namespaces small enough that a full copy is cheaper than a lookup, and a namespace with larger cells selects `lookup`.

### A sorted slice for the route cache

A sorted slice has the fastest lookup and the smallest entries.

**Why not:** a sorted slice copies every entry to publish one change: 12.7 ms and 24 MB per change at 1,000,000 entries. A copy-on-write B-tree publishes a change in about 3 µs at nearly the same lookup cost.

### Redirects without refresh

Reply to every misrouted request with the new owner and let the client update only that entry, as Redis Cluster does.

**Why not:** one generation can move many shards, such as every shard of a failed node. Updating one entry at a time causes one misrouted request per moved shard. Redis measured this cost in its own migrations: its atomic slot migration generates up to 116 times fewer redirects than its legacy migration. A router in `lookup` mode refetches the evicted route with its neighbours, and one in `watch` mode waits for the cell's generation.

### Kubernetes pod disruption budgets instead of a shard budget

Use Kubernetes' own disruption budgets, which limit how many pods of a workload an eviction may take down.

**Why not:** a pod disruption budget counts pods, not replicas of a shard. It treats two pods with replicas of the same shard as independent. The shard budget reads the assignment.

### Operations through the store

Let `sharderctl` and cluster managers write each operation as a pending proposal into the scope of the controller that applies it, and let the controller watch the scope and commit each outcome there. An operation then remains in the store across a controller's failover, and the protocol does not need calls for operations.

**Why not:**

- Operators and cluster managers would need write credentials for the store, and the store's access control would decide who may operate a namespace. A writer to the store could grant itself operator rights.
- Every store adapter would need an unfenced write path next to its fenced commits.
- An operation's result would arrive on a watch, one event after the commit, instead of as the reply to a call.

## Drawbacks

- The agent defines eight application interfaces. Most applications implement one or two, but the documentation and the conformance suite cover all eight.
- The library's checks are only as strong as the transport's authentication. An adapter that passes an identity that it did not prove lets any caller pass them.
- A node registers its own identity through the store, so a writer to the store can register an identity of its own. Only the store's access control prevents it.
- Capabilities are detected at runtime, so an application that omits an interface finds out from the controller's report, not from the compiler.
- `Close` makes a deploy wait for its node's replicas to move. With the graceful protocol and many shards per node, that takes longer than Kubernetes' default 30-second grace period, which then needs raising.
- A router in `lookup` mode pays a lookup round trip for each cold route, and `batch.Loader` adds its accumulation window to that round trip. Bigtable's clients pay three round trips with an empty cache and up to six with a stale one.
- `batch.Loader` passes the lookup function a context without any caller's values, so a lookup's trace does not join the trace of the request that missed.
- `hold` delays requests during transitions and fails them past its limits. Applications that cannot tolerate either leave it off and handle `NotOwnerError` themselves.
- The library ships two transport adapters, and each maps the protocol's Go types to its own encoding. Both must keep their field numbers and the error classes stable across releases.
- Operator identities are part of each controller process's configuration. Adding or removing an operator changes the configuration of every controller process of the namespace.
- In the leaderless topology, a router's computed writer can be ahead of the shard's election, and requests in that window get `NotOwnerError`.
- The route cache needs core's copy-on-write B-tree, which core does not contain at commit `8edf26a`.

## Open questions

None.

## Unresolved and future work

- Client libraries in languages other than Go are not proposed here.
- A graphical console for operators is not proposed here.
- A Kubernetes eviction webhook that consults the shard budget is not proposed here.

## References

| What | Where |
|---|---|
| Guo and Kooburat, "Scaling services with Shard Manager", Engineering at Meta, 2020 | https://engineering.fb.com/2020/08/24/production-engineering/scaling-services-with-shard-manager/ |
| Lee et al., "Shard Manager", SOSP '21, §2.2 and §4 | https://dl.acm.org/doi/10.1145/3477132.3483546 |
| Chang et al., "Bigtable: A Distributed Storage System for Structured Data", OSDI '06, §5.1 | https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf |
| CockroachDB distribution layer, meta ranges and the range cache | https://www.cockroachlabs.com/docs/v26.2/architecture/distribution-layer |
| etcd, watch memory usage benchmark | https://etcd.io/docs/v3.7/benchmarks/etcd-3-watch-memory-benchmark/ |
| Databricks, "Open sourcing Dicer", 2026-01-13 | https://www.databricks.com/blog/open-sourcing-dicer-databricks-auto-sharder |
| Databricks Dicer FAQ at 747e89acb3b7 | https://github.com/databricks/dicer/blob/747e89acb3b7b28fb52a8dc02933e162b86f29b8/docs/FAQ.md |
| Redis Cluster specification | https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/ |
| Redis, atomic slot migration, 2026-04-02 | https://redis.io/blog/atomic-slot-migration/ |
| CockroachDB replication layer, leaseholders | https://www.cockroachlabs.com/docs/stable/architecture/replication-layer |
| Apache Pekko 1.7.0, cluster sharding concepts | https://pekko.apache.org/docs/pekko/current/typed/cluster-sharding-concepts.html |
| Kubernetes v1.37.1, default termination grace period | https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/api/core/v1/types.go |
| Microsoft Learn, Service Fabric `FaultType` | https://learn.microsoft.com/en-us/dotnet/api/system.fabric.faulttype?view=azure-dotnet |
| Kubernetes API concepts, Protobuf encoding | https://kubernetes.io/docs/reference/using-api/api-concepts/#protobuf-encoding |
| go.thesmos.sh/ledger RFC-0011, "Replicated storage": membership changes and `Member.Leading` | `docs/rfc/0011-replicated-storage.md` in the ledger repository |
| Go `net/http`, `Protocols` | https://pkg.go.dev/net/http#Protocols |
| tidwall/btree v1.8.1 | https://github.com/tidwall/btree |
| google/btree v1.1.3 | https://github.com/google/btree |
| go.thesmos.sh/core at 8edf26a: request coalescing (RFC-0024), telemetry (RFC-0004), structured concurrency (RFC-0030), error classes and `resilience/retrier.go` | https://github.com/thesmos-ai/core |
| Our measurement of sorted slices and B-trees, 2026-09-27 | Research-0003 |

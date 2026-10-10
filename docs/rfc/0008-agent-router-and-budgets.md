---
rfc: 0008
title: The replica agent, the router, disruption budgets and the protocol
author: Roy Klopper
status: Draft
created: 2026-09-27
updated: 2026-09-29
discussion: none
supersedes: none
superseded-by: none
produces-adr: tbd
research: Research-0001, Research-0002, Research-0003
related: RFC-0001, RFC-0003, RFC-0005, RFC-0006, RFC-0007, RFC-0009
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# RFC-0008: The replica agent, the router, disruption budgets and the protocol

## Summary

An application uses the library through these components:

- The agent runs inside each application process and calls the application's `Handler` to add and drop replicas. Optional interfaces enable role changes, the graceful and staged handoffs, consensus membership changes and resharding, and the agent reports which ones the application implements. Callbacks arrive at least once, each with an idempotency key, and the agent refuses a stale callback before the handler sees it.
- The router runs inside each client and maps a key to the replicas that serve it. In the support matrix, it keeps every route of the cells it uses, from a watch of the store, and never replaces a route with an older one. It corrects a route when a server replies, before running a request, that it does not serve the shard. The `lookup` and `compute` sources are designs outside the matrix.
- The disruption budget admits planned operations, such as a rolling restart, only when every shard keeps enough replicas. It commits each deferred or admitted operation to the store as a reservation before it replies.
- A protocol defines the calls between controllers, agents and routers, as Go types and interfaces. Every call has metadata, the caller's identity, protocol version and required features, which every receiver checks before any effect. Every message names its scope and its namespace incarnation. The message types have binary codecs that kanon generates. Transport adapters in their own modules run the calls on the embedder's servers and credentials: HTTP/2 through `net/http`, in the support matrix, or gRPC. The library checks each caller's identity against the state in the store.
- Operators and cluster managers call the controller that applies each change through the same protocol. A controller authorises each call by the kind of the operation, against the operators in its process's own configuration.

The library is Go-only. `sharderctl`, a set of metrics reported through core's `telemetry.Reporter`, and logs through `log/slog` cover operations.

## Motivation

SM's authors attribute its adoption to a small application interface, "a small, straightforward interface consisting of `add_shard` and `drop_shard` primitives", and to the handling of planned events. Planned container stops were about 1000 times more frequent than unplanned failures at Facebook, and treating each one as a failure multiplies unavailability by the same factor. At Databricks, planned restarts make up roughly 99.9% of all restarts. A library that makes the application implement more than two calls, or that treats a deploy as a crash, gives up the two things that made SM worth adopting.

Clients need a defined way to learn that an assignment moved. Redis Cluster replies to a request for a moved slot with `MOVED` and the new owner. CockroachDB returns an error that points at the node it believes to be the leaseholder. Pekko's cluster sharding instead buffers messages during a handoff, and drops what it buffered in the region that hands off, to avoid reordering. Each choice is visible to the application, so the library has to make it explicit.

At 1,000,000 shards per cell and many cells, a full copy of the routes costs tens of megabytes per cell in every client process, and every client that watches the store costs the store memory, such as 17 KB per client connection in etcd. Systems at that scale cache locations lazily. Bigtable's client library caches tablet locations from a three-level hierarchy, needs three network round trips with an empty cache and up to six with a stale one, and prefetches the locations of neighbouring tablets. CockroachDB's nodes cache range descriptors from two levels of meta ranges and update the cache when a request arrives at the wrong replica.

Any process that can call an agent can take or disable any shard unless the agent checks who is calling. An `add` in the writer role at an epoch above every epoch that the node has seen makes the agent acquire the fence at that epoch, and every write of the real writer then fails with `epoch.ErrFenced`. An `add` whose `supersedes` list names arbitrary shards raises their fence watermarks to `fence.Retired`, which no epoch that the core issues can pass. Those shards then refuse every write until an operator repairs their fence records. The embedder's transport can prove who is calling, but only sharder's state tells whether that caller is the cell's current controller.

Processes of two releases also talk to each other during every rolling upgrade. A receiver that ignores a field it does not know can act on half of a message whose sender needed all of it, so each call has to state what it requires.

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
// of a local replica. Callbacks arrive at least once, and the agent calls one method at
// a time per shard. The agent refuses a stale callback, and returns its kept result for
// a repeated key, before it calls the handler. A handler whose callback has an effect
// that must not happen twice records r.Key() in the same transaction as the effect,
// and returns the recorded result for a key that it finds. It may delete a recorded
// key once it has returned nil for a callback of a later operation of the shard. The
// context is cancelled when the step's timeout passes, and a handler that ignores the
// cancellation keeps the shard's next callback waiting.
type Handler interface {
 // AddShard starts serving r.Shard in r.Role. For a writer role, the agent acquires
 // the fence at r.Epoch before it calls AddShard. AddShard discards every state that
 // it derived from an earlier tenure of the shard, and reads the shard's data only
 // from a state that contains the acquisition: a new transaction on the authoritative
 // data source, or a replica that is proven to have applied the acquisition and every
 // fenced write before it. It returns when the replica serves.
 AddShard(ctx context.Context, r protocol.Request) error
 // DropShard stops serving r.Shard and releases its local resources. r.Path is
 // protocol.Cleanup when the agent drops a removed tenure by itself.
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
 // Node describes this node. Node.Cell selects the cell that the node joins. A node
 // without a cell joins the namespace's only cell, and is refused in a namespace of
 // more than one cell.
 Node       shard.Node
 Store      store.Store
 Membership membership.Membership
 // Registry resolves the namespace's spec, which the agent reads from the root scope.
 Registry *config.Registry
 // Guard acquires each shard's fence before this node starts to write it. Start
 // refuses an exclusive namespace without it.
 Guard fence.Acquirer
 // Controller sends the agent's reports to the cell's controller, through a transport
 // adapter or, in one process, through protocol.Local.
 Controller protocol.ControllerClient
 // Unauthenticated makes the agent accept callbacks without an identity, for a
 // transport that authenticates no caller. The epoch and incarnation checks still
 // apply. Without it, the agent refuses every callback without an identity.
 Unauthenticated bool
 // Trust anchors the approvals of the namespace's specs. Approvals are outside the
 // support matrix, and Start refuses a Trust when the registry's support matrix has
 // no approvals.
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

// Start waits until the root has created the namespace, reads the namespace's cell map
// and spec from the root scope, and resolves the spec with cfg.Registry. It returns an
// error of class errs.Invalid when the namespace's ownership is exclusive and cfg.Guard
// is nil, or when cfg.Node.Cell names a cell that the cell map does not contain. It
// then joins the node's cell, reads this node's placements from the cell's scope,
// restores the node's non-writer replicas through h, and runs the agent's loops until
// ctx ends.
func Start(ctx context.Context, cfg Config, h Handler) (*Agent, error)

// Token returns the fencing token of a shard that this node writes. It returns
// ErrNotWriter when this node is not the shard's writer, and epoch.ErrFenced after the
// lease deadline under Stop, and once the agent has learned of a newer tenure. It
// returns the tenure's token otherwise, including after the deadline under
// ContinueFenced, where the fence rejects the token's writes once a newer tenure
// acquires. In a delegated namespace, the lease deadline does not apply, and the
// application reports a newer tenure through Deposed.
func (a *Agent) Token(id shard.ID) (fence.Token, error)

// Close calls Disrupt for this node at its incarnation, and calls it again after each
// RetryAfter until the controller allows it, which the controller does once the node's
// replicas have moved away. Close then leaves the cell and calls Done. When ctx ends
// first, Close leaves the cell and returns ctx's error without calling Done. The
// node's reservation then remains, pending with its drain or admitted, and the
// controller ends it once the node's incarnation has ended and no placement names the
// node.
func (a *Agent) Close(ctx context.Context) error

// Do checks md and r before it looks up r's key or calls the handler, and then runs r,
// one callback of the cell's controller. It returns an error of class errs.Transient
// when r.ControllerEpoch is above the epoch of the controller record that the agent
// has read, because its watch has not delivered the newer record yet. It returns an
// error of class errs.Denied when md.Caller is not the identity of that record. It
// returns an error of class errs.Conflict when r names another namespace incarnation,
// another node or node incarnation, or a shard epoch, controller epoch or operation
// below the highest this node has seen, when r is a forward callback of an operation
// whose abort path this node has seen, and when r belongs to an operation that this
// node has seen end. It returns an error of class errs.Invalid when this node has seen
// r's key with another digest, and for a request on the path protocol.Cleanup, which
// only the agent itself builds. A repeated key returns the kept result. A callback that
// starts a tenure, such as an add, runs only once the placements that the agent
// watches list the tenure at r.Epoch. Do waits for the agent's watch until then, and
// returns an error of class errs.Transient when ctx ends first.
func (a *Agent) Do(ctx context.Context, md protocol.Metadata, r protocol.Request) error

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

| Interface | Needed by | In the support matrix |
|---|---|---|
| `Handler` | Every namespace | Yes |
| `LoadReporter` | Nothing. Without it, every shard reports only the `count` metric | Yes |
| `RoleChanger` | State models with more than one serving role | No |
| `Preparer` | The `graceful` handoff protocol | No |
| `Stager` | The `staged` handoff protocol | No |
| `MembershipUpdater` | State models that set `membershipUpdates` | No |
| `Resharder` | Nothing. Without it, resharding uses `AddShard` and `DropShard` | No |
| `Elector` | The `delegated` ownership mode | No |

Each callback is one `protocol.Request`, delivered at least once, and the agent handles it in this order:

1. It checks the call's metadata. It then compares the request's controller epoch and the caller's identity with the controller record that it has read. A controller epoch above the record's returns an error of class `errs.Transient`, because its watch has not delivered the newer record yet. A caller other than the record's identity is denied.
2. It refuses a request for another namespace incarnation, another node or another node incarnation, a request with a lower shard or controller epoch, a request of an operation older than the newest it has seen for the shard, a forward request of an operation whose abort path it has seen, and a request of an operation that it has seen end. It learns of an abort path and of an operation's end from callbacks and from its watch.
3. It looks up the request's key. A key that it has seen with another digest is refused. A key whose callback has finished returns the kept result, and a key whose callback is running waits for that callback's result.
4. A callback that starts a tenure, an `add` in the support matrix, waits until the agent's watched state lists the tenure. The state lists a tenure when the shard's placement names the node at the tenure's epoch. The core commits that placement before it sends the callback, and a callback of a transition in flight runs once the watch has delivered the commit. The callback waits outside the shard's queue, and the agent checks the listing again when the callback's turn comes. When the call's context ends first, the agent returns an error of class `errs.Transient`.
5. It runs the callback, one at a time per shard, and keeps a result that is not a transient error until the shard's next operation starts.

The agent raises its highest controller epoch only from its watch and from a request that passed these checks. A request that it refuses changes none of its state.

The agent starts a tenure only once its watched state lists the tenure. It cleans up each tenure that its watched state no longer lists. The cleanup stops a replica that a late callback started after its operation ended, also when the watch skipped the generations that ended it, and it never stops a newer tenure:

- The agent compares its tenures with its watched state after it applies each event and after it installs each replacement snapshot. It treats a tenure that it has started or is starting, and that the state does not list at the tenure's epoch, as removed. The agent keeps each tenure's epoch itself, and binds each cleanup to the exact tenure that the comparison found. An event lists in `store.Event.Removed` each shard whose placement stopped naming the node. A snapshot that replaces the state after `store.ErrCompacted` does not report removals, and the comparison finds them.
- Absence from a replacement snapshot means removal. The agent starts a tenure only from an installed state that lists it, so the commit that placed the tenure is at or below that state's generation. The snapshot's read is linearizable, and its generation is at or above that of every state that the agent installed before it. The snapshot contains the commit that placed the tenure, and a later commit has removed the tenure. Its epoch is not granted again, because the core grants each epoch to one tenure. A snapshot cannot stop a newer tenure either. The agent has not started any tenure whose commit the snapshot does not contain.
- The agent keeps, with each tenure, the request of the callback that started it. The cleanup request copies that request's operation, node, node incarnation, shard, range, role and epoch, and is a `drop` on the path `protocol.Cleanup`, step 0. Its key depends only on the tenure. An event and a snapshot that report one removal build the same key, and every retry reuses the request.
- The cleanup is local. The agent runs it in the shard's queue, one at a time with the shard's callbacks, and deduplicates it by its key as it does a callback. `Agent.Do` refuses a request on the path `Cleanup` from a caller.
- Ordinary callbacks are refused for an operation that has ended, and the cleanup exists to handle ended operations, so it has an eligibility rule of its own: the targeted tenure, the replica at that epoch in the node's current incarnation, is still active. The agent checks the rule immediately before it revokes the token and before it calls the handler, again after each wait, and on each retry. When the tenure is no longer active, because the replica was dropped or a newer tenure replaced it, the cleanup ends without an effect.
- One transition can remove a tenure and then restore the node at another epoch: forward step 2 of a move away from A removes A's tenure at e, and the move's abort path restores A at e+2. A delayed removal of the tenure at e then finds the tenure at e+2 active, and ends without an effect.
- When the rule is met, `Token` returns `epoch.ErrFenced` for the tenure, and the agent calls `DropShard` with the cleanup request. The handler records the request's key as it records any callback's key. The agent retries a transient error with the same request. An error that is not transient marks only the targeted tenure failed, and its token remains revoked.
- A later `drop` of the replica succeeds without a handler call when the cleanup dropped it, and runs the handler when the cleanup failed.

In the support matrix, every replica is a writer that a callback starts, so its epoch identifies its tenure, and the callback that started it identifies its cleanup. These cases outside the matrix need more before the matrix can include them:

- A replica model with non-writer roles needs another discriminator for a non-writer replica.
- A replica that the agent restores at start-up, or that a delegated authority places, needs another basis for the identity of its cleanup.
- A protocol that adds a replica before it commits the replica, such as `graceful` or `staged`, needs the agent's watch to list the tenure from the transition's record.

In a `delegated` namespace, outside the support matrix, the core does not place replicas, so the application reports every replica that its authority places on the node, through these calls:

- `Hosted` reports a replica in a role other than the writer's, and a later `Hosted` for the same shard changes that role, such as a learner that becomes a voter.
- `Elected` moves a hosted replica into the writer role, or adds one.
- `Deposed` returns the replica to the role that `Hosted` last reported, and releases a replica that `Hosted` never reported.
- `Released` removes the replica.

A ledger member reports to its agent each role that `Member.Replicas` yields. It calls `Hosted` for a voter or a learner, `Elected` for a leader at the epoch in which the role took effect, `Deposed` when it no longer leads a shard, and `Released` for a replica that an epoch removed. Without `Hosted`, the core sees only the replicas that have led, and a plan chooses the writer among those alone.

An application reports a replica that cannot serve, such as one on a failed disk or one whose storage rejects every write, with `ReportFault`. Service Fabric offers the same call with the same two kinds of fault. From the report on, the replica counts as unavailable, for the budget and for the model's availability rule:

- Outside the `delegated` mode, the core moves the writer role off a faulted writer by break-before-make, and replaces a replica with a permanent fault on another node, as it replaces the replicas of a failed node.
- In a `delegated` namespace, the authority replaces a faulted replica. The core moves the writer role off it with `abdicate` and counts it unavailable until the application releases it.
- A replica with a transient fault remains on its node. The core may give it the writer role again after `Recovered`.

The agent runs its loops, the lease renewal, the reports and the watch of its node's index, under `task.Run` from core, and renews with `task.Every`. `Close` returns only after every goroutine the agent started has returned.

An application that restarts for a deploy calls `Close` first, from its shutdown hook. `Close` moves the node's replicas away under the namespace's protocol before the process exits, so a deploy never looks like a crash. Kubernetes gives a pod 30 seconds between the termination signal and a forced kill by default, and Dicer moves its slices away within that period. A `Close` that runs out of time leaves the cell with its remaining replicas, which fail over as after a crash. It does not call `Done`, so the budget keeps counting the node until the controller has replaced those replicas.

### The router

```go
// Package router maps keys to the replicas that serve them.
package router

import (
 "context"
 "fmt"
 "log/slog"

 "go.thesmos.sh/core/clock"
 "go.thesmos.sh/core/epoch"
 "go.thesmos.sh/core/errs"
 "go.thesmos.sh/core/telemetry"
 "go.thesmos.sh/sharder/config"
 "go.thesmos.sh/sharder/shard"
 "go.thesmos.sh/sharder/store"
)

// Endpoint is one replica a client can call.
type Endpoint struct {
 Node    shard.NodeID
 Address string
 Role    shard.Role
}

// Route lists the replicas of one shard at one epoch, the writer first.
type Route struct {
 Incarnation shard.NamespaceIncarnation
 Cell        shard.CellID
 Shard       shard.ID
 Range       shard.Range
 Epoch       epoch.Epoch
 // Generation is the cell's generation of the placement that the route comes from.
 Generation epoch.Epoch
 Replicas   []Endpoint
}

// Router resolves keys to routes. It is safe for concurrent use.
type Router interface {
 // Route returns the route of key. A cached route costs one position and one lookup
 // and does not allocate. It returns an error of class errs.Transient when the
 // router has no route for the key, such as for a shard without a committed
 // placement.
 Route(ctx context.Context, key []byte) (Route, error)
 // Refresh returns when the router has observed generation min of cell in the
 // namespace incarnation inc, or a later generation of it. It returns
 // shard.ErrIncarnation when inc is not the incarnation that the router has
 // installed, or when the router installs another incarnation while Refresh waits,
 // once the router has read the root scope again and installed the root's current
 // incarnation. It returns ctx's error when ctx ends first.
 Refresh(ctx context.Context, inc shard.NamespaceIncarnation, cell shard.CellID, min epoch.Epoch) error
}

// NotOwnerError is returned by a server, before it starts a request, for a shard that
// it does not serve at the request's epoch and namespace incarnation. A server never
// returns it for a request that it has started. Incarnation is the server's namespace
// incarnation, and Generation is the cell's generation in that incarnation that the
// server has observed. Hint names the node the server believes serves the shard.
type NotOwnerError struct {
 Incarnation shard.NamespaceIncarnation
 Cell        shard.CellID
 Shard       shard.ID
 Epoch       epoch.Epoch
 Generation  epoch.Epoch
 Hint        shard.NodeID
}

func (e *NotOwnerError) Error() string {
 return fmt.Sprintf("router: shard %s of cell %s is not served here at epoch %d (incarnation %s, generation %d, hint %s)",
  e.Shard, e.Cell, e.Epoch, e.Incarnation, e.Generation, e.Hint)
}

// Class returns errs.Conflict: the route that the caller used has moved, and the
// caller routes again before it retries.
func (e *NotOwnerError) Class() errs.Class { return errs.Conflict }

// Source is how a router receives routes.
type Source string

const (
 // Watch keeps every route of each cell that the router uses, from a watch of the
 // cell's scope. It is the default in the embedded and service topologies, and the
 // only source in the support matrix.
 Watch Source = "watch"
 // Lookup caches the routes that the router uses, and fetches a missing route from
 // the cell's controller, or from the cell's scope when the controller does not
 // respond. It is outside the support matrix.
 Lookup Source = "lookup"
 // Compute computes each route with the namespace's function placer, from the
 // layout and the cell's membership, as the agents of a leaderless namespace do. It
 // is outside the support matrix.
 Compute Source = "compute"
)

// Do calls call with the route of key. After a *NotOwnerError, it calls r.Refresh with
// the error's incarnation, cell and generation, and routes key again when Refresh
// returns nil or shard.ErrIncarnation, at most attempts times in total. It returns
// every other error of call or of Refresh at once, including a transport error and a
// timeout, because a request that fails after it started may have taken effect.
func Do(ctx context.Context, r Router, key []byte, attempts int, call func(Route) error) error

// Config lists what New needs: the namespace, the store and the registry that a
// router in watch mode reads, and the seams of the router's runtime.
type Config struct {
 Namespace shard.Namespace
 Store     store.Store
 // Registry builds the namespace's partitioner from the spec that the root scope
 // stores.
 Registry *config.Registry
 // Clock is the clock of the router's loops. Nil selects a clock/hlc clock.
 Clock clock.Clock
 // Telemetry reports the router's metrics. Nil selects telemetry/noop.
 Telemetry telemetry.Reporter
 // Logger receives the router's logs. Nil selects slog.Default.
 Logger *slog.Logger
}

// Watcher keeps every route of each cell that it has routed to, from a watch of the
// cell's scope, and the cell map, from a watch of the root scope. It implements Router
// in watch mode, and is safe for concurrent use.
//
// Watcher also rediscovers the namespace. It reads the root scope again when a watch
// returns shard.ErrIncarnation, or when Refresh names an incarnation other than the
// installed one. Concurrent triggers share one read. When the root scope records
// another incarnation, the Watcher builds the partitioner from the stored spec,
// compares its mapping with the recorded one, snapshots the cells that it routes to,
// and installs the mapping and the new caches in one step. It then cancels the
// watches of the old incarnation, ignores their late events, and wakes every Refresh
// that waits on the old incarnation. A mapping that differs from the recorded one
// makes Route return an error of class errs.Unsupported.
type Watcher struct {
 // unexported fields
}

var _ Router = (*Watcher)(nil)

// New returns a router in watch mode for cfg.Namespace. It waits until the root has
// created the namespace or ctx ends, reads the spec and the cell map from the root
// scope, builds the partitioner with cfg.Registry, and compares the partitioner's
// Mapping with the recorded one. It returns an error of class errs.Unsupported when
// the two differ. Its watches run under task.Run until Close, and a watch that
// returns shard.ErrIncarnation makes the router read the root scope again.
func New(ctx context.Context, cfg Config) (*Watcher, error)

// Route implements Router.
func (w *Watcher) Route(ctx context.Context, key []byte) (Route, error)

// Refresh implements Router.
func (w *Watcher) Refresh(ctx context.Context, inc shard.NamespaceIncarnation, cell shard.CellID, min epoch.Epoch) error

// Close stops the router's watches, and returns once every goroutine that the router
// started has returned. Route and Refresh return an error of class errs.Invalid after
// Close.
func (w *Watcher) Close() error
```

A router first maps a key to its position with the namespace's partitioner, then finds the position's cell in the cell map, which it watches in the root scope. The cell map is small: one entry per cell. It then finds the route in the cell's cache. A router compares the mapping of its partitioner with the namespace's recorded mapping when it starts, and refuses the namespace when they differ.

In `watch` mode, the cache is the cell's whole assignment, kept current by a watch of the cell's scope under the store's snapshot and watch rules. A `fixed-slots` cache is an array indexed by slot, with one atomic pointer per slot, so a lookup costs a hash and an index. Each entry records the incarnation and the generation of the placement it came from, and the router replaces an entry only with one from a newer generation of the same incarnation. A new incarnation replaces the mapping and every cache at once. A delayed event or a repeated one after a reconnect therefore never brings back an older route. A shard without a committed placement has no entry, and `Route` returns an error of class `errs.Transient` for its keys.

A server replies to a request for a shard it does not serve, or for a lower epoch or another namespace incarnation than its own, with `NotOwnerError`, before it starts the request. It classifies as `errs.Conflict`, the class whose remedy is to read the state again and retry. Redis Cluster's `MOVED` and CockroachDB's not-leaseholder error follow the same redirect model. The router waits for the server's generation of the cell, in the server's incarnation, and routes again, because one generation can move many shards, such as every shard of a failed node. A generation of another incarnation belongs to another history, so the router rediscovers the namespace instead of waiting for that generation.

`router.Do` retries only after a `NotOwnerError`, because only that error proves that the request did not run. A request whose reply is lost, or that fails after the server started it, may have taken effect. The application retries it, with an idempotency of its own when the request changes state.

#### The lookup mode, outside the support matrix

In `lookup` mode, the cache contains only the routes that the router has used:

- A cache miss becomes a lookup at the cell's controller, through the router's `protocol.ControllerClient`, and the controller serves it from memory. Concurrent misses in one cell are coalesced into one call by a `batch.Loader` from core, which also merges concurrent misses of the same position. Each lookup returns the requested placements and the 16 placements after them, as Bigtable's client prefetches neighbouring locations.
- When the controller does not respond, the router reads the same placements from the cell's scope in the store.
- A `NotOwnerError` evicts the route it names. The next request for the shard looks the route up again, with its neighbours.
- The cache keeps at most `maxRoutes` routes, 1,000,000 by default, and evicts routes at random above that, so the lookup path records no access.

The range partitioners cache ranges in a copy-on-write B-tree: one goroutine applies each batch of changes to a copy and publishes it through an atomic pointer, and readers search the published tree without a lock. We measured the candidates at 1,000,000 entries with 16-byte keys:

| Structure | Lookup | Publish one change | Memory per entry, keys excluded |
|---|---|---|---|
| Sorted slice | 241 to 245 ns | 12.7 ms, copying 24 MB | 24 bytes |
| `github.com/google/btree` v1.1.3 | 291 ns | 3.6 to 3.7 µs | 52 bytes |
| `github.com/tidwall/btree` v1.8.1 | 292 ns | 2.9 to 3.0 µs | 36 bytes |

A cache that changes one route at a time must publish cheaply, so the router would use core's copy-on-write B-tree. The measured B-trees give the figures it has to match: about 290 ns per lookup, about 3 µs per published change, and 36 bytes per entry. The router needs four operations from it: a clone in constant time, a seek to the largest key at or below a position, iteration in order from a key, and a lookup without an allocation. A sorted slice remains the right structure for data that is rebuilt whole, such as a layout.

The lookup mode needs ordering rules before the support matrix can include it. Two lookups can complete in reverse order, and the older reply can overlap ranges that a newer reply split. The cache has to reject a reply older than every cached entry it overlaps, replace overlapping ranges atomically, and never compare the generations of two cells.

#### The compute mode, outside the support matrix

In `compute` mode, the router watches the cell's membership and computes each shard's nodes with the namespace's function placer, the same function that the agents run. The function's writer can be ahead of the shard's election: the node that the function selects campaigns only after it sees the same membership change. A request that arrives before the node leads the election gets `NotOwnerError`. The router cannot compute the writer's epoch at all, so exclusive ownership in this mode needs routers that read each shard's election record.

#### Holding requests, outside the support matrix

The router does not buffer requests. A routing configuration outside the support matrix would set `hold`, such as `{"hold": {"max": "2s", "requests": 1000}}`, so that a request for a shard in the middle of a transition waits until the transition publishes its generation, for at most `max`. Pekko buffers the same way and delivers at most once. Before the matrix can include `hold`, its contract needs these rules:

- a queue per shard, with a bound on its requests
- a deadline that the router checks before it starts a held request
- the removal of a cancelled request from its queue
- no held request starts after `max`

The router's callback takes only a `Route`, so the router cannot bound the memory that a held request captures.

### The protocol

Controllers, agents and routers in separate processes call each other through a transport adapter. The protocol itself is Go types and interfaces:

- The agent implements `AgentServer` for the callbacks of its cell's controller. The controller implements `ControllerServer`, and `AgentServer` for the callbacks of a namespace's root, which place cell controllers on its process. `Dispatch` serves both kinds of callback on one endpoint.
- A controller sends callbacks through an `AgentClient`. Agents and routers send reports and lookups through a `ControllerClient`, and operators and cluster managers send operations and disruptions through it.
- The argument of every call is one message, and so is its result or its `CallError`. Every message names its scope and its namespace incarnation, so one controller can serve many namespaces, and a delayed message of a deleted namespace is refused.
- Every call has metadata besides its message, in `Metadata`: the caller's identity, its protocol version and the features it requires. The metadata is part of the protocol's contract, and a transport only encodes it.
- Each message has a binary codec that kanon generates beside its source file. The generated code imports kanon's small runtime, `go.thesmos.sh/kanon` and `go.thesmos.sh/kanon/wire`, and an embedder with a transport of its own sends these encodings over it.
- `Network` connects the servers of every process of one simulation or test by direct calls. Each process calls through its own `Local`, so every server receives the metadata of the process that calls it, as over an authenticating transport. `NewLocal` gives one process a network of its own, through which a controller calls the agent of its own process.

Each package generates the codecs of its own types, because kanon's `-type` flag names only structs of the directive's package:

- `protocol` generates them for `Request`, `Key`, `Report`, `Ack`, `Located`, `Receipt`, `LookupArgs`, `LookupReply`, `DoneArgs` and `CallError`.
- `config` generates them for `Proposal` and `Signed`, which `Submit` sends, and `budget` for `Disruption`, `Decision` and `Reservation`.
- `shard` generates them for the value types that the messages contain, and `store` for its records.

A field whose type has a kanon codec is encoded through that codec's methods, also when another package declares the type. A type that encodes itself, such as one with `MarshalBinary`, is encoded through its own methods: `crypto.Digest`, and `shard.NamespaceIncarnation` through the methods of `id.ID`. kanon inlines only a struct that has neither, and every package whose messages contain it gets its own copy of the inline functions. Two such structs occur here, both from core: `tag.Tag` inside `shard.Node`, and `sign.Signature` inside `config.Signed`. `Metadata` has no codec, because every transport sends it in its own framing.

A transport adapter implements both clients, and serves both servers on the embedder's server with the embedder's credentials. sharder includes two:

| Module | Wire | Metadata | Dependencies | In the support matrix |
|---|---|---|---|---|
| `go.thesmos.sh/sharder/httptransport` | One HTTP/2 POST per call through `net/http`. The request body is the kanon encoding of the call's argument message, and the response body is the result message or a `CallError`. It is the default | The headers `Sharder-Version` and `Sharder-Requires`. The caller's identity comes from the connection's verified client certificate, through a function that the embedder supplies | The standard library | Yes |
| `go.thesmos.sh/sharder/grpc` | A protobuf service with one method per call | gRPC metadata. The identity comes from the connection | gRPC and protobuf | No |

`net/http` multiplexes calls over HTTP/2 on TLS connections, and on unencrypted connections when `http.Protocols` enables it. It also provides deadlines through the request's context, connection reuse and keepalives. The Kubernetes API serves binary Protobuf bodies over plain HTTP in the same way.

#### Versions and features

Numbered fields let processes of two releases decode each other's messages, but decoding is not agreement. A receiver that ignores a field it does not know can act on part of a message whose sender needed all of it. The protocol therefore states what a call requires:

- `Version` is the protocol's major version. It changes only when a message's encoding changes incompatibly, and a receiver refuses a call of another major version.
- A release that adds a field which a receiver must understand adds a `Feature`, and a caller requires the feature only in the calls that use the field. A receiver of an older release refuses such a call instead of ignoring the field.
- A receiver refuses, before any effect, a call whose version or required feature it does not implement, a callback it does not implement, and an operation kind it does not know. Each refusal is an error of class `errs.Unsupported`.
- A rolling upgrade deploys a release to every process before any process uses the release's new features. A rollback stops using the features before it deploys the older release.
- Every server method calls `Metadata.Check` first. `Local` and every transport adapter call it as well before they call a server, so no server logic runs for a call that fails it, in a simulation or in a deployment.

This release is major version 1 and defines no feature, so every call of this release has version 1 and requires nothing. A transition records the version of the protocol that scripted it, and the store's records declare their schema, so a controller refuses to resume a transition or acquire a scope that a newer release wrote.

```go
// Package protocol defines the requests, reports and lookups between controllers,
// agents and routers, the operations of operators and cluster managers, the metadata
// of every call, and the interfaces that transport adapters implement. Its message
// types have binary codecs that kanon generates.
package protocol

import (
 "context"
 "errors"
 "fmt"

 "go.thesmos.sh/core/crypto"
 "go.thesmos.sh/core/epoch"
 "go.thesmos.sh/core/errs"
 "go.thesmos.sh/sharder/budget"
 "go.thesmos.sh/sharder/config"
 "go.thesmos.sh/sharder/shard"
)

//go:generate go tool kanon -type=Request,Key,Report,Ack,Located,Receipt,LookupArgs,LookupReply,DoneArgs,CallError

// Version is a major version of the protocol.
type Version uint32

// Current is the major version of the protocol that this release implements.
const Current Version = 1

// Feature names a capability that a call requires of its receiver beyond its major
// version. This release defines no feature.
type Feature string

// ErrUnsupported reports a call whose version, required feature, callback or
// operation kind the receiver does not implement. The receiver returns it before any
// effect. It classifies as errs.Unsupported and matches errors.ErrUnsupported.
var ErrUnsupported = errs.WithClass(fmt.Errorf("protocol: unsupported by the receiver: %w", errors.ErrUnsupported), errs.Unsupported)

// Metadata is what every call has besides its message.
type Metadata struct {
 // Caller is the identity that the transport authenticated, or empty when the
 // transport authenticates no caller. A transport takes it from the connection,
 // never from a value that the caller sends.
 Caller shard.Identity
 // Version is the caller's major version of the protocol.
 Version Version
 // Requires lists the features that the call needs its receiver to implement.
 Requires []Feature
}

// Check returns an error that wraps ErrUnsupported and names the version or the
// feature, when md.Version is not Current or md.Requires lists a feature that this
// release does not implement.
func (md Metadata) Check() error

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

// Path is the path of a transition's script that a callback belongs to, or the
// agent's own cleanup.
type Path uint8

const (
 // Forward is the path of the script's steps, from the first to the last.
 Forward Path = 1
 // Abort is the path that undoes the move after a forward step fails.
 Abort Path = 2
 // Cleanup is the path of an agent's own drop of a tenure that the placements it
 // watches no longer list. Only the agent builds requests on it, and Agent.Do refuses
 // them from a caller.
 Cleanup Path = 3
)

// Request is one callback for one replica. The controller of the replica's cell sends
// it, and the agent checks it before the handler sees it. Its Key and its Digest
// identify it across retries. A request on the path Cleanup comes from the agent
// itself.
type Request struct {
 Call  Call
 Scope shard.Scope
 // Operation identifies the transition, and names the namespace incarnation that the
 // controller acts for.
 Operation shard.OperationID
 // Path and Step identify the callback within its transition: the forward path, or
 // the abort path of the step that failed, and the step's index in that path.
 Path Path
 Step int
 // Node and NodeIncarnation name the process that the callback is for.
 Node            shard.NodeID
 NodeIncarnation epoch.Epoch
 Shard           shard.ID
 Range           shard.Range
 Role            shard.Role
 FromRole        shard.Role      // the current role, for ChangeRole
 Peer            shard.NodeID    // the current writer for PrepareAdd, the next for PrepareDrop
 Members         []shard.Replica // the new replica set, for UpdateMembership
 // Supersedes lists the parent shards that this shard's first writer retires. It is
 // empty in the support matrix, and an agent refuses a request that lists a shard in
 // a namespace whose partitioner does not reshape.
 Supersedes []shard.ID
 Epoch      epoch.Epoch
 // ControllerEpoch is the fence epoch of the controller that sends the request. A
 // node refuses a request below the highest controller epoch it has seen. It is the
 // sender's authority, not part of the callback's payload.
 ControllerEpoch epoch.Epoch
}

// Key returns r's idempotency key. No two callbacks share a key.
func (r Request) Key() Key

// Digest returns the SHA-256 digest of the encoding of r with ControllerEpoch set to
// zero: the digest of the callback's payload. A new controller that resends a
// callback under its own epoch therefore sends the same digest. An agent refuses a
// key that it has seen with another digest.
func (r Request) Digest() crypto.Digest

// Key is the idempotency key of one callback: its operation, its path and step, the
// node incarnation that it is for, and the epoch of the tenure that it acts on. The
// operation of a cleanup is the operation of the callback that started the tenure, so
// a removal event and a replacement snapshot build the same key for one tenure.
type Key struct {
 Operation       shard.OperationID
 Path            Path
 Step            int
 Node            shard.NodeID
 NodeIncarnation epoch.Epoch
 Epoch           epoch.Epoch
}

// Report is a full snapshot of one node's replicas, which the node's agent sends to its
// cell's controller.
type Report struct {
 Scope           shard.Scope
 Incarnation     shard.NamespaceIncarnation
 Node            shard.NodeID
 NodeIncarnation epoch.Epoch
 // Sequence increases by one with every report of the node incarnation. The
 // controller discards a report whose Sequence is not above the last one that it
 // accepted from the node incarnation.
 Sequence     uint64
 Capabilities []Call
 // Replicas lists every replica on the node. A replica that an accepted report omits
 // is not on the node.
 Replicas []shard.Observed
}

// Ack is the controller's reply to a report.
type Ack struct {
 // Sequence is the Sequence of the report that the controller accepted last.
 Sequence uint64
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

// AgentServer receives callbacks: an agent receives those of its cell's controller,
// and a controller receives those of a namespace's root.
type AgentServer interface {
 Do(ctx context.Context, md Metadata, r Request) error
}

// Dispatch returns an AgentServer that passes a request for a root scope to root, and
// a request for a cell to the agent that agents maps the request's namespace to. It
// returns an error of class errs.Transient for a namespace without an agent, as a
// transport does for an unreachable node. A process serves the result on its one
// callback endpoint.
func Dispatch(root AgentServer, agents map[shard.Namespace]AgentServer) AgentServer

// ControllerServer receives the reports of agents, the lookups of routers, and the
// operations and disruptions of operators and cluster managers. Each method serves the
// scope that its message names.
type ControllerServer interface {
 // Report accepts one report of an agent.
 Report(ctx context.Context, md Metadata, r Report) (Ack, error)
 // Lookup returns the placements of a.Scope that contain a.Positions, and up to
 // a.Prefetch placements after the last of them, in position order.
 Lookup(ctx context.Context, md Metadata, a LookupArgs) (LookupReply, error)
 // Submit applies a proposal of a kind that this controller applies, as the table of
 // proposal kinds assigns it, and records md.Caller with it.
 Submit(ctx context.Context, md Metadata, s config.Signed) (Receipt, error)
 // Disrupt admits a planned disruption under the budget of its nodes' cell. Every
 // node of d is in one cell.
 Disrupt(ctx context.Context, md Metadata, d budget.Disruption) (budget.Decision, error)
 // Done ends the reservation a.ID.
 Done(ctx context.Context, md Metadata, a DoneArgs) error
}

// Receipt is a controller's reply to a proposal that it accepted.
type Receipt struct {
 Incarnation shard.NamespaceIncarnation
 // Applied reports whether the controller committed the proposal. An explain
 // proposal commits nothing.
 Applied bool
 // Generation is the generation of the scope that applied the proposal.
 Generation epoch.Epoch
 // Result is JSON: the warnings of Resolve for a spec, the budget's decision for a
 // drain, or the constraints and goal costs for an explain proposal.
 Result []byte
}

// LookupArgs is the argument of Lookup.
type LookupArgs struct {
 Scope       shard.Scope
 Incarnation shard.NamespaceIncarnation
 Positions   [][]byte
 Prefetch    int
}

// LookupReply is the result of Lookup.
type LookupReply struct {
 Located []Located
}

// DoneArgs is the argument of Done.
type DoneArgs struct {
 Scope       shard.Scope
 Incarnation shard.NamespaceIncarnation
 ID          string
}

// CallError is an error as a transport returns it in place of a result: its class
// under errs.Classify and its message. The caller's transport returns an error of
// the same class.
type CallError struct {
 Class   errs.Class
 Message string
}

// AgentClient sends callbacks to agents in other processes. Transport adapters
// implement it.
type AgentClient interface {
 Do(ctx context.Context, node shard.Node, r Request) error
}

// ControllerClient sends reports, lookups, operations and disruptions to the controller
// of a cell, or to the root when the cell's ID is empty. The root's cell is the root
// leader's record in the root scope. Transport adapters implement it, fill each call's
// metadata, and check that the controller presents cell's ControllerIdentity.
type ControllerClient interface {
 Report(ctx context.Context, cell shard.Cell, r Report) (Ack, error)
 Lookup(ctx context.Context, cell shard.Cell, a LookupArgs) (LookupReply, error)
 Submit(ctx context.Context, cell shard.Cell, s config.Signed) (Receipt, error)
 Disrupt(ctx context.Context, cell shard.Cell, d budget.Disruption) (budget.Decision, error)
 Done(ctx context.Context, cell shard.Cell, a DoneArgs) error
}

// Network connects the servers of the processes of one simulation or test by direct
// calls, without an encoding. It is safe for concurrent use.
type Network struct {
 // unexported fields
}

// NewNetwork returns a network without servers.
func NewNetwork() *Network

// AddAgent makes the agent of node in namespace ns reachable through every Local of the
// network.
func (n *Network) AddAgent(ns shard.Namespace, node shard.NodeID, s AgentServer)

// AddController makes the controller of scope s, the root when s.Cell is empty,
// reachable through every Local of the network.
func (n *Network) AddController(s shard.Scope, srv ControllerServer)

// Local returns the client of the process whose calls have the metadata md: md.Caller
// is the process's identity, and md.Version and md.Requires are what its calls
// declare. A test gives processes other versions or features to check that receivers
// refuse them.
func (n *Network) Local(md Metadata) *Local

// Local is one process's client of a Network. It implements AgentClient and
// ControllerClient, passes the process's metadata with every call, and checks the
// metadata with Metadata.Check before it calls a server, as a transport adapter does.
type Local struct {
 // unexported fields
}

// NewLocal returns the client of a single process with the metadata md, over a network
// of its own. The process registers its own servers on l.Network(), so that a
// controller calls the agent of its own process without an encoding.
func NewLocal(md Metadata) *Local

// Network returns the network that l calls through.
func (l *Local) Network() *Network

// Do passes r to the agent that the network registered for r.Scope.Namespace and
// node.ID. Without such an agent, it returns an error of class errs.Transient, as a
// transport does for an unreachable node.
func (l *Local) Do(ctx context.Context, node shard.Node, r Request) error

// Report passes r to the controller that the network registered for r.Scope. Without
// such a controller, it returns an error of class errs.Transient.
func (l *Local) Report(ctx context.Context, cell shard.Cell, r Report) (Ack, error)

// Lookup passes a to the controller that the network registered for a.Scope. Without
// such a controller, it returns an error of class errs.Transient.
func (l *Local) Lookup(ctx context.Context, cell shard.Cell, a LookupArgs) (LookupReply, error)

// Submit passes s to the controller that the network registered for the proposal's
// scope. Without such a controller, it returns an error of class errs.Transient.
func (l *Local) Submit(ctx context.Context, cell shard.Cell, s config.Signed) (Receipt, error)

// Disrupt passes d to the controller that the network registered for d.Scope. Without
// such a controller, it returns an error of class errs.Transient.
func (l *Local) Disrupt(ctx context.Context, cell shard.Cell, d budget.Disruption) (budget.Decision, error)

// Done passes a to the controller that the network registered for a.Scope. Without
// such a controller, it returns an error of class errs.Transient.
func (l *Local) Done(ctx context.Context, cell shard.Cell, a DoneArgs) error

var (
 _ AgentClient      = (*Local)(nil)
 _ ControllerClient = (*Local)(nil)
)
```

| Condition | Error class at the caller |
|---|---|
| The call's version or a feature it requires is not implemented by the receiver | `errs.Unsupported` |
| The receiver does not implement the callback or the operation kind | `errs.Unsupported` |
| The request's controller epoch is above the controller record that the receiver has read | `errs.Transient` |
| The request starts a tenure that the receiver's watched placements do not list before the call's context ends | `errs.Transient` |
| The caller's identity is not the identity that the authorisation rules require | `errs.Denied` |
| The message names another namespace incarnation, or another incarnation of the node | `errs.Conflict` |
| The request's shard epoch, controller epoch or operation is older than the node's | `errs.Conflict` |
| The request's key was seen with another digest | `errs.Invalid` |
| The step's timeout passed | `errs.Transient` |
| The handler returned an error | The handler error's class |

A transport adapter keeps each error's class across the wire. `httptransport` returns a `CallError` in the response body, and the gRPC adapter maps each class to a gRPC status. The transition engine retries only `errs.Transient`, so a stale epoch or incarnation ends the step at once, for every call, and a callback that arrives before its controller's record is retried. A handler error without a class is `errs.Unspecified`, which core's `errs.Retryable` reports as not retryable. A handler whose failure a retry can fix classifies its error as `errs.Transient`.

At the default cell bounds, a node of the support matrix has about 200 replicas, and by our estimate its report is about 20 KiB. A report is always one message, because this proposal does not chunk reports.

### Transport security

The embedder is responsible for transport security, and the root module contains no transport:

- A transport adapter serves on the embedder's server and dials with the embedder's credentials: mutual TLS, a service mesh, or whatever else the adapter supports. The library does not set a TLS version, a cipher or a certificate policy.
- The adapter authenticates each caller and passes the caller's identity to `AgentServer` and `ControllerServer` in the call's metadata.
- The store and membership adapters take clients that the embedder built.
- `sharderd`, the program of the `service` topology, is its own embedder, and its `main` package chooses its adapters and configures its credentials.

Whether an authenticated caller may make a call depends on sharder's state: which identity is a cell's current controller, and which identity is node N. The transport knows neither. In the `embedded` topology, every application process can run a controller and must be able to call every agent. Without a check against that state, any process that the transport lets call an agent can fence any writer with a forged `add` at a high epoch, or retire any shard with a forged `supersedes` list. The core issues epochs one at a time from 1, so both are permanent in practice.

The checks limit callbacks to the process that the store names as the cell's controller, and refuse a replaced controller. They do not protect a namespace from a compromised process that can run a controller. In the `embedded` topology, every application process has a controller's store credentials and can write the cell's record directly. The processes of one namespace therefore trust each other.

The library checks each call against its own state, with the identity that the transport adapter passes:

1. Each node registers its identity with its membership session, in `shard.Node.Identity`. Each cell's controller writes its identity to the cell's record when it acquires the cell's scope, and the root's leader campaigns under its identity.
2. An agent accepts `Do` only from the identity that its scope names as its controller, and a controller accepts the callbacks of a root only from the root's leader. Each also refuses a controller epoch below the highest it has seen, and a request for another namespace incarnation or another incarnation of itself. A request whose controller epoch is above the record that the receiver has read returns an error of class `errs.Transient`, and the receiver raises its highest controller epoch only from a request that passed the identity check, and from its watch.
3. A cell's controller accepts a `Report` for node N only from N's registered identity, for N's live incarnation.
4. A controller authorises every `Submit`, `Disrupt` and `Done` by the kind of the operation. A kind without an approval policy, which is every kind in the support matrix, needs an operator of the namespace. A node may disrupt only itself, at its live incarnation, and `Done` comes only from the reservation's owner or an operator.

A call without an identity is denied. A deployment whose transport authenticates no caller selects unauthenticated mode explicitly, with `controller.WithUnauthenticated` and the agent's `Config.Unauthenticated`. That mode skips every identity check. Any process that can call an agent can then take or retire any shard, and the deployment relies on a network policy to limit which processes can call agents.

`Lookup` returns placements and addresses without changing any state, so the embedder authorises its callers, in the transport adapter or with a mesh policy. The shard and controller epoch checks apply whatever the transport.

Identities, cell records and membership sessions are in the store, so the store's access control is the root of trust for rules 2 and 3. A deployment gives each role its own store credentials: controllers write the root and cell scopes, nodes write their own membership keys, and routers only read. `sharderctl` reads the store to find a controller and for `status` and `history`, so an operator needs read credentials. A cluster manager that knows a controller's address needs none. Every change goes through an authenticated call to a controller, so neither needs write credentials. The operators of rule 4 come from the process's own configuration, never from the store, so a writer to the store cannot grant itself operator rights.

### Disruption budgets

```go
// Package budget admits planned disruptions under each namespace's caps, and records
// each admitted disruption as a reservation.
package budget

import (
 "context"
 "time"

 "go.thesmos.sh/core/crypto"
 "go.thesmos.sh/core/epoch"
 "go.thesmos.sh/sharder/shard"
)

//go:generate go tool kanon -type=Disruption,Decision,Reservation

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

// Disruption is a planned operation on nodes of one cell.
type Disruption struct {
 // ID is unique within the scope. A Disrupt with an ID that the scope has reserved
 // evaluates a pending reservation again, and returns Allowed for an admitted one.
 ID          string
 Scope       shard.Scope
 Incarnation shard.NamespaceIncarnation
 Nodes       []shard.NodeID
 // NodeIncarnation is the incarnation of the one node of a disruption that the node
 // requests for itself. The controller refuses it when the node's live incarnation
 // differs. It is zero in an operator's disruption.
 NodeIncarnation epoch.Epoch
 Impact          Impact
 Start           time.Time
 End             time.Time
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

// Reservation is a disruption that the budget deferred or admitted, as the cell's scope
// records it. It is pending while Admitted is zero. Only an admitted reservation
// covers its nodes for the budget.
type Reservation struct {
 Disruption Disruption
 // Digest is the SHA-256 digest of the disruption's encoding. A Disrupt with the same
 // ID and another digest fails with an error of class errs.Invalid, whether the
 // reservation is pending or admitted.
 Digest crypto.Digest
 // Owner is the identity that the disruption was requested for.
 Owner shard.Identity
 // Incarnations maps each node of the disruption to its incarnation when the
 // reservation was recorded.
 Incarnations map[shard.NodeID]epoch.Epoch
 // Admitted is the instant of the admission, or zero while the reservation is
 // pending.
 Admitted time.Time
}

// Budget admits the disruptions of one cell.
type Budget interface {
 // Admit decides d for owner. Before it returns a deferral, it commits a pending
 // Reservation with the drains of d's nodes, and before it returns a Decision that
 // allows d, it commits the admission. A repeated d evaluates a pending reservation
 // again, and returns Allowed for an admitted one. It returns an error of class
 // errs.Invalid for a d whose ID the scope has recorded with another digest.
 Admit(ctx context.Context, owner shard.Identity, d Disruption) (Decision, error)
 // Done ends the reservation id: it releases an admitted reservation, and cancels a
 // pending one together with its drains.
 Done(ctx context.Context, id string) error
}
```

The budget admits a negotiable disruption when these conditions remain true after it starts:

- No shard has more unavailable replicas than the spec's `maxUnavailable`. The budget counts each replica once, whether it is unavailable because its node failed, because a transition moves it, or because an admitted reservation covers its node.
- At most `maxConcurrentNodes` nodes are covered by admitted reservations at once.

The transitions that a drain or a repair starts run within the budget's bounds on work in flight: `maxTransitions` per cell, and `maxNodeTransitions` and `nodeCallbacks` per node.

Before it admits a disruption, the core moves the replicas in `drainRoles` off the disrupted nodes through the handoff protocol, and returns a deferral meanwhile. With the first deferral, it commits the disruption as a pending reservation, with its digest and its owner, together with a `store.Drain` for each of its nodes. A new controller reads both and continues the drain. A repeated `Disrupt` evaluates admission again, and the commit that admits the disruption sets the reservation's `Admitted`. A non-negotiable disruption is admitted at its `Start`, whatever the budget decides.

One commit removes a pending reservation together with its drains, when:

- the disruption's `End` passes before the admission, because the disruption's window has closed
- `Done` cancels it, from the reservation's owner or an operator
- for a node's own disruption, the node's incarnation ends before the admission

The default `drainRoles` is the model's writer role, because most SM applications drain their primaries and leave their secondaries in place. In the support matrix, every replica is a writer. A node is therefore empty when its disruption is admitted. In a `delegated` namespace, the core runs `abdicate` for each writer on the disrupted nodes. A writer that the authority does not move keeps the disruption deferred.

An admitted reservation remains until its disruption ends:

- `Done` ends it, from the reservation's owner or an operator.
- The controller ends it by itself once every node that it covers has left the cell, or has rejoined with a new incarnation, and no placement names the node's old replicas.
- The disruption's `End` time does not end it, because the disruption may have started and not finished.

A new controller of a cell reads every pending and admitted reservation and every drain from the scope, as part of its acquisition's snapshot, before it admits anything. It continues the drain of each pending reservation, and ends each reservation by the rules of its state. Until every live node of the cell has sent it a report, it admits no planned disruption, so that failures that it has not observed yet cannot let it over-admit. Failover and repair run meanwhile.

SM's TaskController works the same way: it approves a subset of pending container operations under a global cap and a per-shard cap that count replicas already down. A non-negotiable disruption is SM's non-negotiable event, which arrives with notice and an impact class. The budget cannot delay it, so the core uses the notice to move replicas away first. Kubernetes records the same gap between an admitted eviction and its observation, in a pod disruption budget's `disruptedPods`.

Every replica of a shard is on a node of the shard's cell, so each cell's controller enforces the budget of its own cell. A cluster manager calls `Disrupt` on the controller of the disrupted nodes' cell, and `Done` when the disruption ends. A process's shutdown hook does the same for its own node through `Agent.Close`. `Disrupt` returns the budget's `Decision`. A cluster manager whose disruption is deferred calls `Disrupt` again after `RetryAfter`, as Twine notifies SM's TaskController of its pending operations again at each period and delays the ones that the TaskController has not approved.

### Operations

A command that changes a namespace sends a proposal to the controller that applies it, through `Submit`, and prints the `Receipt` or the error that `Submit` returns. `sharderctl` finds the controller in the controller's record: the cell's record for a cell, and the root leader's record for the root. `status` and `history` read the store and do not need a controller. `why` submits an `explain` proposal, and the cell's controller returns the explanation without committing anything. A proposal whose controller fails over before it replies returns `errs.Transient`, and `sharderctl` sends it again to the successor. The successor reports an ID that its scope has already applied instead of applying it twice. `sharderctl` reports a proposal as applied when the scope's `Applied` contains its ID. An ID that `Applied` lacks means that the outcome is unknown, not that the proposal was rejected, because the scope forgets an ID once its `Expires` passes.

| Command | Does | In the support matrix |
|---|---|---|
| `sharderctl status <ns>` | Prints the cells, the generation of each, the accepted spec version and each cell's applied version, the shards per node, the transitions in flight, the pending and admitted reservations, the drains, the unplaced shards, and the ownership guarantee | Yes |
| `sharderctl apply <spec>` | Proposes a new spec version and prints the root's reply: the acceptance with the warnings of `Resolve`, or the reasons for rejecting it | Yes |
| `sharderctl drain <node>` / `undrain` | Adds the node's manual drain, which moves every replica off the node under the budget, or removes that drain, and prints the budget's decision. A disruption's drain of the node remains in effect | Yes |
| `sharderctl move <shard> <node>` | Moves one shard's writer to a node of its cell through the handoff protocol | Yes |
| `sharderctl why <shard>` | Prints the constraints and goal costs that placed a shard where it is | Yes |
| `sharderctl history <ns> <cell>` | Lists recent generations of a cell and what changed in each | Yes |
| `sharderctl reshard <ns> --factor <k>` | Multiplies or divides a `fixed-slots` namespace's slot count | No. `Submit` returns an error of class `errs.Unsupported` |
| `sharderctl cell split <ns> <cell>` | Divides a cell at the shard boundary nearest its median, and prints the nodes to relabel | No. `Submit` returns an error of class `errs.Unsupported` |
| `sharderctl sign <proposal>` | Signs a proposal with the operator's key and prints the signature | No. The command is absent |
| `sharderctl submit <proposal> <signature>...` | Sends a signed proposal to the controller that applies it, and prints the controller's reply | No. The command is absent |
| `sharderctl anchor <proposal>` | Prints the digest of a namespace's first spec proposal, which each process's `config.Trust` names as its anchor | No. The command is absent |

The core validates each operation as it validates a placer's plan, before the operation's controller commits anything. A manual move that breaks a hard constraint or a role bound is refused. In a namespace with approvals, outside the support matrix, `apply`, `drain`, `undrain`, `move`, `reshard` and `cell split` print a proposal instead of sending it, and `submit` sends the proposal once enough approvers have signed.

The library reports these instruments through core's `telemetry.Reporter`. Each binds its attributes, such as the namespace and the cell, once with `With`, so that every emission is free of allocations, as core's telemetry contract requires.

| Instrument | Kind | Unit | Meaning |
|---|---|---|---|
| `sharder.generation.lag` | Gauge | `{generation}` | Generations between the store and each agent or router |
| `sharder.transitions.in_flight` | Gauge | `{transition}` | Transitions started and not finished, per protocol and phase |
| `sharder.writer.gap` | Histogram | `s` | Time from a writer's stop to the next writer's first serving report, per reason: planned move, failure |
| `sharder.shards.unplaced` | Gauge | `{shard}` | Shards the placer could not place |
| `sharder.planner.pass.duration` | Histogram | `s` | Duration of each planning pass, per mode |
| `sharder.moves` | Counter | `{move}` | Replicas moved, per reason: failure, rebalance, drain |
| `sharder.commits.unknown` | Counter | `{commit}` | Commits that failed with an unknown outcome, per scope |
| `sharder.lease.renewal.duration` | Histogram | `s` | Latency of lease renewals per node |
| `sharder.budget.deferred` | Counter | `{disruption}` | Disruptions and transitions the budget deferred |
| `sharder.budget.reservations` | Gauge | `{reservation}` | Reservations that have not ended, per cell and state: pending, admitted |
| `sharder.fence.refusals` | Counter | `{acquisition}` | Fence acquisitions that returned `epoch.ErrFenced` |
| `sharder.router.lookups` | Counter | `{lookup}` | Route lookups, per result: hit, miss, redirect |
| `sharder.router.cache.routes` | Gauge | `{route}` | Routes in a router's cache, per cell |

The library logs through `log/slog`, as core does.

## Alternatives considered

### Applications implement the RPC service directly

Define only the wire protocol, as SM does, and let each application implement it without a Go library. There is then nothing to embed.

**Why not:** the agent's work is more than the calls. It keeps the highest epoch per shard, refuses stale callbacks, deduplicates repeated keys, runs the lease timer, acquires fences, restores replicas at start-up and moves them at shutdown. Each application would re-implement that work, and the library is Go-only.

### A proxy in front of the servers

Route requests through a proxy tier that knows the assignment, as Vitess routes through its gateway. Clients then run without a router, and a proxy can buffer during moves.

**Why not:** a proxy adds a network hop and a tier to scale and operate for every request. SM, Slicer and Dicer all route in a client library for the same reason. An application that wants a proxy can build one on the router.

### A full copy of every route in every router

Keep the whole assignment of every cell in every router, as the `watch` mode does for the cells a router uses.

**Why not as the only mode:** at 1,000,000 shards per cell, a sorted index costs about 40 MB per cell in every client process, and every client adds a watch connection to the store, at 17 KB of memory each in etcd. Bigtable and CockroachDB cache locations lazily for the same reason. `watch` is the default for namespaces small enough that a full copy is cheaper than a lookup, and the support matrix contains it alone.

### A sorted slice for the route cache

A sorted slice has the fastest lookup and the smallest entries.

**Why not:** a sorted slice copies every entry to publish one change: 12.7 ms and 24 MB per change at 1,000,000 entries. A copy-on-write B-tree publishes a change in about 3 µs at nearly the same lookup cost.

### Redirects without refresh

Reply to every misrouted request with the new owner and let the client update only that entry, as Redis Cluster does.

**Why not:** one generation can move many shards, such as every shard of a failed node. Updating one entry at a time causes one misrouted request per moved shard. Redis measured this cost in its own migrations: its atomic slot migration generates up to 116 times fewer redirects than its legacy migration. A router in `watch` mode waits for the cell's generation, and one in `lookup` mode refetches the evicted route with its neighbours.

### Retrying every failed request in the router

Let `router.Do` retry a request after any error, not only after `NotOwnerError`, so that the application never sees a transient network error.

**Why not:** a request whose reply is lost may have taken effect, and a router that sends it again repeats a write that is not idempotent. Only `NotOwnerError`, which a server returns before it starts a request, proves that the request did not run. The application knows which of its requests are idempotent, and retries those itself.

### Protocol versions as field numbers alone

Rely on numbered fields for rolling upgrades, as protobuf does, and let a receiver ignore the fields it does not know.

**Why not:** protobuf's rules keep old and new messages decodable by each other, and do not make a receiver understand a field it does not know. A new field can contain a precondition, such as a fence or a phase, whose absence changes what the receiver must do. A receiver that ignores it acts on part of the message. A required feature makes an old receiver refuse the call instead.

### Kubernetes pod disruption budgets instead of a shard budget

Use Kubernetes' own disruption budgets, which limit how many pods of a workload an eviction may take down.

**Why not:** a pod disruption budget counts pods, not replicas of a shard. It treats two pods with replicas of the same shard as independent. The shard budget reads the assignment.

### Operations through the store

Let `sharderctl` and cluster managers write each operation as a pending proposal into the scope of the controller that applies it, and let the controller watch the scope and commit each outcome there. An operation then remains in the store across a controller's failover, and the protocol does not need calls for operations.

**Why not:**

- Operators and cluster managers would need write credentials for the store, and the store's access control would decide who may operate a namespace. A writer to the store could grant itself operator rights.
- Every store adapter would need an unfenced write path next to its fenced commits.
- An operation's result would arrive on a watch, one event after the commit, instead of as the reply to a call.

The controller still persists what it accepts: it commits each admitted disruption as a reservation, and each applied proposal with its caller, before it replies.

## Drawbacks

- The agent defines eight application interfaces. Most applications implement one or two, and the support matrix needs only `Handler`, but the documentation and the conformance suite cover all eight.
- The library's checks are only as strong as the transport's authentication. An adapter that passes an identity that it did not prove lets any caller pass them.
- A node registers its own identity through the store, so a writer to the store can register an identity of its own. Only the store's access control prevents it.
- Capabilities are detected at runtime, so an application that omits an interface finds out from the controller's report, not from the compiler.
- `Close` makes a deploy wait for its node's replicas to move. A node with 200 replicas moves them 16 at a time by default, in at least 13 rounds: about 26 seconds at the writer gap's target of 2 seconds, before planning, retries and the handlers' own work. Kubernetes' default grace period is 30 seconds, so the whole drain needs measuring, and a node with more replicas needs a longer grace period. A `Close` that runs out of time leaves a reservation that the budget counts until the controller has replaced the node's replicas.
- An `add` starts only once the agent's watch has delivered the commit that placed the replica, so each move's writer gap includes that delivery, which we have not measured. An agent that cannot watch its cell's scope starts no tenure, and every move to its node aborts.
- The bounds on transitions in flight trade the speed of a drain for the number of moves in flight, at most `maxTransitions`, 64 by default, per cell. They do not bound the shards that are without a serving writer outside a transition.
- `router.Do` retries only redirects. An application whose requests fail on a transport error retries them itself, with its own idempotency.
- A report is a full snapshot of its node's replicas, so its size grows with the replicas per node, and the protocol has no chunking.
- Every message names its scope and its namespace incarnation, which adds about 40 bytes to each message by our estimate.
- The library ships two transport adapters, and each maps the protocol's Go types and metadata to its own encoding. Both must keep their field numbers, their metadata and the error classes stable across releases.
- Operator identities are part of each controller process's configuration. Adding or removing an operator changes the configuration of every controller process of the namespace.
- A router in `lookup` mode, outside the matrix, pays a lookup round trip for each cold route, and `batch.Loader` adds its accumulation window to that round trip. `batch.Loader` also passes the lookup function a context without any caller's values, so a lookup's trace does not join the trace of the request that missed.
- The route cache of the range partitioners, outside the matrix, needs core's copy-on-write B-tree, which core does not contain at commit `8edf26a`.

## Open questions

None.

## Unresolved and future work

- The `lookup` and `compute` sources and `hold` are outside the support matrix. `lookup` needs ordering rules for overlapping replies, `compute` needs routers that read each shard's election, and `hold` needs a bounded queue with deadlines.
- The gRPC transport and `sharderd` are outside the support matrix.
- `Hosted`, `Released`, `Elected` and `Deposed` serve delegated ownership, which is outside the support matrix.
- Chunked reports for nodes with more replicas than one message can list are not proposed here.
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
| Kubernetes policy/v1 PodDisruptionBudget, `status.disruptedPods` | https://kubernetes.io/docs/reference/kubernetes-api/policy/pod-disruption-budget-v1/ |
| Microsoft Learn, Service Fabric `FaultType` | https://learn.microsoft.com/en-us/dotnet/api/system.fabric.faulttype?view=azure-dotnet |
| Kubernetes API concepts, Protobuf encoding | https://kubernetes.io/docs/reference/using-api/api-concepts/#protobuf-encoding |
| Protocol Buffers, proto3 language guide: updating a message type and unknown fields | https://protobuf.dev/programming-guides/proto3/#updating |
| go.thesmos.sh/ledger RFC-0011, "Cluster and Replication": planned transfers and leadership, and `Member.Replicas` | `docs/rfc/0011-replicated-storage.md` in the ledger repository |
| Go `net/http`, `Protocols` | https://pkg.go.dev/net/http#Protocols |
| tidwall/btree v1.8.1 | https://github.com/tidwall/btree |
| google/btree v1.1.3 | https://github.com/google/btree |
| go.thesmos.sh/core at 8edf26a: request coalescing (RFC-0024), telemetry (RFC-0004), structured concurrency (RFC-0030), error classes and `resilience/retrier.go` | https://github.com/thesmos-ai/core |
| Our measurement of sorted slices and B-trees, 2026-09-27 | Research-0003 |

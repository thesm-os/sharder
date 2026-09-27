---
rfc: 0007
title: Membership, the assignment store and assignment distribution
author: Roy Klopper
status: Draft
created: 2026-09-27
updated: 2026-09-27
discussion: none
supersedes: none
superseded-by: none
produces-adr: tbd
research: Research-0001, Research-0002, Research-0003
related: RFC-0001, RFC-0002, RFC-0006, RFC-0008, RFC-0009
---

# RFC-0007: Membership, the assignment store and assignment distribution

## Summary

Every strategy depends on these mechanisms:

- Membership reports which nodes of a cell are alive.
- The store persists the cell map, each cell's assignment, the transitions in flight, and the proposals that controllers applied.
- Distribution delivers each committed change to agents and routers.

We propose one interface for each, written in core's vocabulary: epochs from `epoch`, conditional-write errors from `version`, pages from `page`, and error classes from `errs`. The store and membership are interfaces with conformance suites. The root module contains an in-memory store and a static membership, and adapters in their own modules implement both over real backends. Each adapter reports its own limits, so the core depends on no backend. Each cell has its own scope in the store, and an adapter can place scopes on separate backend clusters, so the store scales with the number of cells.

Every commit goes through a writer bound to one epoch, and the store applies core's fence laws to it: a commit from a replaced controller fails with `epoch.ErrFenced`, and a commit whose generation moved fails with `version.ErrMismatch`. An agent that restarts reads its own placements from its cell's scope through a per-node index, and resumes its non-writer replicas before a controller contacts it.

## Motivation

These mechanisms set the bounds within which the strategies work:

- Detection time spans an order of magnitude across published defaults. Pekko's phi accrual detector triggers after about 5.5 seconds by default. Kubernetes marks a node unreachable after a 50-second grace period and waits 5 minutes before evicting its pods. Dicer detects a crashed pod in roughly 30 seconds.
- Gossip runs without a consensus store, and its views are weaker than a store's. SWIM's suspicion mechanism "reduces (but does not eliminate)" false failure detections, and Lifeguard cut the remaining false positives by more than 50 times. A gossip view cannot back a lease that a fence relies on.
- Every consistent store bounds the size of a commit and of a scope, and the bounds differ between stores. etcd, for example, allows 128 operations in one transaction and 1.5 MiB in one request by default, and suggests at most 8 GiB of data. A controller that writes a large plan as one commit fails at those limits, so the core has to learn them from the adapter.
- A consistent store bounds throughput and fan-out. etcd's documentation measures a three-member cluster at more than 30,000 requests per second under heavy load, and its watch benchmark measures 17 KB of server memory per client connection. Kubernetes found that node heartbeats brought etcd to its storage limit in clusters of more than 2,000 nodes whose Node objects listed many images, and moved the heartbeats to small Lease objects.
- The control plane can fail without taking the data plane down. SM's application servers read their assignment at start-up "without dependency on the SM control plane", and Slicer's clients keep routing on cached assignments when every component of the Slicer service fails.
- A deposed controller can continue to act as leader. Slicer's preferred-assigner rule is eventually consistent, and its storage concurrency control prevents divergence. Helix's issue 1027 traced double masters to two controller pipelines running at once.

Storage must also be replaceable. A team that runs another consistent store writes an adapter, and the conformance suite decides whether the adapter is correct. The root module then does not import the client of any store.

## Detailed design

### Membership

```go
// Package membership tracks the live nodes of each cell.
package membership

import (
 "context"
 "iter"

 "go.thesmos.sh/core/epoch"
 "go.thesmos.sh/sharder/shard"
)

// View is one list of the live nodes of a cell.
type View struct {
 Namespace shard.Namespace
 Cell      shard.CellID
 // Revision increases with every change of the view.
 Revision epoch.Epoch
 Nodes    []shard.Node
}

// Session is one incarnation of one node in one cell.
type Session interface {
 // Incarnation returns a value higher than the incarnation of every earlier session
 // of the same node.
 Incarnation() epoch.Epoch
 // Done is closed when the session's lease is lost: when the backend reports the
 // loss, or when the local deadline passes without an accepted renewal. The deadline
 // is a clock timer, armed for Lease.Validity when each renewal is sent.
 Done() <-chan struct{}
 // Leave ends the session and removes the node from the view at once.
 Leave(ctx context.Context) error
}

// Membership registers nodes in cells and reports which nodes are live. A node is
// live while the backend renews its lease.
type Membership interface {
 // Join registers n in the cell n.Cell of ns and renews its lease until ctx ends or
 // Leave is called.
 Join(ctx context.Context, ns shard.Namespace, n shard.Node) (Session, error)
 // Watch yields the current view of the cell, then every later view, in revision
 // order, until ctx ends.
 Watch(ctx context.Context, ns shard.Namespace, cell shard.CellID) iter.Seq2[View, error]
}
```

| Implementation | Module | Liveness mechanism | Detection time with defaults | Provides |
|---|---|---|---|---|
| `membership/static` | the root module | None. The embedder lists the nodes | Never | `ordinal-nodes` |
| etcd adapter | `go.thesmos.sh/sharder/etcd` | A key per node in the cell's scope, attached to a lease | The lease's TTL, 10 s | `leases` |

Each adapter declares its capabilities through `config.Provider`, and the controller checks the spec's requirements against them. Every implementation keeps each node's description with its session, including the node's identity, so the cell's controller knows which identity each node's reports must come from.

Each cell's nodes register in the cell's own scope, so the renewals of a namespace spread over its cells and over the backend clusters that store them. A cell of 5,000 nodes that renew every 3.3 seconds, a third of the default TTL, sends about 1,500 renewals per second to its store. We have not measured any backend's renewal throughput.

The core waits `failoverDelay` after a node's session ends before it moves the node's replicas, as SM lets applications configure. The default is zero, and every preset keeps it, so a failed writer is replaced as soon as its lease expires. An application whose replicas are expensive to rebuild sets a delay of its own. A node that restarts inside the delay registers a new incarnation, and the core restores its replicas through the normal protocols, with a new epoch for every writer.

### The store

```go
// Package store persists the cell map, the assignments and the transitions of
// namespaces, one scope at a time.
package store

import (
 "context"
 "errors"
 "iter"
 "time"

 "go.thesmos.sh/core/epoch"
 "go.thesmos.sh/core/errs"
 "go.thesmos.sh/core/page"
 "go.thesmos.sh/sharder/shard"
)

// Scope names one keyspace of a store: the root of a namespace when Cell is empty,
// or one of its cells.
type Scope struct {
 Namespace shard.Namespace
 Cell      shard.CellID
}

// ErrCompacted reports that a watch asked for a generation that the store no longer
// keeps. The watcher reads the scope again. It classifies as errs.Conflict.
var ErrCompacted = errs.WithClass(errors.New("store: generation compacted"), errs.Conflict)

// Transition records the progress of one move.
type Transition struct {
 Shard    shard.ID
 Sequence uint64 // counts the shard's transitions
 Protocol string
 Phase    uint8 // the transition's handoff.Phase
 Step     int
 OldEpoch epoch.Epoch
 NewEpoch epoch.Epoch
 Started  time.Time
}

// State is a scope's committed state apart from its placements.
type State struct {
 Generation    epoch.Epoch
 LayoutVersion epoch.Epoch
 // Cell is the record that the scope's controller writes in its first commit: the
 // cell's record in a cell's scope, and the root leader's record, with an empty ID, in
 // the root scope.
 Cell shard.Cell
 // Spec is the accepted spec, with the proposal and the signatures that approved
 // it, as JSON, in the root scope only.
 Spec []byte
 // Approvals lists every version of the namespace's approvals section in order,
 // each as the signed spec proposal that introduced it, in the root scope only.
 Approvals [][]byte
 // Applied maps the ID of each applied proposal to its Expires, until Expires
 // passes.
 Applied     map[string]time.Time
 Transitions []Transition
}

// Query selects the placements that Read returns. The zero Query selects every
// placement of the scope, in position order.
type Query struct {
 // Node selects the placements with a replica on Node.
 Node shard.NodeID
 // From starts at the placement whose range contains the position From.
 From []byte
}

// Mutation is the set of changes one commit applies to a scope.
type Mutation struct {
 Put         []shard.Placement
 Delete      []shard.ID
 Transitions []Transition // created or advanced
 Done        []shard.ID   // shards whose transition ended
 // Cell replaces the cell's record. Nil leaves it unchanged.
 Cell *shard.Cell
 // Spec replaces the root scope's spec. Nil leaves it unchanged.
 Spec []byte
 // Approvals appends the next version of the approvals section. Nil appends nothing.
 Approvals []byte
 // Applied records the proposals that the commit applies.
 Applied []Applied
 // Forget removes applied proposals whose Expires has passed.
 Forget []string
 // LayoutVersion replaces the layout version. epoch.Zero leaves it unchanged.
 LayoutVersion epoch.Epoch
}

// Applied is one proposal that a commit applies.
type Applied struct {
 ID      string
 Expires time.Time
 // Signed is the proposal with its signatures, as JSON.
 Signed []byte
 // Caller is the identity that submitted the proposal, as the transport adapter
 // authenticated it.
 Caller shard.Identity
}

// Event is one committed generation of a scope.
type Event struct {
 Generation epoch.Epoch
 Mutation   Mutation
}

// Limits bounds what one commit may contain. The controller splits a larger plan
// across consecutive generations, each a valid assignment.
type Limits struct {
 // MaxPlacements is the most placements that one commit may put or delete,
 // together with their transitions.
 MaxPlacements int
 // MaxBytes is the most bytes that one commit may write.
 MaxBytes int
}

// Leadership is one tenure in an election. It lasts until Done is closed.
type Leadership interface {
 // Term is the tenure's fence epoch. The store grants each term of an election to
 // one candidate.
 Term() epoch.Epoch
 Done() <-chan struct{}
 Resign(ctx context.Context) error
}

// Writer commits to one scope under one fence epoch, bound when the Writer is created.
type Writer interface {
 // Commit applies m as the generation after expect. It returns version.ErrMismatch
 // when the scope is not at generation expect, and epoch.ErrFenced when the scope's
 // watermark is above the writer's epoch. A commit that returns an error changes
 // nothing. An admitted commit advances the watermark to the writer's epoch.
 Commit(ctx context.Context, expect epoch.Epoch, m Mutation) (epoch.Epoch, error)
}

// Store persists namespaces, one scope at a time. Implementations are safe for
// concurrent use.
type Store interface {
 // Read returns the scope's state and a cursor over the placements that q selects,
 // one page per call. Every page of one Read is at the state's generation.
 Read(ctx context.Context, s Scope, q Query, p page.Page) (State, page.Cursor[shard.Placement], error)
 // Watch yields every generation of s later than from, once each and in order, until
 // ctx ends. With node set, each event contains only the changes of placements that
 // have or had a replica on node, and the changes of the cell's record. It yields
 // ErrCompacted when from is older than the store's history.
 Watch(ctx context.Context, s Scope, from epoch.Epoch, node shard.NodeID) iter.Seq2[Event, error]
 // Writer returns a writer for s bound to term. It performs no I/O.
 Writer(s Scope, term epoch.Epoch) Writer
 // Campaign blocks until candidate leads election in scope s, or until ctx ends.
 // The root campaigns in its scope for the election "root". In the leaderless
 // topology, a shard's writer campaigns in the cell's scope for the shard's ID.
 // candidate is the process's identity when the transport authenticates peers.
 Campaign(ctx context.Context, s Scope, election, candidate string) (Leadership, error)
 // Limits returns the bounds of one commit.
 Limits() Limits
 Close() error
}
```

The store keeps one watermark per scope and applies core's fence laws to every commit. A commit is admitted when its writer's epoch is at or above the watermark, a greater epoch advances the watermark, and the check runs in the same transaction as the commit. A cell's controller writes with the epoch that the root granted it, and the root writes with the term that `Campaign` granted it.

A controller's first commit in a scope advances the watermark to the controller's epoch before the controller reads the scope, so every later commit of the controller it replaced fails, as a fenced writer's acquisition does before its first read. A controller's first commit writes its record: its own address, its identity and its epoch. The root's record is a `shard.Cell` with an empty ID, in the root scope, and its epoch is the root's term. Agents read the record to learn which identity their controller's calls must come from, `sharderctl` reads it to learn where to submit an operation, and a replaced controller cannot overwrite it.

A backend qualifies when it provides these mechanisms:

- an atomic commit of one scope's changes, conditioned on the scope's generation and watermark
- an ordered change feed of each scope that resumes from a generation, by push or by polling
- an election that grants each term to one candidate, and ends a term within a bounded time after its holder fails

A store needs leases that expire on a timer only when it also provides the membership, or runs the per-shard elections of the leaderless topology. An election can instead come from the backend's own failure detection, such as the leader election of a consensus group, whose term ends when the leader loses the support of its quorum. A candidate waits for the backend to elect it, and never takes a term from a live holder.

The ledger is such a backend. Each shard's voters elect its leader and grant its epoch, and at most one candidate collects a fence quorum for an epoch, so each epoch has one leader. A leader's term ends when a write quorum of its voters stops supporting it, within 3 seconds by default. The ledger commits by compare-and-swap on a stream's head, and a reader in another process polls, because the ledger has no cross-process push.

#### Laws and conformance

Every store adapter passes `store/storetest`, which checks these laws:

1. A commit at a generation other than the scope's returns `version.ErrMismatch` and changes nothing.
2. Every `Writer` is a fenced writer. The suite runs core's `coretest/epochtest.AssertFencedWriter` over it: a commit through a superseded writer returns `epoch.ErrFenced`, classifies as `errs.Conflict` and changes nothing, and a writer at the current epoch commits.
3. Every page of one `Read` is at one generation, and walking the pages to the end yields every selected placement once.
4. `Watch` yields every generation after the given one exactly once, in order. A node-filtered `Watch` and `Read` return exactly the placements with a replica on the node.
5. `Campaign` grants each term of an election to one candidate. Core's admit-equal law is sound only under this condition.
6. A commit within `Limits` succeeds unless its generation moved or its writer is superseded.
7. A commit that returned is readable, with the same generation, after the store is opened again.

```go
// Package storetest checks a store.Store against the laws of package store.
package storetest

import (
 "testing"

 "go.thesmos.sh/sharder/store"
)

// Option configures a run of AssertStore.
type Option func(*config)

type config struct {
 reopen func(t *testing.T, s store.Store) store.Store
}

// WithReopen gives the suite a way to open the storage behind s again, as after a
// process restart. The suite then checks that every commit that returned is readable
// with the same generation.
func WithReopen(reopen func(t *testing.T, s store.Store) store.Store) Option

// AssertStore checks every law of package store against stores that newStore returns.
// It creates a new store for each case, so cases cannot observe each other's scopes.
func AssertStore(t *testing.T, newStore func(t *testing.T) store.Store, opts ...Option)
```

The suite follows core's own conformance suites for storage, which check every adapter against the laws of its package and across a restart. The in-memory store, `store/memory`, is its first subject.

### The controller

One `Controller` serves every namespace of its process. It takes the store, the membership and the client for calls to agents as arguments, and core's seams as options. It resolves every other strategy of a namespace from the namespace's spec through the registry, with the capabilities that the store and the membership provide. The controller implements `protocol.ControllerServer`, which the embedder serves through a transport adapter.

```go
// Package controller runs the root and cell controllers of namespaces: the planning
// loop, the transition engine and the disruption budget.
package controller

import (
 "context"
 "log/slog"

 "go.thesmos.sh/core/clock"
 "go.thesmos.sh/core/rand"
 "go.thesmos.sh/core/telemetry"
 "go.thesmos.sh/sharder/budget"
 "go.thesmos.sh/sharder/config"
 "go.thesmos.sh/sharder/membership"
 "go.thesmos.sh/sharder/protocol"
 "go.thesmos.sh/sharder/shard"
 "go.thesmos.sh/sharder/store"
)

// Topology is where the controllers of a namespace run.
type Topology string

const (
 // Embedded runs controllers in the application processes of the namespace.
 Embedded Topology = "embedded"
 // Service runs controllers in dedicated sharderd processes that serve many
 // namespaces.
 Service Topology = "service"
 // Leaderless runs without a controller. Each agent computes placement with a
 // function placer.
 Leaderless Topology = "leaderless"
)

// Controller campaigns for the root of each namespace it is given, and hosts the cells
// that the root assigns to it.
type Controller struct {
 // unexported fields
}

var _ protocol.ControllerServer = (*Controller)(nil)

// Option configures a Controller.
type Option func(*options)

type options struct {
 clock        clock.Clock
 rand         rand.Rand
 telemetry    telemetry.Reporter
 logger       *slog.Logger
 trust        map[shard.Namespace]config.Trust
 operators    map[shard.Namespace][]shard.Identity
 cellCapacity int
}

// WithClock sets the clock that every lease, timer and deadline of the controller
// reads. The default is a clock/hlc clock.
func WithClock(c clock.Clock) Option

// WithRand sets the source from which the controller seeds each planning pass. The
// default is a rand/pcg source with a seed from rand/crypto.
func WithRand(r rand.Rand) Option

// WithTelemetry sets the reporter of the controller's metrics and traces. The default
// is telemetry/noop.
func WithTelemetry(r telemetry.Reporter) Option

// WithLogger sets the controller's logger. The default is slog.Default.
func WithLogger(l *slog.Logger) Option

// WithTrust makes the controller apply only the specs and proposals of namespace ns
// whose approvals verify from the anchor in t. A namespace without an anchor accepts
// every spec.
func WithTrust(ns shard.Namespace, t config.Trust) Option

// WithCellCapacity sets how many cells the root may place on this controller process,
// as the process's "cells" capacity in the root. The default is one.
func WithCellCapacity(n int) Option

// WithOperators sets the identities from which the controller accepts the operations
// of namespace ns that do not need approval, and every planned disruption. They come from
// the process's own configuration, never from the store. Without it, the controller
// accepts such calls from no identity.
func WithOperators(ns shard.Namespace, ids ...shard.Identity) Option

// New returns a controller that keeps namespace state in st, follows the live nodes
// of each cell through m, and calls agents through agents. It builds each namespace's
// strategies from the namespace's spec with reg.
func New(reg *config.Registry, st store.Store, m membership.Membership, agents protocol.AgentClient, opts ...Option) (*Controller, error)

// Run campaigns for the root of each namespace and hosts the cells that the roots
// assign to this controller, until ctx ends. It runs every loop under task.Run, so it
// returns only after every goroutine it started has returned: ctx.Err() once ctx
// ends, or the first error that stops it before then.
func (c *Controller) Run(ctx context.Context, namespaces ...shard.Namespace) error

// Report accepts one report of an agent of a cell that this controller hosts. It
// returns an error of class errs.Denied when caller is not the registered identity of
// r.Node.
func (c *Controller) Report(ctx context.Context, caller shard.Identity, r protocol.Report) (protocol.Ack, error)

// Lookup returns the placements of cell that contain positions, and up to prefetch
// placements after the last of them, in position order, from the controller's memory.
func (c *Controller) Lookup(ctx context.Context, caller shard.Identity, cell shard.CellID, positions [][]byte, prefetch int) ([]protocol.Located, error)

// Submit applies a signed proposal of a kind that this controller applies. It returns
// an error of class errs.Denied when the namespace has no approvals and caller is not
// an identity that WithOperators lists, and of class errs.Invalid when the proposal
// fails validation, with every reason.
func (c *Controller) Submit(ctx context.Context, caller shard.Identity, s config.Signed) (protocol.Receipt, error)

// Disrupt admits d under the budget of its nodes' cell. It returns an error of class
// errs.Denied when caller is neither an identity that WithOperators lists nor, for a
// disruption of one node, that node's registered identity.
func (c *Controller) Disrupt(ctx context.Context, caller shard.Identity, d budget.Disruption) (budget.Decision, error)

// Done ends the planned disruption id, under the same rule for caller as Disrupt.
func (c *Controller) Done(ctx context.Context, caller shard.Identity, id string) error
```

### Distribution

Agents and routers follow the store by watching it, each only as much as it needs:

| Consumer | Reads | Load on the store |
|---|---|---|
| Agent | Its node's index and its cell's record, in its cell's scope | One watch per agent, with events only for the node's replicas and the cell's record |
| Router with `watch` | Every placement of each cell it routes to | One watch per router and cell |
| Router with `lookup` | The root's cell map. Routes come from the cell controllers on demand, and from the cell's scope when a controller does not respond | One watch per router, on the root scope |
| Router with `compute`, in the leaderless topology | The cell's membership, from which it computes routes | One membership watch per router |

`watch` is the default routing source in the embedded and service topologies. A namespace whose cells are too large for every client to keep selects `lookup`.

A change of one replica reaches the agents of that replica's nodes and the routers that watch its cell, not every agent of the namespace. Slicer's subscribers received 95% of its assignments within 2 seconds, through a tier of distributors. We have not measured either routing source.

### Start-up without the controller

An agent reads its node's placements from its cell's scope, through the per-node index, before it serves:

- It restores every non-writer replica at once, through `AddShard`.
- It restores no writer replica. The core gives writer roles back only with a new epoch, through the handoff protocol, because another node may have written under the old epoch while the node was down.
- It serves the replicas it restored while the controller is unavailable, and joins the normal protocols when the controller contacts it.

### The leaderless topology

A leaderless namespace has one cell. Without a controller, every agent computes placement with the namespace's function placer from the layout and the membership view. For `exclusive` ownership, the agent that the function selects as a shard's writer campaigns in the cell's scope for the shard's ID, and the term that `Campaign` grants is the shard's epoch. An agent that the function no longer selects resigns after it stops writing. A crashed agent's tenure ends with its lease. The terms of one election increase, but not necessarily by one.

Routers use the `compute` source: each router computes a shard's nodes with the same function placer, from the layout and the membership it watches.

Every agent watches the cell's membership, so each change of membership reaches every agent. The default bound of 5,000 nodes per cell bounds that fan-out. A leaderless deployment that outgrows one cell runs more than one namespace.

### Control-plane failures

| Component down | Agents | Routers | Transitions | Leases |
|---|---|---|---|---|
| A cell's controller | Keep serving | Keep routing on cached routes. Lookups of uncached routes read the cell's scope | Stop until the root places the cell on another controller, which continues them | Renew normally |
| The root | Keep serving | Keep routing on the last cell map | Continue in every cell. No cell can change controller | Renew normally |
| The store of a cell | Keep serving | Keep routing on cached routes | Stop in that cell | Cannot renew. Writers stop at their deadline or continue fenced, as the ownership mode configures |

### A backend that meets the contract

The store and the membership are adapters, each a Go module of its own, so an application imports only the backends it uses. The one adapter proposed, `go.thesmos.sh/sharder/etcd`, shows that a real backend meets every requirement of the contract with features it already has:

| Requirement | etcd v3 feature |
|---|---|
| An atomic commit of one scope's changes, conditioned on the scope's generation and watermark | A transaction whose compares check the scope's generation and watermark |
| An ordered change feed of each scope that resumes from a generation | A watch from a revision, which pushes |
| An election that grants each term to one candidate, and ends a term within a bounded time after its holder fails | `concurrency.Election`. A term is the create revision of the candidate's key, which core's fence documentation names as a valid source of fence epochs, and it ends when the candidate's lease expires |
| Leases that expire on a timer, for the membership and the leaderless topology | Leases with a TTL, which membership keys and election keys attach to |
| `Limits` | 128 operations per transaction and 1.5 MiB per request by default. A move writes about four keys, so `MaxPlacements` is 30 and `MaxBytes` is 1.5 MiB |

The adapter's key layout, its sizing, and its mapping of scopes to etcd clusters belong to the adapter's own documentation. The core depends on none of them.

## Alternatives considered

### Gossip for membership and for the assignment

Spread membership and the assignment by gossip, as Cassandra and Ringpop do, or through `hashicorp/memberlist`. With gossip, there is no store to operate and no single point of failure.

**Why not:** gossip views converge only eventually, so two nodes can disagree on a shard's owner for a while, and gossip lacks a lease for a fence to rely on. Gossip does not remove the need for cells either. HashiCorp recommends at most 5,000 Consul agents per gossip pool, because gossip messages can queue faster than a pool with churn transmits them, and it names churn rather than size as the risk. Cells already bound a namespace's membership to 5,000 nodes through store leases that do support fencing. The `Membership` interface admits a gossip adapter, and this proposal does not include one.

### Heartbeats to the cell controller instead of store leases

Let agents renew their leases by heartbeats in their reports to the cell's controller, as Kafka's group coordinators time out consumer sessions, which removes the renewals from the store.

**Why not in this proposal:** a lease granted by a controller needs a rule for the controller's own failure. A new controller must wait until every lease of the old controller has expired before it moves a writer. Store leases avoid that rule, and cells already divide the renewals across store clusters. The alternative becomes necessary if a cell's renewals exceed what one store cluster sustains, which we have not measured.

### ZooKeeper

SM and Helix use ZooKeeper for membership, election and the assignment, at large scale. Its ephemeral nodes and sessions match this design's leases.

**Why not as the first adapter:** Helix's maintainers report ZooKeeper write load as a cause of slow mastership handoff and session expiry at scale, and ZooKeeper is a JVM service. A ZooKeeper adapter can implement `store.Store` and pass `store/storetest`.

### Raft inside the library

Embed a Raft log in the controller processes, as Splitter does. The library then runs without an external store.

**Why not:** the library would operate a consensus log, with its data directories, snapshots, membership changes and recovery procedures. An external consistent store moves that work to a component that already does it.

### The Kubernetes API as the store

Store specs and assignments as custom resources, with resource versions for compare-and-set and watches for distribution.

**Why not as an adapter here:** the Kubernetes API server is itself backed by etcd. It adds its own request limits, and every watch passes through it.

### PostgreSQL

Many applications already run PostgreSQL. A transaction gives the atomic commit with its fence check, a change table read with `LISTEN` and `NOTIFY` gives the watch, and a row per scope holds the generation and the watermark.

**Why not as the first adapter:** PostgreSQL has no lease that expires on a timer. A session-level advisory lock ends only with its connection, so a hung process that keeps its connection open keeps its lock. `Campaign` and membership would need a lease table, expiry by the database's clock, and a sequence for terms. The store interface admits such an adapter, and `store/storetest` decides whether it is correct.

### A unified key-value interface

Define the store over a generic key-value interface, with the sharder's layout of keys written once above it.

**Why not:** core refuses a unified key-value seam, because an intersection of stores discards what each is good at. The store needs atomic multi-key commits, an ordered change feed and leases, which no common subset of stores offers with the same semantics. `store.Store` is one kind with its own laws and its own conformance suite, which is the shape core prescribes for storage.

## Drawbacks

- Every namespace with exclusive ownership needs a store adapter whose sessions and elections end within a bounded time, and an operated consistent store behind it. The etcd adapter is the only one proposed, so its operation, including compaction, defragmentation and quota, is the application team's work.
- A store's commit limit splits large plans across generations. Routers then see intermediate assignments. Each is valid, but with the etcd adapter a large rebalance takes one commit per 30 replica moves.
- The per-node index adds writes to every replica move, in exchange for agents that read and watch only their own replicas.
- A router with `watch` puts one store watch per cell on the store. With many clients this load grows linearly, and `lookup` exists for that case.
- Restoring writers only with a new epoch delays write availability after a fast restart by one handoff protocol run.
- The store and membership implementations are four to keep correct: `store/memory` and `membership/static` in the root module, and the etcd module's store and membership.

## Open questions

None.

## Unresolved and future work

- Membership through Kubernetes Lease objects, and every other Kubernetes integration, is not proposed here.
- A dedicated distribution service, like Slicer's distributors, is not proposed here.
- A PostgreSQL store adapter is not proposed here. The store interface and `store/storetest` define what one must do.
- A gossip membership adapter is not proposed here.

## References

| What | Where |
|---|---|
| etcd v3.7.2 server defaults: transaction operations and request size | https://github.com/etcd-io/etcd/blob/v3.7.2/server/embed/config.go |
| etcd v3.7.2 backend quota | https://github.com/etcd-io/etcd/blob/v3.7.2/server/storage/quota.go |
| etcd, system limits | https://etcd.io/docs/v3.6/dev-guide/limit/ |
| etcd, performance and benchmarks | https://etcd.io/docs/v3.6/op-guide/performance/ |
| etcd, watch memory usage benchmark | https://etcd.io/docs/v3.7/benchmarks/etcd-3-watch-memory-benchmark/ |
| etcd v3.7, notes on lock and lease | https://etcd.io/docs/v3.7/learning/why/ |
| Kubernetes KEP-589, efficient node heartbeats | https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/589-efficient-node-heartbeats/README.md |
| HashiCorp, operating Consul at scale | https://docs.hashicorp.com/consul/docs/manage/scale |
| Das, Gupta and Motivala, "SWIM", DSN 2002 | https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf |
| Dadgar et al., "Lifeguard", arXiv:1707.00788 | https://arxiv.org/abs/1707.00788 |
| Apache Pekko 1.7.0, cluster failure detector defaults | https://github.com/apache/pekko/blob/v1.7.0/cluster/src/main/resources/reference.conf |
| Kubernetes v1.37.1, node lifecycle defaults | https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/nodelifecycle/config/v1alpha1/defaults.go |
| Adya et al., "Slicer", OSDI '16, §4 | https://www.usenix.org/system/files/conference/osdi16/osdi16-adya.pdf |
| Lee et al., "Shard Manager", SOSP '21, §3.2 | https://dl.acm.org/doi/10.1145/3477132.3483546 |
| Apache Helix issue 1027, double masters from two controller pipelines | https://github.com/apache/helix/issues/1027 |
| Apache Helix wiki, ZooKeeper scalability | https://github.com/apache/helix/wiki/FederatedZkClient |
| go.thesmos.sh/core at 8edf26a: storage is per kind (ADR-0012), conformance for durable adapters (RFC-0038), `epoch/fence.go` | https://github.com/thesmos-ai/core |
| go.thesmos.sh/ledger RFC-0011, "Replicated storage": leases and leader election | `docs/rfc/0011-replicated-storage.md` in the ledger repository |
| go.thesmos.sh/ledger RFC-0009, "Storage contract": watch and follow | `docs/rfc/0009-storage-contract.md` in the ledger repository |

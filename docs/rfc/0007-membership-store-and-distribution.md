---
rfc: 0007
title: Membership, the assignment store and assignment distribution
author: Roy Klopper
status: Draft
created: 2026-09-27
updated: 2026-09-29
discussion: none
supersedes: none
superseded-by: none
produces-adr: tbd
research: Research-0001, Research-0002, Research-0003
related: RFC-0001, RFC-0002, RFC-0006, RFC-0008, RFC-0009
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# RFC-0007: Membership, the assignment store and assignment distribution

## Summary

Every strategy depends on these mechanisms:

- Membership reports which nodes of a cell are alive.
- The store persists the cell map, each cell's assignment, the transitions in flight, the disruptions that the budget deferred or admitted, and the proposals that controllers applied.
- Distribution delivers each committed change to agents and routers.

We propose one interface for each, written in core's vocabulary: epochs from `epoch`, conditional-write errors from `version`, pages from `page`, and error classes from `errs`. The store and membership are interfaces with conformance suites. The root module contains an in-memory store with fault hooks, which also provides a membership, for tests and simulations. Adapters in their own modules implement both over real backends, and the support matrix contains the etcd adapter. Each adapter reports its own limits per scope, so the core depends on no backend. Each cell has its own scope in the store, and an adapter can place scopes on separate backend clusters, so the store scales with the number of cells.

Every commit goes through a writer bound to one namespace incarnation and one epoch, and the store applies core's fence laws to it. A commit from a replaced controller fails with `epoch.ErrFenced`, and a commit whose generation moved fails with `version.ErrMismatch`. Each commit also has an ID and writes a receipt in its own transaction. A controller that loses a commit's reply retries the same commit or reads the receipt, instead of guessing the outcome.

A controller acquires a scope with one conditional commit before it reads the scope, and each snapshot and watch has a stated consistency. An agent that restarts reads its own placements from its cell's scope through a per-node index. It resumes its non-writer replicas before a controller contacts it.

## Motivation

These mechanisms set the bounds within which the strategies work:

- Detection time spans an order of magnitude across published defaults. Pekko's phi accrual detector triggers after about 5.5 seconds by default. Kubernetes marks a node unreachable after a 50-second grace period and waits 5 minutes before evicting its pods. Dicer detects a crashed pod in roughly 30 seconds.
- Gossip runs without a consensus store, and its views are weaker than a store's. SWIM's suspicion mechanism "reduces (but does not eliminate)" false failure detections, and Lifeguard cut the remaining false positives by more than 50 times. A gossip view cannot back a lease that a fence relies on.
- Every consistent store bounds the size of a commit and of a scope, and the bounds differ between stores. etcd, for example, allows 128 operations in one transaction and 1.5 MiB in one request by default, and suggests at most 8 GiB of data. A controller that writes a large plan as one commit fails at those limits, so the core has to learn them from the adapter.
- A consistent store bounds throughput and fan-out. etcd's documentation measures a three-member cluster at more than 30,000 requests per second under heavy load, and its watch benchmark measures 17 KB of server memory per client connection. Kubernetes found that node heartbeats brought etcd to its storage limit in clusters of more than 2,000 nodes whose Node objects listed many images, and moved the heartbeats to small Lease objects.
- A commit's outcome can be unknown. etcd's API guarantees state that "the client may be uncertain about the status of an operation if it times out, or there is a network disruption between the client and the etcd member". A controller that treats such an error as a rejection can undo work that the store kept.
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
 "go.thesmos.sh/sharder/ownership"
 "go.thesmos.sh/sharder/shard"
)

// View is one list of the live nodes of a cell in one namespace incarnation.
type View struct {
 Scope       shard.Scope
 Incarnation shard.NamespaceIncarnation
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
 // loss, or when the local deadline passes. The deadline is the send time of the
 // latest accepted renewal plus Lease.Validity, on a clock timer. A renewal that fails
 // or has no reply does not move it, and a reply that arrives after Done has closed
 // does not reopen the session.
 Done() <-chan struct{}
 // Leave ends the session and removes the node from the view at once.
 Leave(ctx context.Context) error
}

// Membership registers nodes in cells and reports which nodes are live. A node is
// live while the backend renews its lease.
type Membership interface {
 // Join registers n in scope s, the root or one cell, of the namespace incarnation
 // inc, and renews its lease every l.Renew, for a lease of l.TTL, until ctx ends or
 // Leave is called. It returns shard.ErrIncarnation when the scope belongs to another
 // incarnation.
 Join(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation, n shard.Node, l ownership.Lease) (Session, error)
 // Watch yields the current view of scope s, then every later view, in revision
 // order, until ctx ends. It yields shard.ErrIncarnation and stops when the scope
 // belongs, or comes to belong, to another incarnation than inc.
 Watch(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation) iter.Seq2[View, error]
}
```

| Implementation | Module | Liveness mechanism | Detection time with defaults | Provides | In the support matrix |
|---|---|---|---|---|---|
| etcd adapter | `go.thesmos.sh/sharder/etcd` | A key per node in the cell's scope, attached to a lease | The lease's TTL, 10 s | `leases` | Yes |
| `store/memory` | the root module | A lease per node, on the store's clock | The lease's TTL, 10 s | `leases` | In tests and simulations |
| `membership/static` | the root module | None. The embedder lists the nodes | Never | `ordinal-nodes` | No, because `exclusive` requires `leases` |

Each adapter declares its capabilities through `config.Provider`, and the controller checks the spec's requirements against the capabilities of each role: those of the store, and those of the membership. Every implementation keeps each node's description with its session, including the node's identity, so the cell's controller knows which identity each node's reports must come from.

`Join` takes the lease of the namespace's spec, so every adapter renews at the spec's interval and moves each session's deadline on each accepted renewal, whatever its backend's own default TTL. `Join` and `Watch` are bound to a namespace incarnation, so a delayed call of a namespace that was deleted and created again under the same name changes nothing in the new incarnation.

Each cell's nodes register in the cell's own scope, so the renewals of a namespace spread over its cells and over the backend clusters that store them. A cell of 5,000 nodes that renew every 3.3 seconds, a third of the default TTL, sends about 1,500 renewals per second to its store. The root's membership adds one session in the root scope for each process that runs a controller: a cell whose 5,000 processes all run one adds about 1,500 renewals per second to the root's store. We have not measured any backend's renewal throughput.

The core waits `failoverDelay` after a node's session ends before it moves the node's replicas, as SM lets applications configure. The default is zero, and every preset keeps it, so a failed writer is replaced as soon as its lease expires. An application whose replicas are expensive to rebuild sets a delay of its own. A node that restarts inside the delay registers a new incarnation, and the core restores its replicas through the normal protocols, with a new epoch for every writer.

### The store

```go
// Package store persists the cell map, the assignments, the transitions and the
// reservations of namespaces, one scope at a time.
package store

import (
 "context"
 "errors"
 "iter"
 "time"

 "go.thesmos.sh/core/crypto"
 "go.thesmos.sh/core/epoch"
 "go.thesmos.sh/core/errs"
 "go.thesmos.sh/core/page"
 "go.thesmos.sh/sharder/budget"
 "go.thesmos.sh/sharder/shard"
)

//go:generate go tool kanon -type=Head,Transition,Endpoint,Drain,DrainKey,CommitID,Receipt,Applied,Mutation

// Schema is the version of the record formats and of the handoff protocols' scripts
// that this release writes and reads. A release that changes either raises it. A
// controller refuses to acquire a scope whose Head.Schema is above it, before it
// commits anything to the scope.
const Schema uint32 = 1

// ErrCompacted reports a read or a watch at a generation that the store no longer
// keeps, or a watcher that fell behind its buffer. The reader starts a new snapshot. It
// classifies as errs.Conflict.
var ErrCompacted = errs.WithClass(errors.New("store: generation compacted"), errs.Conflict)

// ErrReused reports a commit whose ID the scope has recorded with another digest. It
// classifies as errs.Invalid: the caller gave one ID to two mutations.
var ErrReused = errs.WithClass(errors.New("store: commit ID reused with another mutation"), errs.Invalid)

// ErrTooLarge reports a mutation that no commit to the scope can apply. Fits and
// Commit return it before anything is sent. It classifies as errs.Invalid.
var ErrTooLarge = errs.WithClass(errors.New("store: mutation exceeds the scope's limits"), errs.Invalid)

// Head is a scope's header: the values that condition every commit.
type Head struct {
 // Incarnation is the namespace incarnation that created the scope. It is zero for a
 // scope that no commit has created.
 Incarnation shard.NamespaceIncarnation
 Generation  epoch.Epoch
 // Watermark is the highest writer epoch that the scope has admitted.
 Watermark epoch.Epoch
 // Schema is the highest schema of any record or transition that the scope contains.
 // The commit that writes a record or a transition of a higher schema raises it.
 Schema uint32
}

// Transition is the durable record of one move. The core commits it before the move's
// first callback, and never changes its identity, endpoints or epochs afterwards, so a
// new controller resumes the move from the record alone.
type Transition struct {
 Operation shard.OperationID
 // Protocol and ProtocolVersion identify the script. A controller resumes the
 // transition only with a protocol of the same name and version.
 Protocol        string
 ProtocolVersion uint32
 // SpecVersion is the version of the spec that the move was planned under.
 SpecVersion epoch.Epoch
 From        Endpoint
 To          Endpoint
 // Fallback is the node that the abort path makes the writer. It is empty when no
 // node qualifies, and the abort path then ends with no assigned writer.
 Fallback Endpoint
 OldEpoch epoch.Epoch
 // NewEpoch and AbortEpoch are the epochs that the core reserved for the move.
 NewEpoch   epoch.Epoch
 AbortEpoch epoch.Epoch
 Phase      uint8 // the transition's handoff.Phase
 // Step is the next step of the current path: the forward path while Phase is
 // Running, and the abort path of step Failed while Phase is Aborting.
 Step    int
 Failed  int
 Started time.Time
}

// Endpoint is one side of a transition: the node, the incarnation of the node's process
// that the transition addresses, and the replica's role on it. Node is empty for the
// side of a move that only adds or only removes a replica.
type Endpoint struct {
 Node        shard.NodeID
 Incarnation epoch.Epoch
 Role        shard.Role
}

// Drain is one reason to exclude a node from new placements, and a node is draining
// while it has at least one drain. The node and the disruption ID identify a drain.
// The empty disruption ID is the node's manual drain, which sharderctl drain adds and
// sharderctl undrain removes, and which operators share: a second manual drain of the
// node changes nothing. Every other ID is the drain of that disruption's reservation,
// which ends with the reservation.
type Drain struct {
 Node shard.NodeID
 // Disruption is the ID of the reservation that drains the node, pending or
 // admitted, or empty for the manual drain.
 Disruption string
 // Caller is the identity that requested the drain.
 Caller shard.Identity
 Since  time.Time
}

// DrainKey identifies one drain: the node and the disruption ID, which is empty for the
// manual drain.
type DrainKey struct {
 Node       shard.NodeID
 Disruption string
}

// State is a scope's committed state apart from its placements.
type State struct {
 Head          Head
 LayoutVersion epoch.Epoch
 // Cell is the record that the scope's controller commits when it acquires the scope:
 // the cell's record in a cell's scope, and the root leader's record, with an empty
 // ID, in the root scope.
 Cell shard.Cell
 // Mapping is the namespace's mapping, in the root scope only.
 Mapping shard.Mapping
 // Spec is the accepted spec, resolved with every default applied, with the proposal
 // that introduced it, as JSON, in the root scope only.
 Spec []byte
 // Approvals lists every version of the namespace's approvals section in order,
 // each as the signed spec proposal that introduced it, in the root scope only. It is
 // empty in the support matrix.
 Approvals [][]byte
 // Applied maps the ID of each applied proposal to its Expires, until Expires
 // passes. An ID that Applied contains proves that the proposal applied. An ID that
 // it lacks does not show that the proposal was rejected, because Forget removes each
 // ID once its Expires passes.
 Applied     map[string]time.Time
 Transitions []Transition
 // Reservations lists the disruptions that the budget deferred or admitted and that
 // have not ended. A reservation whose Admitted is zero is pending.
 Reservations []budget.Reservation
 // Drains lists every drain. A node with at least one drain accepts no new replicas.
 Drains []Drain
}

// Query selects the placements that Read returns. The zero Query selects every
// placement of the scope, in position order.
type Query struct {
 // Node selects the placements with a replica on Node.
 Node shard.NodeID
 // From starts at the placement whose range contains the position From.
 From []byte
}

// CommitID identifies one commit to a scope. A writer numbers its commits from one in
// Seq, and no term is used by two writers, so no two commits of a scope share an ID. A
// retry of a commit reuses its ID. A commit ID differs from the operation ID of the
// transition that the commit advances.
type CommitID struct {
 Incarnation shard.NamespaceIncarnation
 Term        epoch.Epoch
 Seq         uint64
}

// Receipt records one applied commit. The store writes it in the same transaction as
// the commit's mutation.
type Receipt struct {
 ID CommitID
 // Digest is the SHA-256 digest of the mutation's kanon encoding.
 Digest crypto.Digest
 // Generation is the generation that the commit created.
 Generation epoch.Epoch
}

// Mutation is the set of changes one commit applies to a scope.
type Mutation struct {
 // ID identifies the commit. A retry of the same mutation reuses it.
 ID          CommitID
 Put         []shard.Placement
 Delete      []shard.ID
 Transitions []Transition // created or advanced
 Done        []shard.ID   // shards whose transition ended
 // Cell replaces the scope's controller record. Nil leaves it unchanged.
 Cell *shard.Cell
 // Mapping sets the namespace's mapping, in the commit that creates the root scope.
 // Every later commit leaves it nil.
 Mapping *shard.Mapping
 // Spec replaces the root scope's spec. Nil leaves it unchanged.
 Spec []byte
 // Approvals appends the next version of the approvals section. Nil appends nothing.
 Approvals []byte
 // Applied records the proposals that the commit applies.
 Applied []Applied
 // Forget removes applied proposals whose Expires has passed.
 Forget []string
 // Reserve adds reservations, or replaces the reservations with the same IDs, such as
 // a pending reservation that the budget admits. Release removes the reservations
 // with these IDs.
 Reserve []budget.Reservation
 Release []string
 // Drain adds drains, and Undrain removes the drains with these keys. Removing one
 // drain of a node leaves the node's other drains in effect.
 Drain   []Drain
 Undrain []DrainKey
 // Confirm removes the receipts of the writer's earlier commits whose outcome the
 // writer knows.
 Confirm []CommitID
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

// Event is one committed generation of a scope, or a progress report when Mutation is
// empty. A progress report states that the scope is at Generation, and that no change
// up to Generation concerns the watcher. Watch yields progress reports only to a
// watcher filtered by node.
type Event struct {
 Generation epoch.Epoch
 Mutation   Mutation
 // Removed lists, for a watcher filtered by node, each shard whose placement stops
 // naming the node in this generation. A move to another node does not delete the
 // placement, so Mutation.Delete does not list the shard. Removed is empty for every
 // other watcher.
 Removed []shard.ID
}

// Limits bounds what one commit to a scope may contain. The planner splits a larger
// plan across consecutive generations, each a valid assignment.
type Limits struct {
 // MaxPlacements is the most placements that one commit may put or delete, together
 // with their transitions and the indexes that the adapter keeps for them.
 MaxPlacements int
 // MaxBytes is the most bytes of the transaction that the adapter sends, keys,
 // indexes, compares and receipt included.
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

// Writer commits to one scope under one namespace incarnation and one fence epoch,
// both bound when the Writer is created.
type Writer interface {
 // Commit applies m as the generation after expect, and writes m's receipt in the
 // same transaction. It returns the generation that m created. It returns ErrTooLarge
 // before it sends anything. In one atomic step with the mutation, it then checks the
 // scope's incarnation, the fence, the receipt of m's ID and the generation, in that
 // order, and returns the error of the first check that fails: shard.ErrIncarnation
 // when the scope has another incarnation, epoch.ErrFenced when the scope's watermark
 // is above the writer's epoch, ErrReused when the scope has recorded m's ID with
 // another digest, and version.ErrMismatch when the scope is not at expect. A receipt
 // of m's ID with m's digest returns the recorded generation and applies nothing
 // again. These five errors are definite rejections, which apply nothing. Every other
 // error wraps version.ErrOutcomeUnknown: m may have applied. An admitted commit
 // advances the watermark to the writer's epoch. A commit at generation zero creates
 // the scope with the writer's incarnation.
 Commit(ctx context.Context, expect epoch.Epoch, m Mutation) (epoch.Epoch, error)
}

// Store persists namespaces, one scope at a time. Implementations are safe for
// concurrent use.
type Store interface {
 // Head returns s's header, with a linearizable read. A controller reads it to learn
 // the generation that its acquisition expects, and decides nothing else from it.
 Head(ctx context.Context, s shard.Scope) (Head, error)
 // Read returns the scope's state and a cursor over the placements that q selects,
 // one page per call. The read of the state is linearizable, and every page is at
 // the state's generation. inc is the namespace incarnation that the reader expects,
 // or zero for a reader that has not learned it yet, and a scope of another
 // incarnation fails with shard.ErrIncarnation. Each page token encodes the state's
 // incarnation and generation, so a later page of a scope that was deleted and
 // created again fails with shard.ErrIncarnation as well. A page whose generation the
 // store no longer keeps fails with ErrCompacted.
 Read(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation, q Query, p page.Page) (State, page.Cursor[shard.Placement], error)
 // Watch yields the generations of s later than from, in order and each whole, until
 // ctx ends. With node set, it yields only the generations that change a placement
 // that has or had a replica on node, or the scope's controller record, and yields
 // empty events as progress. It yields ErrCompacted when from is older than the
 // store's history, or when the watcher falls behind its buffer. It yields
 // shard.ErrIncarnation and stops when the scope belongs, or comes to belong, to
 // another incarnation than inc.
 Watch(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation, from epoch.Epoch, node shard.NodeID) iter.Seq2[Event, error]
 // Writer returns a writer for s bound to the namespace incarnation inc and the epoch
 // term. It performs no I/O.
 Writer(s shard.Scope, inc shard.NamespaceIncarnation, term epoch.Epoch) Writer
 // Outcome returns the receipt of commit id, and reports whether s has one. It
 // applies nothing. An absent receipt does not prove that the commit was rejected.
 Outcome(ctx context.Context, s shard.Scope, id CommitID) (Receipt, bool, error)
 // Campaign blocks until candidate leads election in scope s, or until ctx ends.
 // The root campaigns in its scope for the election "root". In the leaderless
 // topology, a shard's writer campaigns in the cell's scope for the shard's ID.
 // candidate is the process's identity when the transport authenticates peers.
 Campaign(ctx context.Context, s shard.Scope, election, candidate string) (Leadership, error)
 // Limits returns the bounds of one commit to s.
 Limits(s shard.Scope) Limits
 // Fits returns nil when one commit to s can apply m, and ErrTooLarge otherwise. It
 // computes the size of the transaction that the adapter would send for m, and
 // performs no I/O.
 Fits(s shard.Scope, m Mutation) error
 Close() error
}
```

`version.ErrOutcomeUnknown` is the error for a write whose outcome is unknown. It classifies as `errs.Transient`, and core does not contain it at commit `8edf26a`. The ledger defines the same error as `storage.ErrOutcomeUnknown`, which a retry with the same idempotency key settles.

The store keeps one watermark per scope and applies core's fence laws to every commit. A commit is admitted when its writer's epoch is at or above the watermark, a greater epoch advances the watermark, and the check runs in the same transaction as the commit. A cell's controller writes with the epoch that the root granted it, and the root writes with the term that `Campaign` granted it.

A backend qualifies when it provides these mechanisms:

- an atomic commit of one scope's changes, conditioned on the scope's generation, incarnation and watermark
- linearizable reads, and reads of a scope at a fixed generation
- an ordered change feed of each scope that resumes from a generation, by push or by polling
- an election that grants each term to one candidate, and ends a term within a bounded time after its holder fails

A store needs leases that expire on a timer only when it also provides the membership, or runs the per-shard elections of the leaderless topology. An election can instead come from the backend's own failure detection, such as the leader election of a consensus group, whose term ends when the leader loses the support of its quorum. A candidate waits for the backend to elect it, and never takes a term from a live holder.

The ledger is such a backend. Each shard's voters elect its leader and grant its epoch. A candidate that has fenced a quorum appends its epoch to the metadata ledger, and the compare-and-set on the shard's stream head there makes the epoch's leader unique. A leader's term ends when a write quorum of its voters stops supporting it, within 3 seconds by default. The ledger commits by compare-and-swap on a stream's head. A reader in another process follows it through the ledger's network API without polling. `Feed` pushes each record of a query as it commits, and `WaitStream` returns once a stream is past a sequence number.

#### Commit outcomes

A commit ends in one of three outcomes:

- **Applied.** `Commit` returns the new generation, and the scope has recorded the commit's receipt.
- **Rejected.** `Commit` returns one of the five definite rejections, and the scope is unchanged.
- **Unknown.** `Commit` returns an error that wraps `version.ErrOutcomeUnknown`. A timeout, a lost connection or a cancelled context during the send can leave the commit applied or not.

A commit is idempotent by its ID. A retry with the same ID and the same mutation returns the generation that the first attempt created, if the first attempt applied, and applies nothing again. The store checks the incarnation, the fence, the receipt and the generation in that order, in one atomic step. A writer that a newer term has fenced therefore gets `epoch.ErrFenced` whatever its receipts and whatever the generation, and an unfenced writer's retry succeeds even when its own later commits have moved the generation.

An absent receipt does not prove that the commit was rejected, because the original request may still arrive at the backend and apply. A controller ends the uncertainty with a retry or a barrier:

- **A retry** settles the outcome. The controller retries the same commit, with the same ID, `expect` and mutation, until the commit returns a generation or a definite rejection. When the retry returns `version.ErrMismatch`, and `Outcome`, which reads a receipt and applies nothing, then finds no receipt, the first attempt did not apply. The fence check comes before the generation check, so a mismatch shows that no newer term has fenced the writer, and that no acquisition has deleted the writer's receipts. The first attempt cannot apply later either, because the generation has moved past `expect`.
- **A barrier** prevents the commit from applying in the future. The controller establishes a state that makes the commit's precondition impossible. An acquisition by a newer controller is such a barrier. It raises the watermark above the old term, so every request of the old writer fails its fence check, whenever it arrives. A barrier does not recover whether the commit applied before it, because the acquisition deletes the receipts of older terms. The newer controller does not need to know: it recovers from its snapshot, which contains every commit that applied.

A controller sends no callback, admits no disruption and returns no reply to an operator on the strength of a commit whose outcome is unknown. It never makes the same change under a new commit ID while the first one is unsettled.

A retry settles an outcome by applying the commit at most once. `Outcome` settles it without applying anything, and a controller reads it after a mismatch. `sharderctl` reports whether a proposal applied from `State.Applied`: an ID that `Applied` contains proves that the proposal applied, and an absent ID means that the outcome is unknown, not that the proposal was rejected.

The store keeps a receipt until one of two commits removes it. The writer's own later commit confirms it in `Confirm`, once the writer knows the commit's outcome. Otherwise an acquisition by a newer term deletes every receipt of the older terms, in the same transaction. A scope therefore keeps the receipts of one writer's unconfirmed commits: the commits in flight and the last one. A receipt cannot outlive its namespace incarnation, because its ID contains the incarnation.

#### Acquiring a scope

A controller acquires a scope before it reads the scope's contents:

1. It reads the scope's `Head`. It uses only the generation, the incarnation and the schema, and decides nothing from the scope's contents. It refuses a scope whose `Head.Schema` is above `store.Schema`, with an error of class `errs.Unsupported`, before it commits anything.
2. It commits its record, a `shard.Cell` with its address, identity and epoch, at that generation, through a writer bound to the namespace incarnation and its epoch. The commit advances the scope's watermark to its epoch, and deletes the receipts of older terms.
3. On `version.ErrMismatch`, it reads the head again and repeats. On `epoch.ErrFenced`, it stops, because a newer controller has the scope. On an unknown outcome, it retries the same commit.
4. Once the acquisition has applied, it reads the scope with `Read`, which is linearizable, and recovers from that snapshot only. It never acts on a read that it made before the acquisition.

A commit of the replaced controller between steps 1 and 2 moves the generation, and the acquisition's compare fails and repeats. A rise of the schema after step 1 moves the generation too, and the controller checks the new schema when it repeats. Once the acquisition applies, every commit of an older term fails with `epoch.ErrFenced`. The snapshot of step 4 then contains every commit that the replaced controller can ever make. Every new protocol version raises the schema, so step 1 also refuses a scope with a transition that the release cannot resume. As a last check, the controller stops with an error of class `errs.Unsupported` at a transition in its snapshot whose protocol version or phase its release does not implement.

A controller's first commit in a scope writes its record: its own address, its identity and its epoch. The root's record is a `shard.Cell` with an empty ID, in the root scope, and its epoch is the root's term. Agents read the record to learn which identity their controller's calls must come from, `sharderctl` reads it to learn where to submit an operation, and a replaced controller cannot overwrite it.

A commit at generation zero creates a scope with its writer's incarnation. The root creates a namespace from the initial spec that its process passes to `Controller.Run`, when the root scope's head shows no incarnation. It generates the namespace incarnation with core's `id/ulid`, resolves the spec, and commits its record, the mapping and the resolved spec at generation zero, in one commit. The commit is conditional: of two roots that race to create one namespace, the commit of the second fails with `shard.ErrIncarnation`, because the scope already has the first one's incarnation. A retry of a creation whose outcome is unknown reuses its incarnation and its commit ID. A process uses its initial spec only in that commit, and resolves the spec that the root scope stores once the namespace exists. When two processes start with different initial specs, the committed one applies to both.

A cell's controller creates its cell's scope in its first acquisition, with the incarnation that the root's callback names. A commit through a writer of another incarnation fails with `shard.ErrIncarnation`, so a process of a deleted namespace cannot write to its successor.

#### Transaction shapes and limits

The core commits five shapes, and every commit is one of them:

| Shape | Contains | Atomic because |
|---|---|---|
| Transition steps | The records or next steps of one or more transitions, the placements that they change with their indexes, and the reservations that the steps create or end | A step's placement and its recorded progress must agree |
| A batch of placements | Up to `Limits.MaxPlacements` placements from one plan, with their indexes and transitions | Each generation must be a valid assignment |
| A reservation | One reservation added, admitted or removed, with its drains and the transitions that its drain starts | A deferral, a grant and an expiry must each be durable before `Disrupt` returns it or a new controller acts on it |
| An acquisition | A controller's record and the scope's watermark, and the deletion of older receipts | Every later commit of an older term must fail |
| A creation | The root's record, the incarnation, the mapping and the resolved first spec | A namespace exists with its mapping or not at all |

Every commit also writes its receipt and confirms earlier ones. Only the batch of placements grows with a plan: the planner splits a plan into batches that fit, and the engine commits the steps of ready transitions together while they fit. The other shapes have a fixed size, apart from the spec in a creation and the drains of a reservation, which grow with the disruption's nodes. `Disrupt` returns `ErrTooLarge`, and commits nothing, for a disruption whose reservation does not fit one commit.

`Limits(s)` gives a scope's bounds, and `Fits(s, m)` computes the size of the transaction that the adapter would send for `m`, with its compares, keys, indexes and receipt. `Commit` returns `ErrTooLarge` for a mutation that does not fit, before it sends anything. An adapter that places scopes on more than one backend cluster returns each scope's limits from its own cluster's configuration.

The first release needs no production-scale benchmark before these interfaces are agreed. The etcd adapter's feasibility check measures the operations and bytes of each shape's largest instance against the cluster's configured limits, and the conformance suite checks `Fits` against the encoded transactions of generated mutations. The supported limits of a cell are published after those measurements.

#### Snapshots and watches

An agent or a router keeps a scope's state in memory, from a snapshot and a watch, under these rules:

1. `Read` returns the state at one generation of one namespace incarnation, and every page of the read is at that generation. A page whose generation the backend has compacted fails with `ErrCompacted`. The reader discards the partial snapshot, keeps its last complete state, and starts a new read.
2. The reader watches from the snapshot's generation, in the snapshot's incarnation. `Watch` yields every later generation that concerns the reader, in order, and never part of a generation: a generation's placements, indexes and records arrive in one event.
3. A watch filtered by node skips the generations that do not concern the node, so its generations increase with gaps. It yields the removal of a replica from the node, in the generation that removes it, and lists the shard in `Event.Removed`, although the removed placement no longer names the node. It yields an empty event as progress when the scope has moved without a change for the node, at an interval that the adapter documents, so the reader knows how current its state is.
4. A watch whose starting generation the backend has compacted yields `ErrCompacted`, and the reader starts again from rule 1. It never resumes at the oldest generation that the backend kept.
5. The store and the reader bound their buffers. A watcher that falls behind its buffer receives `ErrCompacted` and starts again from rule 1, instead of losing events or growing without bound.
6. A reader applies events by generation, so an event that arrives again after a reconnect changes nothing.
7. A snapshot that replaces the reader's state reports no removal. A replica that the old state listed and the snapshot omits was removed while the watch was broken, so an agent compares each replacement snapshot with its tenures.

A generation is a position in one scope's history of one namespace incarnation. A reader never compares the generations of two scopes. `Read`, its page tokens and `Watch` check the incarnation that the reader expects, so a reader cannot resume in the history of a namespace that was deleted and created again under the same name, even at an equal generation. A reader that receives `shard.ErrIncarnation` reads the root scope again. A router then snapshots the new incarnation, and an agent or a controller of the old incarnation stops serving the namespace.

#### Restoring a store from a backup

Restoring a store from a backup is outside the support matrix, and the store does not detect it. A restored store returns a scope to an older generation and an older watermark, so it can reissue epochs that the application's fences have already passed. It also returns the scope's transitions, reservations and receipts to an older state than the application's. A namespace recovers from the loss of its store with a new namespace incarnation, over a fresh application-data namespace or behind an administrative barrier that stops every writer of the old incarnation. The fencing design describes both.

#### Laws and conformance

Every store adapter passes `store/storetest`, which checks these laws:

1. A commit at a generation other than the scope's returns `version.ErrMismatch` and applies nothing, unless the scope has a receipt for the commit's ID with the same digest.
2. Every `Writer` is a fenced writer. The suite runs core's `coretest/epochtest.AssertFencedWriter` over it: a commit through a superseded writer returns `epoch.ErrFenced`, classifies as `errs.Conflict` and applies nothing, and a writer at the current epoch commits.
3. A commit through a writer of another namespace incarnation returns `shard.ErrIncarnation` and applies nothing. A commit at generation zero creates the scope with the writer's incarnation.
4. A commit writes its receipt in the same transaction as its mutation. A retry with the same ID and digest returns the recorded generation and applies nothing again, and a retry with the same ID and another digest returns `ErrReused`. `Outcome` returns a receipt exactly when the commit applied and no later commit confirmed or deleted the receipt.
5. After a commit fails with an error that wraps `version.ErrOutcomeUnknown`, a retry of the same commit and `Outcome` settle the outcome, whether the commit applied or not.
6. Every page of one `Read` is at one generation, and walking the pages to the end yields every selected placement once, or fails with `ErrCompacted`.
7. `Watch` yields every generation after the given one that concerns the watcher once, in order and whole. A node-filtered `Watch` yields every change of a placement that has or had a replica on the node, and lists in `Event.Removed` each shard whose placement stops naming the node.
8. `Campaign` grants each term of an election to one candidate. Core's admit-equal law is sound only under this condition.
9. `Fits` returns `ErrTooLarge` for every mutation that the adapter cannot commit in one transaction, and nil for every other. A commit that fits fails only with a definite rejection or an unknown outcome.
10. A commit that returned a generation is readable, with the same generation and receipt, after the store is opened again.
11. `Commit` checks the incarnation, the fence, the receipt and the generation in that order, in one atomic step. A superseded writer gets `epoch.ErrFenced` when the generation has also moved, and a writer of another incarnation gets `shard.ErrIncarnation`.
12. `Read`, a page token of `Read`, and `Watch` return `shard.ErrIncarnation` for a scope whose incarnation differs from the one that the caller expects, including a scope that was deleted and created again at an equal generation.

```go
// Package storetest checks a store.Store against the laws of package store.
package storetest

import (
 "testing"

 "go.thesmos.sh/sharder/shard"
 "go.thesmos.sh/sharder/store"
)

// Option configures a run of AssertStore.
type Option func(*config)

type config struct {
 reopen func(t *testing.T, s store.Store) store.Store
 lose   func(t *testing.T, s store.Store, l Loss)
 del    func(t *testing.T, s store.Store, sc shard.Scope)
}

// Loss selects where a fault interrupts a commit.
type Loss uint8

const (
 // LoseRequest loses a commit before the backend applies it.
 LoseRequest Loss = 1
 // LoseReply loses a commit's reply after the backend applies it.
 LoseReply Loss = 2
)

// WithReopen gives the suite a way to open the storage behind s again, as after a
// process restart. The suite then checks that every commit that returned is readable
// with the same generation and receipt.
func WithReopen(reopen func(t *testing.T, s store.Store) store.Store) Option

// WithLostCommits gives the suite a way to make the next commit to s fail with an
// error that wraps version.ErrOutcomeUnknown, at the point that l selects. The suite
// then checks that a retry and Outcome settle both cases.
func WithLostCommits(lose func(t *testing.T, s store.Store, l Loss)) Option

// WithDelete gives the suite a way to delete scope sc from the storage behind s, as an
// operator does before a namespace is created again under the same name. The suite
// then creates the scope with another incarnation, brings it to the old generation,
// and checks that a read, a page token and a watch of the old incarnation return
// shard.ErrIncarnation.
func WithDelete(del func(t *testing.T, s store.Store, sc shard.Scope)) Option

// AssertStore checks every law of package store against stores that newStore returns.
// It creates a new store for each case, so cases cannot observe each other's scopes.
func AssertStore(t *testing.T, newStore func(t *testing.T) store.Store, opts ...Option)
```

The suite follows core's own conformance suites for storage, which check every adapter against the laws of its package and across a restart. The in-memory store, `store/memory`, is its first subject. The etcd adapter passes the suite against a real etcd cluster, with a proxy that drops a connection before or after a commit applies.

#### The memory store

`store/memory` is for tests and simulations. It keeps nothing beyond its process, so it does not provide the capability `durable`, and `controller.New` rejects it unless the process passes `controller.WithVolatileStore`. It also implements `membership.Membership`, with leases on its clock. Its fault hooks produce the outcomes that the core has to handle, as the ledger's memory store does for its appends:

```go
// Package memory is an in-memory store.Store and membership.Membership with hooks that
// inject faults, for tests and simulations. It keeps nothing beyond its process, so it
// does not provide the capability "durable".
package memory

import "go.thesmos.sh/core/clock"

// The points at which FailAt interrupts a commit.
const (
 // PointCommitApply is before a commit applies. The commit is lost.
 PointCommitApply = "commit/apply"
 // PointCommitAck is after a commit applies, before Commit returns. The commit and
 // its receipt remain, and the reply is lost.
 PointCommitAck = "commit/ack"
)

// Store is an in-memory store and membership. It is safe for concurrent use.
type Store struct {
 // unexported fields
}

// New returns an empty store whose leases and progress events run on clk.
func New(clk clock.Clock) *Store

// Points returns the names of the store's fault points: PointCommitApply and
// PointCommitAck.
func (*Store) Points() []string

// FailAt makes the next Commit that passes point fail with an error that wraps
// version.ErrOutcomeUnknown. A name that is not one of Points never fires.
func (s *Store) FailAt(point string)

// StartOutage makes every later call fail with an error of class errs.Transient until
// EndOutage, as during an outage of the backend. A commit during an outage does not
// apply, and its error wraps version.ErrOutcomeUnknown, because a caller of a real
// backend could not tell.
func (s *Store) StartOutage()

// EndOutage ends the outage that StartOutage began.
func (s *Store) EndOutage()

// Compact discards every generation below the current one of every scope, so a read
// page or a watch from an older generation fails with store.ErrCompacted.
func (s *Store) Compact()
```

### The controller

One `Controller` serves every namespace of its process. It takes the store, the membership and the client for calls to agents as arguments, and core's seams and the process's own description as options. It resolves every other strategy of a namespace from the namespace's spec through the registry, with the capabilities that the store and the membership provide. The controller implements `protocol.ControllerServer`, which the embedder serves through a transport adapter. It also implements `protocol.AgentServer` for the callbacks of each namespace's root, which place cell controllers on the process. The embedder serves those callbacks and its agents' callbacks on one endpoint through `protocol.Dispatch`.

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

var (
 _ protocol.ControllerServer = (*Controller)(nil)
 _ protocol.AgentServer      = (*Controller)(nil)
)

// Option configures a Controller.
type Option func(*options)

type options struct {
 node            shard.Node
 clock           clock.Clock
 rand            rand.Rand
 telemetry       telemetry.Reporter
 logger          *slog.Logger
 trust           map[shard.Namespace]config.Trust
 operators       map[shard.Namespace][]shard.Identity
 cellCapacity    int
 volatile        bool
 unauthenticated bool
}

// WithNode sets this process's description in the root of each namespace: its ID,
// address, identity and labels. The controller joins each root's membership with it,
// campaigns for each root under its identity, and writes its address and identity in
// the record of every scope that it acquires. New returns an error of class
// errs.Invalid without it.
func WithNode(n shard.Node) Option

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
// whose approvals verify from the anchor in t. Approvals are outside the support
// matrix, and New rejects this option when the registry's support matrix has no
// approvals.
func WithTrust(ns shard.Namespace, t config.Trust) Option

// WithCellCapacity sets how many cells the root may place on this controller process,
// as the process's "cells" capacity in the root. The default is one.
func WithCellCapacity(n int) Option

// WithOperators sets the operators of namespace ns: the identities from which the
// controller accepts the operations of a kind without an approval policy, and every
// planned disruption and its end. They come from the process's own configuration,
// never from the store. Without it, the namespace has no operator.
func WithOperators(ns shard.Namespace, ids ...shard.Identity) Option

// WithVolatileStore lets New accept a store that does not provide the capability
// "durable", such as store/memory in tests and simulations. A controller on a
// volatile store loses its transitions and reservations with the store's process.
func WithVolatileStore() Option

// WithUnauthenticated makes the controller skip every identity check, for a
// deployment whose transport authenticates no caller. The epoch and incarnation checks
// still apply. Without it, the controller denies every call without an identity.
func WithUnauthenticated() Option

// New returns a controller that keeps namespace state in st, follows the live nodes
// of each cell through m, and calls agents through agents. It builds each namespace's
// strategies from the namespace's spec with reg. It returns an error of class
// errs.Invalid when WithNode is not set, or when st does not provide the capability
// "durable" and WithVolatileStore is not set. It returns an error of class
// errs.Unsupported when WithTrust is set and reg's support matrix has no approvals.
func New(reg *config.Registry, st store.Store, m membership.Membership, agents protocol.AgentClient, opts ...Option) (*Controller, error)

// Run campaigns for the root of the namespace of each spec in specs, and hosts the
// cells that the roots assign to this controller, until ctx ends. A root that finds
// its root scope without an incarnation creates the namespace from the spec, in one
// conditional commit. Once the namespace exists, the controller resolves the spec
// that the root scope stores, and ignores its own. Run runs every loop under
// task.Run, so it returns only after every goroutine it started has returned:
// ctx.Err() once ctx ends, or the first error that stops it before then.
func (c *Controller) Run(ctx context.Context, specs ...config.Spec) error

// Do runs one callback of a namespace's root. An add starts the controller of the
// cell r.Shard at the epoch r.Epoch, and returns once that controller's acquisition
// of the cell's scope has applied, or with the acquisition's error, such as an error
// of class errs.Unsupported for a scope of a newer schema. A drop returns once that
// controller's loops have returned. Do checks md and r as an agent checks the
// callbacks of its cell, against the root's leader, this process's session in the
// root and the root's epochs. It returns an error of class errs.Invalid for a request
// whose scope is not a root scope.
func (c *Controller) Do(ctx context.Context, md protocol.Metadata, r protocol.Request) error

// Report accepts one report of an agent of a cell that this controller hosts. It
// returns an error of class errs.Denied when md.Caller is not the registered identity
// of r.Node, and of class errs.Conflict when r names another namespace incarnation or
// a node incarnation that is not the node's live one. It discards a report whose
// Sequence is not above the last one it accepted from the node incarnation, and
// returns the accepted Sequence in the Ack. Outside the delegated mode, an accepted
// report updates the status, the load and the faults of the replicas that the
// assignment names, and never changes the assignment.
func (c *Controller) Report(ctx context.Context, md protocol.Metadata, r protocol.Report) (protocol.Ack, error)

// Lookup returns the placements of a.Scope that contain a.Positions, and up to
// a.Prefetch placements after the last of them, in position order, from the
// controller's memory. It returns an error of class errs.Unsupported in a namespace
// whose routing source is not lookup.
func (c *Controller) Lookup(ctx context.Context, md protocol.Metadata, a protocol.LookupArgs) (protocol.LookupReply, error)

// Submit applies a proposal of a kind that this controller applies. It returns an
// error of class errs.Denied when md.Caller is not authorised for the proposal's kind,
// of class errs.Unsupported for a kind outside the support matrix, of class
// errs.Conflict for another namespace incarnation, and of class errs.Invalid when the
// proposal fails validation, with every reason.
func (c *Controller) Submit(ctx context.Context, md protocol.Metadata, s config.Signed) (protocol.Receipt, error)

// Disrupt decides d under the budget of its nodes' cell, and commits each decision
// before it returns it: a deferral as a pending reservation with the drains of d's
// nodes, and an admission by setting the reservation's Admitted. A repeated d
// evaluates a pending reservation again, and returns Allowed for an admitted one. It
// returns store.ErrTooLarge, and commits nothing, when d's reservation and drains do
// not fit one commit. It returns an error of class errs.Invalid for a d whose ID the
// scope has recorded with another digest, and of class errs.Denied when md.Caller is
// neither an operator nor, for a disruption of one node at its live incarnation, that
// node's registered identity.
func (c *Controller) Disrupt(ctx context.Context, md protocol.Metadata, d budget.Disruption) (budget.Decision, error)

// Done ends the reservation a.ID: it releases an admitted reservation, and cancels a
// pending one together with its drains. It returns an error of class errs.Denied when
// md.Caller is neither the reservation's owner nor an operator.
func (c *Controller) Done(ctx context.Context, md protocol.Metadata, a protocol.DoneArgs) error
```

### Distribution

Agents and routers follow the store by watching it, each only as much as it needs:

| Consumer | Reads | Load on the store | In the support matrix |
|---|---|---|---|
| Agent | Its node's index and its cell's record, in its cell's scope | One watch per agent, with events only for the node's replicas and the cell's record | Yes |
| Router with `watch` | Every placement of each cell it routes to | One watch per router and cell | Yes |
| Router with `lookup` | The root's cell map. Routes come from the cell controllers on demand, and from the cell's scope when a controller does not respond | One watch per router, on the root scope | No |
| Router with `compute`, in the leaderless topology | The cell's membership, from which it computes routes | One membership watch per router | No |

`watch` is the default routing source in the embedded and service topologies, and the only one in the support matrix. A namespace whose cells are too large for every client to keep would select `lookup`.

A change of one replica reaches the agents of that replica's nodes and the routers that watch its cell, not every agent of the namespace. Slicer's subscribers received 95% of its assignments within 2 seconds, through a tier of distributors. We have not measured either routing source.

### Start-up without the controller

An agent reads its node's placements from its cell's scope, through the per-node index, before it serves:

- It restores every non-writer replica at once, through `AddShard`.
- It restores no writer replica. The core gives writer roles back only with a new epoch, through the handoff protocol, because another node may have written under the old epoch while the node was down. In the support matrix, every replica is its shard's writer, so a restarted node restores none, and the core moves each of its shards again with a new epoch.
- It serves the replicas it restored while the controller is unavailable, and joins the normal protocols when the controller contacts it.

### The leaderless topology

The leaderless topology is outside the support matrix.

A leaderless namespace has one cell. Without a controller, every agent computes placement with the namespace's function placer from the layout and the membership view. For `exclusive` ownership, the agent that the function selects as a shard's writer campaigns in the cell's scope for the shard's ID, and the term that `Campaign` grants is the shard's epoch. An agent that the function no longer selects resigns after it stops writing. A crashed agent's tenure ends with its lease. The terms of one election increase, but not necessarily by one.

Routers use the `compute` source: each router computes a shard's nodes with the same function placer, from the layout and the membership it watches.

The function selects a preferred writer, but it cannot tell whether that node leads the shard's election, or at which term. Exclusive ownership in this topology needs routers to observe each shard's election record, with its holder and its term, which the store does not offer. Without it, the topology suits affinity only.

Every agent watches the cell's membership, so each change of membership reaches every agent. The default bound of 5,000 nodes per cell bounds that fan-out. A leaderless deployment that outgrows one cell runs more than one namespace.

### Store failures

| Failure | Detected by | Action |
|---|---|---|
| A commit's reply is lost | `Commit` returns an error that wraps `version.ErrOutcomeUnknown` | The controller retries the same commit, or reads `Outcome` after a mismatch. No callback, admission or reply depends on the commit before its outcome is known |
| A mutation does not fit | `Fits` or `Commit` returns `ErrTooLarge` before anything is sent | The planner splits the plan into smaller batches. The fixed shapes fit the default limits of etcd, which the feasibility check confirms |
| The store is unavailable | Failed commits and renewals | No new transitions or admissions. The data plane serves within the limits of the core's outage table |
| A read page or a watch meets compaction | `ErrCompacted` | The reader keeps its last complete state and starts a new snapshot. An agent then cleans up each tenure that the new snapshot omits |
| A watcher falls behind its buffer | `ErrCompacted` | As compaction |
| Two controllers race to acquire a scope | One acquisition's compare or fence fails | The loser reads the head again, or stops when it is fenced |
| The store is restored from a backup | Not detected | Not supported. The namespace recovers with a new incarnation |

### A backend that meets the contract

The store and the membership are adapters, each a Go module of its own, so an application imports only the backends it uses. The one adapter proposed, `go.thesmos.sh/sharder/etcd`, shows that a real backend meets every requirement of the contract with features it already has:

| Requirement | etcd v3 feature |
|---|---|
| An atomic commit of one scope's changes, conditioned on the scope's generation, incarnation and watermark | A transaction whose compares check the scope's header keys |
| A receipt in the same transaction | A key per receipt in the scope, written by the commit's transaction and read by `Outcome` |
| Linearizable reads, and reads at a fixed generation | A range request without `serializable`, which etcd serves linearizably by default, and a range at a fixed revision |
| An ordered change feed of each scope that resumes from a generation | A watch from a revision, which pushes. Events of one revision arrive together, and progress notifications report the revision that the watch has delivered |
| An election that grants each term to one candidate, and ends a term within a bounded time after its holder fails | `concurrency.Election`. A term is the create revision of the candidate's key, which core's fence documentation names as a valid source of fence epochs, and it ends when the candidate's lease expires |
| Leases that expire on a timer, for the membership and the leaderless topology | Leases with a TTL, which membership keys and election keys attach to. A membership key's lease has the TTL that `Join` passes |
| `Limits` and `Fits` | 128 operations per transaction and 1.5 MiB per request by default, per cluster. A move writes about four keys and a receipt, so `MaxPlacements` is about 30 by our estimate, and `Fits` computes each commit's operations and bytes from the transaction it compiles |

The adapter's key layout, its sizing, and its mapping of scopes to etcd clusters belong to the adapter's own documentation. The core depends on none of them. Moving a live scope to another cluster is a migration with a new store identity, which this proposal does not provide: pointing a scope at an empty cluster loses its generation and its watermark.

## Alternatives considered

### Gossip for membership and for the assignment

Spread membership and the assignment by gossip, as Cassandra and Ringpop do, or through `hashicorp/memberlist`. With gossip, there is no store to operate and no single point of failure.

**Why not:** gossip views converge only eventually, so two nodes can disagree on a shard's owner for a while, and gossip lacks a lease for a fence to rely on. Gossip does not remove the need for cells either. HashiCorp recommends at most 5,000 Consul agents per gossip pool, because gossip messages can queue faster than a pool with churn transmits them, and it names churn rather than size as the risk. Cells already bound a namespace's membership to 5,000 nodes through store leases that do support fencing. The `Membership` interface admits a gossip adapter, and this proposal does not include one.

### Heartbeats to the cell controller instead of store leases

Let agents renew their leases by heartbeats in their reports to the cell's controller, as Kafka's group coordinators time out consumer sessions, which removes the renewals from the store.

**Why not in this proposal:** a lease granted by a controller needs a rule for the controller's own failure. A new controller must wait until every lease of the old controller has expired before it moves a writer. Store leases avoid that rule, and cells already divide the renewals across store clusters. The alternative becomes necessary if a cell's renewals exceed what one store cluster sustains, which we have not measured.

### A commit that waits until it knows its outcome

Let an adapter retry internally until it learns whether each commit applied, so that `Commit` never returns an unknown outcome.

**Why not:** a partitioned backend can keep the outcome unknown for as long as the partition lasts, so the call would ignore its context's deadline and block the controller's commit path. The caller needs to know that the outcome is unknown, so that it withholds the callbacks and replies that depend on the commit. etcd's own client returns the same uncertainty.

### A dedicated acquisition call

Add `Store.Acquire(scope, record)`, which advances the watermark and writes the controller's record atomically and chooses the generation internally.

**Why not:** acquisition is an ordinary conditional commit once the controller can read the scope's head. `Head` plus `Commit` gives the same guarantee with one read method instead of a second write path, and the acquisition's retry and outcome rules are those of every commit.

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
- A commit writes a receipt, and a later commit deletes it, so every commit costs one more key write and one more key delete.
- A controller withholds every action that depends on a commit whose outcome is unknown, so a flaky connection to the store delays callbacks and admissions until the retries settle.
- A store's commit limit splits large plans across generations. Routers then see intermediate assignments. Each is valid, but with the etcd adapter a large rebalance takes one commit per about 30 replica moves.
- The per-node index adds writes to every replica move, in exchange for agents that read and watch only their own replicas.
- A router with `watch` puts one store watch per cell on the store. With many clients this load grows linearly, and `lookup`, outside the matrix, exists for that case.
- Restoring writers only with a new epoch delays write availability after a fast restart by one handoff protocol run.
- A restored backup of the store is not detected, and a namespace that loses its store recovers only with a new incarnation.
- The store and membership implementations are four to keep correct: `store/memory` and `membership/static` in the root module, and the etcd module's store and membership.

## Open questions

None.

## Unresolved and future work

- The leaderless topology is outside the support matrix. Exclusive ownership in it needs routers that observe each shard's election, with its holder and its term.
- Restoring a store from a backup, and moving a live scope to another backend cluster, are not supported.
- Membership through Kubernetes Lease objects, and every other Kubernetes integration, is not proposed here.
- A dedicated distribution service, like Slicer's distributors, is not proposed here.
- A PostgreSQL store adapter is not proposed here. The store interface and `store/storetest` define what one must do.
- A gossip membership adapter is not proposed here.

## References

| What | Where |
|---|---|
| etcd v3.7.2 server defaults: transaction operations and request size | https://github.com/etcd-io/etcd/blob/v3.7.2/server/embed/config.go |
| etcd v3.7.2 backend quota | https://github.com/etcd-io/etcd/blob/v3.7.2/server/storage/quota.go |
| etcd v3.6, API guarantees: operation completion, watch ordering and progress notifications, linearizable reads | https://etcd.io/docs/v3.6/learning/api_guarantees/ |
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
| go.thesmos.sh/core at 8edf26a: storage is per kind (ADR-0012), conformance for durable adapters (RFC-0038), `epoch/fence.go`, `version/errors.go`, `crypto/digest.go` | https://github.com/thesmos-ai/core |
| go.thesmos.sh/ledger, `pkg/storage/errors.go` and `pkg/storage/memory/fault.go`: `ErrOutcomeUnknown` and crash points | The ledger repository |
| go.thesmos.sh/ledger RFC-0011, "Cluster and Replication": leases and leader election | `docs/rfc/0011-replicated-storage.md` in the ledger repository |
| go.thesmos.sh/ledger RFC-0020, "Gateway and Network API": `Feed` and `WaitStream` | `docs/rfc/0020-gateway-and-network-api.md` in the ledger repository |

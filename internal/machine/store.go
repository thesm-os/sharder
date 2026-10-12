// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine

import (
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/page"

	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// Head is a read of the header of a scope through store.Reader.Head. The runtime passes
// the result as a [HeadRead].
type Head struct {
	// Scope is the scope of the header.
	Scope shard.Scope
	// Token is the token of the effect.
	Token Token
}

var _ Effect = Head{}

// effect marks Head as an Effect.
func (Head) effect() {}

// HeadRead is the result of a [Head].
type HeadRead struct {
	// Err is the error of store.Reader.Head, or nil.
	Err error
	// Token is the token of the Head.
	Token Token
	// Head is the header that store.Reader.Head returned.
	Head store.Head
}

var _ Event = HeadRead{}

// event marks HeadRead as an Event.
func (HeadRead) event() {}

// Read is a read of one page of a scope through store.Reader.Read. The runtime reads the
// placements of the page and the token of the next page from the cursor of the read, and
// then closes the cursor. It passes the result as a [PageRead].
type Read struct {
	// Scope is the scope of the read.
	Scope shard.Scope
	// Query is the query of the read.
	Query store.Query
	// Page is the page to read. Its Token is empty for the first page, and it is the Next
	// of a PageRead for a later page.
	Page page.Page
	// Token is the token of the effect.
	Token Token
	// Incarnation is the namespace incarnation that the machine expects, or zero for a
	// machine that has not learned it yet.
	Incarnation shard.NamespaceIncarnation
}

var _ Effect = Read{}

// effect marks Read as an Effect.
func (Read) effect() {}

// PageRead is the result of a [Read]. It contains the state of the scope, the placements
// of one page and the token of the next page.
type PageRead struct {
	// Placements lists the placements of the page in the order of the cursor.
	Placements []shard.Placement
	// Next is the token of the next page, which page.Cursor.NextPage returned. It is empty
	// after the last page.
	Next string
	// Err is the error of store.Reader.Read or of the cursor of the read. It is nil for a
	// read without an error.
	Err error
	// State is the state of the scope that store.Reader.Read returned.
	State store.State
	// Token is the token of the Read.
	Token Token
}

var _ Event = PageRead{}

// event marks PageRead as an Event.
func (PageRead) event() {}

// Watch is a watch of the generations of a scope through store.Reader.Watch. The runtime
// passes each item of the watch as a [Watched], until an error or an [Unwatch] ends the
// watch.
type Watch struct {
	// Scope is the scope of the watch.
	Scope shard.Scope
	// Node is the node whose changes the watch yields. The watch yields every change for
	// an empty Node.
	Node shard.NodeID
	// Token is the token of the effect.
	Token Token
	// From is the generation after which the watch starts.
	From epoch.Epoch
	// Incarnation is the namespace incarnation that the machine expects, or zero for a
	// machine that has not learned it yet.
	Incarnation shard.NamespaceIncarnation
}

var _ Effect = Watch{}

// effect marks Watch as an Effect.
func (Watch) effect() {}

// Watched is one item of a [Watch]. Each item contains a generation of the scope, until an
// item with an error ends the watch.
type Watched struct {
	// Err is the error that ends the watch, or nil.
	Err error
	// Event is the generation that the watch yielded, or a report of progress.
	Event store.Event
	// Token is the token of the Watch.
	Token Token
}

var _ Event = Watched{}

// event marks Watched as an Event.
func (Watched) event() {}

// Unwatch is the end of the watch that a [Watch] or a [WatchViews] started. The runtime
// cancels the context of the watch, and does not pass an item that the watch yields after
// the cancellation. An item that the runtime passed before can still arrive, and the
// machine ignores it. Unwatch has no result.
type Unwatch struct {
	// Token is the token of the Watch or the WatchViews.
	Token Token
}

var _ Effect = Unwatch{}

// effect marks Unwatch as an Effect.
func (Unwatch) effect() {}

// Commit is a commit of a mutation to a scope through store.Writer.Commit. The runtime
// commits through the writer that store.Store.Writer returns for Scope, Incarnation and
// Term, and passes the result as a [Committed].
type Commit struct {
	// Mutation is the mutation of the commit. A retry of the commit has the same mutation.
	Mutation *store.Mutation
	// Scope is the scope of the commit.
	Scope shard.Scope
	// Token is the token of the effect.
	Token Token
	// Term is the fence epoch of the writer.
	Term epoch.Epoch
	// Expect is the generation of the scope that the commit expects.
	Expect epoch.Epoch
	// Incarnation is the namespace incarnation of the writer.
	Incarnation shard.NamespaceIncarnation
}

var _ Effect = Commit{}

// effect marks Commit as an Effect.
func (Commit) effect() {}

// Committed is the result of a [Commit].
type Committed struct {
	// Err is the error of store.Writer.Commit, or nil. After an error that wraps
	// version.ErrOutcomeUnknown, the commit may have applied.
	Err error
	// Token is the token of the Commit.
	Token Token
	// Generation is the generation that the commit created.
	Generation epoch.Epoch
}

var _ Event = Committed{}

// event marks Committed as an Event.
func (Committed) event() {}

// Outcome is a read of the receipt of a commit through store.Store.Outcome. The runtime
// passes the result as an [OutcomeRead].
type Outcome struct {
	// Scope is the scope of the commit.
	Scope shard.Scope
	// Token is the token of the effect.
	Token Token
	// ID is the ID of the commit.
	ID store.CommitID
}

var _ Effect = Outcome{}

// effect marks Outcome as an Effect.
func (Outcome) effect() {}

// OutcomeRead is the result of an [Outcome].
type OutcomeRead struct {
	// Err is the error of store.Store.Outcome, or nil.
	Err error
	// Token is the token of the Outcome.
	Token Token
	// Receipt is the receipt of the commit when Found is true.
	Receipt store.Receipt
	// Found is true when the scope has a receipt of the commit. A missing receipt does not
	// prove that the store rejected the commit.
	Found bool
}

var _ Event = OutcomeRead{}

// event marks OutcomeRead as an Event.
func (OutcomeRead) event() {}

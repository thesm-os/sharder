// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package maintenance_test

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"slices"
	"strconv"
	"time"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand/seeded"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/maintenance"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/membership/membershiptest"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/protocoltest"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/storetest"
)

// The namespace and the records of the tests.
const (
	// namespace is the namespace of the tests.
	namespace shard.Namespace = "orders"
	// rootAddress is the address of the leader of the root.
	rootAddress = "root-1:7000"
	// cellAddress is the address of the controller of the cell.
	cellAddress = "cell-1:7000"
	// accepted is the version of the accepted spec of the namespace.
	accepted epoch.Epoch = 3
	// rootGeneration is the generation of the root scope.
	rootGeneration epoch.Epoch = 9
	// cellGeneration is the generation of the scope of the cell.
	cellGeneration epoch.Epoch = 17
	// epochA and epochB are the epochs of the placements of shardA and shardB.
	epochA epoch.Epoch = 5
	epochB epoch.Epoch = 2
	// lifetime is the lifetime of a proposal of the tests.
	lifetime = time.Hour
)

// The errors of the tests.
var (
	// errRead is the error of a read of the store.
	errRead = errors.New("test: the read fails")
	// errPage is the error of a page of the placements of a scope.
	errPage = errors.New("test: the page fails")
	// errClose is the error of the close of a cursor.
	errClose = errors.New("test: the close fails")
	// errWatch is the error of a watch of the views.
	errWatch = errors.New("test: the watch fails")
	// errLater is the error that a watch yields after its first view. The real watch waits for
	// the next change of the view instead.
	errLater = errors.New("test: the watch has no later view")
	// errCall is the error of a call of a controller.
	errCall = errors.New("test: the call fails")
)

// The IDs, the scopes, the nodes and the shards of the tests.
var (
	// origin is the time of the clock of each test.
	origin = time.Date(2026, time.November, 9, 0, 0, 0, 0, time.UTC)
	// inc is the namespace incarnation of the tests.
	inc = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: 0x1a, 15: 0x01})}
	// other is a namespace incarnation that the namespace of the tests does not have.
	other = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: 0x1a, 15: 0x02})}
	// cellKey is the shard of the cell in the root scope.
	cellKey = shard.ID{ID128: id.ID128{0: 0x01, 15: 0xce}}
	// rootScope is the root scope of the namespace.
	rootScope = shard.Scope{Namespace: namespace}
	// cellScope is the scope of the cell of the namespace.
	cellScope = shard.Scope{Namespace: namespace, Cell: shard.CellID(ulid.Format(cellKey.ID()))}
	// nodeA and nodeB are the nodes of the view of the cell.
	nodeA = shard.Node{ID: "node-a", Address: "node-a:7000", Incarnation: 3}
	nodeB = shard.Node{ID: "node-b", Address: "node-b:7000", Incarnation: 1}
	// shardA has its writer on node A, and shardB on a node that has left the view. shardC has
	// no writer and a secondary on that node.
	shardA = shard.ID{ID128: id.ID128{0: 0x5d, 15: 0x01}}
	shardB = shard.ID{ID128: id.ID128{0: 0x5d, 15: 0x02}}
	shardC = shard.ID{ID128: id.ID128{0: 0x5d, 15: 0x03}}
	// gone is the node of the writer of shardB, which the view does not have. Its ID sorts
	// before the IDs of the nodes of the view.
	gone shard.NodeID = "node-0"
	// deferral is the decision of the budget in the receipt of each Submit.
	deferral = budget.Decision{Reason: "test: the budget defers the disruption", RetryAfter: time.Minute}
)

// world is the namespace that the doubles of a test serve, and what they received. A test
// changes its fields before it calls the client.
type world struct {
	// readErr is the error of each read of the scope readFails names.
	readErr error
	// pageErr is the error that the first page of the scope readFails names yields.
	pageErr error
	// closeErr is the error of the close of the cursor of the first page of the scope readFails
	// names.
	closeErr error
	// watchErr is the error of each watch of the views.
	watchErr error
	// callErr is the error of each call of a controller.
	callErr error
	// root and cell are the states of the root scope and of the scope of the cell.
	root, cell store.State
	// cells are the placements of the root scope, and placements those of the cell.
	cells, placements []shard.Placement
	// view is the view of the cell.
	view membership.View
	// receipt is the receipt of each Submit.
	receipt protocol.Receipt
	// decision is the decision of each Disrupt.
	decision budget.Decision
	// submitted lists the record and the proposal of each Submit, in order.
	submitted []submit
	// disrupted lists the record and the disruption of each Disrupt, in order.
	disrupted []disrupt
	// done lists the record and the arguments of each Done, in order.
	done []end
	// readFails is the scope whose reads fail, and whose first page and first cursor fail.
	readFails shard.Scope
	// viewless makes each watch of the views end before it yields a view.
	viewless bool
}

// submit is one Submit that the transport received.
type submit struct {
	// record is the record of the controller of the call.
	record shard.Cell
	// proposal is the proposal of the call.
	proposal proposal.Proposal
}

// disrupt is one Disrupt that the transport received.
type disrupt struct {
	// record is the record of the controller of the call.
	record shard.Cell
	// disruption is the disruption of the call.
	disruption budget.Disruption
}

// end is one Done that the transport received.
type end struct {
	// record is the record of the controller of the call.
	record shard.Cell
	// args are the arguments of the call.
	args protocol.DoneArgs
}

// cursor is the cursor of one page of placements of the double reader. Each page has one
// placement, and the token of a page is the index of its placement.
type cursor struct {
	// err is the error that Seq yields in place of the placement of the page.
	err error
	// closeErr is the error of Close.
	closeErr error
	// placements lists the placements of every page.
	placements []shard.Placement
	// at is the index of the placement of the page.
	at int
}

// Seq yields the placement of the page, or err.
func (c *cursor) Seq(context.Context) iter.Seq2[shard.Placement, error] {
	return func(yield func(shard.Placement, error) bool) {
		switch {
		case c.err != nil:
			yield(shard.Placement{}, c.err)
		case c.at < len(c.placements):
			yield(c.placements[c.at], nil)
		}
	}
}

// NextPage returns the index of the next placement, or no token after the last placement.
func (c *cursor) NextPage() string {
	if c.at+1 >= len(c.placements) {
		return ""
	}
	return strconv.Itoa(c.at + 1)
}

// Close returns closeErr.
func (c *cursor) Close() error {
	return c.closeErr
}

// newWorld returns a namespace with a root scope, one cell and a view of node A and node B.
// The root scope records the leader of the root and the accepted spec. The cell records its
// controller and a drain of node B. shardA has its writer on node A, and shardB on a node that
// has left the view. shardC has no writer and a secondary on that node. The receipt of each
// Submit contains deferral.
func newWorld(tb assert.TB) *world {
	tb.Helper()
	spec, err := json.Marshal(&config.Spec{Namespace: namespace, Version: accepted})
	assert.NoError(tb, err, "the spec of the tests encodes")
	result, err := json.Marshal(&deferral)
	assert.NoError(tb, err, "the decision of the tests encodes")
	return &world{
		root: store.State{
			Applied: map[proposal.ID]time.Time{{ID: id.New128([16]byte{0: 0xa1})}: origin},
			Spec:    spec,
			Cell:    shard.Cell{Controller: rootAddress, Epoch: 4, SpecVersion: accepted},
			Head:    store.Head{Generation: rootGeneration, Incarnation: inc},
		},
		cells: []shard.Placement{
			{Shard: cellKey, Replicas: []shard.Replica{{Node: "root-1", Role: shard.RolePrimary}}},
		},
		cell: store.State{
			Drains: []store.Drain{{Node: nodeB.ID}},
			Cell:   shard.Cell{ID: cellScope.Cell, Controller: cellAddress, Epoch: 6, SpecVersion: accepted},
			Head:   store.Head{Generation: cellGeneration, Incarnation: inc},
		},
		placements: []shard.Placement{
			{Shard: shardA, Replicas: []shard.Replica{{Node: nodeA.ID, Role: shard.RolePrimary}}, Epoch: epochA},
			{Shard: shardB, Replicas: []shard.Replica{{Node: gone, Role: shard.RolePrimary}}, Epoch: epochB},
			{Shard: shardC, Replicas: []shard.Replica{{Node: gone, Role: shard.RoleSecondary}}},
		},
		view:     membership.View{Scope: cellScope, Nodes: []shard.Node{nodeA, nodeB}, Incarnation: inc},
		receipt:  protocol.Receipt{Result: result, Generation: cellGeneration + 1, Incarnation: inc, Applied: true},
		decision: budget.Decision{Allowed: true},
	}
}

// client returns a client over the doubles of w, whose clock reads origin. The reader returns
// the zero State for a scope other than the root scope and the scope of the cell, and
// shard.ErrIncarnation for a read with an incarnation that a created scope does not have. The
// membership yields an empty view for another scope than the scope of the cell, and
// shard.ErrIncarnation for another incarnation.
func (w *world) client(tb assert.TB) *maintenance.Client {
	tb.Helper()
	read := func(
		_ context.Context,
		s shard.Scope,
		ni shard.NamespaceIncarnation,
		_ store.Query,
		p page.Page,
	) (store.State, page.Cursor[shard.Placement], error) {
		var st store.State
		c := &cursor{}
		switch s {
		case rootScope:
			st, c.placements = w.root, w.cells
		case cellScope:
			st, c.placements = w.cell, w.placements
		}
		switch {
		case s == w.readFails && w.readErr != nil:
			return store.State{}, nil, w.readErr
		case !ni.IsZero() && !st.Head.Incarnation.IsZero() && ni != st.Head.Incarnation:
			return store.State{}, nil, shard.ErrIncarnation
		case s == w.readFails && p.Token == "":
			c.err, c.closeErr = w.pageErr, w.closeErr
		}
		if p.Token != "" {
			at, err := strconv.Atoi(p.Token)
			assert.NoError(tb, err, "the token of a page must be the index of its placement")
			c.at = at
		}
		return st, c, nil
	}
	watch := func(_ context.Context, s shard.Scope, ni shard.NamespaceIncarnation) iter.Seq2[membership.View, error] {
		return func(yield func(membership.View, error) bool) {
			switch {
			case w.viewless:
				// The watch ends before it yields a view.
			case w.watchErr != nil:
				yield(membership.View{}, w.watchErr)
			case ni != w.view.Incarnation:
				yield(membership.View{}, shard.ErrIncarnation)
			case s != w.view.Scope:
				yield(membership.View{Scope: s, Incarnation: ni}, nil)
			case yield(w.view, nil):
				yield(membership.View{}, errLater)
			}
		}
	}
	submitted := func(_ context.Context, record *shard.Cell, p *proposal.Proposal) (protocol.Receipt, error) {
		w.submitted = append(w.submitted, submit{record: *record, proposal: *p})
		return w.receipt, w.callErr
	}
	disrupted := func(_ context.Context, record *shard.Cell, d *budget.Disruption) (budget.Decision, error) {
		dd := *d
		dd.Nodes = slices.Clone(d.Nodes)
		w.disrupted = append(w.disrupted, disrupt{record: *record, disruption: dd})
		return w.decision, w.callErr
	}
	ended := func(_ context.Context, record *shard.Cell, a *protocol.DoneArgs) error {
		w.done = append(w.done, end{record: *record, args: *a})
		return w.callErr
	}
	c, err := maintenance.New(&maintenance.Config{
		Store:      storetest.NewReaderStub(tb, storetest.WithReaderRead(read)),
		Membership: membershiptest.NewMembershipStub(tb, membershiptest.WithMembershipWatch(watch)),
		Transport: protocoltest.NewControllerClientStub(tb,
			protocoltest.WithControllerClientSubmit(submitted),
			protocoltest.WithControllerClientDisrupt(disrupted),
			protocoltest.WithControllerClientDone(ended),
		),
		Clock:    fake.New(origin),
		Rand:     seeded.New(1),
		Lifetime: lifetime,
	})
	assert.NoError(tb, err, "New must return a client of a complete config")
	return c
}

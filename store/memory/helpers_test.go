// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package memory_test

import (
	"context"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The names of the records of the tests.
const (
	namespace shard.Namespace = "orders"
	cell      shard.CellID    = "cell-1"
	node      shard.NodeID    = "node-a"
	otherNode shard.NodeID    = "node-b"
)

// The first bytes of the identifiers of the tests, one per kind of identifier.
const (
	tagIncarnation byte = 0x1a
	tagShard       byte = 0x5d
	tagProposal    byte = 0x9c
	tagDisruption  byte = 0xd1
)

// caller is the identity that requests the drains and the disruptions, and submits the
// proposals, of the tests.
const caller shard.Identity = "spiffe://memory_test/operator"

// term is the term of the writers of the tests.
const term epoch.Epoch = 1

// waitFor bounds each wait of the tests for an item, an event or a closed channel that the
// store delivers at once.
const waitFor = 10 * time.Second

// The scope, the incarnations, the placements and the clock of the tests.
var (
	// scope is the scope of the tests.
	scope = shard.Scope{Namespace: namespace, Cell: cell}
	// incA is the namespace incarnation of the commits of the tests.
	incA = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0a})}
	// incB is the namespace incarnation of the tests of another incarnation.
	incB = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0b})}
	// placements lists the two placements of the tests, in position order.
	placements = []shard.Placement{
		{
			Shard:    shard.ID{ID128: id.ID128{0: tagShard, 15: 0x01}},
			Range:    shard.Range{End: []byte{0x80}},
			Replicas: []shard.Replica{{Node: node, Role: shard.RolePrimary}},
		},
		{
			Shard:    shard.ID{ID128: id.ID128{0: tagShard, 15: 0x02}},
			Range:    shard.Range{Start: []byte{0x80}},
			Replicas: []shard.Replica{{Node: node, Role: shard.RolePrimary}},
		},
	}
	// origin is the start of the clock of each store of the tests.
	origin = time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC)
)

// lease is the lease of the sessions of the tests. It renews every 3 seconds, with a
// validity of 5 seconds and a TTL of 10 seconds.
var lease = ownership.Lease{
	TTL:          shard.Duration(10 * time.Second),
	Renew:        shard.Duration(3 * time.Second),
	MaxDriftRate: 0.5,
	OnLoss:       ownership.Stop,
}

// tracked is a context that a test follows with a weak pointer, to learn whether the store
// keeps a reference to it.
type tracked struct {
	context.Context //nolint:containedctx // the test follows this context to learn whether the store releases it.
}

// commit commits m to the scope of s through a writer of inc at the generation of the scope,
// with the commit ID of that generation, and returns the generation that m created. It fails
// t when the commit does not apply.
func commit(t *testing.T, s *memory.Store, inc shard.NamespaceIncarnation, m *store.Mutation) epoch.Epoch {
	t.Helper()
	h, err := s.Head(t.Context(), scope)
	assert.NoError(t, err, "Head returns the header of the scope")
	m.ID = store.CommitID{Incarnation: inc, Term: term, Seq: uint64(h.Generation) + 1}
	g, err := s.Writer(scope, inc, term).Commit(t.Context(), h.Generation, m)
	assert.NoError(t, err, "Commit applies the mutation")
	return g
}

// joinEach joins the node and then the other node to the scope of s in the incarnation A,
// each with the context ctx.
func joinEach(ctx context.Context, t *testing.T, s *memory.Store) {
	t.Helper()
	for _, id := range [...]shard.NodeID{node, otherNode} {
		n := shard.Node{ID: id}
		_, err := s.Membership().Join(ctx, scope, incA, &n, lease)
		assert.NoError(t, err, "Join registers the node")
	}
}

// open returns a store without limits on a clock of its own, and closes it when t ends.
func open(t *testing.T) *memory.Store {
	t.Helper()
	s := memory.New(fake.New(origin))
	t.Cleanup(func() { assert.NoError(t, s.Close(), "Close returns nil") })
	return s
}

// manual returns a store under WithManualTime whose leases run on clk, and closes it when t
// ends.
func manual(t *testing.T, clk *fake.Clock) *memory.Store {
	t.Helper()
	s := memory.New(clk, memory.WithManualTime())
	t.Cleanup(func() { assert.NoError(t, s.Close(), "Close returns nil") })
	return s
}

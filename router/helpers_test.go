// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package router_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"iter"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/rand/seeded"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/router"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The names of the tests.
const (
	// namespace is the namespace of the tests.
	namespace shard.Namespace = "orders"
	// slotsConfig is the configuration of fixed-slots in the spec of the tests.
	slotsConfig = `{"slots":4}`
	// keyPrefix starts each key that the tests route.
	keyPrefix = "key-"
)

// waitFor bounds each wait of a test. Inside a bubble of testing/synctest, the wait runs on
// the virtual time of the bubble.
const waitFor = time.Hour

// The epochs of the tests.
const (
	// rootTerm is the term of the root that writes the root scope.
	rootTerm epoch.Epoch = 1
	// cellTerm is the term of the controller of each cell.
	cellTerm epoch.Epoch = 3
	// firstVersion is the version of the spec of the tests.
	firstVersion epoch.Epoch = 1
	// firstEpoch is the epoch of the first writer of a shard.
	firstEpoch epoch.Epoch = 1
)

// leaseTTL is the TTL of the lease of each session that a test joins.
const leaseTTL = 10 * time.Second

// The nodes of the tests.
var (
	nodeA = shard.Node{ID: "node-a", Address: "node-a:7000"}
	nodeB = shard.Node{ID: "node-b", Address: "node-b:7000"}
)

// shards lists the shard of each slot of the tests, in slot order. The shard of slot i has
// the last byte i+1.
var shards = []shard.ID{
	{ID128: id.ID128{0: 0x02, 15: 1}},
	{ID128: id.ID128{0: 0x02, 15: 2}},
	{ID128: id.ID128{0: 0x02, 15: 3}},
	{ID128: id.ID128{0: 0x02, 15: 4}},
}

// The cells, the scopes, the incarnations and the lease of the tests.
var (
	// cellKey is the 16 bytes of the ULID of the cell, which is the shard of the cell in the
	// root scope.
	cellKey = id.ID128{0: 0x01, 15: 0xce}
	// cellID is the cell of the tests: the text of the ULID of cellKey.
	cellID = shard.CellID(ulid.Format(cellKey.ID()))
	// cellScope is the scope of the cell of the tests.
	cellScope = shard.Scope{Namespace: namespace, Cell: cellID}
	// otherKey is the 16 bytes of the ULID of the cell of the namespace that the root creates
	// again.
	otherKey = id.ID128{0: 0x01, 15: 0xcf}
	// otherCell is the cell of otherKey.
	otherCell = shard.CellID(ulid.Format(otherKey.ID()))
	// rootScope is the root scope of the namespace of the tests.
	rootScope = shard.Scope{Namespace: namespace}
	// incA is the namespace incarnation of the tests.
	incA = shard.NamespaceIncarnation{ID: id.New128([id.Size128]byte{0: 0x1a, 15: 0x0a})}
	// incB is the namespace incarnation of the namespace that the root creates again.
	incB = shard.NamespaceIncarnation{ID: id.New128([id.Size128]byte{0: 0x1a, 15: 0x0b})}
	// lease is the lease of each session that a test joins.
	lease = ownership.Lease{
		TTL:          shard.Duration(leaseTTL),
		Renew:        shard.Duration(leaseTTL / 3),
		MaxDriftRate: 0.01,
		OnLoss:       ownership.ContinueFenced,
	}
)

// errStop is the cause with which a test ends a context.
var errStop = errors.New("test: the caller stops")

// cleaner is an assert.TB that runs functions when its test ends, such as a *testing.T or a
// *testing.B.
type cleaner interface {
	assert.TB
	// Cleanup registers f to run when the test ends.
	Cleanup(f func())
}

// fixture is the store and the registry that the router of a test reads. The test writes
// the root scope and the scope of each cell in place of the root and of the controllers of
// the cells.
type fixture struct {
	// st is the store and the membership of the router.
	st *memory.Store
	// clock is the clock of the store and of the router.
	clock *fake.Clock
	// reg is the registry of the router.
	reg *config.Registry
	// seqs maps each scope to the Seq of its last commit.
	seqs map[shard.Scope]uint64
	// spec is the resolved spec of the tests as JSON, as the root records it.
	spec []byte
	// spans lists the slots of the namespace, as the partitioner divides the whole range.
	spans []partition.Span
	// keys lists a key of each slot, in slot order.
	keys [][]byte
	// mapping is the mapping of the spec.
	mapping shard.Mapping
	// cell is the scope of the cell of the namespace that the root created last.
	cell shard.Scope
	// incarnation is the namespace incarnation of the commits and the sessions of the
	// tests.
	incarnation shard.NamespaceIncarnation
	// placed is the generation of the commit that placed the slots of the cell.
	placed epoch.Epoch
}

// newFixture returns a fixture whose root has created the namespace of the tests with its
// cell. The controller of the cell has placed every slot on node A, and the nodes A and B
// have sessions in the cell until tb ends.
func newFixture(tb cleaner) *fixture {
	tb.Helper()
	f := bare(tb)
	f.write(tb, rootScope, &store.Mutation{
		Spec:    f.spec,
		Mapping: &f.mapping,
		Put:     []shard.Placement{{Shard: shard.ID{ID128: cellKey}}},
	})
	f.placed = f.write(tb, cellScope, &store.Mutation{Put: f.placements(nodeA.ID, firstEpoch)})
	f.join(tb, cellScope, nodeA, nodeB)
	return f
}

// bare returns a fixture of an empty store. It resolves the spec of the tests and divides
// the whole range into its slots. The store closes when tb ends.
func bare(tb cleaner) *fixture {
	tb.Helper()
	clk := fake.New(time.Now())
	st := memory.New(clk)
	tb.Cleanup(func() { assert.NoError(tb, st.Close(), "the store closes") })
	reg := config.NewRegistry(builtin.Support())
	assert.NoError(tb, builtin.Register(reg), "the built-ins register")
	s := config.Spec{
		Namespace:    namespace,
		Preset:       builtin.ExclusiveWorker,
		Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
		Version:      firstVersion,
	}
	provided := map[config.Axis][]config.Capability{
		config.Store:      st.Provides(),
		config.Membership: st.Membership().Provides(),
	}
	res, err := config.Resolve(reg, &s, provided)
	assert.NoError(tb, err, "the spec of the tests resolves")
	raw, err := json.Marshal(&res.Spec)
	assert.NoError(tb, err, "the resolved spec encodes")
	p, err := config.Build[partition.Partitioner](reg, res, config.Partitioning)
	assert.NoError(tb, err, "the partitioner of the tests builds")
	spans, err := p.Initial(shard.Range{})
	assert.NoError(tb, err, "the partitioner divides the whole range")
	return &fixture{
		st:          st,
		clock:       clk,
		reg:         reg,
		seqs:        make(map[shard.Scope]uint64),
		spec:        raw,
		spans:       spans,
		keys:        keysOf(p, len(spans)),
		mapping:     res.Mapping,
		cell:        cellScope,
		incarnation: incA,
	}
}

// config returns the Config of the router of the tests over f.
func (f *fixture) config() *router.Config {
	return &router.Config{
		Store:      f.st,
		Membership: f.st.Membership(),
		Clock:      f.clock,
		Rand:       seeded.New(1),
		Reporter:   noop.New(),
		Registry:   f.reg,
		Logger:     slog.New(slog.DiscardHandler),
		Namespace:  namespace,
	}
}

// start starts the router of the tests after each option changes its Config. The router
// stops when tb ends.
func (f *fixture) start(tb cleaner, opts ...func(c *router.Config)) *router.Watcher {
	tb.Helper()
	cfg := f.config()
	for _, opt := range opts {
		opt(cfg)
	}
	w, err := router.New(assert.Context(tb), cfg)
	assert.NoError(tb, err, "New starts the router")
	tb.Cleanup(func() { assert.NoError(tb, w.Close(), "Close stops the router") })
	return w
}

// placements returns a placement of each slot on the node n at the epoch e, in slot order.
func (f *fixture) placements(n shard.NodeID, e epoch.Epoch) []shard.Placement {
	ps := make([]shard.Placement, 0, len(f.spans))
	for i := range f.spans {
		ps = append(ps, f.placement(i, n, e))
	}
	return ps
}

// placement returns the placement of slot i on the node n at the epoch e, and a placement
// without replicas for an empty n.
func (f *fixture) placement(i int, n shard.NodeID, e epoch.Epoch) shard.Placement {
	p := shard.Placement{Shard: shards[i], Range: f.spans[i].Range, Epoch: e, Reserved: e, Sequence: uint64(e)}
	if n != "" {
		p.Replicas = []shard.Replica{{Node: n, Role: shard.RolePrimary}}
	}
	return p
}

// write commits m to the scope s as its writer, and returns the generation of the commit.
func (f *fixture) write(tb assert.TB, s shard.Scope, m *store.Mutation) epoch.Epoch {
	tb.Helper()
	gen, err := f.commit(assert.Context(tb), s, m)
	assert.NoError(tb, err, "the writer of the scope commits")
	return gen
}

// commit commits m to the scope s as its writer: the root at its term for the root scope,
// and the controller of the cell at its term otherwise. It returns the generation of the
// commit and the error of the store. It does not assert, so a client of a history calls it.
func (f *fixture) commit(ctx context.Context, s shard.Scope, m *store.Mutation) (epoch.Epoch, error) {
	head, err := f.st.Head(ctx, s)
	if err != nil {
		return 0, err
	}
	term := cellTerm
	if s.Cell == "" {
		term = rootTerm
	}
	f.seqs[s]++
	m.ID = store.CommitID{Incarnation: f.incarnation, Term: term, Seq: f.seqs[s]}
	return f.st.Writer(s, f.incarnation, term).Commit(ctx, head.Generation, m)
}

// join gives each node of nodes a session in the cell of the scope s, in the namespace
// incarnation of f. Each session lasts until tb ends.
func (f *fixture) join(tb assert.TB, s shard.Scope, nodes ...shard.Node) {
	tb.Helper()
	for _, n := range nodes {
		_, err := f.st.Membership().Join(assert.Context(tb), s, f.incarnation, &n, lease)
		assert.NoError(tb, err, "the node joins the cell")
	}
}

// gated is a store whose watches pass no item until waitFor has passed since newGated built
// it. A watch then passes the items of the watch of the memory store. It embeds a
// memory.Store, so it provides the capabilities of that store.
type gated struct {
	*memory.Store
	// gate is closed once waitFor has passed.
	gate chan struct{}
	// opened is true once gate is closed.
	opened atomic.Bool
}

// newGated returns a store over st whose watches pass no item until waitFor has passed.
func newGated(st *memory.Store) *gated {
	g := &gated{Store: st, gate: make(chan struct{})}
	time.AfterFunc(waitFor, func() {
		g.opened.Store(true)
		close(g.gate)
	})
	return g
}

// Watch waits until gate is closed, also when ctx ends first. It then yields the items of
// the watch of the memory store.
func (g *gated) Watch(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation, from epoch.Epoch,
	n shard.NodeID,
) iter.Seq2[store.Event, error] {
	return func(yield func(store.Event, error) bool) {
		<-g.gate
		for ev, err := range g.Store.Watch(ctx, s, inc, from, n) {
			if !yield(ev, err) {
				return
			}
		}
	}
}

// keysOf returns a key of each of the n slots of p, in slot order.
func keysOf(p partition.Partitioner, n int) [][]byte {
	keys := make([][]byte, n)
	for i, found := 0, 0; found < n; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i))
		s := binary.BigEndian.Uint32(p.Position(nil, key))
		if keys[s] == nil {
			keys[s], found = key, found+1
		}
	}
	return keys
}

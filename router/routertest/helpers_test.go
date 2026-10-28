// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routertest_test

import (
	"encoding/json"
	"log/slog"
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
	"go.thesmos.sh/sharder/router/routertest"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The namespace of the router of the suite of Watcher.
const (
	// namespace is the namespace.
	namespace shard.Namespace = "orders"
	// slotsConfig is the configuration of fixed-slots in the spec of the namespace.
	slotsConfig = `{"slots":4}`
)

// The epochs of the commits of the namespace of the suite of Watcher.
const (
	// rootTerm is the term of the root that writes the root scope.
	rootTerm epoch.Epoch = 1
	// cellTerm is the term of the controller of the cell.
	cellTerm epoch.Epoch = 3
	// firstVersion is the version of the spec.
	firstVersion epoch.Epoch = 1
	// firstEpoch is the epoch of the writer of each shard.
	firstEpoch epoch.Epoch = 1
)

// leaseTTL is the TTL of the lease of the session of each node.
const leaseTTL = 10 * time.Second

// The records of the namespace of the suite of Watcher.
var (
	// cellKey is the 16 bytes of the ULID of the cell, which is the shard of the cell in the
	// root scope.
	cellKey = id.ID128{0: 0x01, 15: 0xce}
	// incarnation is the namespace incarnation.
	incarnation = shard.NamespaceIncarnation{ID: id.New128([id.Size128]byte{0: 0x1a, 15: 0x0a})}
	// origin is the start of the clock of the store and of the router.
	origin = time.Date(2026, time.October, 27, 0, 0, 0, 0, time.UTC)
	// nodes are the nodes of the view of the cell. Node A hosts every shard.
	nodes = []shard.Node{{ID: "node-a", Address: "node-a:7000"}, {ID: "node-b", Address: "node-b:7000"}}
	// lease is the lease of the session of each node.
	lease = ownership.Lease{
		TTL:          shard.Duration(leaseTTL),
		Renew:        shard.Duration(leaseTTL / 3),
		MaxDriftRate: 0.01,
		OnLoss:       ownership.ContinueFenced,
	}
)

// newWatcher returns a started router of the namespace of the suite of Watcher, the subject
// of the suite. The root of a memory store has created the namespace with one cell, the
// controller of the cell has placed each of its four slots on node A, and the nodes A and B
// have sessions in the cell. The handle closes the router and then the store when the check
// ends.
func newWatcher(h *routertest.WatcherHandle) *router.Watcher {
	clk := fake.New(origin)
	st := memory.New(clk)
	h.Cleanup(func() { assert.NoError(h, st.Close(), "the store must close") })
	reg := config.NewRegistry(builtin.Support())
	assert.NoError(h, builtin.Register(reg), "the built-ins must register")
	res, err := config.Resolve(reg, &config.Spec{
		Namespace:    namespace,
		Preset:       builtin.ExclusiveWorker,
		Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
		Version:      firstVersion,
	}, map[config.Axis][]config.Capability{
		config.Store:      st.Provides(),
		config.Membership: st.Membership().Provides(),
	})
	assert.NoError(h, err, "the spec of the namespace must resolve")
	spec, err := json.Marshal(&res.Spec)
	assert.NoError(h, err, "the resolved spec must encode")
	p, err := config.Build[partition.Partitioner](reg, res, config.Partitioning)
	assert.NoError(h, err, "the partitioner of the namespace must build")
	spans, err := p.Initial(shard.Range{})
	assert.NoError(h, err, "the partitioner must divide the whole range")

	cell := shard.Scope{Namespace: namespace, Cell: shard.CellID(ulid.Format(cellKey.ID()))}
	placements := make([]shard.Placement, len(spans))
	for i, s := range spans {
		placements[i] = shard.Placement{
			Shard:    shard.ID{ID128: id.ID128{0: 0x02, 15: byte(i + 1)}},
			Range:    s.Range,
			Epoch:    firstEpoch,
			Reserved: firstEpoch,
			Sequence: 1,
			Replicas: []shard.Replica{{Node: nodes[0].ID, Role: shard.RolePrimary}},
		}
	}
	commit := func(s shard.Scope, term epoch.Epoch, m *store.Mutation) {
		m.ID = store.CommitID{Incarnation: incarnation, Term: term, Seq: 1}
		_, err = st.Writer(s, incarnation, term).Commit(h.Context(), epoch.Zero, m)
		assert.NoError(h, err, "the writer of the scope must commit")
	}
	commit(shard.Scope{Namespace: namespace}, rootTerm, &store.Mutation{
		Spec:    spec,
		Mapping: &res.Mapping,
		Put:     []shard.Placement{{Shard: shard.ID{ID128: cellKey}}},
	})
	commit(cell, cellTerm, &store.Mutation{Put: placements})
	for _, n := range nodes {
		_, err = st.Membership().Join(h.Context(), cell, incarnation, &n, lease)
		assert.NoError(h, err, "the node must join the cell")
	}

	w, err := router.New(h.Context(), &router.Config{
		Store:      st,
		Membership: st.Membership(),
		Clock:      clk,
		Rand:       seeded.New(1),
		Reporter:   noop.New(),
		Registry:   reg,
		Logger:     slog.New(slog.DiscardHandler),
		Namespace:  namespace,
	})
	assert.NoError(h, err, "New must start the router")
	h.Cleanup(func() { assert.NoError(h, w.Close(), "Close must stop the router") })
	return w
}

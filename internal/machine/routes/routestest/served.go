// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routestest

import (
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"slices"
	"strconv"
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
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/routes"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The namespace that Serve serves.
const (
	// namespace is the namespace.
	namespace shard.Namespace = "orders"
	// pageSize is the number of placements of each page that Serve reads.
	pageSize = 1000
	// movedAddress is the address that the first view of Views gives the node that it moves.
	movedAddress = "moved:7000"
	// shardTag is the first byte of the ID of each shard of the cell.
	shardTag = 0x02
)

// The epochs of the commits of the namespace that Serve serves.
const (
	// rootTerm is the term of the root that writes the root scope.
	rootTerm epoch.Epoch = 1
	// cellTerm is the term of the controller of the cell.
	cellTerm epoch.Epoch = 3
	// firstEpoch is the epoch of the writer of each shard, and the version of the spec.
	firstEpoch epoch.Epoch = 1
)

// The positions of the nodes of the views of the cell in nodes.
const (
	// hostIndex is the position of node A, which hosts every shard.
	hostIndex = 0
	// idleIndex is the position of node C, which hosts no shard.
	idleIndex = 2
)

// The records of the namespace that Serve serves.
var (
	// cellKey is the 16 bytes of the ULID of the cell, which is the shard of the cell in the
	// root scope.
	cellKey = id.ID128{0: 0x01, 15: 0xce}
	// incarnation is the namespace incarnation.
	incarnation = shard.NamespaceIncarnation{ID: id.New128([id.Size128]byte{0: 0x1a, 15: 0x0a})}
	// origin is the start of the clock of the store.
	origin = time.Date(2026, time.October, 27, 0, 0, 0, 0, time.UTC)
	// nodes are the nodes of the views of the cell: node A, node B and node C.
	nodes = []shard.Node{
		{ID: "node-a", Address: "node-a:7000", Incarnation: 1},
		{ID: "node-b", Address: "node-b:7000", Incarnation: 1},
		{ID: "node-c", Address: "node-c:7000", Incarnation: 1},
	}
)

// Cleaner is the test, the case or the handle that Serve reports to. Serve registers the
// close of its store with Cleanup.
type Cleaner interface {
	assert.TB
	// Cleanup registers f to run when the test, the case or the check ends.
	Cleanup(f func())
}

// Viewed is a router whose cell takes the views of a check. ViewsReplaceTheirRoutesAlone
// reads the views of its subject through it, and a subject that does not implement it ends
// that check with a fault that wraps ErrUnviewed.
type Viewed interface {
	// Views returns two views of the cell of the router for its watch of views. The first
	// view gives one node another address, and the second view gives the node its address
	// again. With hosting true, the node hosts the replica of each slot of the cell. With
	// hosting false, the node hosts no replica. Views also returns the number of slots whose
	// route each of the views replaces.
	Views(hosting bool) (views [2]machine.Viewed, moved int)
}

// Served is a router that serves the namespace of Serve. Serve performs the reads of the
// router until it serves, and no later effect of the router runs.
//
// # Concurrency
//
// Route and Err of a Served are safe for concurrent use, as those of its router are. Step
// and Views are not.
type Served struct {
	*routes.Machine

	// watch is the watch of the views of the cell.
	watch machine.WatchViews
	// revision is the revision of the last view of the cell.
	revision epoch.Epoch
	// slots is the number of slots of the cell.
	slots int
}

var _ Viewed = (*Served)(nil)

// Serve returns a router that serves a namespace of one cell of n slots, as a runtime
// leaves it. The root of a memory store has created the namespace, and the controller of
// the cell has placed every slot on node A. The router has read the scope of the cell and
// two views of the nodes A, B and C, so a later view allocates only the routes that it
// replaces. The router reports to a reporter that keeps no value. Serve stops tb when the
// router does not serve, and closes the store when tb ends.
func Serve(tb Cleaner, n int) *Served {
	tb.Helper()
	ctx := assert.Context(tb)
	st := memory.New(fake.New(origin))
	tb.Cleanup(func() { assert.NoError(tb, st.Close(), "the store must close") })
	reg := config.NewRegistry(builtin.Support())
	assert.NoError(tb, builtin.Register(reg), "the built-ins must register")
	provided := map[config.Axis][]config.Capability{
		config.Store:      st.Provides(),
		config.Membership: st.Membership().Provides(),
	}
	slots := json.RawMessage(`{"slots":` + strconv.Itoa(n) + `}`)
	res, err := config.Resolve(reg, &config.Spec{
		Namespace:    namespace,
		Preset:       builtin.ExclusiveWorker,
		Partitioning: config.Selection{Name: fixedslots.Name, Config: slots},
		Version:      firstEpoch,
	}, provided)
	assert.NoError(tb, err, "the spec of the namespace must resolve")
	spec, err := json.Marshal(&res.Spec)
	assert.NoError(tb, err, "the resolved spec must encode")
	p, err := config.Build[partition.Partitioner](reg, res, config.Partitioning)
	assert.NoError(tb, err, "the partitioner of the namespace must build")
	spans, err := p.Initial(shard.Range{})
	assert.NoError(tb, err, "the partitioner must divide the whole range")

	placements := make([]shard.Placement, len(spans))
	for i, s := range spans {
		var sid id.ID128
		sid[0] = shardTag
		binary.BigEndian.PutUint32(sid[12:], uint32(i))
		placements[i] = shard.Placement{
			Shard:    shard.ID{ID128: sid},
			Range:    s.Range,
			Epoch:    firstEpoch,
			Reserved: firstEpoch,
			Sequence: 1,
			Replicas: []shard.Replica{{Node: nodes[hostIndex].ID, Role: shard.RolePrimary}},
		}
	}
	commit := func(s shard.Scope, term epoch.Epoch, m *store.Mutation) {
		m.ID = store.CommitID{Incarnation: incarnation, Term: term, Seq: 1}
		_, err = st.Writer(s, incarnation, term).Commit(ctx, epoch.Zero, m)
		assert.NoError(tb, err, "the writer of the scope must commit")
	}
	commit(shard.Scope{Namespace: namespace}, rootTerm, &store.Mutation{
		Spec:    spec,
		Mapping: &res.Mapping,
		Put:     []shard.Placement{{Shard: shard.ID{ID128: cellKey}}},
	})
	commit(shard.Scope{Namespace: namespace, Cell: shard.CellID(ulid.Format(cellKey.ID()))}, cellTerm,
		&store.Mutation{Put: placements})

	m, err := routes.New(&routes.Config{
		Rand:      seeded.New(1),
		Reporter:  noop.New(),
		Logger:    slog.New(slog.DiscardHandler),
		Registry:  reg,
		Provided:  provided,
		Namespace: namespace,
	})
	assert.NoError(tb, err, "New must return a router of a complete config")
	s := &Served{Machine: m, slots: n}
	started := machine.ReplyToken(1)
	var replies []machine.Reply[struct{}]
	pending := m.Step(machine.Started{Reply: started}, nil)
	for len(pending) > 0 {
		e := pending[0]
		pending = slices.Delete(pending, 0, 1)
		switch e := e.(type) {
		case machine.Read:
			pending = m.Step(read(tb, st, &e), pending)
		case machine.WatchViews:
			s.watch = e
		case machine.Reply[struct{}]:
			replies = append(replies, e)
		}
	}
	for range 2 {
		s.revision++
		for _, e := range m.Step(s.view(nodes), nil) {
			if r, ok := e.(machine.Reply[struct{}]); ok {
				replies = append(replies, r)
			}
		}
	}
	assert.Equal(tb, replies, []machine.Reply[struct{}]{{To: started}}, "the router must serve the namespace")
	return s
}

// Views returns two views of the cell of s. The first gives node A another address, or
// node C with hosting false, and the second view gives the node its address again.
func (s *Served) Views(hosting bool) ([2]machine.Viewed, int) {
	i, moved := idleIndex, 0
	if hosting {
		i, moved = hostIndex, s.slots
	}
	away := slices.Clone(nodes)
	away[i].Address = movedAddress
	s.revision++
	first := s.view(away)
	s.revision++
	return [2]machine.Viewed{first, s.view(nodes)}, moved
}

// view returns the event of a view of the cell of s with the nodes listed, at the revision
// of s.
func (s *Served) view(listed []shard.Node) machine.Viewed {
	return machine.Viewed{
		View: membership.View{
			Scope:       s.watch.Scope,
			Nodes:       listed,
			Revision:    s.revision,
			Incarnation: s.watch.Incarnation,
		},
		Token: s.watch.Token,
	}
}

// read reads the page of the Read e from st with pageSize placements, and returns the event
// of its result.
func read(tb assert.TB, st *memory.Store, e *machine.Read) machine.Event {
	tb.Helper()
	ctx := assert.Context(tb)
	pg := e.Page
	pg.Limit = pageSize
	state, cursor, err := st.Read(ctx, e.Scope, e.Incarnation, e.Query, pg)
	if err != nil {
		return machine.PageRead{Err: err, Token: e.Token}
	}
	var ps []shard.Placement
	for p, err := range cursor.Seq(ctx) {
		assert.NoError(tb, err, "the cursor of the memory store must yield its placements")
		ps = append(ps, p)
	}
	next := cursor.NextPage()
	assert.NoError(tb, cursor.Close(), "the cursor must close")
	return machine.PageRead{Placements: ps, Next: next, State: state, Token: e.Token}
}

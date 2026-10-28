// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"iter"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/coretest/telemetrytest"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/rand/constant"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/routes"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/shard/shardtest"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
	"go.thesmos.sh/sharder/store/storetest"
)

// The names of the records of the tests.
const (
	// namespace is the namespace of the tests.
	namespace shard.Namespace = "orders"
	// slotsConfig is the configuration of fixed-slots in the spec of the tests.
	slotsConfig = `{"slots":4}`
	// slots is the number of slots of the namespace of the tests.
	slots = 4
	// keyPrefix starts each key that the tests route.
	keyPrefix = "key-"
)

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

// maxDeliveries bounds the effects that run performs and the generations that follow
// passes, so a router that never settles fails its test instead of the timeout of go test.
const maxDeliveries = 10000

// pageLimit is the number of placements on each page that the harness reads, so a snapshot
// of the four slots of a cell takes two pages.
const pageLimit = 2

// serial is the byte of the ID of an incarnation and of the key of a cell that recreation
// sets to the number of the recreation.
const serial = 14

// noNode is the writer of a placement without a replica.
const noNode shard.NodeID = ""

// half is the value that rand.Float64 returns for the source of randomness of the tests, so
// each backoff is half of its ceiling.
const half = 0.5

// marker is the effect that the harness passes in out with each event: an Unwatch without a
// watch, which the router never returns itself.
var marker machine.Effect = machine.Unwatch{}

// The errors of the stores and the memberships of the tests.
var (
	// errBroken is an error of a backend that a retry does not fix.
	errBroken = errs.WithClass(errors.New("test: the backend is broken"), errs.Invalid)
	// errBusy is an error of a backend that a retry can fix.
	errBusy = errs.WithClass(errors.New("test: the backend is busy"), errs.Transient)
)

// The nodes of the tests, each at its first incarnation. Node C hosts no replica.
var (
	nodeA = shard.Node{ID: "node-a", Address: "node-a:7000", Incarnation: 1}
	nodeB = shard.Node{ID: "node-b", Address: "node-b:7000", Incarnation: 1}
	nodeC = shard.Node{ID: "node-c", Address: "node-c:7000", Incarnation: 1}
)

// shards lists the shard of each slot of the tests, in slot order. The shard of slot i has
// the last byte i+1.
var shards = []shard.ID{
	{ID128: id.ID128{0: 0x02, 15: 1}},
	{ID128: id.ID128{0: 0x02, 15: 2}},
	{ID128: id.ID128{0: 0x02, 15: 3}},
	{ID128: id.ID128{0: 0x02, 15: 4}},
}

// The cells, the scopes, the incarnations and the clock of the tests.
var (
	// cellKey is the 16 bytes of the ULID of the cell, which is the shard of the cell in the
	// root scope.
	cellKey = id.ID128{0: 0x01, 15: 0xce}
	// cellID is the cell of the tests: the text of the ULID of cellKey.
	cellID = shard.CellID(ulid.Format(cellKey.ID()))
	// cellScope is the scope of the cell of the tests.
	cellScope = shard.Scope{Namespace: namespace, Cell: cellID}
	// otherKey is the 16 bytes of the ULID of the second cell of the tests that divide the
	// namespace into two cells.
	otherKey = id.ID128{0: 0x01, 15: 0xcf}
	// otherScope is the scope of the second cell.
	otherScope = shard.Scope{Namespace: namespace, Cell: shard.CellID(ulid.Format(otherKey.ID()))}
	// middle is the position of slot 2, where the second cell starts.
	middle = []byte{0, 0, 0, 2}
	// rootScope is the root scope of the namespace of the tests.
	rootScope = shard.Scope{Namespace: namespace}
	// incBytes is the 16 bytes of the ID of incA.
	incBytes = [id.Size128]byte{0: 0x1a, 15: 0x0a}
	// incA is the namespace incarnation of the tests.
	incA = shard.NamespaceIncarnation{ID: id.New128(incBytes)}
	// incB is the namespace incarnation of a namespace that the root creates again.
	incB = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: 0x1a, 15: 0x0b})}
	// origin is the start of the clock of the store.
	origin = time.Date(2026, time.October, 12, 0, 0, 0, 0, time.UTC)
	// provided lists the capabilities of the store and the membership of the tests.
	provided = map[config.Axis][]config.Capability{
		config.Store:      {config.Durable},
		config.Membership: {config.Leases},
	}
)

// scopeKey is a scope in one namespace incarnation.
type scopeKey struct {
	// scope is the scope.
	scope shard.Scope
	// incarnation is the namespace incarnation.
	incarnation shard.NamespaceIncarnation
}

// testCase is the test, or the case of a property, that a harness runs in.
type testCase interface {
	assert.TB
	// Cleanup registers f to run when the test or the case ends.
	Cleanup(f func())
}

// setup is the configuration of a harness before New.
type setup struct {
	// spec is the spec that the root records.
	spec config.Spec
	// cfg is the Config of the router, which newHarness completes.
	cfg routes.Config
	// root is the mutation that creates the root scope, which newHarness completes with the
	// spec and the mapping when they are nil.
	root store.Mutation
	// create is true when the root has created the namespace before the router starts.
	create bool
	// place is true when the controller of the cell has placed every slot on node A before
	// the router starts.
	place bool
}

// option changes the setup of a harness.
type option func(*setup)

// harness is the runtime of the tests. It performs the reads of one router against a memory
// store, passes the generations of its watches with follow and its views with view, and
// keeps what the router appended. The tests commit to the root scope as the root does, and
// to the scope of each cell as its controller does.
type harness struct {
	// observe receives each effect that the router appends, before route keeps it, or is nil.
	observe func(e machine.Effect)
	// wrap runs each step of the router, such as to record it in a history, or is nil.
	wrap func(step func())
	// pause reports whether run leaves the effect e pending. With a nil pause, run performs
	// every effect.
	pause func(e machine.Effect) bool
	// st is the store of the router.
	st *memory.Store
	// machine is the router under test.
	machine *routes.Machine
	// registry is the registry of the router.
	registry *config.Registry
	// partitioner is the partitioner of the spec of the setup.
	partitioner partition.Partitioner
	// logs keeps every record that the router logs.
	logs *telemetrytest.Records
	// metrics keeps every value that the instruments of the router report.
	metrics *telemetrytest.Recorder
	// spec is the spec that the root records, as JSON.
	spec []byte
	// mapping is the mapping of the spec.
	mapping shard.Mapping
	// spans lists the slots of the namespace, as the partitioner divides the whole range.
	spans []partition.Span
	// keys lists a key of each slot, in slot order.
	keys [][]byte
	// pending lists the effects that run performs, the oldest first.
	pending []machine.Effect
	// watches lists the watches of scopes that run.
	watches []machine.Watch
	// viewers lists the watches of views that run.
	viewers []machine.WatchViews
	// timers lists the Timers that the router armed, the oldest first.
	timers []machine.Timer
	// replies lists every reply of the router, in order.
	replies []machine.Reply[struct{}]
	// unwatched lists the tokens of every Unwatch of the router, in order.
	unwatched []machine.Token
	// issued lists the token of every effect of the router that waits for a result, in order.
	issued []machine.Token
	// seqs maps each scope to the Seq of its last commit.
	seqs map[shard.Scope]uint64
	// delivered maps each scope in each incarnation to the highest generation that the harness
	// has passed to the router: the generation of the last page of a snapshot or of an item of
	// a watch.
	delivered map[scopeKey]epoch.Epoch
	// cell is the scope of the cell of the namespace that the root created last.
	cell shard.Scope
	// incarnation is the namespace incarnation of the commits of the tests.
	incarnation shard.NamespaceIncarnation
	// placed is the generation of the commit that placed the slots of the cell before the
	// router started, and zero without one.
	placed epoch.Epoch
	// limit is the number of placements on each page that the harness reads.
	limit int
	// revision is the revision of the last view that the harness passed.
	revision epoch.Epoch
	// calls is the token of the last inbound call of the harness.
	calls machine.ReplyToken
}

// newHarness returns a harness of the router of the namespace of the tests. The root scope
// places the cell and records the spec of the setup, and the controller of the cell has
// placed every slot on node A. Each option changes the setup before New. When the test
// ends, the harness checks that each effect that waits for a result has a token of its own.
func newHarness(tb testCase, opts ...option) *harness {
	tb.Helper()
	s := setup{
		spec: config.Spec{
			Namespace:    namespace,
			Preset:       builtin.ExclusiveWorker,
			Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
			Version:      firstVersion,
		},
		cfg:    routes.Config{Rand: constant.NewCounter(half), Provided: provided, Namespace: namespace},
		root:   store.Mutation{Put: []shard.Placement{{Shard: shard.ID{ID128: cellKey}}}},
		create: true,
		place:  true,
	}
	for _, opt := range opts {
		opt(&s)
	}
	h := &harness{
		st:          memory.New(fake.New(origin)),
		logs:        telemetrytest.NewRecords(slog.LevelDebug),
		metrics:     telemetrytest.NewRecorder(),
		seqs:        make(map[shard.Scope]uint64),
		delivered:   make(map[scopeKey]epoch.Epoch),
		cell:        cellScope,
		incarnation: incA,
		limit:       pageLimit,
	}
	tb.Cleanup(func() { assert.NoError(tb, h.st.Close(), "the store must close") })
	tb.Cleanup(func() {
		expect.NotContains(tb, h.issued, machine.Token(0), "each effect that waits for a result must have a token")
		expect.NoDuplicates(tb, func() ([]machine.Token, error) { return h.issued, nil },
			"each effect that waits for a result must have a token of its own")
		expect.NotContains(tb, h.unwatched, machine.Token(0), "each Unwatch must end a watch")
	})
	h.registry = config.NewRegistry(builtin.Support())
	assert.NoError(tb, builtin.Register(h.registry), "the built-ins must register")
	res, err := config.Resolve(h.registry, &s.spec, provided)
	assert.NoError(tb, err, "the spec of the tests must resolve")
	h.spec, err = json.Marshal(&res.Spec)
	assert.NoError(tb, err, "the resolved spec must encode")
	h.mapping = res.Mapping
	h.partitioner, err = config.Build[partition.Partitioner](h.registry, res, config.Partitioning)
	assert.NoError(tb, err, "the partitioner of the tests must build")
	h.spans, err = h.partitioner.Initial(shard.Range{})
	assert.NoError(tb, err, "the partitioner must divide the whole range")
	h.keys = keysOf(h.partitioner, len(h.spans))
	if s.root.Spec == nil {
		s.root.Spec = h.spec
	}
	if s.root.Mapping == nil {
		s.root.Mapping = &h.mapping
	}
	if s.create {
		h.commit(tb, rootScope, &s.root)
	}
	if s.place {
		h.placed = h.commit(tb, cellScope, &store.Mutation{Put: h.placements(nodeA.ID, firstEpoch)})
	}
	if s.cfg.Reporter == nil {
		s.cfg.Reporter = h.metrics
	}
	s.cfg.Registry = h.registry
	s.cfg.Logger = slog.New(h.logs)
	h.machine, err = routes.New(&s.cfg)
	assert.NoError(tb, err, "New must return a router of a complete config")
	return h
}

// placements returns a placement of each slot on the node n at the epoch e, in slot order.
// The shard of slot i has the last byte i+1.
func (h *harness) placements(n shard.NodeID, e epoch.Epoch) []shard.Placement {
	ps := make([]shard.Placement, 0, len(h.spans))
	for i := range h.spans {
		ps = append(ps, h.placement(i, n, e))
	}
	return ps
}

// placement returns the placement of slot i on the node n at the epoch e, and a placement
// without replicas for an empty n.
func (h *harness) placement(i int, n shard.NodeID, e epoch.Epoch) shard.Placement {
	b := shardtest.NewPlacementBuilder().
		WithShard(shards[i]).
		WithRange(h.spans[i].Range).
		WithEpoch(e).
		WithReserved(e).
		WithSequence(uint64(e))
	if n != "" {
		b.WithReplicas(shard.Replica{Node: n, Role: shard.RolePrimary})
	}
	return b.Build()
}

// creation returns the mutation of the root that creates the namespace of the tests with the
// spec of h, the mapping m and the cell whose shard in the root scope is key.
func (h *harness) creation(key id.ID128, m shard.Mapping) *store.Mutation {
	return new(storetest.NewMutationBuilder().
		WithSpec(h.spec).
		WithMapping(m).
		WithPut(shard.Placement{Shard: shard.ID{ID128: key}}).
		Build())
}

// commit commits m to the scope s as its writer: the root at its term for the root scope,
// and the controller of the cell at its term otherwise. It returns the generation of the
// commit.
func (h *harness) commit(tb assert.TB, s shard.Scope, m *store.Mutation) epoch.Epoch {
	tb.Helper()
	ctx := assert.Context(tb)
	head, err := h.st.Head(ctx, s)
	assert.NoError(tb, err, "the store must return the header of the scope")
	term := cellTerm
	if s.Cell == "" {
		term = rootTerm
	}
	h.seqs[s]++
	m.ID = store.CommitID{Incarnation: h.incarnation, Term: term, Seq: h.seqs[s]}
	gen, err := h.st.Writer(s, h.incarnation, term).Commit(ctx, head.Generation, m)
	assert.NoError(tb, err, "the writer of the scope must commit")
	return gen
}

// recreate deletes the root scope and the scope of the cell, and creates the namespace again
// in the incarnation inc with the same spec and one cell of the key key. The controller of the
// new cell places every slot on node A, and placed is the generation of that commit. The new
// cell is the cell of the harness from then on, and recreate returns its scope.
func (h *harness) recreate(tb assert.TB, inc shard.NamespaceIncarnation, key id.ID128) shard.Scope {
	tb.Helper()
	h.st.Delete(rootScope)
	h.st.Delete(h.cell)
	h.incarnation = inc
	clear(h.seqs)
	h.commit(tb, rootScope, h.creation(key, h.mapping))
	h.cell = shard.Scope{Namespace: namespace, Cell: shard.CellID(ulid.Format(key.ID()))}
	h.placed = h.commit(tb, h.cell, &store.Mutation{Put: h.placements(nodeA.ID, firstEpoch)})
	return h.cell
}

// call returns the token of the next inbound call.
func (h *harness) call() machine.ReplyToken {
	h.calls++
	return h.calls
}

// start passes Started to the router, and passes the reads, the generations and a view of
// the nodes A and B of the cell until the router serves.
func (h *harness) start(tb assert.TB) machine.ReplyToken {
	tb.Helper()
	to := h.call()
	h.step(tb, machine.Started{Reply: to})
	h.run(tb)
	h.follow(tb)
	h.view(tb, cellScope, nodeA, nodeB)
	return to
}

// step passes ev to the router with the marker in out through wrap, checks that the router
// keeps out, and keeps the effects that the router appends.
func (h *harness) step(tb assert.TB, ev machine.Event) {
	tb.Helper()
	var got []machine.Effect
	run := func() { got = h.machine.Step(ev, []machine.Effect{marker}) }
	if h.wrap != nil {
		h.wrap(run)
	} else {
		run()
	}
	assert.NotEmpty(tb, got, "the router must keep the effects that out has")
	assert.Equal(tb, got[0], marker, "the router must keep the effects that out has")
	h.route(got[1:])
}

// route passes each effect of effects to observe, and keeps it in the list of its kind with
// the token of each effect that waits for a result.
func (h *harness) route(effects []machine.Effect) {
	for _, e := range effects {
		if h.observe != nil {
			h.observe(e)
		}
		switch e := e.(type) {
		case machine.Read:
			h.issued = append(h.issued, e.Token)
			h.pending = append(h.pending, e)
		case machine.Watch:
			h.issued = append(h.issued, e.Token)
			h.watches = append(h.watches, e)
		case machine.WatchViews:
			h.issued = append(h.issued, e.Token)
			h.viewers = append(h.viewers, e)
		case machine.Unwatch:
			h.unwatched = append(h.unwatched, e.Token)
			h.watches = slices.DeleteFunc(h.watches, func(w machine.Watch) bool { return w.Token == e.Token })
			h.viewers = slices.DeleteFunc(h.viewers, func(w machine.WatchViews) bool { return w.Token == e.Token })
		case machine.Timer:
			h.issued = append(h.issued, e.Token)
			h.timers = append(h.timers, e)
		case machine.Reply[struct{}]:
			h.replies = append(h.replies, e)
		}
	}
}

// run performs the pending reads, the oldest first, and passes their results to the router
// until no read is pending. It leaves a read pending when pause reports true for it.
func (h *harness) run(tb assert.TB) {
	tb.Helper()
	for range maxDeliveries {
		i := slices.IndexFunc(h.pending, func(e machine.Effect) bool { return h.pause == nil || !h.pause(e) })
		if i < 0 {
			return
		}
		e := h.pending[i].(machine.Read)
		h.pending = slices.Delete(h.pending, i, i+1)
		h.step(tb, h.read(tb, &e))
	}
	tb.Fatalf("the router still reads after %d deliveries", maxDeliveries)
}

// read reads the page of e from the store and returns the event of its result.
func (h *harness) read(tb assert.TB, e *machine.Read) machine.Event {
	tb.Helper()
	ctx := assert.Context(tb)
	p := e.Page
	p.Limit = h.limit
	state, cursor, err := h.st.Read(ctx, e.Scope, e.Incarnation, e.Query, p)
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
	if next == "" {
		k := scopeKey{scope: e.Scope, incarnation: state.Head.Incarnation}
		h.delivered[k] = max(h.delivered[k], state.Head.Generation)
	}
	return machine.PageRead{Placements: ps, Next: next, State: state, Token: e.Token}
}

// follow passes the generations of the scopes that the watches of the router have not
// passed, one at a time, and runs the router after each.
func (h *harness) follow(tb assert.TB) {
	tb.Helper()
	for range maxDeliveries {
		if !h.deliver(tb) {
			return
		}
	}
	tb.Fatalf("the watches still pass generations after %d deliveries", maxDeliveries)
}

// deliver passes the next item of the first watch that has one, runs the router, and
// reports whether a watch had an item.
func (h *harness) deliver(tb assert.TB) bool {
	tb.Helper()
	for i := range h.watches {
		if ev, ok := h.item(tb, &h.watches[i]); ok {
			h.pass(tb, i, &ev)
			return true
		}
	}
	return false
}

// item returns the event of the next item of the watch w in the store. It reports whether
// the store has one.
func (h *harness) item(tb assert.TB, w *machine.Watch) (machine.Watched, bool) {
	ctx, cancel := context.WithCancel(assert.Context(tb))
	cancel()
	next, stop := iter.Pull2(h.st.Watch(ctx, w.Scope, w.Incarnation, w.From, w.Node))
	defer stop()
	ev, err, ok := next()
	return machine.Watched{Event: ev, Err: err, Token: w.Token}, ok
}

// pass passes the item ev of the watch i to the router and runs the router. An item with an
// error ends the watch.
func (h *harness) pass(tb assert.TB, i int, ev *machine.Watched) {
	tb.Helper()
	if ev.Err != nil {
		h.watches = slices.Delete(h.watches, i, i+1)
	} else {
		w := &h.watches[i]
		w.From = ev.Event.Generation
		k := scopeKey{scope: w.Scope, incarnation: ev.Event.Mutation.ID.Incarnation}
		h.delivered[k] = max(h.delivered[k], w.From)
	}
	h.step(tb, *ev)
	h.run(tb)
}

// view passes a view of nodes to each watch of the views of the scope s that runs, and runs
// the router and its watches afterwards.
func (h *harness) view(tb assert.TB, s shard.Scope, nodes ...shard.Node) {
	tb.Helper()
	h.revision++
	for _, w := range slices.Clone(h.viewers) {
		if w.Scope == s {
			v := membership.View{Scope: s, Nodes: slices.Clone(nodes), Revision: h.revision, Incarnation: w.Incarnation}
			h.step(tb, machine.Viewed{View: v, Token: w.Token})
		}
	}
	h.run(tb)
	h.follow(tb)
}

// watchFails ends the watch of the scope s that runs with err, as a store does, and runs
// the router and its watches afterwards.
func (h *harness) watchFails(tb assert.TB, s shard.Scope, err error) {
	tb.Helper()
	i := slices.IndexFunc(h.watches, func(w machine.Watch) bool { return w.Scope == s })
	assert.True(tb, i >= 0, "a watch of the scope must run")
	w := h.watches[i]
	h.watches = slices.Delete(h.watches, i, i+1)
	h.step(tb, machine.Watched{Err: err, Token: w.Token})
	h.run(tb)
	h.follow(tb)
}

// viewFails ends each watch of the views of the scope s that runs with err, and runs the
// router afterwards. A runtime ends a watch this way when the call of the watch fails.
func (h *harness) viewFails(tb assert.TB, s shard.Scope, err error) {
	tb.Helper()
	for _, w := range slices.Clone(h.viewers) {
		if w.Scope == s {
			h.viewers = slices.DeleteFunc(h.viewers, func(x machine.WatchViews) bool { return x.Token == w.Token })
			h.step(tb, machine.Viewed{Err: err, Token: w.Token})
		}
	}
	h.run(tb)
}

// elapse passes the end of the oldest Timer whose Duration is d, and runs the router and its
// watches afterwards.
func (h *harness) elapse(tb assert.TB, d time.Duration) {
	tb.Helper()
	i := slices.IndexFunc(h.timers, func(tm machine.Timer) bool { return tm.Duration == d })
	assert.True(tb, i >= 0, "a Timer of the duration must be armed")
	tm := h.timers[i]
	h.timers = slices.Delete(h.timers, i, i+1)
	h.step(tb, machine.Elapsed{Token: tm.Token})
	h.run(tb)
	h.follow(tb)
}

// refresh passes a Refresh of the cell c in the incarnation inc for the generation gen, and
// runs the router and its watches afterwards. It returns the token of the call.
func (h *harness) refresh(
	tb assert.TB,
	inc shard.NamespaceIncarnation,
	c shard.CellID,
	gen epoch.Epoch,
) machine.ReplyToken {
	tb.Helper()
	to := h.call()
	h.step(tb, machine.Refresh{Cell: c, Reply: to, Min: gen, Incarnation: inc})
	h.run(tb)
	h.follow(tb)
	return to
}

// replyTo returns the reply of the router to the call to, and reports whether the router has
// replied.
func (h *harness) replyTo(to machine.ReplyToken) (machine.Reply[struct{}], bool) {
	i := slices.IndexFunc(h.replies, func(r machine.Reply[struct{}]) bool { return r.To == to })
	if i < 0 {
		return machine.Reply[struct{}]{}, false
	}
	return h.replies[i], true
}

// replied returns the reply of the router to the call to, which must exist.
func (h *harness) replied(tb assert.TB, to machine.ReplyToken) machine.Reply[struct{}] {
	tb.Helper()
	r, ok := h.replyTo(to)
	assert.True(tb, ok, "the router must have replied to the call")
	return r
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

// recreation returns the namespace incarnation and the key of the cell of the namespace that
// the root creates again for the nth time. It copies the ID of incA and cellKey, and sets
// their byte serial to n.
func recreation(n byte) (shard.NamespaceIncarnation, id.ID128) {
	b, key := incBytes, cellKey
	b[serial], key[serial] = n, n
	return shard.NamespaceIncarnation{ID: id.New128(b)}, key
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package cell_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"time"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/coretest/telemetrytest"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand/constant"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/cell"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The names of the records and the callers of the tests.
const (
	// namespace is the namespace of the tests.
	namespace shard.Namespace = "orders"
	// operator is the identity of the operator of the tests.
	operator shard.Identity = "operator"
	// controllerAddress is the address of the controller of the tests.
	controllerAddress = "controller-1:7000"
	// controllerIdentity is the identity of the controller of the tests.
	controllerIdentity shard.Identity = "controller-1"
)

// The epochs of the tests.
const (
	// rootTerm is the term of the root that writes the root scope.
	rootTerm epoch.Epoch = 1
	// cellTerm is the term of the machine of each test.
	cellTerm epoch.Epoch = 3
	// firstVersion is the version of the spec of the tests.
	firstVersion epoch.Epoch = 1
)

// slotsConfig is the configuration of fixed-slots in the spec of the tests. With it,
// fixed-slots divides each cell into four slots, so each cell has four shards.
const slotsConfig = `{"slots":4}`

// The configurations and the specs of the tests.
const (
	// otherSlots is the configuration of fixed-slots with other slots than the tests.
	otherSlots = `{"slots":8}`
	// malformed is text that is not JSON.
	malformed = "{"
)

// attrCellKey is the key of the attribute that contains the cell in the logs of the cell.
const attrCellKey = "cell"

// maxDeliveries bounds the effects that run performs, so a machine that never settles
// fails its test instead of the timeout of go test.
const maxDeliveries = 10000

// half is the value that rand.Float64 returns for the source of randomness of the tests, so
// each backoff is half of its ceiling.
const half = 0.5

// marker is the effect that the harness passes in out with each event: an Unwatch without a
// watch, which the machine never returns itself.
var marker machine.Effect = machine.Unwatch{}

// pageLimit is the number of placements on each page that the harness reads, so a snapshot
// of the four shards of a cell takes two pages.
const pageLimit = 2

// The nodes of the tests, each at its first incarnation.
var (
	nodeA = shard.Node{ID: "node-a", Address: "node-a:7000", Identity: "agent-a", Incarnation: 1}
	nodeB = shard.Node{ID: "node-b", Address: "node-b:7000", Identity: "agent-b", Incarnation: 1}
	nodeC = shard.Node{ID: "node-c", Address: "node-c:7000", Identity: "agent-c", Incarnation: 1}
)

// The scopes, the incarnation, the clock and the capabilities of the tests.
var (
	// cellKey is the 16 bytes of the ULID of the cell, which is the root shard of the cell.
	cellKey = id.ID128{0: 0x01, 15: 0xce}
	// cellID is the cell of the tests: the text of the ULID of cellKey.
	cellID = shard.CellID(ulid.Format(cellKey.ID()))
	// cellScope is the scope of the cell of the tests.
	cellScope = shard.Scope{Namespace: namespace, Cell: cellID}
	// rootScope is the root scope of the namespace of the tests.
	rootScope = shard.Scope{Namespace: namespace}
	// incA is the namespace incarnation of the tests.
	incA = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: 0x1a, 15: 0x0a})}
	// origin is the start of the clock of each test.
	origin = time.Date(2026, time.October, 12, 0, 0, 0, 0, time.UTC)
	// provided lists the capabilities of the store and the membership of the tests.
	provided = map[config.Axis][]config.Capability{
		config.Store:      {config.Durable},
		config.Membership: {config.Leases},
	}
)

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
	// cfg is the Config of the machine. newHarness completes it.
	cfg cell.Config
	// root is the mutation that creates the root scope. newHarness completes it with the
	// spec and the mapping.
	root store.Mutation
	// fits is the bound of a commit. A nil fits takes the bound of the store.
	fits func(m *store.Mutation) error
	// extend registers more strategies in the registry of the machine. A nil extend
	// registers none. The doubles of the strategies report to tb.
	extend func(tb assert.TB, r *config.Registry) error
	// remap changes the mapping that the root records after the spec resolves. A nil remap
	// keeps the mapping.
	remap func(m *shard.Mapping)
	// support is the support matrix of the registry of the machine.
	support config.Support
}

// option changes the setup of a harness.
type option func(*setup)

// harness is the runtime of the tests. It performs the effects of one cell machine against a
// memory store, replies to the callbacks of the machine as the agents of the nodes do, runs
// its passes and keeps its replies.
type harness struct {
	// observe receives each effect that the cell appends, before route keeps it, or is nil.
	observe func(e machine.Effect)
	// st is the store of the machine.
	st *memory.Store
	// clock is the clock of the machine.
	clock *fake.Clock
	// machine is the machine under test.
	machine *cell.Machine
	// logs keeps every record that the machine logs.
	logs *telemetrytest.Records
	// pending lists the effects that run performs, the oldest first: the reads and the
	// commits of the store, the callbacks and the passes.
	pending []machine.Effect
	// watches lists the watches of the root scope that run.
	watches []machine.Watch
	// opened lists the token of every watch of the root scope that the machine started, in
	// order.
	opened []machine.Token
	// timers lists the Timers that the machine armed, the oldest first.
	timers []machine.Timer
	// replies lists every reply of the machine, in order.
	replies []machine.Effect
	// plans lists every Plan that the machine sent, in order.
	plans []machine.Plan
	// unwatched lists the tokens of every Unwatch of the machine, in order.
	unwatched []machine.Token
	// views is the token of the WatchViews that runs, and zero without one.
	views machine.Token
	// revision is the revision of the last view that the harness passed.
	revision epoch.Epoch
	// calls is the token of the last inbound call of the harness.
	calls machine.ReplyToken
	// rootSeq is the Seq of the last commit to the root scope.
	rootSeq uint64
}

// newHarness returns a harness of a machine of the cell of the tests. The root scope places
// the cell and records the spec of the setup. Each option changes the setup before New.
func newHarness(tb testCase, opts ...option) *harness {
	tb.Helper()
	s := setup{
		spec: config.Spec{
			Namespace:    namespace,
			Preset:       builtin.ExclusiveWorker,
			Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
			Version:      firstVersion,
		},
		cfg: cell.Config{
			Rand:        constant.NewCounter(half),
			Reporter:    noop.New(),
			Provided:    provided,
			Operators:   []shard.Identity{operator},
			Scope:       cellScope,
			Node:        shard.Node{ID: "controller-1", Address: controllerAddress, Identity: controllerIdentity},
			Term:        cellTerm,
			Parallelism: 1,
			Incarnation: incA,
		},
		root:    store.Mutation{Put: []shard.Placement{{Shard: shard.ID{ID128: cellKey}}}},
		support: builtin.Support(),
	}
	for _, opt := range opts {
		opt(&s)
	}
	h := &harness{
		st:    memory.New(fake.New(origin)),
		clock: fake.New(origin),
		logs:  telemetrytest.NewRecords(slog.LevelDebug),
	}
	tb.Cleanup(func() { assert.NoError(tb, h.st.Close(), "the store must close") })
	r := config.NewRegistry(s.support)
	assert.NoError(tb, builtin.Register(r), "the built-ins must register")
	if s.extend != nil {
		assert.NoError(tb, s.extend(tb, r), "the strategies of the test must register")
	}
	res, err := config.Resolve(r, &s.spec, provided)
	assert.NoError(tb, err, "the spec of the tests must resolve")
	raw, err := json.Marshal(&res.Spec)
	assert.NoError(tb, err, "the resolved spec must encode")
	if s.root.Spec == nil {
		s.root.Spec = raw
	}
	if s.root.Mapping == nil {
		s.root.Mapping = &res.Mapping
		if s.remap != nil {
			s.remap(s.root.Mapping)
		}
	}
	h.commitRoot(tb, &s.root)
	s.cfg.Clock = h.clock
	s.cfg.Registry = r
	s.cfg.Logger = slog.New(h.logs)
	s.cfg.Fits = s.fits
	if s.fits == nil {
		s.cfg.Fits = func(m *store.Mutation) error { return h.st.Fits(cellScope, m) }
	}
	h.machine, err = cell.New(&s.cfg)
	assert.NoError(tb, err, "New must return a machine of a complete config")
	return h
}

// commitRoot commits m to the root scope through the root of the tests.
func (h *harness) commitRoot(tb assert.TB, m *store.Mutation) {
	tb.Helper()
	ctx := assert.Context(tb)
	head, err := h.st.Head(ctx, rootScope)
	assert.NoError(tb, err, "the store must return the header of the root scope")
	h.rootSeq++
	m.ID = store.CommitID{Incarnation: incA, Term: rootTerm, Seq: h.rootSeq}
	_, err = h.st.Writer(rootScope, incA, rootTerm).Commit(ctx, head.Generation, m)
	assert.NoError(tb, err, "the root must commit")
}

// commitSpec commits the spec of the tests at the version v to the root scope, after
// change changes it, and passes the generation to the machine.
func (h *harness) commitSpec(tb testCase, v epoch.Epoch, change func(*config.Spec)) {
	tb.Helper()
	h.commitRoot(tb, specMutation(tb, v, change))
	h.follow(tb)
}

// call returns the token of the next inbound call.
func (h *harness) call() machine.ReplyToken {
	h.calls++
	return h.calls
}

// start passes Started to the machine and runs it until it serves.
func (h *harness) start(tb assert.TB) {
	tb.Helper()
	h.step(tb, machine.Started{Reply: h.call()})
	h.run(tb)
}

// step passes ev to the machine with the marker in out, checks that the machine keeps out,
// and keeps the effects that the machine appends.
func (h *harness) step(tb assert.TB, ev machine.Event) {
	tb.Helper()
	got := h.machine.Step(ev, []machine.Effect{marker})
	assert.NotEmpty(tb, got, "the machine must keep the effects that out has")
	assert.Equal(tb, got[0], marker, "the machine must keep the effects that out has")
	h.route(got[1:])
}

// route passes each effect of effects to observe, and keeps it in the list of its kind.
func (h *harness) route(effects []machine.Effect) {
	for _, e := range effects {
		if h.observe != nil {
			h.observe(e)
		}
		switch e := e.(type) {
		case machine.Watch:
			h.watches = append(h.watches, e)
			h.opened = append(h.opened, e.Token)
		case machine.Unwatch:
			h.unwatched = append(h.unwatched, e.Token)
			h.watches = slices.DeleteFunc(h.watches, func(w machine.Watch) bool { return w.Token == e.Token })
			if h.views == e.Token {
				h.views = 0
			}
		case machine.WatchViews:
			h.views = e.Token
		case machine.Timer:
			h.timers = append(h.timers, e)
		case machine.Reply[struct{}], machine.Reply[protocol.Ack], machine.Reply[protocol.Receipt],
			machine.Reply[budget.Decision]:
			h.replies = append(h.replies, e)
		case machine.Plan:
			h.plans = append(h.plans, e)
			h.pending = append(h.pending, e)
		default:
			h.pending = append(h.pending, e)
		}
	}
}

// run performs the pending effects, the oldest first, and passes their results to the
// machine until no effect is pending.
func (h *harness) run(tb assert.TB) {
	tb.Helper()
	for range maxDeliveries {
		if len(h.pending) == 0 {
			return
		}
		e := h.pending[0]
		h.pending = slices.Delete(h.pending, 0, 1)
		h.step(tb, h.perform(tb, e))
	}
	tb.Fatalf("the machine still runs after %d deliveries", maxDeliveries)
}

// perform performs e and returns the event of its result. Every agent replies to its
// callbacks with success.
func (h *harness) perform(tb assert.TB, e machine.Effect) machine.Event {
	tb.Helper()
	ctx := assert.Context(tb)
	switch e := e.(type) {
	case machine.Head:
		head, err := h.st.Head(ctx, e.Scope)
		return machine.HeadRead{Err: err, Token: e.Token, Head: head}
	case machine.Read:
		return h.read(tb, &e)
	case machine.Commit:
		gen, err := h.st.Writer(e.Scope, e.Incarnation, e.Term).Commit(ctx, e.Expect, e.Mutation)
		return machine.Committed{Err: err, Token: e.Token, Generation: gen}
	case machine.Outcome:
		receipt, found, err := h.st.Outcome(ctx, e.Scope, e.ID)
		return machine.OutcomeRead{Err: err, Token: e.Token, Receipt: receipt, Found: found}
	case machine.Callback:
		return machine.Called{Token: e.Token}
	case machine.Plan:
		plan, err := e.Placer.Place(ctx, e.Input)
		var violations []placement.Violation
		if err == nil {
			violations, err = placement.Validate(e.Input, plan, e.Constraints)
		}
		return machine.Planned{Err: err, Violations: violations, Plan: plan, Token: e.Token}
	default:
		tb.Fatalf("the machine returned a %T, which the harness does not perform", e)
		return nil
	}
}

// read reads the page of e from the store and returns the event of its result.
func (h *harness) read(tb assert.TB, e *machine.Read) machine.Event {
	tb.Helper()
	ctx := assert.Context(tb)
	p := e.Page
	p.Limit = pageLimit
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
	return machine.PageRead{Placements: ps, Next: next, State: state, Token: e.Token}
}

// follow passes the generations of the root scope that the watches of the machine have
// not passed, and runs the machine afterwards.
func (h *harness) follow(tb assert.TB) {
	tb.Helper()
	for len(h.watches) > 0 {
		w := h.watches[0]
		ctx, cancel := context.WithCancel(assert.Context(tb))
		cancel()
		passed := false
		for ev, err := range h.st.Watch(ctx, w.Scope, w.Incarnation, w.From, w.Node) {
			if err != nil {
				h.watches = slices.Delete(h.watches, 0, 1)
				h.step(tb, machine.Watched{Err: err, Token: w.Token})
				passed = true
				break
			}
			h.watches[0].From = ev.Generation
			h.step(tb, machine.Watched{Event: ev, Token: w.Token})
			passed = true
			break
		}
		h.run(tb)
		if !passed {
			return
		}
	}
}

// view passes the view of nodes to the machine, and runs the machine afterwards.
func (h *harness) view(tb assert.TB, nodes ...shard.Node) {
	tb.Helper()
	assert.NotEqual(tb, h.views, machine.Token(0), "the machine must watch the views")
	h.revision++
	v := membership.View{Scope: cellScope, Nodes: slices.Clone(nodes), Revision: h.revision, Incarnation: incA}
	h.step(tb, machine.Viewed{View: v, Token: h.views})
	h.run(tb)
}

// elapse performs the oldest Timer whose Duration is d, and runs the machine afterwards.
func (h *harness) elapse(tb assert.TB, d time.Duration) {
	tb.Helper()
	i := slices.IndexFunc(h.timers, func(tm machine.Timer) bool { return tm.Duration == d })
	assert.True(tb, i >= 0, "a Timer of the duration must be armed")
	tm := h.timers[i]
	h.timers = slices.Delete(h.timers, i, i+1)
	h.step(tb, machine.Elapsed{Token: tm.Token})
	h.run(tb)
}

// state returns the state of the scope of the cell.
func (h *harness) state(tb assert.TB) store.State {
	tb.Helper()
	state, cursor, err := h.st.Read(assert.Context(tb), cellScope, incA, store.Query{}, page.Page{})
	assert.NoError(tb, err, "the store must read the scope of the cell")
	assert.NoError(tb, cursor.Close(), "the cursor must close")
	return state
}

// logged returns the attributes of each record that the machine has logged with the message
// msg, in order. Each map has the text of the value of each attribute by its key.
func (h *harness) logged(msg string) []map[string]string {
	var got []map[string]string
	for _, r := range h.logs.All() {
		if r.Message != msg {
			continue
		}
		attrs := make(map[string]string, r.NumAttrs())
		r.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value.String()
			return true
		})
		got = append(got, attrs)
	}
	return got
}

// registry returns a registry with every built-in strategy of the support matrix.
func registry(tb assert.TB) *config.Registry {
	tb.Helper()
	r := config.NewRegistry(builtin.Support())
	assert.NoError(tb, builtin.Register(r), "the built-ins must register")
	return r
}

// specMutation returns the mutation of the root that accepts the spec of the tests at the
// version v, after change changes it.
func specMutation(tb testCase, v epoch.Epoch, change func(*config.Spec)) *store.Mutation {
	tb.Helper()
	spec := config.Spec{
		Namespace:    namespace,
		Preset:       builtin.ExclusiveWorker,
		Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
		Version:      v,
	}
	if change != nil {
		change(&spec)
	}
	res, err := config.Resolve(registry(tb), &spec, provided)
	assert.NoError(tb, err, "the spec must resolve")
	raw, err := json.Marshal(&res.Spec)
	assert.NoError(tb, err, "the resolved spec must encode")
	return &store.Mutation{Spec: raw}
}

// otherCells returns the root placements of n other cells. Each starts at its own
// position, after the start of the cell of the tests.
func otherCells(n int) []shard.Placement {
	ps := make([]shard.Placement, 0, n)
	for i := range n {
		k := cellKey
		k[14] = byte(i) + 1
		ps = append(ps, shard.Placement{Shard: shard.ID{ID128: k}, Range: shard.Range{Start: []byte{0xf0, byte(i)}}})
	}
	return ps
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/coretest/telemetrytest"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand/constant"
	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/handoff/breakbeforemake"
	"go.thesmos.sh/sharder/internal/follow"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/shard/shardtest"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The names of the records and the callers of the tests.
const (
	// namespace is the namespace of the tests.
	namespace shard.Namespace = "orders"
	// controllerAddress is the address of the controller of the cell of the tests.
	controllerAddress = "controller-1:7000"
	// controllerIdentity is the identity of the controller of the cell of the tests.
	controllerIdentity shard.Identity = "controller-1"
	// slotsConfig is the configuration of fixed-slots in the spec of the tests.
	slotsConfig = `{"slots":4}`
)

// The epochs of the tests.
const (
	// rootTerm is the term of the root that writes the root scope.
	rootTerm epoch.Epoch = 1
	// cellTerm is the term of the controller of the cell of the tests.
	cellTerm epoch.Epoch = 3
	// firstVersion is the version of the spec of the tests.
	firstVersion epoch.Epoch = 1
)

// The epochs of the tenures of the tests.
const (
	// firstEpoch is the epoch of the first writer of a shard.
	firstEpoch epoch.Epoch = 1
	// secondEpoch is the epoch of the second writer of a shard.
	secondEpoch epoch.Epoch = 2
	// thirdEpoch is the epoch of the third writer of a shard.
	thirdEpoch epoch.Epoch = 3
)

// maxDeliveries bounds the effects that run performs, so a machine that never settles
// fails its test instead of the timeout of go test.
const maxDeliveries = 10000

// pageLimit is the number of placements on each page that the harness reads.
const pageLimit = 2

// grace is the time between a Close and its deadline.
const grace = 30 * time.Second

// loadWindow is the default load window of a spec, the time between two reports of a node
// whose replicas do not change.
const loadWindow = time.Minute

// firstBackoff is the first backoff of a retry, with the source of randomness of the tests:
// half of agent.RetryBase.
const firstBackoff = agent.RetryBase / 2

// stopOnLoss is the configuration of exclusive ownership whose writers stop at the deadline
// of their lease.
const stopOnLoss = `{"fenced":true,"lease":{"onLoss":"stop"}}`

// The names of the instruments of an agent.
const (
	refusalsInstrument telemetry.InstrumentName = "sharder.fence.refusals"
	lagInstrument      telemetry.InstrumentName = "sharder.generation.lag"
)

// The keys of the attributes that an agent binds to each of its instruments.
const (
	keyNamespace = "namespace"
	keyNode      = "node"
)

// half is the value that rand.Float64 returns for the source of randomness of the tests, so
// each backoff is half of its ceiling.
const half = 0.5

// The nodes and the cells other than the node and the cell of the tests.
const (
	// otherNode is a node of the cell other than the node of the tests.
	otherNode shard.NodeID = "node-b"
	// otherCell is a cell other than the cell of the tests, and not the text of a shard ID.
	otherCell shard.CellID = "other"
)

// marker is the effect that the harness passes in out with each event: an Unwatch without a
// watch, which the machine never returns itself.
var marker machine.Effect = machine.Unwatch{}

// The node, the cell, the scopes, the incarnation and the clock of the tests.
var (
	// node is the node of the agent of the tests.
	node = shard.Node{ID: "node-a", Address: "node-a:7000", Identity: "agent-a"}
	// cellKey is the 16 bytes of the ULID of the cell, which is the shard of the cell in the
	// root scope.
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
	// series is the attribute set of the series of each instrument of the agent of the
	// tests: the namespace and the node.
	series = []telemetry.Attr{
		telemetry.AttrString(keyNamespace, string(namespace)),
		telemetry.AttrString(keyNode, string(node.ID)),
	}
)

// The shards of the tests.
var (
	shardA = shard.ID{ID128: id.ID128{0: 0x02, 15: 0x0a}}
	shardB = shard.ID{ID128: id.ID128{0: 0x02, 15: 0x0b}}
	shardC = shard.ID{ID128: id.ID128{0: 0x02, 15: 0x0c}}
)

// allShards lists the shards of the property of an agent.
var allShards = []shard.ID{shardA, shardB, shardC}

// The errors of the handlers of the tests.
var (
	// errBusy is a transient error of a handler.
	errBusy = errs.WithClass(errors.New("test: the handler is busy"), errs.Transient)
	// errBroken is an error of a handler that a retry does not fix.
	errBroken = errs.WithClass(errors.New("test: the handler is broken"), errs.Invalid)
)

// errDisk is the cause of the faults of the tests.
var errDisk = errors.New("test: the disk fails")

// outcomes lists the results that a step of the property of an agent returns for a fence
// acquisition or a handler call. The list has a success, a transient error, an error that a
// retry does not fix, the refusal of a fence, and the end of the context of an attempt at
// its deadline.
var outcomes = []error{nil, errBusy, errBroken, epoch.ErrFenced, context.DeadlineExceeded}

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
	// cfg is the Config of the agent, which newHarness completes.
	cfg agent.Config
	// root is the mutation that creates the root scope, which newHarness completes with the
	// spec and the mapping.
	root store.Mutation
	// extend registers more strategies in the registry of the agent, or is nil. The doubles
	// of the strategies report to tb.
	extend func(tb assert.TB, r *config.Registry) error
	// support is the support matrix of the registry of the agent.
	support config.Support
	// create is true when the root has created the namespace before the agent starts.
	create bool
	// acquire is true when the controller of the cell has acquired its scope before the
	// agent starts.
	acquire bool
}

// option changes the setup of a harness.
type option func(*setup)

// rooted makes the agent of a harness the agent of a controller process in the root scope.
// The root scope has the record of the root, and the scope of the cell has no controller.
var rooted option = func(s *setup) {
	s.cfg.Root = true
	s.root.Cell = &shard.Cell{
		Controller:         controllerAddress,
		ControllerIdentity: controllerIdentity,
		Epoch:              rootTerm,
		SpecVersion:        firstVersion,
	}
	s.acquire = false
}

// stops makes the ownership of the spec of a harness stop the writers at the deadline of
// their lease.
var stops option = func(s *setup) {
	s.spec.Ownership = config.Selection{Config: json.RawMessage(stopOnLoss)}
}

// harness is the runtime of the tests. It performs the effects of one agent against a
// memory store. The function fields of the harness return the results of the joins, the
// fence acquisitions, the handler calls, the revocations and the calls to the controller. The harness keeps
// every effect that the agent sends. The tests commit to the scope of the cell as its
// controller does, and pass the generations to the agent with follow.
type harness struct {
	// handler returns the result of the handler of the application for the request r, and
	// nil for success. With a nil handler, every call succeeds.
	handler func(r *protocol.Request) error
	// fence returns the result of the fence acquisition of the token t, and nil for success.
	// With a nil fence, every acquisition succeeds.
	fence func(t *fence.Token) error
	// revoking returns the result of the revocation of the tenure of the token t, and nil for
	// success. With a nil revoking, every revocation succeeds.
	revoking func(t *fence.Token) error
	// joining returns the error of the next join, and nil for a session. With a nil joining,
	// every join starts a session.
	joining func() error
	// ack returns the reply of the controller to the report r. With a nil ack, the
	// controller accepts every report.
	ack func(r *protocol.Report) (protocol.Ack, error)
	// decide returns the reply of the controller to the disruption d. With a nil decide,
	// the controller admits every disruption.
	decide func(d *budget.Disruption) (budget.Decision, error)
	// ending returns the reply of the controller to the end of a reservation, and nil for
	// success. With a nil ending, every end succeeds.
	ending func(a *protocol.DoneArgs) error
	// pause reports whether run leaves the effect e pending. With a nil pause, run performs
	// every effect.
	pause func(e machine.Effect) bool
	// observe receives each effect that the agent appends, before route keeps it, or is nil.
	observe func(e machine.Effect)
	// wrap runs each step of the agent, such as to record it in a history, or is nil.
	wrap func(step func())
	// root is the mutation that creates the root scope of the setup.
	root store.Mutation
	// st is the store of the agent.
	st *memory.Store
	// clock is the clock of the agent.
	clock *fake.Clock
	// machine is the agent under test.
	machine *agent.Machine
	// logs keeps every record that the agent logs.
	logs *telemetrytest.Records
	// metrics keeps every value that the instruments of the agent report.
	metrics *telemetrytest.Recorder
	// pending lists the effects that run performs, the oldest first.
	pending []machine.Effect
	// watches lists the watches that run.
	watches []machine.Watch
	// timers lists the Timers that the agent armed, the oldest first.
	timers []machine.Timer
	// replies lists every reply of the agent, in order.
	replies []machine.Effect
	// joins lists every Join of the agent, in order.
	joins []machine.Join
	// leaves lists the token of every Leave of the agent, in order.
	leaves []machine.Token
	// acquires lists every Acquire of the agent, in order.
	acquires []machine.Acquire
	// handles lists every Handle of the agent, in order.
	handles []machine.Handle
	// revokes lists every Revoke of the agent, in order.
	revokes []machine.Revoke
	// reports lists every report of the agent, in order.
	reports []*protocol.Report
	// disrupts lists every disruption that the agent asked for, in order.
	disrupts []machine.SendDisrupt
	// dones lists every end of a reservation that the agent asked for, in order.
	dones []machine.SendDone
	// unwatched lists the tokens of every Unwatch of the agent, in order.
	unwatched []machine.Token
	// issued lists the token of every effect of the agent that waits for a result, in order.
	issued []machine.Token
	// join is the token of the last Join that started a session, and zero without one.
	join machine.Token
	// incarnation is the incarnation of the last session that the harness started.
	incarnation epoch.Epoch
	// calls is the token of the last inbound call of the harness.
	calls machine.ReplyToken
	// rootSeq and cellSeq are the Seq of the last commit to the root scope and to the scope
	// of the cell.
	rootSeq, cellSeq uint64
}

// newHarness returns a harness of the agent of the node of the tests. The root scope places
// the cell and records the spec of the setup, and the controller of the cell has acquired
// its scope. Each option changes the setup before New. When the test ends, the harness
// checks that each effect that waits for a result has a token of its own.
func newHarness(tb testCase, opts ...option) *harness {
	tb.Helper()
	s := setup{
		spec: config.Spec{
			Namespace:    namespace,
			Preset:       builtin.ExclusiveWorker,
			Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
			Version:      firstVersion,
		},
		cfg:     agent.Config{Rand: constant.NewCounter(half), Provided: provided, Namespace: namespace, Node: node},
		root:    store.Mutation{Put: []shard.Placement{{Shard: shard.ID{ID128: cellKey}}}},
		support: builtin.Support(),
		create:  true,
		acquire: true,
	}
	for _, opt := range opts {
		opt(&s)
	}
	h := &harness{
		st:      memory.New(fake.New(origin)),
		clock:   fake.New(origin),
		logs:    telemetrytest.NewRecords(slog.LevelDebug),
		metrics: telemetrytest.NewRecorder(),
	}
	tb.Cleanup(func() { assert.NoError(tb, h.st.Close(), "the store must close") })
	tb.Cleanup(func() {
		expect.NotContains(tb, h.issued, machine.Token(0), "each effect that waits for a result must have a token")
		expect.NoDuplicates(tb, func() ([]machine.Token, error) { return h.issued, nil },
			"each effect that waits for a result must have a token of its own")
	})
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
	}
	h.root = s.root
	if s.create {
		h.commitRoot(tb, &s.root)
	}
	if s.acquire {
		h.commitCell(tb, &store.Mutation{Cell: record(cellTerm)})
	}
	s.cfg.Clock = h.clock
	s.cfg.Reporter = h.metrics
	s.cfg.Registry = r
	s.cfg.Logger = slog.New(h.logs)
	h.machine, err = agent.New(&s.cfg)
	assert.NoError(tb, err, "New must return an agent of a complete config")
	return h
}

// commitRoot commits m to the root scope as the root of the tests.
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

// commitCell commits m to the scope of the cell as the controller of the cell at its term,
// and returns the generation of the commit.
func (h *harness) commitCell(tb assert.TB, m *store.Mutation) epoch.Epoch {
	tb.Helper()
	ctx := assert.Context(tb)
	head, err := h.st.Head(ctx, cellScope)
	assert.NoError(tb, err, "the store must return the header of the scope of the cell")
	h.cellSeq++
	m.ID = store.CommitID{Incarnation: incA, Term: cellTerm, Seq: h.cellSeq}
	gen, err := h.st.Writer(cellScope, incA, cellTerm).Commit(ctx, head.Generation, m)
	assert.NoError(tb, err, "the controller of the cell must commit")
	return gen
}

// call returns the token of the next inbound call.
func (h *harness) call() machine.ReplyToken {
	h.calls++
	return h.calls
}

// start passes Started to the agent and runs it until it serves.
func (h *harness) start(tb assert.TB) {
	tb.Helper()
	h.step(tb, machine.Started{Reply: h.call()})
	h.run(tb)
	h.follow(tb)
}

// step passes ev to the agent through wrap, with the marker in out. It checks that the agent
// keeps out, and routes the effects that the agent appends.
func (h *harness) step(tb assert.TB, ev machine.Event) {
	tb.Helper()
	var got []machine.Effect
	run := func() { got = h.machine.Step(ev, []machine.Effect{marker}) }
	if h.wrap != nil {
		h.wrap(run)
	} else {
		run()
	}
	assert.NotEmpty(tb, got, "the agent must keep the effects that out has")
	assert.Equal(tb, got[0], marker, "the agent must keep the effects that out has")
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
		case machine.Watch:
			h.issued = append(h.issued, e.Token)
			h.watches = append(h.watches, e)
		case machine.Unwatch:
			h.unwatched = append(h.unwatched, e.Token)
			h.watches = slices.DeleteFunc(h.watches, func(w machine.Watch) bool { return w.Token == e.Token })
		case machine.Timer:
			h.issued = append(h.issued, e.Token)
			h.timers = append(h.timers, e)
		case machine.Reply[struct{}]:
			h.replies = append(h.replies, e)
		case machine.Leave:
			h.leaves = append(h.leaves, e.Token)
		default:
			h.issued = append(h.issued, tokenOf(e))
			h.pending = append(h.pending, e)
		}
	}
}

// run performs the pending effects, the oldest first, and passes their results to the agent
// until no effect is pending. It leaves an effect that pause reports pending.
func (h *harness) run(tb assert.TB) {
	tb.Helper()
	for range maxDeliveries {
		i := slices.IndexFunc(h.pending, func(e machine.Effect) bool { return h.pause == nil || !h.pause(e) })
		if i < 0 {
			return
		}
		e := h.pending[i]
		h.pending = slices.Delete(h.pending, i, i+1)
		h.step(tb, h.perform(tb, e))
	}
	tb.Fatalf("the agent still runs after %d deliveries", maxDeliveries)
}

// release performs the pending effects that pause left pending, and every later effect.
func (h *harness) release(tb assert.TB) {
	tb.Helper()
	h.pause = nil
	h.run(tb)
}

// perform performs e and returns the event of its result.
func (h *harness) perform(tb assert.TB, e machine.Effect) machine.Event {
	tb.Helper()
	switch e := e.(type) {
	case machine.Read:
		return h.read(tb, &e)
	case machine.Join:
		h.joins = append(h.joins, e)
		if h.joining != nil {
			if err := h.joining(); err != nil {
				return machine.Joined{Err: err, Token: e.Token}
			}
		}
		h.incarnation++
		h.join = e.Token
		return machine.Joined{Token: e.Token, Incarnation: h.incarnation}
	case machine.Acquire:
		h.acquires = append(h.acquires, e)
		var err error
		if h.fence != nil {
			err = h.fence(&e.Fence)
		}
		return machine.Acquired{Err: err, Token: e.Token}
	case machine.Handle:
		h.handles = append(h.handles, e)
		var err error
		if h.handler != nil {
			err = h.handler(e.Request)
		}
		return machine.Handled{Err: err, Token: e.Token}
	case machine.Revoke:
		h.revokes = append(h.revokes, e)
		var err error
		if h.revoking != nil {
			err = h.revoking(&e.Fence)
		}
		return machine.Revoked{Err: err, Token: e.Token}
	case machine.SendReport:
		h.reports = append(h.reports, e.Report)
		ack := protocol.Ack{Sequence: e.Report.Sequence, ControllerEpoch: cellTerm}
		var err error
		if h.ack != nil {
			ack, err = h.ack(e.Report)
		}
		return machine.Reported{Err: err, Token: e.Token, Ack: ack}
	case machine.SendDisrupt:
		h.disrupts = append(h.disrupts, e)
		decision := budget.Decision{Allowed: true}
		var err error
		if h.decide != nil {
			decision, err = h.decide(e.Disruption)
		}
		return machine.Decided{Err: err, Decision: decision, Token: e.Token}
	case machine.SendDone:
		h.dones = append(h.dones, e)
		var err error
		if h.ending != nil {
			err = h.ending(e.Args)
		}
		return machine.Ended{Err: err, Token: e.Token}
	default:
		tb.Fatalf("the agent returned a %T, which the harness does not perform", e)
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

// follow passes the generations of the scopes that the watches of the agent have not
// passed, one at a time, and runs the agent after each.
func (h *harness) follow(tb assert.TB) {
	tb.Helper()
	for range maxDeliveries {
		passed := false
		for _, w := range slices.Clone(h.watches) {
			ctx, cancel := context.WithCancel(assert.Context(tb))
			cancel()
			for ev, err := range h.st.Watch(ctx, w.Scope, w.Incarnation, w.From, w.Node) {
				i := slices.IndexFunc(h.watches, func(x machine.Watch) bool { return x.Token == w.Token })
				if err != nil {
					h.watches = slices.Delete(h.watches, i, i+1)
					h.step(tb, machine.Watched{Err: err, Token: w.Token})
				} else {
					h.watches[i].From = ev.Generation
					h.step(tb, machine.Watched{Event: ev, Token: w.Token})
				}
				passed = true
				break
			}
			h.run(tb)
		}
		if !passed {
			return
		}
	}
	tb.Fatalf("the watches still pass generations after %d deliveries", maxDeliveries)
}

// elapse passes the end of the newest Timer whose Duration is d, and runs the agent
// afterwards. A newer Timer of one wait replaces the older ones, which the agent ignores.
func (h *harness) elapse(tb assert.TB, d time.Duration) {
	tb.Helper()
	i := len(h.timers) - 1
	for i >= 0 && h.timers[i].Duration != d {
		i--
	}
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

// request returns a callback call on the forward path at step 0 for the operation n of the
// shard sid. The request has the node of the tests in its last session, the epoch e and the
// term of the controller of the tests.
func (h *harness) request(call protocol.Call, sid shard.ID, n uint64, e epoch.Epoch) *protocol.Request {
	return &protocol.Request{
		Scope:           cellScope,
		Node:            node.ID,
		Operation:       shard.OperationID{Incarnation: incA, Shard: sid, Sequence: n},
		NodeIncarnation: h.incarnation,
		Epoch:           e,
		ControllerEpoch: cellTerm,
		Shard:           sid,
		Call:            call,
		Path:            protocol.Forward,
		Role:            shard.RolePrimary,
	}
}

// do passes the callback r of the controller of the cell to the agent and runs the agent. It
// returns the token of the call.
func (h *harness) do(tb assert.TB, r *protocol.Request) machine.ReplyToken {
	tb.Helper()
	to := h.call()
	md := protocol.Metadata{Caller: controllerIdentity, Version: protocol.Current}
	h.step(tb, machine.Do{Request: r, Metadata: md, Reply: to})
	h.run(tb)
	return to
}

// place commits the placement of the shard sid with its writer on the node of the tests at
// the epoch e, and passes the generation to the agent.
func (h *harness) place(tb assert.TB, sid shard.ID, e epoch.Epoch) {
	tb.Helper()
	h.commitCell(tb, &store.Mutation{Put: []shard.Placement{placement(sid, e, node.ID)}})
	h.follow(tb)
}

// unplace commits the placement of the shard sid with its writer on another node at the
// epoch e, and passes the generation to the agent.
func (h *harness) unplace(tb assert.TB, sid shard.ID, e epoch.Epoch) {
	tb.Helper()
	h.commitCell(tb, &store.Mutation{Put: []shard.Placement{placement(sid, e, otherNode)}})
	h.follow(tb)
}

// serve places the shard sid on the node at the epoch e, and passes the add of the operation
// n of the shard to the agent. The add succeeds.
func (h *harness) serve(tb assert.TB, sid shard.ID, n uint64, e epoch.Epoch) {
	tb.Helper()
	h.place(tb, sid, e)
	to := h.do(tb, h.request(protocol.Add, sid, n, e))
	assert.NoError(tb, replyTo[struct{}](tb, h, to).Err, "the add of the shard must succeed")
}

// host places the cell of the tests on the node at the epoch e in the root scope, and passes
// the add of the root for the cell to the agent of the root. It returns the token of the
// call.
func (h *harness) host(tb assert.TB, e epoch.Epoch) machine.ReplyToken {
	tb.Helper()
	cell := shard.ID{ID128: cellKey}
	h.commitRoot(tb, &store.Mutation{Put: []shard.Placement{{
		Shard:    cell,
		Epoch:    e,
		Reserved: e,
		Replicas: []shard.Replica{{Node: node.ID, Role: shard.RolePrimary}},
	}}})
	h.follow(tb)
	r := h.request(protocol.Add, cell, 1, e)
	r.Scope, r.ControllerEpoch = rootScope, rootTerm
	return h.do(tb, r)
}

// observed returns the entry of the replica of the shard sid in the last report of the agent.
func (h *harness) observed(tb assert.TB, sid shard.ID) shard.Observed {
	tb.Helper()
	assert.NotEmpty(tb, h.reports, "the agent must have reported")
	last := h.reports[len(h.reports)-1]
	i := slices.IndexFunc(last.Replicas, func(o shard.Observed) bool { return o.Shard == sid })
	assert.True(tb, i >= 0, "the last report must have the replica")
	return last.Replicas[i]
}

// logged returns the message of each record that the agent has logged, in order.
func (h *harness) logged() []string {
	records := h.logs.All()
	messages := make([]string, len(records))
	for i, r := range records {
		messages[i] = r.Message
	}
	return messages
}

// lag returns the last value of the gauge of the lag of the agent.
func (h *harness) lag(tb assert.TB) float64 {
	tb.Helper()
	values := h.metrics.Values(lagInstrument, series...)
	assert.NotEmpty(tb, values, "the agent must have set the lag")
	return values[len(values)-1]
}

// compact ends the watch of the scope of the cell with store.ErrCompacted, as a store does
// after a compaction. It then ends the backoff of the follower, and runs the agent until it
// has read and installed a new snapshot.
func (h *harness) compact(tb assert.TB) {
	tb.Helper()
	i := slices.IndexFunc(h.watches, func(w machine.Watch) bool { return w.Scope == cellScope })
	assert.True(tb, i >= 0, "the agent must watch the scope of its cell")
	w := h.watches[i]
	h.watches = slices.Delete(h.watches, i, i+1)
	h.step(tb, machine.Watched{Err: store.ErrCompacted, Token: w.Token})
	h.elapse(tb, follow.RetryBase/2)
	h.follow(tb)
}

// record returns the record of the controller of the cell of the tests at the epoch e.
func record(e epoch.Epoch) *shard.Cell {
	return &shard.Cell{
		ID:                 cellID,
		Controller:         controllerAddress,
		ControllerIdentity: controllerIdentity,
		Epoch:              e,
		SpecVersion:        firstVersion,
	}
}

// placement returns the placement of the shard sid with the writer on the node n at the
// epoch e. Its range starts at the bytes of sid, so the placements of two shards of the
// tests never start at one position.
func placement(sid shard.ID, e epoch.Epoch, n shard.NodeID) shard.Placement {
	return shardtest.NewPlacementBuilder().
		WithShard(sid).
		WithRange(shard.Range{Start: slices.Clone(sid.ID128[:])}).
		WithEpoch(e).
		WithReserved(e).
		WithReplicas(shard.Replica{Node: n, Role: shard.RolePrimary}).
		Build()
}

// transition returns the transition of the operation op in the phase phase, from another
// node to the node of the tests.
func transition(op shard.OperationID, phase shard.Phase) store.Transition {
	return store.Transition{
		Operation:  op,
		Protocol:   breakbeforemake.Name,
		From:       store.Endpoint{Node: otherNode, Incarnation: 1, Role: shard.RolePrimary},
		To:         store.Endpoint{Node: node.ID, Incarnation: 1, Role: shard.RolePrimary},
		OldEpoch:   firstEpoch,
		NewEpoch:   secondEpoch,
		AbortEpoch: thirdEpoch,
		Phase:      phase,
	}
}

// tokenOf returns the token of e, an effect that the harness performs, and zero for an
// effect of another kind.
func tokenOf(e machine.Effect) machine.Token {
	switch e := e.(type) {
	case machine.Read:
		return e.Token
	case machine.Join:
		return e.Token
	case machine.Acquire:
		return e.Token
	case machine.Handle:
		return e.Token
	case machine.Revoke:
		return e.Token
	case machine.SendReport:
		return e.Token
	case machine.SendDisrupt:
		return e.Token
	case machine.SendDone:
		return e.Token
	}
	return 0
}

// replyTo returns the reply of the agent to the call to, which must have the type
// machine.Reply[T].
func replyTo[T machine.Result](tb assert.TB, h *harness, to machine.ReplyToken) machine.Reply[T] {
	tb.Helper()
	for _, e := range h.replies {
		if r, ok := e.(machine.Reply[T]); ok && r.To == to {
			return r
		}
	}
	tb.Fatalf("the agent has not replied to the call %d", to)
	return machine.Reply[T]{}
}

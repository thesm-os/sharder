// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/fsm"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/telemetry"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
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
	// otherIdentity is the identity of a caller other than the controller.
	otherIdentity shard.Identity = "intruder"
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
)

// The shards of the tests.
var (
	shardA = shard.ID{ID128: id.ID128{0: 0x02, 15: 0x0a}}
	shardB = shard.ID{ID128: id.ID128{0: 0x02, 15: 0x0b}}
	shardC = shard.ID{ID128: id.ID128{0: 0x02, 15: 0x0c}}
)

// The nodes and the cells other than the node and the cell of the tests.
const (
	// otherNode is a node of the cell other than the node of the tests.
	otherNode shard.NodeID = "node-b"
	// thirdNode is a node of the cell other than the node of the tests and otherNode.
	thirdNode shard.NodeID = "node-c"
	// otherCell is a cell other than the cell of the tests, and not the text of a shard ID.
	otherCell shard.CellID = "other"
	// noNode is the writer of a placement without replicas.
	noNode shard.NodeID = ""
)

// The weights of the actions of the property of an agent that a case takes more often than
// the others.
const (
	// placements is the weight of the commit of a placement.
	placements = 3
	// deliveries is the weight of the delivery of a generation.
	deliveries = 4
	// callbacks is the weight of a callback.
	callbacks = 4
	// answers is the weight of the answer to a fence acquisition or a handler call.
	answers = 4
)

// The labels of the cases of the property of an agent, which its coverage requirements count.
const (
	// labelFenced counts a case that answers a fence acquisition with epoch.ErrFenced.
	labelFenced = "a fence acquisition fails with ErrFenced"
	// labelCleanup counts a case in which the agent cleans up a tenure.
	labelCleanup = "the agent cleans up a tenure"
	// labelResend counts a case that sends a callback again.
	labelResend = "a callback arrives again"
	// labelSession counts a case in which the session of the node ends.
	labelSession = "the session of the node ends"
	// labelCompaction counts a case in which the watch of the scope of the cell ends with
	// store.ErrCompacted.
	labelCompaction = "the watch of the cell is compacted"
	// labelServe counts a case in which the handler of an add succeeds.
	labelServe = "a tenure serves"
	// labelDrop counts a case in which the handler of a drop runs.
	labelDrop = "the agent drops a tenure"
)

// The shares of the cases of the property of an agent that its labels must count, about half
// of the shares that 100 cases of one seed measured.
const (
	// common is the share of labelResend, labelSession and labelCompaction, whose steps
	// need no state. The measured shares were 22%, 26% and 28%.
	common = 0.1
	// occasional is the share of labelCleanup and labelServe, which need a tenure. The
	// measured shares were 15% and 18%.
	occasional = 0.05
	// rare is the share of labelFenced and labelDrop, which need a tenure and one result in
	// four of an answer. The measured shares were 6% and 5%.
	rare = 0.02
)

// allShards lists the shards of the property of an agent.
var allShards = []shard.ID{shardA, shardB, shardC}

// outcomes lists the results with which a step of the property of an agent answers a fence
// acquisition or a handler call: success, a transient error, an error that a retry does not
// fix, and a fence that refuses the epoch.
var outcomes = []error{nil, errBusy, errBroken, epoch.ErrFenced}

// halves is a source of randomness whose every draw of rand.Float64 is one half, so each
// backoff is half of its ceiling. Its values differ in their low bits, so the IDs that the
// agent generates differ.
type halves struct {
	// n counts the draws.
	n uint64
}

var _ rand.Rand = (*halves)(nil)

// Uint64 returns the next value: the top bit and the count of the draws.
func (h *halves) Uint64() uint64 {
	h.n++
	return 1<<63 | h.n
}

// Read fills p with the bytes of successive values of Uint64.
func (h *halves) Read(p []byte) (int, error) {
	for i := 0; i < len(p); i += 8 {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], h.Uint64())
		copy(p[i:], b[:])
	}
	return len(p), nil
}

// journal is a slog.Handler that keeps the message of every record.
type journal struct {
	// messages lists the messages of the records in order.
	messages []string
	// mu guards messages.
	mu sync.Mutex
}

var _ slog.Handler = (*journal)(nil)

// Enabled reports true for every level.
func (*journal) Enabled(context.Context, slog.Level) bool { return true }

// Handle keeps the message of r.
func (j *journal) Handle(_ context.Context, r slog.Record) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.messages = append(j.messages, r.Message)
	return nil
}

// WithAttrs returns j. The tests read the messages alone.
func (j *journal) WithAttrs([]slog.Attr) slog.Handler { return j }

// WithGroup returns j. The agent does not group its attributes.
func (j *journal) WithGroup(string) slog.Handler { return j }

// logged returns the messages of the records in order.
func (j *journal) logged() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return slices.Clone(j.messages)
}

// recorder is a telemetry.Reporter that keeps the value of each counter and gauge by the
// name of its instrument, and the names of the instruments that were released. It ignores
// the attributes, because an agent binds every instrument to its namespace and its node.
type recorder struct {
	// values maps the name of each counter and gauge to its value.
	values map[telemetry.InstrumentName]float64
	// released contains the name of each instrument that was released.
	released map[telemetry.InstrumentName]bool
}

var _ telemetry.Reporter = (*recorder)(nil)

// Counter returns a counter of the instrument of spec.
func (r *recorder) Counter(spec telemetry.InstrumentSpec) telemetry.Counter {
	return &counter{r: r, name: spec.Name}
}

// Gauge returns a gauge of the instrument of spec.
func (r *recorder) Gauge(spec telemetry.InstrumentSpec) telemetry.Gauge {
	return &gauge{r: r, name: spec.Name}
}

// Histogram returns a histogram that discards its records. An agent has no histogram.
func (*recorder) Histogram(spec telemetry.InstrumentSpec) telemetry.Histogram {
	return noop.New().Histogram(spec)
}

// Tracer returns a tracer that discards every span.
func (*recorder) Tracer(name telemetry.InstrumentName) telemetry.Tracer {
	return noop.New().Tracer(name)
}

// counter is a counter of a recorder.
type counter struct {
	// r is the recorder.
	r *recorder
	// name is the name of the instrument.
	name telemetry.InstrumentName
}

var _ telemetry.Counter = (*counter)(nil)

// Add adds value to the counter.
func (c *counter) Add(_ context.Context, value int64) { c.r.values[c.name] += float64(value) }

// With returns c, because the recorder ignores attributes.
func (c *counter) With([]telemetry.Attr) telemetry.Counter { return c }

// Release records the release of the counter.
func (c *counter) Release() { c.r.released[c.name] = true }

// gauge is a gauge of a recorder.
type gauge struct {
	// r is the recorder.
	r *recorder
	// name is the name of the instrument.
	name telemetry.InstrumentName
}

var _ telemetry.Gauge = (*gauge)(nil)

// Set sets the gauge to value.
func (g *gauge) Set(_ context.Context, value float64) { g.r.values[g.name] = value }

// Add adds delta to the gauge.
func (g *gauge) Add(_ context.Context, delta float64) { g.r.values[g.name] += delta }

// With returns g, because the recorder ignores attributes.
func (g *gauge) With([]telemetry.Attr) telemetry.Gauge { return g }

// Release records the release of the gauge.
func (g *gauge) Release() { g.r.released[g.name] = true }

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
	// extend registers more strategies in the registry of the agent, or is nil.
	extend func(r *config.Registry) error
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

// harness is the runtime of the tests. It performs the effects of one agent against a
// memory store, replies to its joins, its fence acquisitions, its handler calls and its
// calls to the controller, and keeps what the agent sent. The tests commit to the scope of the
// cell as its controller does, and pass the generations to the agent with follow.
type harness struct {
	// handler returns the result of the handler of the application for the request r, and
	// nil for success. With a nil handler, every call succeeds.
	handler func(r *protocol.Request) error
	// fence returns the result of the fence acquisition of the token t, and nil for success.
	// With a nil fence, every acquisition succeeds.
	fence func(t *fence.Token) error
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
	// held reports whether run leaves the effect e pending. With a nil held, run performs
	// every effect.
	held func(e machine.Effect) bool
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
	// logs are the logs of the agent.
	logs *journal
	// metrics are the instruments of the agent.
	metrics *recorder
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
		cfg:     agent.Config{Rand: &halves{}, Provided: provided, Namespace: namespace, Node: node},
		root:    store.Mutation{Put: []shard.Placement{{Shard: shard.ID{ID128: cellKey}}}},
		support: builtin.Support(),
		create:  true,
		acquire: true,
	}
	for _, opt := range opts {
		opt(&s)
	}
	h := &harness{
		st:    memory.New(fake.New(origin)),
		clock: fake.New(origin),
		logs:  &journal{},
		metrics: &recorder{
			values:   make(map[telemetry.InstrumentName]float64),
			released: make(map[telemetry.InstrumentName]bool),
		},
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
		assert.NoError(tb, s.extend(r), "the strategies of the test must register")
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

// model is the expected state of an agent that the property of an agent drives, and the
// record of what the agent did. Its harness holds every fence acquisition and handler call
// until a step answers it, and passes each effect of the agent to observe.
type model struct {
	// c is the case of the property.
	c *prop.Case
	// h is the harness of the agent.
	h *harness
	// granted maps each shard to the fencing token that Token returns for it.
	granted map[shard.ID]fence.Token
	// read maps each shard to the placement of the last commit that the agent has read.
	read map[shard.ID]shard.Placement
	// retried maps the key of each callback and cleanup whose handler the agent has called
	// to whether the last call ended with a transient error.
	retried map[protocol.Key]bool
	// calls maps the token of each callback that the agent took to its request.
	calls map[machine.ReplyToken]*protocol.Request
	// cancelled contains the token of each call that the runtime cancelled.
	cancelled map[machine.ReplyToken]bool
	// replies counts the replies of the agent to each call.
	replies map[machine.ReplyToken]int
	// started maps the token of each fence acquisition to the number of ends of sessions
	// before the agent started it.
	started map[machine.Token]int
	// epochs maps each shard to the epoch of its last placement.
	epochs map[shard.ID]epoch.Epoch
	// operations maps each shard to the sequence of its newest operation.
	operations map[shard.ID]uint64
	// commits lists each commit to the scope of the cell after the start, in order.
	commits []commit
	// sent lists the request of each callback, in order.
	sent []*protocol.Request
	// applied is the number of commits that the agent has read.
	applied int
	// ends counts the ends of the sessions of the node.
	ends int
	// sequence is the Sequence of the last report.
	sequence uint64
	// incarnation is the NodeIncarnation of the last report.
	incarnation epoch.Epoch
	// term is the epoch of the last record of the controller of the cell.
	term epoch.Epoch
	// closing is the token of the Close, and zero before it.
	closing machine.ReplyToken
	// stops is true when the ownership of the case stops the writers at the end of the
	// lease.
	stops bool
	// live is true while the node has a session.
	live bool
	// stopping is true from the reply to the Close until the end of its step.
	stopping bool
	// stopped is true once the agent has stopped.
	stopped bool
	// failing is true while the controller refuses reports.
	failing bool
}

// commit is one commit to the scope of the cell: its generation and the placements that it
// puts.
type commit struct {
	// puts lists the placements that the commit puts.
	puts []shard.Placement
	// generation is the generation of the commit.
	generation epoch.Epoch
}

// newModel returns the model of an agent that serves. The ownership of the agent stops its
// writers at the end of the lease when stops is true.
func newModel(c *prop.Case, stops bool) *model {
	opts := make([]option, 0, 1)
	if stops {
		opts = append(opts, func(s *setup) {
			s.spec.Ownership = config.Selection{Config: json.RawMessage(stopOnLoss)}
		})
	}
	m := &model{
		c:          c,
		h:          newHarness(c, opts...),
		granted:    make(map[shard.ID]fence.Token),
		read:       make(map[shard.ID]shard.Placement),
		retried:    make(map[protocol.Key]bool),
		calls:      make(map[machine.ReplyToken]*protocol.Request),
		cancelled:  make(map[machine.ReplyToken]bool),
		replies:    make(map[machine.ReplyToken]int),
		started:    make(map[machine.Token]int),
		epochs:     make(map[shard.ID]epoch.Epoch),
		operations: make(map[shard.ID]uint64),
		term:       cellTerm,
		stops:      stops,
	}
	for _, sid := range allShards {
		m.operations[sid] = 1
	}
	m.h.observe = m.observe
	m.h.ack = m.ack
	m.h.start(c)
	m.h.held = func(e machine.Effect) bool {
		switch e.(type) {
		case machine.Acquire, machine.Handle:
			return true
		}
		return false
	}
	return m
}

// observe checks the effect e that the agent appends, and takes what e changes in the
// expected state.
func (m *model) observe(e machine.Effect) {
	c := m.c
	c.Helper()
	assert.False(c, m.stopped, "a stopped agent must not append an effect")
	switch e := e.(type) {
	case machine.Join:
		m.live = true
	case machine.Acquire:
		m.acquire(e)
	case machine.Handle:
		m.handle(e.Request)
	case machine.SendReport:
		want := uint64(1)
		if e.Report.NodeIncarnation == m.incarnation {
			want = m.sequence + 1
		}
		expect.Equal(c, e.Report.Sequence, want, "the agent must number the reports of an incarnation from one")
		m.sequence, m.incarnation = e.Report.Sequence, e.Report.NodeIncarnation
	case machine.Reply[struct{}]:
		m.replies[e.To]++
		expect.Equal(c, m.replies[e.To], 1, "the agent must reply to a call once")
		expect.False(c, m.cancelled[e.To], "the agent must not reply to a call that the runtime cancelled")
		if e.To == m.closing {
			m.stopping = true
		}
	}
}

// acquire checks the fence acquisition e. The agent starts a tenure only while the node has a
// session, at the epoch of the placement on the node that it has read last.
func (m *model) acquire(e machine.Acquire) {
	c := m.c
	p := m.read[e.Fence.Shard]
	expect.True(c, m.live, "the agent must start a tenure only while the node has a session")
	expect.Equal(
		c,
		e.Fence.Epoch,
		p.Epoch,
		"the agent must acquire the fence at the epoch of the placement that it read",
	)
	expect.True(c, slices.Contains(p.Replicas, shard.Replica{Node: node.ID, Role: shard.RolePrimary}),
		"the agent must acquire the fence of a placement on the node")
	m.started[e.Token] = m.ends
}

// handle checks the handler call of the request r. The agent calls the handler for a key
// again only after a transient error, and a drop or a cleanup revokes the token of its
// tenure.
func (m *model) handle(r *protocol.Request) {
	key := r.Key()
	if transient, ok := m.retried[key]; ok {
		expect.True(m.c, transient, "the agent must call the handler for a key again only after a transient error")
	}
	m.retried[key] = false
	if r.Path == protocol.Cleanup {
		m.c.Classify(labelCleanup)
	}
	if g, ok := m.granted[r.Shard]; ok && r.Call == protocol.Drop && g.Epoch == r.Epoch {
		delete(m.granted, r.Shard)
	}
}

// ack returns the reply of the controller to the report r: an acceptance at the term of the
// last record, or protocol.ErrUnreachable while the controller refuses reports.
func (m *model) ack(r *protocol.Report) (protocol.Ack, error) {
	if m.failing {
		return protocol.Ack{}, protocol.ErrUnreachable
	}
	return protocol.Ack{Sequence: r.Sequence, ControllerEpoch: m.term}, nil
}

// place commits the placement of a shard that c draws at the next epoch of the shard, with
// the node, another node or no node as its writer.
func (m *model) place(c *prop.Case) {
	sid := c.Draw(prop.SampledFrom(allShards...), "shard")
	writer := c.Draw(prop.SampledFrom(node.ID, otherNode, noNode), "writer")
	m.epochs[sid]++
	p := placement(sid, m.epochs[sid], writer)
	if writer == noNode {
		p.Replicas = nil
	}
	m.commit(c, &store.Mutation{Put: []shard.Placement{p}})
}

// assign commits the placement of a shard that c draws on the node at the next epoch of the
// shard, and sends the add of the next operation of the shard at that epoch, as the
// controller of the cell moves a shard to the node.
func (m *model) assign(c *prop.Case) {
	sid := c.Draw(prop.SampledFrom(allShards...), "shard")
	m.epochs[sid]++
	m.operations[sid]++
	m.commit(c, &store.Mutation{Put: []shard.Placement{placement(sid, m.epochs[sid], node.ID)}})
	r := m.h.request(protocol.Add, sid, m.operations[sid], m.epochs[sid])
	r.ControllerEpoch = m.term
	m.send(c, r)
}

// release sends the drop of the next operation of a shard that c draws at the epoch of its
// last placement, as the controller of the cell starts to move a shard away from the node.
func (m *model) release(c *prop.Case) {
	sid := c.Draw(prop.SampledFrom(allShards...), "shard")
	m.operations[sid]++
	r := m.h.request(protocol.Drop, sid, m.operations[sid], m.epochs[sid])
	r.ControllerEpoch = m.term
	m.send(c, r)
}

// transition commits a transition of the newest operation of a shard that c draws, or of the
// operation after it, in a phase that c draws, with the node as its target or its source.
func (m *model) transition(c *prop.Case) {
	sid := c.Draw(prop.SampledFrom(allShards...), "shard")
	m.operations[sid] += c.Draw(prop.Integer[uint64](0, 1), "next operation")
	op := shard.OperationID{Incarnation: incA, Shard: sid, Sequence: m.operations[sid]}
	t := transition(op, c.Draw(prop.SampledFrom(shard.PhaseRunning, shard.PhaseAborting), "phase"))
	if c.Draw(prop.Boolean(), "from the node") {
		t.From.Node, t.To.Node = node.ID, otherNode
	}
	m.commit(c, &store.Mutation{Transitions: []store.Transition{t}})
}

// done commits the end of the transition of a shard that c draws.
func (m *model) done(c *prop.Case) {
	sid := c.Draw(prop.SampledFrom(allShards...), "shard")
	m.commit(c, &store.Mutation{Done: []shard.ID{sid}})
}

// record commits the record of the next controller of the cell, with the identity of the
// controller of the tests.
func (m *model) record(c *prop.Case) {
	m.term++
	m.commit(c, &store.Mutation{Cell: record(m.term)})
}

// commit commits mu to the scope of the cell, and keeps the placements that mu puts with the
// generation of the commit.
func (m *model) commit(c *prop.Case, mu *store.Mutation) {
	gen := m.h.commitCell(c, mu)
	m.commits = append(m.commits, commit{puts: mu.Put, generation: gen})
}

// deliver passes the next generation of the scope of the cell to the agent. The placements of
// the commits up to that generation become the placements that the agent has read.
func (m *model) deliver(c *prop.Case) {
	i := slices.IndexFunc(m.h.watches, func(w machine.Watch) bool { return w.Scope == cellScope })
	w := m.h.watches[i]
	ctx, cancel := context.WithCancel(assert.Context(c))
	cancel()
	passed := false
	for ev, err := range m.h.st.Watch(ctx, w.Scope, w.Incarnation, w.From, w.Node) {
		assert.NoError(c, err, "the watch of the scope of the cell must yield its next generation")
		m.h.watches[i].From = ev.Generation
		m.advance(ev.Generation)
		m.h.step(c, machine.Watched{Event: ev, Token: w.Token})
		passed = true
		break
	}
	assert.True(c, passed, "the watch of the scope of the cell must yield a generation that the agent has not read")
	m.h.run(c)
	m.sync()
}

// advance takes the placements of the commits up to the generation gen as the placements that
// the agent has read.
func (m *model) advance(gen epoch.Epoch) {
	for m.applied < len(m.commits) && m.commits[m.applied].generation <= gen {
		for _, p := range m.commits[m.applied].puts {
			m.read[p.Shard] = p
		}
		m.applied++
	}
}

// callback sends a callback for a shard that c draws: an add or a drop of the newest operation
// of the shard or of the operation after it, at the epoch of the last placement of the shard
// or at the epoch before it, on a path and at a step that c draws.
func (m *model) callback(c *prop.Case) {
	sid := c.Draw(prop.SampledFrom(allShards...), "shard")
	call := c.Draw(prop.SampledFrom(protocol.Add, protocol.Drop), "call")
	m.operations[sid] += c.Draw(prop.Integer[uint64](0, 1), "next operation")
	earlier := min(c.Draw(prop.Integer[epoch.Epoch](0, 1), "earlier epoch"), m.epochs[sid])
	r := m.h.request(call, sid, m.operations[sid], m.epochs[sid]-earlier)
	r.Path = c.Draw(prop.SampledFrom(protocol.Forward, protocol.Abort), "path")
	r.Step = c.Draw(prop.Integer(0, 1), "step")
	r.ControllerEpoch = m.term
	m.send(c, r)
}

// resend sends a callback that a step sent before, which c draws, again.
func (m *model) resend(c *prop.Case) {
	c.Classify(labelResend)
	m.send(c, m.sent[c.Draw(prop.Integer(0, len(m.sent)-1), "callback")])
}

// send passes the callback r to the agent, and records it. The model does not wait for the
// reply to a callback that it sends after the agent has stopped.
func (m *model) send(c *prop.Case, r *protocol.Request) {
	to := m.h.call()
	if !m.stopped {
		m.calls[to] = r
	}
	m.sent = append(m.sent, r)
	md := protocol.Metadata{Caller: controllerIdentity, Version: protocol.Current}
	m.h.step(c, machine.Do{Request: r, Metadata: md, Reply: to})
	m.h.run(c)
	m.sync()
}

// answer answers a fence acquisition or a handler call that the harness holds, which c
// draws, with a result that c draws.
func (m *model) answer(c *prop.Case) {
	i := c.Draw(prop.Integer(0, len(m.h.pending)-1), "effect")
	m.finish(c, i, c.Draw(prop.SampledFrom(outcomes...), "outcome"))
}

// finish passes the result err of the held effect i to the agent, and takes what the result
// changes in the expected state. A successful acquisition grants its token, unless the agent
// has stopped, or the session of the node has ended since its start under ownership.Stop. An
// add whose handler fails loses its token.
func (m *model) finish(c *prop.Case, i int, err error) {
	e := m.h.pending[i]
	m.h.pending = slices.Delete(m.h.pending, i, i+1)
	switch e := e.(type) {
	case machine.Acquire:
		if err == nil && !m.stopped && (!m.stops || m.started[e.Token] == m.ends) {
			m.granted[e.Fence.Shard] = e.Fence
		}
		if errors.Is(err, epoch.ErrFenced) {
			c.Classify(labelFenced)
		}
		m.h.step(c, machine.Acquired{Err: err, Token: e.Token})
	case machine.Handle:
		r := e.Request
		m.retried[r.Key()] = errs.Retryable(err)
		if err != nil && r.Call == protocol.Add {
			delete(m.granted, r.Shard)
		}
		if err == nil && r.Call == protocol.Add {
			c.Classify(labelServe)
		}
		if r.Call == protocol.Drop && r.Path != protocol.Cleanup {
			c.Classify(labelDrop)
		}
		m.h.step(c, machine.Handled{Err: err, Token: e.Token})
	}
	m.h.run(c)
	m.sync()
}

// expire passes the end of the Timer i of the harness to the agent.
func (m *model) expire(c *prop.Case, i int) {
	tm := m.h.timers[i]
	m.h.timers = slices.Delete(m.h.timers, i, i+1)
	m.h.step(c, machine.Elapsed{Token: tm.Token})
	m.h.run(c)
	m.sync()
}

// end ends the session of the node. Under ownership.Stop, the end revokes every token.
func (m *model) end(c *prop.Case) {
	c.Classify(labelSession)
	m.live = false
	m.ends++
	if m.stops {
		clear(m.granted)
	}
	m.h.step(c, machine.SessionEnded{Token: m.h.join})
	m.h.run(c)
	m.sync()
}

// cancel ends the wait of the caller of a call without a reply, which c draws. An agent whose
// Close loses its caller stops.
func (m *model) cancel(c *prop.Case) {
	calls := m.waiting()
	to := calls[c.Draw(prop.Integer(0, len(calls)-1), "call")]
	m.cancelled[to] = true
	m.h.step(c, machine.Cancelled{Reply: to})
	m.h.run(c)
	if to == m.closing {
		m.stopping = true
	}
	m.sync()
}

// waiting returns the tokens of the callbacks and of the Close that have no reply and whose
// callers wait, in their order.
func (m *model) waiting() []machine.ReplyToken {
	var calls []machine.ReplyToken
	for _, to := range slices.Sorted(maps.Keys(m.calls)) {
		if m.replies[to] == 0 && !m.cancelled[to] {
			calls = append(calls, to)
		}
	}
	if m.closing != 0 && m.replies[m.closing] == 0 && !m.cancelled[m.closing] {
		calls = append(calls, m.closing)
	}
	return calls
}

// compact ends the watch of the scope of the cell with store.ErrCompacted, so the agent reads
// a snapshot of every commit.
func (m *model) compact(c *prop.Case) {
	c.Classify(labelCompaction)
	m.advance(m.h.state(c).Head.Generation)
	m.h.compact(c)
	m.sync()
}

// fault reports a fault in the replica of a shard that c draws, or the recovery of the
// replica.
func (m *model) fault(c *prop.Case) {
	sid := c.Draw(prop.SampledFrom(allShards...), "shard")
	to := m.h.call()
	if c.Draw(prop.Boolean(), "recovery") {
		m.h.step(c, machine.Recovered{Reply: to, Shard: sid})
	} else {
		f := c.Draw(prop.SampledFrom(shard.FaultTransient, shard.FaultPermanent), "fault")
		m.h.step(c, machine.ReportFault{Cause: errDisk, Reply: to, Shard: sid, Fault: f})
	}
	m.h.run(c)
	m.sync()
}

// close passes a Close of the application with a deadline after now.
func (m *model) close(c *prop.Case) {
	m.closing = m.h.call()
	m.h.step(c, machine.Close{Deadline: origin.Add(grace), Reply: m.closing})
	m.h.run(c)
	m.sync()
}

// halt passes the Stopped of the run.
func (m *model) halt(c *prop.Case) {
	m.h.step(c, machine.Stopped{})
	m.stopped = true
	clear(m.granted)
}

// complete answers each fence acquisition and handler call that the harness holds with
// success, passes each generation of the scope that the agent has not read, and ends each
// Timer of a backoff. The controller accepts reports from then on.
func (m *model) complete(c *prop.Case) {
	m.failing = false
	for range len(m.h.pending) {
		m.finish(c, 0, nil)
	}
	for m.applied < len(m.commits) && !m.stopped &&
		slices.ContainsFunc(m.h.watches, func(w machine.Watch) bool { return w.Scope == cellScope }) {
		m.deliver(c)
	}
	for i, tm := range slices.Backward(m.h.timers) {
		if tm.Duration < loadWindow {
			m.expire(c, i)
		}
	}
}

// sync takes the end of a step: an agent that has replied to its Close, or whose Close has
// lost its caller, has stopped.
func (m *model) sync() {
	if m.stopping {
		m.stopping, m.stopped = false, true
		clear(m.granted)
	}
}

// invariant checks what holds after every step: no step stops the agent with an error, the
// agent runs one callback of a shard at a time, and Token returns the token that the model
// expects for each shard.
func (m *model) invariant(c *prop.Case) {
	assert.Nil(c, m.h.machine.Err(), "no step of the property may stop the agent with an error")
	running := make(map[shard.ID]int)
	for _, e := range m.h.pending {
		switch e := e.(type) {
		case machine.Acquire:
			running[e.Fence.Shard]++
		case machine.Handle:
			running[e.Request.Shard]++
		}
	}
	for _, sid := range allShards {
		expect.InRange(c, running[sid], 0, 1, "the agent must run one callback of a shard at a time")
		got, err := m.h.machine.Token(sid)
		if want, ok := m.granted[sid]; ok {
			expect.NoError(c, err, "Token must return the token of a tenure that has acquired its fence")
			expect.Equal(c, got, want, "Token must return the token of the last acquisition of the shard")
		} else {
			expect.HasError(c, err, "Token must return an error for a shard without a token")
		}
	}
}

// settle checks the agent once the drain has answered every call with success and passed
// every generation. Every callback has its reply, unless it is an add whose placement on the
// node the agent has not read at the epoch of the add. Token returns a token only for the
// placement on the node that the agent has read, at the epoch of that placement.
func (m *model) settle(c *prop.Case) {
	for _, to := range slices.Sorted(maps.Keys(m.calls)) {
		r := m.calls[to]
		if m.replies[to] > 0 || m.cancelled[to] {
			continue
		}
		p := m.read[r.Shard]
		expect.Equal(c, r.Call, protocol.Add, "every drop must have its reply")
		expect.False(
			c,
			p.Epoch == r.Epoch && slices.Contains(p.Replicas, shard.Replica{Node: node.ID, Role: shard.RolePrimary}),
			"an add whose placement the agent has read must have its reply",
		)
	}
	for _, sid := range allShards {
		got, err := m.h.machine.Token(sid)
		if err != nil {
			continue
		}
		p := m.read[sid]
		expect.Equal(c, got.Epoch, p.Epoch, "a token must have the epoch of the placement that the agent read")
		expect.True(c, slices.Contains(p.Replicas, shard.Replica{Node: node.ID, Role: shard.RolePrimary}),
			"a token must be of a placement on the node")
	}
}

// TestMachine checks the construction, the lifecycle and the replies of an agent.
func TestMachine(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give func(*agent.Config)
			name string
		}{
			{name: "returns ErrConfig for a config without a clock", give: func(c *agent.Config) { c.Clock = nil }},
			{
				name: "returns ErrConfig for a config without a reporter",
				give: func(c *agent.Config) { c.Reporter = nil },
			},
			{name: "returns ErrConfig for a config without a logger", give: func(c *agent.Config) { c.Logger = nil }},
			{
				name: "returns ErrConfig for a config without a registry",
				give: func(c *agent.Config) { c.Registry = nil },
			},
			{name: "returns ErrConfig for a node without an ID", give: func(c *agent.Config) { c.Node.ID = "" }},
			{
				name: "returns ErrConfig for a node without an address",
				give: func(c *agent.Config) { c.Node.Address = "" },
			},
			{
				name: "returns ErrConfig for a cell that is not the text of a shard ID",
				give: func(c *agent.Config) { c.Node.Cell = otherCell },
			},
			{
				name: "returns ErrConfig for a config without a source of randomness",
				give: func(c *agent.Config) { c.Rand = nil },
			},
			{
				name: "returns ErrConfig for a config without a namespace",
				give: func(c *agent.Config) { c.Namespace = "" },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				cfg := completeConfig(t)
				tt.give(&cfg)
				_, err := agent.New(&cfg)
				assert.ErrorIs(t, err, agent.ErrConfig, "New must refuse a config without a required field")
			})
		}

		t.Run("returns ErrConfig for a nil config", func(t *testing.T) {
			t.Parallel()
			_, err := agent.New(nil)
			assert.ErrorIs(t, err, agent.ErrConfig, "New must refuse a nil config")
		})
	})

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("replies to Started once the node has a session and a snapshot of its cell", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			started := h.call()
			h.step(t, machine.Started{Reply: started})
			assert.Empty(t, h.replies, "the agent must not reply before it serves")
			h.run(t)
			assert.Equal(t, h.replies, []machine.Effect{machine.Reply[struct{}]{To: started}},
				"the agent must reply to Started once it serves")
		})

		t.Run("joins the cell of the node with the lease of the spec", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.Length(t, h.joins, 1, "the agent must join its cell once")
			got := h.joins[0]
			expect.Equal(t, got.Scope, cellScope, "the agent must join the scope of its cell")
			expect.Equal(t, got.Incarnation, incA, "the join must name the namespace incarnation")
			expect.Equal(t, got.Node.ID, node.ID, "the join must register the node")
			expect.Equal(t, got.Lease.TTL, shard.Duration(10*time.Second), "the join must have the lease of the spec")
		})

		t.Run("stops reading the root scope once it knows its cell", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.Length(t, h.watches, 1, "the agent must watch the scope of its cell alone")
			assert.Equal(t, h.watches[0].Scope, cellScope, "the watch must be of the scope of the cell")
		})

		t.Run("watches the scope of its cell for its node", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.Length(t, h.watches, 1, "the agent must watch the scope of its cell")
			expect.Equal(t, h.watches[0].Node, node.ID, "the watch must select the changes of the node")
			expect.Equal(t, h.watches[0].Incarnation, incA, "the watch must name the namespace incarnation")
		})

		t.Run("stops with fsm.ErrRejected for a second Started", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			first := h.call()
			h.step(t, machine.Started{Reply: first})
			second := h.call()
			h.step(t, machine.Started{Reply: second})
			expect.ErrorIs(t, h.machine.Err(), fsm.ErrRejected, "the lifecycle must refuse a second Started")
			expect.ErrorIs(t, replyTo[struct{}](t, h, first).Err, fsm.ErrRejected,
				"the first Started must get the error that stopped the agent")
			expect.ErrorIs(t, replyTo[struct{}](t, h, second).Err, fsm.ErrRejected,
				"the second Started must get the error that stopped the agent")
		})

		t.Run("replies with ErrNotServing to Started at Stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			started := h.call()
			h.step(t, machine.Started{Reply: started})
			h.step(t, machine.Stopped{})
			assert.ErrorIs(t, replyTo[struct{}](t, h, started).Err, agent.ErrNotServing,
				"Stopped must reply to Started with ErrNotServing")
			assert.Nil(t, h.machine.Err(), "an agent that Stopped ends must not report an error")
		})

		t.Run("ends the reads of its scopes when an error stops it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Started{Reply: h.call()})
			assert.NotNil(t, h.machine.Err(), "a second Started must stop the agent")
			expect.Empty(t, h.watches, "the agent must end the watch of its cell")
			expect.Contains(t, h.leaves, h.join, "the agent must end the session of its node")
		})

		t.Run("ends the read of the root scope when an error stops it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.create = false })
			h.step(t, machine.Started{Reply: h.call()})
			h.run(t)
			h.follow(t)
			assert.Length(t, h.watches, 1, "the agent must watch the root scope for the namespace")
			h.step(t, machine.Started{Reply: h.call()})
			assert.NotNil(t, h.machine.Err(), "a second Started must stop the agent")
			assert.Empty(t, h.watches, "the agent must end the watch of the root scope")
		})

		t.Run("appends no effect at Stopped when no call waits", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Stopped{}, out)
			assert.Equal(t, got, out, "Stopped must append only the replies to the calls that wait")
		})

		t.Run("returns out unchanged after it has stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Stopped{})
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Started{Reply: h.call()}, out)
			assert.Equal(t, got, out, "a stopped agent must not take an event")
		})

		t.Run("returns out unchanged after an error has stopped it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Started{Reply: h.call()})
			assert.NotNil(t, h.machine.Err(), "a second Started must stop the agent")
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Started{Reply: h.call()}, out)
			assert.Equal(t, got, out, "an agent that an error stopped must not take an event")
		})

		t.Run("drops the reply to a Started that the runtime cancelled", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			started := h.call()
			h.step(t, machine.Started{Reply: started})
			h.step(t, machine.Cancelled{Reply: started})
			h.run(t)
			assert.Empty(t, h.replies, "the agent must not reply to a call that the runtime cancelled")
		})

		t.Run("returns out unchanged for a Timer that it did not arm", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Elapsed{Token: 999}, out)
			assert.Equal(t, got, out, "the agent must ignore a Timer that it did not arm")
		})

		t.Run("returns out unchanged for a Timer before it knows its cell", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Elapsed{Token: 999}, out)
			assert.Equal(t, got, out, "the agent must ignore a Timer that it did not arm")
		})

		t.Run("returns out unchanged for the Cancelled of a call that does not wait", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Cancelled{Reply: 999}, out)
			assert.Equal(t, got, out, "the agent must ignore the Cancelled of a call that does not wait")
		})

		t.Run("returns out unchanged for an event of no part", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Planned{Token: 999}, out)
			assert.Equal(t, got, out, "the agent must ignore an event that no part waits for")
		})

		t.Run("releases its instruments at Stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Stopped{})
			expect.True(t, h.metrics.released[refusalsInstrument], "the agent must release the counter of refusals")
			expect.True(t, h.metrics.released[lagInstrument], "the agent must release the gauge of the lag")
		})

		t.Run("keeps its contract under every sequence of events", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "an agent keeps its contract under every sequence of events", exercise,
				prop.Require(labelFenced, rare),
				prop.Require(labelCleanup, occasional),
				prop.Require(labelResend, common),
				prop.Require(labelSession, common),
				prop.Require(labelCompaction, common),
				prop.Require(labelServe, occasional),
				prop.Require(labelDrop, rare),
			)
		})
	})

	t.Run("Err", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil while the agent runs", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.Nil(t, h.machine.Err(), "an agent that runs must not report an error")
		})
	})
}

// The names of the instruments of an agent.
const (
	refusalsInstrument telemetry.InstrumentName = "sharder.fence.refusals"
	lagInstrument      telemetry.InstrumentName = "sharder.generation.lag"
)

// registry returns a registry with every built-in strategy of the support matrix.
func registry(tb assert.TB) *config.Registry {
	tb.Helper()
	r := config.NewRegistry(builtin.Support())
	assert.NoError(tb, builtin.Register(r), "the built-ins must register")
	return r
}

// completeConfig returns a Config that New accepts.
func completeConfig(tb assert.TB) agent.Config {
	tb.Helper()
	return agent.Config{
		Clock:     fake.New(origin),
		Rand:      &halves{},
		Reporter:  noop.New(),
		Logger:    slog.New(&journal{}),
		Registry:  registry(tb),
		Namespace: namespace,
		Node:      node,
	}
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

// step passes ev to the agent with the marker in out through wrap, checks that the agent
// keeps out, and keeps the effects that the agent appends.
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
// until no effect is pending. It leaves an effect that held reports pending.
func (h *harness) run(tb assert.TB) {
	tb.Helper()
	for range maxDeliveries {
		i := slices.IndexFunc(h.pending, func(e machine.Effect) bool { return h.held == nil || !h.held(e) })
		if i < 0 {
			return
		}
		e := h.pending[i]
		h.pending = slices.Delete(h.pending, i, i+1)
		h.step(tb, h.perform(tb, e))
	}
	tb.Fatalf("the agent still runs after %d deliveries", maxDeliveries)
}

// release performs the pending effects that held left pending, and every later effect.
func (h *harness) release(tb assert.TB) {
	tb.Helper()
	h.held = nil
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

// request returns the request of the callback call of the operation n of the shard sid, on
// the forward path at step 0. It names the node in its last session, the epoch e and the
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

// placement returns the placement of the shard sid with the writer on the node n at the
// epoch e. Its range starts at the bytes of sid, so the placements of two shards of the
// tests never start at one position.
func placement(sid shard.ID, e epoch.Epoch, n shard.NodeID) shard.Placement {
	return shard.Placement{
		Shard:    sid,
		Range:    shard.Range{Start: slices.Clone(sid.ID128[:])},
		Epoch:    e,
		Reserved: e,
		Replicas: []shard.Replica{{Node: n, Role: shard.RolePrimary}},
	}
}

// place commits the placement of the shard sid that names the node at the epoch e, and
// passes the generation to the agent.
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

// exercise runs the steps of the case c over an agent that serves, as the controller of its
// cell, the membership and the application drive it. The steps commit placements,
// transitions, their ends and records of the controller, pass the generations to the agent
// one at a time, send callbacks and send them again, answer the held fence acquisitions and
// handler calls, end Timers, cancel calls, end the session of the node, compact the watch,
// report faults, and close or stop the agent. The model checks the contract of the agent
// after every step:
//   - No step stops the agent with an error, and a stopped agent appends no effect.
//   - The agent replies to each call once, and never to a call that the runtime cancelled.
//   - It runs one callback of a shard at a time, and calls the handler for a key again only
//     after a transient error.
//   - It starts a tenure only while the node has a session, at the epoch of the placement on
//     the node that it has read.
//   - Token returns the token of each tenure from its acquisition until a drop or a cleanup
//     of the tenure starts, its add fails, the session ends under ownership.Stop, or the
//     agent stops.
//   - It numbers the reports of each incarnation from one.
//
// The drain answers every call with success and passes every generation. Every callback then
// has its reply, unless it is an add whose placement the agent has not read at its epoch, and
// Token returns a token only for the placement on the node that the agent has read.
func exercise(c *prop.Case) {
	m := newModel(c, c.Draw(prop.Boolean(), "stop at the end of the lease"))
	watching := func(struct{}) bool {
		return !m.stopped &&
			slices.ContainsFunc(m.h.watches, func(w machine.Watch) bool { return w.Scope == cellScope })
	}
	behind := func(s struct{}) bool { return m.applied < len(m.commits) && watching(s) }
	holding := func(struct{}) bool { return len(m.h.pending) > 0 }
	timing := func(struct{}) bool { return len(m.h.timers) > 0 }
	incomplete := func(s struct{}) bool {
		return holding(s) || behind(s) ||
			slices.ContainsFunc(m.h.timers, func(tm machine.Timer) bool { return tm.Duration < loadWindow })
	}
	stateful.Steps(c, stateful.Machine[struct{}]{
		Actions: []stateful.Action[struct{}]{
			{Name: "place", Weight: placements, Run: func(c *prop.Case, _ int, _ any) { m.place(c) }},
			{Name: "assign", Weight: placements, Run: func(c *prop.Case, _ int, _ any) { m.assign(c) }},
			{Name: "release", Weight: placements, Run: func(c *prop.Case, _ int, _ any) { m.release(c) }},
			{Name: "start a transition", Run: func(c *prop.Case, _ int, _ any) { m.transition(c) }},
			{Name: "end a transition", Run: func(c *prop.Case, _ int, _ any) { m.done(c) }},
			{Name: "record a controller", Run: func(c *prop.Case, _ int, _ any) { m.record(c) }},
			{
				Name: "deliver", Weight: deliveries, Enabled: behind,
				Run: func(c *prop.Case, _ int, _ any) { m.deliver(c) },
			},
			{Name: "callback", Weight: callbacks, Run: func(c *prop.Case, _ int, _ any) { m.callback(c) }},
			{
				Name: "send a callback again", Enabled: func(struct{}) bool { return len(m.sent) > 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.resend(c) },
			},
			{
				Name: "answer", Weight: answers, Enabled: holding,
				Run: func(c *prop.Case, _ int, _ any) { m.answer(c) },
			},
			{
				Name: "cancel", Enabled: func(struct{}) bool { return len(m.waiting()) > 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.cancel(c) },
			},
			{
				Name: "elapse", Enabled: timing,
				Run: func(c *prop.Case, _ int, _ any) {
					m.expire(c, c.Draw(prop.Integer(0, len(m.h.timers)-1), "timer"))
				},
			},
			{
				Name: "end the session", Enabled: func(struct{}) bool { return m.live && !m.stopped },
				Run: func(c *prop.Case, _ int, _ any) { m.end(c) },
			},
			{Name: "compact", Enabled: watching, Run: func(c *prop.Case, _ int, _ any) { m.compact(c) }},
			{Name: "report a fault", Run: func(c *prop.Case, _ int, _ any) { m.fault(c) }},
			{Name: "refuse reports", Run: func(*prop.Case, int, any) { m.failing = !m.failing }},
			{
				Name: "close", Enabled: func(struct{}) bool { return m.closing == 0 && !m.stopped },
				Run: func(c *prop.Case, _ int, _ any) { m.close(c) },
			},
			{
				Name: "stop", Enabled: func(struct{}) bool { return !m.stopped },
				Run: func(c *prop.Case, _ int, _ any) { m.halt(c) },
			},
			{
				Name: "complete", Drain: true, Enabled: incomplete,
				Run: func(c *prop.Case, _ int, _ any) { m.complete(c) },
			},
		},
		Invariant: func(c *prop.Case, _ struct{}) { m.invariant(c) },
		Settle:    func(c *prop.Case, _ struct{}) { m.settle(c) },
	})
}

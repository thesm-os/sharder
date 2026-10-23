// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
	"weak"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/coretest/telemetrytest"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/fsm"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand/constant"
	"go.thesmos.sh/core/telemetry"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/proposal"
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
	// unknownCaller is an identity that the controller does not authorise.
	unknownCaller shard.Identity = "unknown"
	// controllerAddress is the address of the controller of the tests.
	controllerAddress = "controller-1:7000"
	// controllerIdentity is the identity of the controller of the tests.
	controllerIdentity shard.Identity = "controller-1"
	// otherCell is a cell other than the cell of the tests.
	otherCell shard.CellID = "other"
)

// The epochs of the tests.
const (
	// priorTerm is the term of the controller of the cell before the controller of a test.
	priorTerm epoch.Epoch = 2
	// cellTerm is the term of the controller of each test.
	cellTerm epoch.Epoch = 3
	// firstVersion is the version of the spec of the tests.
	firstVersion epoch.Epoch = 1
)

// The tests divide each cell into four slots, so each cell has four shards.
const (
	// slotsConfig is the configuration of fixed-slots in the spec of the tests.
	slotsConfig = `{"slots":4}`
	// shards is the number of shards of the cell.
	shards = 4
)

// maxDeliveries bounds the effects that run performs, so a controller that never settles
// fails its test instead of the timeout of go test.
const maxDeliveries = 10000

// half is the value that rand.Float64 returns for the source of randomness of the tests, so
// each backoff is half of its ceiling.
const half = 0.5

// The weights of the actions of the property of a controller that a case takes more often
// than the others.
const (
	// answers is the weight of the answer to a callback.
	answers = 4
	// reports is the weight of a report.
	reports = 2
)

// The labels of the cases of the property of a controller, which its coverage requirements
// count.
const (
	// labelDefinite counts a case in which an agent replies to a callback with an error that a
	// retry does not fix.
	labelDefinite = "a callback fails for good"
	// labelLostReply counts a case that loses the reply of a commit.
	labelLostReply = "the reply of a commit is lost"
	// labelOutage counts a case in which the store has an outage.
	labelOutage = "the store has an outage"
	// labelAdmitted counts a case in which the controller admits a disruption.
	labelAdmitted = "a disruption is admitted"
	// labelRestart counts a case in which a node restarts.
	labelRestart = "a node restarts"
	// labelDeparture counts a case in which a node leaves the view.
	labelDeparture = "a node leaves the view"
)

// The least shares of the cases of the property of a controller that the labels must count.
// Over 1,000 cases, each common label counts 18% to 42% of the cases, and the admitted
// disruptions count 4%.
const (
	// common is the least share of a common label.
	common = 0.1
	// rare is the least share of the admitted disruptions.
	rare = 0.02
)

// answersOfAgents lists the replies of an agent to a callback in the property of a controller.
// The list has a success, a transient error, an error that a retry does not fix, and the end
// of the timeout of the callback.
var answersOfAgents = []error{nil, protocol.ErrUnreachable, errRefused, context.DeadlineExceeded}

// marker is the effect that the harness passes in out with each event: an Unwatch without a
// watch, which the controller never returns itself.
var marker machine.Effect = machine.Unwatch{}

// pageLimit is the number of placements on each page that the harness reads, so a snapshot
// of the four shards of a cell takes two pages.
const pageLimit = 2

// The log messages that the tests check. A test pins each message that the controller
// logs.
const (
	logPass      = "control: a pass failed"
	logViolation = "control: a replica breaks a constraint"
	logMove      = "control: the engine does not start a move"
	logAdmission = "control: the admission of a plan failed"
	logRefusal   = "control: the admission refuses a move"
)

// The nodes of the tests, each at its first incarnation.
var (
	nodeA = shard.Node{ID: "node-a", Address: "node-a:7000", Identity: "agent-a", Incarnation: 1}
	nodeB = shard.Node{ID: "node-b", Address: "node-b:7000", Identity: "agent-b", Incarnation: 1}
	nodeC = shard.Node{ID: "node-c", Address: "node-c:7000", Identity: "agent-c", Incarnation: 1}
	nodeD = shard.Node{ID: "node-d", Address: "node-d:7000", Identity: "agent-d", Incarnation: 1}
	// nameless is a node without an ID. A pass over a view with it fails.
	nameless = shard.Node{Address: "nameless:7000", Incarnation: 1}
)

// The scopes, the incarnation, the clock and the capabilities of the tests.
var (
	// cellKey is the 16 bytes of the ULID of the cell, which is the root shard of the cell.
	cellKey = id.ID128{0: 0x01, 15: 0xce}
	// cellID is the cell of the tests: the text of the ULID of cellKey.
	cellID = shard.CellID(ulid.Format(cellKey.ID()))
	// cellScope is the scope of the cell of the tests.
	cellScope = shard.Scope{Namespace: namespace, Cell: cellID}
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

// readings reads what the instruments of a controller of the cell of the tests report into a
// telemetrytest.Recorder. Each method takes the name of an instrument and the values of the
// attributes that the controller binds after the namespace and the cell, in their order.
type readings struct {
	*telemetrytest.Recorder
}

// value returns the last value of the gauge name, the count of the records of the histogram
// name, or the sum of the values of the counter name, in the series of attrs. It returns 0
// for a series without a value.
func (r readings) value(name string, attrs ...string) float64 {
	values := r.Values(telemetry.InstrumentName(name), series(name, attrs)...)
	if slices.Contains(gauges, name) {
		if len(values) == 0 {
			return 0
		}
		return values[len(values)-1]
	}
	if slices.Contains(histograms, name) {
		return float64(len(values))
	}
	return r.sum(name, attrs...)
}

// sum returns the sum of the values of the instrument name in the series of attrs: the sum
// of the records of a histogram, and the sum of the additions of a counter.
func (r readings) sum(name string, attrs ...string) float64 {
	total := 0.0
	for _, v := range r.Values(telemetry.InstrumentName(name), series(name, attrs)...) {
		total += v
	}
	return total
}

// isReleased reports whether the controller has bound the series of attrs of the instrument
// name, and has released every instrument of that series.
func (r readings) isReleased(name string, attrs ...string) bool {
	set := series(name, attrs)
	slices.SortFunc(set, func(a, b telemetry.Attr) int { return strings.Compare(a.Key, b.Key) })
	bound := slices.ContainsFunc(r.Series(telemetry.InstrumentName(name)), func(s []telemetry.Attr) bool {
		return slices.EqualFunc(
			s,
			set,
			func(a, b telemetry.Attr) bool { return a.Key == b.Key && a.Value.Str == b.Value.Str },
		)
	})
	return bound && r.Bound(telemetry.InstrumentName(name), set...) == 0
}

// setup is the configuration of a harness before New.
type setup struct {
	// spec is the spec whose strategies the controller applies.
	spec config.Spec
	// cfg is the Config of the controller, which newHarness completes.
	cfg control.Config
	// prior lists the mutations that a controller of priorTerm commits to the scope of the
	// cell before the controller of the test starts, in order.
	prior []*store.Mutation
	// fits is the bound of a commit, and nil for the bound of the store.
	fits func(m *store.Mutation) error
	// extend registers more strategies in the registry of the controller, or is nil.
	extend func(r *config.Registry) error
	// support is the support matrix of the registry of the controller.
	support config.Support
	// accepted is the accepted spec that the acquisition of a controller of spec proposals
	// commits.
	accepted []byte
	// rng is the range of the scope.
	rng shard.Range
}

// option changes the setup of a harness.
type option func(*setup)

// harness is the runtime of the tests. It performs the effects of one controller against a
// memory store, replies to the callbacks of the controller as the agents of the nodes do,
// runs its passes and keeps its replies. It stops the controller after an error, as its
// machine does.
type harness struct {
	// answer returns the answer of an agent to the request r, and nil for success. With a
	// nil answer, every agent replies with success.
	answer func(r *protocol.Request) error
	// pause reports whether run leaves the effect e pending. With a nil pause, run performs
	// every effect but a Plan while paused is set.
	pause func(e machine.Effect) bool
	// observe receives each effect that the controller appends, before route keeps it, or is
	// nil.
	observe func(e machine.Effect)
	// err is the first error that a Result of the controller reported.
	err error
	// st is the store of the controller.
	st *memory.Store
	// clock is the clock of the controller.
	clock *fake.Clock
	// controller is the controller under test.
	controller *control.Controller
	// spec contains the strategies of the spec of the setup.
	spec *control.Strategies
	// registry is the registry of the controller.
	registry *config.Registry
	// metrics reads every value that the instruments of the controller report.
	metrics readings
	// logs keeps every record that the controller logs.
	logs *telemetrytest.Records
	// pending lists the effects that run performs, the oldest first: the reads and the
	// commits of the store, the callbacks and the passes.
	pending []machine.Effect
	// timers lists the Timers that the controller armed, the oldest first.
	timers []machine.Timer
	// replies lists every reply of the controller, in order.
	replies []machine.Effect
	// sent lists the request of every callback of the controller, in order.
	sent []*protocol.Request
	// plans lists every Plan that the controller sent, in order.
	plans []machine.Plan
	// planned lists the plan of every pass that the harness performed, in order.
	planned []placement.Plan
	// tokens lists the token of every effect of the controller that has a token of its own,
	// in order.
	tokens []machine.Token
	// commits lists the mutation of every commit of the controller that applied, in order.
	commits []*store.Mutation
	// unwatched lists the tokens of every Unwatch of the controller, in order.
	unwatched []machine.Token
	// nodes lists the nodes of the last view that the harness passed.
	nodes []shard.Node
	// rng is the range of the scope.
	rng shard.Range
	// tokenCount is the counter of the tokens of the controller.
	tokenCount machine.Token
	// views is the token of the WatchViews that runs, and zero without one.
	views machine.Token
	// revision is the revision of the last view that the harness passed.
	revision epoch.Epoch
	// sequence is the Sequence of the last report that reportAll passed.
	sequence uint64
	// calls is the token of the last inbound call of the harness.
	calls machine.ReplyToken
	// acquired counts the Results that reported the end of the acquisition.
	acquired int
	// paused reports that run does not perform a Plan, so a pass remains in flight.
	paused bool
}

// newHarness returns a harness of a controller of the cell of the tests. The scope of the
// cell contains the prior mutations of the setup. Each option changes the setup before
// New.
func newHarness(tb testCase, opts ...option) *harness {
	tb.Helper()
	s := setup{
		spec: testSpec(firstVersion),
		cfg: control.Config{
			Rand:        constant.NewCounter(half),
			Provided:    provided,
			Kinds:       []proposal.OperationKind{proposal.Move, proposal.Drain, proposal.Undrain, proposal.Explain},
			Operators:   []shard.Identity{operator},
			Scope:       cellScope,
			Node:        shard.Node{ID: "controller-1", Address: controllerAddress, Identity: controllerIdentity},
			Term:        cellTerm,
			Parallelism: 1,
			Acquisition: func(store.Head, *store.Mutation) shard.NamespaceIncarnation { return incA },
		},
		support: builtin.Support(),
	}
	for _, opt := range opts {
		opt(&s)
	}
	h := &harness{
		st:      memory.New(fake.New(origin)),
		clock:   fake.New(origin),
		metrics: readings{telemetrytest.NewRecorder()},
		logs:    telemetrytest.NewRecords(slog.LevelDebug),
		rng:     s.rng,
	}
	tb.Cleanup(func() { assert.NoError(tb, h.st.Close(), "the store must close") })
	h.registry = config.NewRegistry(s.support)
	assert.NoError(tb, builtin.Register(h.registry), "the built-ins must register")
	if s.extend != nil {
		assert.NoError(tb, s.extend(h.registry), "the strategies of the test must register")
	}
	res, err := config.Resolve(h.registry, &s.spec, provided)
	assert.NoError(tb, err, "the spec of the tests must resolve")
	h.spec, err = control.Build(h.registry, res)
	assert.NoError(tb, err, "the strategies of the spec of the tests must build")
	for i, m := range s.prior {
		m.ID = store.CommitID{Incarnation: incA, Term: priorTerm, Seq: uint64(i) + 1}
		var head store.Head
		head, err = h.st.Head(assert.Context(tb), cellScope)
		assert.NoError(tb, err, "the store must return the header of the scope of the cell")
		_, err = h.st.Writer(cellScope, incA, priorTerm).Commit(assert.Context(tb), head.Generation, m)
		assert.NoError(tb, err, "the prior controller must commit")
	}
	s.cfg.Clock = h.clock
	s.cfg.Reporter = h.metrics
	s.cfg.Registry = h.registry
	s.cfg.Logger = slog.New(h.logs)
	s.cfg.Tokens = &h.tokenCount
	s.cfg.Fits = s.fits
	if s.fits == nil {
		s.cfg.Fits = func(m *store.Mutation) error { return h.st.Fits(cellScope, m) }
	}
	h.controller, err = control.New(&s.cfg)
	assert.NoError(tb, err, "New must return a controller of a complete config")
	return h
}

// model is the expected state of a controller that the property of a controller drives, and
// the record of what the controller did. Its harness keeps every callback pending until a
// step passes its reply, and passes each effect of the controller to observe.
type model struct {
	// c is the case of the property.
	c *prop.Case
	// h is the harness of the controller.
	h *harness
	// reserved maps each shard to the highest Reserved of its placement in the scope.
	reserved map[shard.ID]epoch.Epoch
	// owners maps each shard to the operation of each epoch that a transition of the shard
	// has reserved.
	owners map[shard.ID]map[epoch.Epoch]shard.OperationID
	// refusals maps each shard to the incarnation of each node that refused an add of the
	// shard.
	refusals map[shard.ID]map[shard.NodeID]epoch.Epoch
	// faulted maps each node to the shards that the last report of its live incarnation
	// lists with a permanent fault.
	faulted map[shard.NodeID]map[shard.ID]bool
	// replies counts the replies of the controller to each call.
	replies map[machine.ReplyToken]int
	// sequences maps each node to the Sequence of the last report of its live incarnation.
	sequences map[shard.NodeID]uint64
	// drained contains each node that a step drained by a proposal.
	drained map[shard.NodeID]bool
	// nodes lists the nodes of the property at their live incarnations.
	nodes []shard.Node
	// present reports for each node of nodes whether the view lists it.
	present []bool
	// disruptions lists the ID of each disruption that a step asked for.
	disruptions []budget.ID
	// disrupts lists the token of each Disrupt call of a step, in order.
	disrupts []machine.ReplyToken
	// ended is the index in disruptions of the first disruption whose reservation the drain
	// has not ended.
	ended int
	// proposals counts the proposals of the steps.
	proposals byte
	// outage is true while the store has an outage.
	outage bool
}

// newModel returns the model of a controller that serves with a view of the nodes A and B,
// whose first pass has placed every shard. Its spec bounds each node to pairedMax
// replicas.
func newModel(c *prop.Case) *model {
	m := &model{
		c:         c,
		h:         newHarness(c, bounded),
		reserved:  make(map[shard.ID]epoch.Epoch),
		owners:    make(map[shard.ID]map[epoch.Epoch]shard.OperationID),
		refusals:  make(map[shard.ID]map[shard.NodeID]epoch.Epoch),
		faulted:   make(map[shard.NodeID]map[shard.ID]bool),
		replies:   make(map[machine.ReplyToken]int),
		sequences: make(map[shard.NodeID]uint64),
		drained:   make(map[shard.NodeID]bool),
		nodes:     []shard.Node{nodeA, nodeB, nodeC, nodeD},
		present:   []bool{true, true, false, false},
	}
	m.h.observe = m.observe
	m.h.start(c)
	m.h.view(c, nodeA, nodeB)
	m.h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Callback); return ok }
	return m
}

// observe checks the effect e that the controller appends. The controller reserves the epoch
// of a callback in the placement of its shard before it sends the callback, and replies to
// each call once. The store cannot be read during an outage, so the check of the epoch waits
// for its end.
func (m *model) observe(e machine.Effect) {
	c := m.c
	switch e := e.(type) {
	case machine.Callback:
		if !m.outage {
			p := m.h.placements(c)[e.Request.Shard]
			expect.True(c, e.Request.Epoch <= p.Reserved,
				"the controller must reserve the epoch of a callback before it sends the callback")
		}
	case machine.Reply[protocol.Ack]:
		m.replied(e.To)
	case machine.Reply[protocol.Receipt]:
		m.replied(e.To)
	case machine.Reply[budget.Decision]:
		m.replied(e.To)
		if e.Value.Allowed {
			c.Classify(labelAdmitted)
		}
	case machine.Reply[struct{}]:
		m.replied(e.To)
	}
}

// replied counts a reply to the call to, and checks that it is the first.
func (m *model) replied(to machine.ReplyToken) {
	m.replies[to]++
	expect.Equal(m.c, m.replies[to], 1, "the controller must reply to a call once")
}

// view passes the view of the nodes that are present to the controller.
func (m *model) view(c *prop.Case) {
	nodes := make([]shard.Node, 0, len(m.nodes))
	for i, n := range m.nodes {
		if m.present[i] {
			nodes = append(nodes, n)
		}
	}
	m.h.view(c, nodes...)
}

// change passes a view in which each node is present or absent as c chooses.
func (m *model) change(c *prop.Case) {
	for i := range m.nodes {
		present := c.Draw(prop.Boolean(), "present")
		if m.present[i] && !present {
			c.Classify(labelDeparture)
		}
		m.present[i] = present
	}
	m.view(c)
}

// restart passes a view in which a node that c chooses has its next incarnation.
func (m *model) restart(c *prop.Case) {
	c.Classify(labelRestart)
	i := c.Draw(prop.Integer(0, len(m.nodes)-1), "node")
	m.nodes[i].Incarnation++
	m.present[i] = true
	m.sequences[m.nodes[i].ID] = 0
	delete(m.faulted, m.nodes[i].ID)
	m.view(c)
}

// report passes a report of a node of the view that c chooses. The report lists each replica
// of the node in the scope as serving, and c chooses which of them have a permanent fault.
// The model records the shards that the report lists with a fault.
func (m *model) report(c *prop.Case) {
	var live []shard.Node
	for i, n := range m.nodes {
		if m.present[i] {
			live = append(live, n)
		}
	}
	n := live[c.Draw(prop.Integer(0, len(live)-1), "node")]
	var replicas []shard.Observed
	faulted := make(map[shard.ID]bool)
	for _, sid := range m.h.shardIDs(c) {
		p := m.h.placements(c)[sid]
		if !slices.ContainsFunc(p.Replicas, func(r shard.Replica) bool { return r.Node == n.ID }) {
			continue
		}
		o := healthy(sid)
		if c.Draw(prop.Boolean(), "fault") {
			o = faulty(sid)
			faulted[sid] = true
		}
		o.Epoch = p.Epoch
		replicas = append(replicas, o)
	}
	m.faulted[n.ID] = faulted
	m.sequences[n.ID]++
	m.h.step(c, m.h.report(&n, m.sequences[n.ID], replicas...))
	m.h.run(c)
}

// callbacks returns the indexes of the callbacks that the harness keeps pending, in order.
func (m *model) callbacks() []int {
	var held []int
	for i, e := range m.h.pending {
		if _, ok := e.(machine.Callback); ok {
			held = append(held, i)
		}
	}
	return held
}

// answer passes a reply to a pending callback. c chooses the callback and the reply. The
// model records each add that the reply refuses.
func (m *model) answer(c *prop.Case) {
	held := m.callbacks()
	i := held[c.Draw(prop.Integer(0, len(held)-1), "callback")]
	err := c.Draw(prop.SampledFrom(answersOfAgents...), "answer")
	if errors.Is(err, errRefused) {
		c.Classify(labelDefinite)
		if r := m.h.pending[i].(machine.Callback).Request; r.Call == protocol.Add {
			if m.refusals[r.Shard] == nil {
				m.refusals[r.Shard] = make(map[shard.NodeID]epoch.Epoch)
			}
			m.refusals[r.Shard][r.Node] = r.NodeIncarnation
		}
	}
	m.finish(c, i, err)
}

// finish passes the answer err to the callback i of the harness.
func (m *model) finish(c *prop.Case, i int, err error) {
	e := m.h.pending[i].(machine.Callback)
	m.h.pending = slices.Delete(m.h.pending, i, i+1)
	m.h.step(c, machine.Called{Err: err, Token: e.Token})
	m.h.run(c)
}

// expire passes the end of the Timer i of the harness to the controller.
func (m *model) expire(c *prop.Case, i int) {
	tm := m.h.timers[i]
	m.h.timers = slices.Delete(m.h.timers, i, i+1)
	m.h.step(c, machine.Elapsed{Token: tm.Token})
	m.h.run(c)
}

// plan performs a Plan that the harness keeps pending while plans pause.
func (m *model) plan(c *prop.Case) {
	i := slices.IndexFunc(m.h.pending, func(e machine.Effect) bool { _, ok := e.(machine.Plan); return ok })
	m.h.pass(c, m.h.pending[i].(machine.Plan).Token)
}

// disrupt asks for the disruption of a node that c chooses, as an operator. Every second call
// has a deadline, so its deferral waits for the admission or the end of its reservation.
func (m *model) disrupt(c *prop.Case) {
	n := m.nodes[c.Draw(prop.Integer(0, len(m.nodes)-1), "node")]
	d := disruption(byte(len(m.disruptions)+1), n.ID)
	m.disruptions = append(m.disruptions, d.ID)
	call := m.h.disruptCall(&d, operator)
	if len(m.disruptions)%2 == 0 {
		call.Deadline = origin.Add(callFor)
	}
	m.disrupts = append(m.disrupts, call.Reply)
	m.h.step(c, call)
	m.h.run(c)
}

// end ends the reservation of a disruption that c chooses, as an operator.
func (m *model) end(c *prop.Case) {
	id := m.disruptions[c.Draw(prop.Integer(0, len(m.disruptions)-1), "disruption")]
	m.h.step(c, m.h.doneCall(id, operator))
	m.h.run(c)
}

// submit submits the proposal of the kind k with the base b and the arguments args, as an
// operator.
func (m *model) submit(c *prop.Case, k proposal.OperationKind, b epoch.Epoch, args any) {
	m.proposals++
	p := proposalOf(c, m.proposals, k, b, args)
	m.h.step(c, m.h.submitCall(&p, operator))
	m.h.run(c)
}

// drain drains or undrains a node that c chooses by a proposal.
func (m *model) drain(c *prop.Case) {
	n := m.nodes[c.Draw(prop.Integer(0, len(m.nodes)-1), "node")]
	k := c.Draw(prop.SampledFrom(proposal.Drain, proposal.Undrain), "kind")
	m.drained[n.ID] = k == proposal.Drain
	m.submit(c, k, n.Incarnation, proposal.NodeArgs{Node: n.ID})
}

// move proposes the move of a shard to a node. c chooses the shard and the node.
func (m *model) move(c *prop.Case) {
	ids := m.h.shardIDs(c)
	sid := ids[c.Draw(prop.Integer(0, len(ids)-1), "shard")]
	n := m.nodes[c.Draw(prop.Integer(0, len(m.nodes)-1), "node")]
	m.submit(c, proposal.Move, m.h.placements(c)[sid].Epoch, proposal.MoveArgs{Node: n.ID, Shard: sid})
}

// outages starts an outage of the store, or ends it.
func (m *model) outages(c *prop.Case) {
	if m.outage {
		m.h.st.EndOutage()
	} else {
		c.Classify(labelOutage)
		m.h.st.StartOutage()
	}
	m.outage = !m.outage
}

// complete ends every fault that remains. It ends the outage, passes a view of every node,
// resumes the passes, undrains each drained node and ends the reservation of each
// disruption. A node has a fault of a shard when its live incarnation refused an add of the
// shard or reported the shard with a permanent fault. complete restarts every node for a
// shard without a writer while every node has a fault of the shard or hosts pairedMax
// shards, because a pass does not place such a shard, and an emergency pass does not move a
// healthy replica to make room for it. It then passes success as the result of each pending
// callback and ends each Timer of a retry, until neither remains, so the retries use up every
// lost commit that a step armed. It ends each Timer of the interval of the passes last.
func (m *model) complete(c *prop.Case) {
	if m.outage {
		m.h.st.EndOutage()
		m.outage = false
	}
	restart := false
	occupied := m.occupancy(c)
	for sid, p := range m.h.placements(c) {
		blocked := !slices.ContainsFunc(m.nodes, func(n shard.Node) bool {
			return m.refusals[sid][n.ID] != n.Incarnation && !m.faulted[n.ID][sid] && len(occupied[n.ID]) < pairedMax
		})
		restart = restart || len(p.Replicas) == 0 && blocked
	}
	if restart {
		for i := range m.nodes {
			m.nodes[i].Incarnation++
			m.sequences[m.nodes[i].ID] = 0
			delete(m.faulted, m.nodes[i].ID)
		}
	}
	if restart || slices.Contains(m.present, false) {
		for i := range m.present {
			m.present[i] = true
		}
		m.view(c)
	}
	for _, id := range m.disruptions[m.ended:] {
		m.h.step(c, m.h.doneCall(id, operator))
	}
	m.ended = len(m.disruptions)
	for _, n := range m.nodes {
		if m.drained[n.ID] {
			m.drained[n.ID] = false
			m.submit(c, proposal.Undrain, n.Incarnation, proposal.NodeArgs{Node: n.ID})
		}
	}
	m.h.paused = false
	m.h.run(c)
	for range maxDeliveries {
		if held := m.callbacks(); len(held) > 0 {
			m.finish(c, held[0], nil)
			continue
		}
		i := slices.IndexFunc(m.h.timers, func(tm machine.Timer) bool { return tm.Duration != interval })
		if i < 0 {
			break
		}
		m.expire(c, i)
	}
	for i := range slices.Backward(m.h.timers) {
		m.expire(c, i)
	}
}

// incomplete reports whether the drain has more to do: a fault remains, a pass waits to run,
// a callback waits for its answer, a Timer other than the interval of the passes waits, a
// transition has not ended, or a shard does not have a writer. Such a Timer is the backoff
// of a retry, such as the retry of a commit whose reply was lost. Every backoff is below the
// interval.
func (m *model) incomplete() bool {
	if m.outage || slices.Contains(m.present, false) || m.ended < len(m.disruptions) || len(m.callbacks()) > 0 {
		return true
	}
	if slices.ContainsFunc(m.h.pending, func(e machine.Effect) bool { _, ok := e.(machine.Plan); return ok }) {
		return true
	}
	if slices.ContainsFunc(m.h.timers, func(tm machine.Timer) bool { return tm.Duration != interval }) {
		return true
	}
	for _, drained := range m.drained {
		if drained {
			return true
		}
	}
	if len(m.h.state(m.c).Transitions) > 0 {
		return true
	}
	for _, p := range m.h.placements(m.c) {
		if len(p.Replicas) == 0 {
			return true
		}
	}
	return false
}

// invariant checks the controller after every step:
//   - No step stops the controller with an error.
//   - A placement has one writer at most, at an epoch that it has reserved.
//   - The reserved epoch of a shard never falls.
//   - A transition does not reserve an epoch of another transition.
//   - A node of an admitted disruption hosts no replica.
//   - A node hosts pairedMax shards at most. A shard counts on the node of its writer while
//     the node has the incarnation of the placement, and on the destination and the
//     fallback of its transition.
//
// The store cannot be read during an outage, so the checks of the scope wait for its end.
func (m *model) invariant(c *prop.Case) {
	assert.Nil(c, m.h.err, "no step of the property may stop the controller with an error")
	if m.outage {
		return
	}
	placements := m.h.placements(c)
	for sid, p := range placements {
		expect.InRange(c, len(p.Replicas), 0, 1, "a placement must have one writer at most")
		expect.True(c, p.Epoch <= p.Reserved, "a placement must have reserved its epoch")
		expect.True(c, p.Reserved >= m.reserved[sid], "the reserved epoch of a shard must never fall")
		m.reserved[sid] = p.Reserved
	}
	for _, sids := range m.occupancy(c) {
		expect.InRange(c, len(sids), 0, pairedMax,
			"a node must host max-per-node shards at most, with the transitions that can end on it")
	}
	st := m.h.state(c)
	for _, t := range st.Transitions {
		owners := m.owners[t.Operation.Shard]
		if owners == nil {
			owners = make(map[epoch.Epoch]shard.OperationID)
			m.owners[t.Operation.Shard] = owners
		}
		for _, e := range []epoch.Epoch{t.NewEpoch, t.AbortEpoch} {
			if e.IsZero() {
				continue
			}
			if op, ok := owners[e]; ok {
				expect.Equal(c, op, t.Operation, "the controller must reserve each epoch of a shard for one transition")
			}
			owners[e] = t.Operation
		}
	}
	for _, r := range st.Reservations {
		if r.Admitted.IsZero() {
			continue
		}
		for _, n := range r.Disruption.Nodes {
			for _, p := range placements {
				expect.False(c, slices.ContainsFunc(p.Replicas, func(x shard.Replica) bool { return x.Node == n }),
					"a node of an admitted disruption must host no replica")
			}
		}
	}
}

// occupancy returns the shards that count on each node: the shard of each writer while its
// node has the incarnation of the placement, and the shard of each transition on its
// destination and on its fallback.
func (m *model) occupancy(c *prop.Case) map[shard.NodeID]map[shard.ID]bool {
	occupied := make(map[shard.NodeID]map[shard.ID]bool)
	occupy := func(n shard.NodeID, sid shard.ID) {
		if n == "" {
			return
		}
		if occupied[n] == nil {
			occupied[n] = make(map[shard.ID]bool)
		}
		occupied[n][sid] = true
	}
	for sid, p := range m.h.placements(c) {
		for _, r := range p.Replicas {
			live := slices.ContainsFunc(m.nodes, func(n shard.Node) bool {
				return n.ID == r.Node && n.Incarnation == p.Incarnation
			})
			if live {
				occupy(r.Node, sid)
			}
		}
	}
	for _, t := range m.h.state(c).Transitions {
		occupy(t.To.Node, t.Operation.Shard)
		occupy(t.Fallback.Node, t.Operation.Shard)
	}
	return occupied
}

// settle checks the controller once the drain has ended the faults: every transition has
// ended, the writer of every shard is on the live incarnation of a node of the view, and every
// Disrupt call has its reply.
func (m *model) settle(c *prop.Case) {
	expect.Empty(c, m.h.state(c).Transitions, "every transition must end once the faults have ended")
	m.h.live(c, m.nodes...)
	for _, to := range m.disrupts {
		expect.Equal(c, m.replies[to], 1, "every Disrupt call must have its reply once its reservation has ended")
	}
}

// TestController checks the construction, the lifecycle and the steps of a controller.
func TestController(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give func(*control.Config)
			name string
		}{
			{name: "returns ErrConfig for a config without a clock", give: func(c *control.Config) { c.Clock = nil }},
			{
				name: "returns ErrConfig for a config without a reporter",
				give: func(c *control.Config) { c.Reporter = nil },
			},
			{name: "returns ErrConfig for a config without a logger", give: func(c *control.Config) { c.Logger = nil }},
			{
				name: "returns ErrConfig for a config without a registry",
				give: func(c *control.Config) { c.Registry = nil },
			},
			{
				name: "returns ErrConfig for a config without a bound of a commit",
				give: func(c *control.Config) { c.Fits = nil },
			},
			{
				name: "returns ErrConfig for a config without an acquisition",
				give: func(c *control.Config) { c.Acquisition = nil },
			},
			{
				name: "returns ErrConfig for a config without the address of the controller",
				give: func(c *control.Config) { c.Node.Address = "" },
			},
			{
				name: "returns ErrConfig for a parallelism below one",
				give: func(c *control.Config) { c.Parallelism = 0 },
			},
			{
				name: "returns ErrConfig for a kind that is not valid",
				give: func(c *control.Config) { c.Kinds = []proposal.OperationKind{proposal.Drain, 0} },
			},
			{
				name: "returns ErrConfig for a first kind that is not valid",
				give: func(c *control.Config) { c.Kinds = []proposal.OperationKind{0} },
			},
			{
				name: "returns ErrConfig for a config without a source of randomness",
				give: func(c *control.Config) { c.Rand = nil },
			},
			{
				name: "returns ErrConfig for a config without a counter of tokens",
				give: func(c *control.Config) { c.Tokens = nil },
			},
			{
				name: "returns ErrConfig for a config without a namespace",
				give: func(c *control.Config) { c.Scope.Namespace = "" },
			},
			{
				name: "returns ErrConfig for a config without a term",
				give: func(c *control.Config) { c.Term = epoch.Zero },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				cfg := completeConfig(t)
				tt.give(&cfg)
				_, err := control.New(&cfg)
				assert.ErrorIs(t, err, control.ErrConfig, "New must refuse a config without a required field")
			})
		}

		t.Run("returns ErrConfig for a nil config", func(t *testing.T) {
			t.Parallel()
			_, err := control.New(nil)
			assert.ErrorIs(t, err, control.ErrConfig, "New must refuse a nil config")
		})

		t.Run("releases its instruments when it refuses a config", func(t *testing.T) {
			t.Parallel()
			r := readings{telemetrytest.NewRecorder()}
			cfg := completeConfig(t)
			cfg.Reporter, cfg.Term = r, epoch.Zero
			_, err := control.New(&cfg)
			assert.ErrorIs(t, err, control.ErrConfig, "New must refuse a config without a term")
			expect.True(t, r.isReleased(unplacedInstrument), "New must release the gauge of the unplaced shards")
			expect.True(t, r.isReleased(unknownInstrument),
				"New must release the counter of the commits with an unknown outcome")
		})
	})

	t.Run("Acquire", func(t *testing.T) {
		t.Parallel()

		t.Run("commits the record of the controller when it acquires its scope", func(t *testing.T) {
			t.Parallel()
			others := shard.Range{Start: []byte{0x40}}
			h := newHarness(t, func(s *setup) { s.rng = others })
			h.start(t)
			got := h.state(t).Cell
			expect.Equal(t, got.ID, cellID, "the record must name the cell")
			expect.Equal(t, got.Controller, controllerAddress, "the record must contain the address of the controller")
			expect.Equal(t, got.ControllerIdentity, controllerIdentity,
				"the record must contain the identity of the controller")
			expect.Equal(t, got.Range, others, "the record must contain the range of the scope")
			expect.Equal(t, got.Epoch, cellTerm, "the record must contain the term of the controller")
			expect.Equal(t, got.SpecVersion, firstVersion, "the record must contain the version of the spec")
		})

		t.Run("commits the mutation that the acquisition completes", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) {
				s.cfg.Acquisition = func(_ store.Head, m *store.Mutation) shard.NamespaceIncarnation {
					m.Spec = []byte(committedSpec)
					return incA
				}
			})
			h.start(t)
			assert.Equal(t, h.state(t).Spec, []byte(committedSpec),
				"the acquisition must commit the mutation that it completes")
		})

		t.Run("reports the end of the acquisition once", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			assert.Equal(t, h.acquired, 1, "the controller must report the end of its acquisition once")
		})

		t.Run("watches the views of the scope once it serves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.NotEqual(t, h.views, machine.Token(0), "the controller must watch the views of its membership")
		})

		t.Run("returns fsm.ErrRejected for a second call", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got, err := h.controller.Acquire(h.spec, h.rng, out)
			expect.That(t, err).ErrorIs(fsm.ErrRejected, "the lifecycle must refuse a second acquisition")
			expect.Equal(t, got, out, "a refused acquisition must return out unchanged")
		})
	})

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("returns out unchanged for a Timer that no part armed", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			out := []machine.Effect{marker}
			_, got, ok := h.controller.Step(machine.Elapsed{Token: 99}, out)
			expect.False(t, ok, "the controller must not take a Timer that no part armed")
			expect.Equal(t, got, out, "the controller must return out unchanged")
		})

		t.Run("returns out unchanged for the result of an effect that no part sent", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			out := []machine.Effect{marker}
			_, got, ok := h.controller.Step(machine.Called{Token: 99}, out)
			expect.False(t, ok, "the controller must not take the result of an effect that no part sent")
			expect.Equal(t, got, out, "the controller must return out unchanged")
		})

		t.Run("returns out unchanged for an event of another part of the machine", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			out := []machine.Effect{marker}
			_, got, ok := h.controller.Step(machine.Started{}, out)
			expect.False(t, ok, "the controller must not take an event of another part")
			expect.Equal(t, got, out, "the controller must return out unchanged")
		})

		t.Run("returns out unchanged for a Cancelled of a call that it does not have", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			out := []machine.Effect{marker}
			_, got, ok := h.controller.Step(machine.Cancelled{Reply: h.call()}, out)
			expect.False(t, ok, "the controller must not take the end of a call that it does not have")
			expect.Equal(t, got, out, "the controller must return out unchanged")
		})

		t.Run("fails at an error of its scope", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			ctx := assert.Context(t)
			head, err := h.st.Head(ctx, cellScope)
			assert.NoError(t, err, "the store must return the header of the scope of the cell")
			later := cellTerm + 1
			fence := &store.Mutation{ID: store.CommitID{Incarnation: incA, Term: later, Seq: 1}}
			_, err = h.st.Writer(cellScope, incA, later).Commit(ctx, head.Generation, fence)
			assert.NoError(t, err, "a controller of a later term must commit")
			h.start(t)
			assert.ErrorIs(t, h.err, epoch.ErrFenced, "the controller must fail when a later term fences its scope")
		})

		t.Run("fails at an error of a commit", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.fits = func(*store.Mutation) error { return store.ErrTooLarge } })
			h.start(t)
			h.view(t, nodeA, nodeB)
			assert.ErrorIs(t, h.err, store.ErrTooLarge, "the controller must fail when a commit does not fit")
		})

		t.Run("fails at an error of a commit that follows a batch", func(t *testing.T) {
			t.Parallel()
			h := served(t, refuseRelease)
			h.reportAll(t)
			h.paused = true
			forced := disruption(1, nodeA.ID)
			forced.Negotiable = false
			h.disrupt(t, forced, operator)
			h.elapse(t, startAfter)
			h.view(t, nodeB)
			assert.Nil(t, h.err, "the controller must run while the node of the reservation hosts replicas")
			h.paused = false
			h.pass(t, h.plans[len(h.plans)-1].Token)
			assert.ErrorIs(t, h.err, store.ErrTooLarge,
				"the controller must fail when the release of the reservation after the last move does not fit")
		})

		t.Run("drops the effects of an event that fails", func(t *testing.T) {
			t.Parallel()
			full := false
			h := served(t, func(s *setup) {
				s.fits = func(*store.Mutation) error {
					if full {
						return store.ErrTooLarge
					}
					return nil
				}
			})
			full = true
			d := disruption(1, nodeA.ID)
			out := []machine.Effect{marker}
			res, got, ok := h.controller.Step(h.disruptCall(&d, operator), out)
			assert.True(t, ok, "the controller must take the disruption")
			expect.ErrorIs(t, res.Err, store.ErrTooLarge, "the Result must report the error of the commit")
			expect.Equal(t, got, out, "the controller must drop the effects of the event")
		})

		t.Run("takes no event after an error", func(t *testing.T) {
			t.Parallel()
			full := false
			h := served(t, func(s *setup) {
				s.fits = func(*store.Mutation) error {
					if full {
						return store.ErrTooLarge
					}
					return nil
				}
			})
			full = true
			d := disruption(1, nodeA.ID)
			res, _, _ := h.controller.Step(h.disruptCall(&d, operator), nil)
			assert.ErrorIs(t, res.Err, store.ErrTooLarge, "the commit of the disruption must fail the controller")
			out := []machine.Effect{marker}
			_, got, ok := h.controller.Step(h.report(&nodeA, 1), out)
			expect.False(t, ok, "a failed controller must not take an event")
			expect.Equal(t, got, out, "a failed controller must return out unchanged")
		})

		t.Run("drops the reply to a proposal that the runtime cancelled before its commit applied", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			call := h.submitCall(&p, operator)
			h.step(t, call)
			h.step(t, machine.Cancelled{Reply: call.Reply})
			before := len(h.replies)
			h.run(t)
			assert.Length(t, h.replies, before, "the controller must not reply to a call that the runtime cancelled")
		})

		t.Run("drops the reply to an explanation that the runtime cancelled during its pass", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.paused = true
			p := proposalOf(t, 1, proposal.Explain, h.state(t).Head.Generation,
				proposal.ShardArgs{Shard: h.shardOn(t, nodeA.ID)})
			call := h.submitCall(&p, operator)
			h.step(t, call)
			h.step(t, machine.Cancelled{Reply: call.Reply})
			before := len(h.replies)
			h.paused = false
			h.pass(t, h.plans[len(h.plans)-1].Token)
			assert.Length(t, h.replies, before, "the controller must not reply to a call that the runtime cancelled")
		})

		t.Run("gives each effect a token of its own", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			h.elapse(t, interval)
			assert.NoDuplicates(t, func() ([]machine.Token, error) { return h.tokens, nil },
				"each effect must have a token that no other effect has")
		})

		t.Run("releases the mutation of a commit once the commit has applied", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			before := len(h.commits)
			h.submit(t, &p, operator)
			assert.NotEmpty(t, h.commits[before:], "the drain must commit")
			w := weak.Make(h.commits[before])
			h.commits = nil
			runtime.GC()
			assert.Nil(t, w.Value(), "the controller must not keep the mutation of a commit that has applied")
		})

		t.Run("releases a pass once its result has arrived", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			w := func() weak.Pointer[proposal.Proposal] {
				p := new(proposal.Proposal)
				*p = proposalOf(t, 1, proposal.Explain, h.state(t).Head.Generation,
					proposal.ShardArgs{Shard: h.shardOn(t, nodeA.ID)})
				h.step(t, h.submitCall(p, operator))
				h.run(t)
				return weak.Make(p)
			}()
			runtime.GC()
			assert.Nil(t, w.Value(), "the controller must not keep the proposal of a pass whose result has arrived")
		})

		t.Run("keeps the safety rules of the core under every sequence of events", func(t *testing.T) {
			t.Parallel()
			coverage := []prop.Option{
				prop.Require(labelDefinite, common),
				prop.Require(labelLostReply, common),
				prop.Require(labelOutage, common),
				prop.Require(labelRestart, common),
				prop.Require(labelDeparture, common),
				prop.Require(labelAdmitted, rare),
			}
			prop.ForAll(t, "a controller keeps the safety rules of the core under every sequence of events", exercise,
				coverage...)
		})
	})

	t.Run("Accept", func(t *testing.T) {
		t.Parallel()

		t.Run("adopts a newer spec at the next pass", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.accept(t, firstVersion+1, nil)
			assert.Length(t, h.plans, 2, "the newer spec must start a pass")
			assert.Equal(t, h.state(t).Cell.SpecVersion, firstVersion+1,
				"the first commit of the pass must record the version of the newer spec")
		})

		t.Run("bounds the transitions by the settings of a newer spec", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.accept(t, firstVersion+1, func(s *config.Spec) { s.Budget.MaxTransitions = new(1) })
			passes := len(h.plans)
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			assert.Length(t, h.plans, passes+2, "the bound of the newer spec must refuse the second move of the drain")
		})

		t.Run("ignores a spec whose version is not above its own", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.accept(t, firstVersion, nil)
			assert.Length(t, h.plans, 1, "a spec of the same version must not start a pass")
		})

		t.Run("adopts the latest of two newer specs", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.accept(t, firstVersion+1, nil)
			h.accept(t, firstVersion+2, nil)
			h.view(t, nodeA, nodeB)
			assert.Equal(t, h.state(t).Cell.SpecVersion, firstVersion+2, "the pass must adopt the latest spec")
		})

		t.Run("adopts a newer spec that arrives during the acquisition", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			out, err := h.controller.Acquire(h.spec, h.rng, nil)
			assert.NoError(t, err, "Acquire must start the acquisition of a new controller")
			h.route(out)
			res, got := h.controller.Accept(h.newer(t, firstVersion+1, nil), nil)
			assert.NoError(t, res.Err, "Accept must take a newer spec during the acquisition")
			h.route(got)
			h.run(t)
			h.view(t, nodeA, nodeB)
			assert.Equal(t, h.state(t).Cell.SpecVersion, firstVersion+1, "the first pass must adopt the newer spec")
		})

		t.Run("ignores a newer spec below the spec that waits", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.accept(t, firstVersion+2, nil)
			h.accept(t, firstVersion+1, nil)
			h.view(t, nodeA, nodeB)
			assert.Equal(t, h.state(t).Cell.SpecVersion, firstVersion+2, "the pass must adopt the spec that waits")
		})

		t.Run("returns out unchanged before Acquire", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			out := []machine.Effect{marker}
			res, got := h.controller.Accept(h.newer(t, 2, nil), out)
			expect.Equal(t, res, control.Result{}, "Accept before Acquire must report nothing")
			expect.Equal(t, got, out, "Accept before Acquire must return out unchanged")
		})

		t.Run("returns out unchanged after it has stopped", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.stop(t)
			out := []machine.Effect{marker}
			_, got := h.controller.Accept(h.newer(t, 2, nil), out)
			assert.Equal(t, got, out, "Accept after Stop must return out unchanged")
		})

		t.Run("fails at an error of the pass of a newer spec", func(t *testing.T) {
			t.Parallel()
			full := false
			h := served(t, func(s *setup) {
				s.fits = func(*store.Mutation) error {
					if full {
						return store.ErrTooLarge
					}
					return nil
				}
			})
			full = true
			out := []machine.Effect{marker}
			res, got := h.controller.Accept(h.newer(t, 2, nil), out)
			expect.ErrorIs(t, res.Err, store.ErrTooLarge, "the Result must report the error of the commit of the pass")
			expect.Equal(t, got, out, "a failed Accept must drop its effects")
		})
	})

	t.Run("Size", func(t *testing.T) {
		t.Parallel()

		t.Run("returns no shards before the acquisition", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			nodes, n := h.controller.Size()
			expect.Equal(t, nodes, 0, "the controller must have no nodes before its first view")
			expect.Equal(t, n, 0, "the controller must have no shards before its acquisition")
		})

		t.Run("returns the nodes of the last view and the shards of the layout", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.view(t, nodeA, nodeB, nodeC)
			nodes, n := h.controller.Size()
			expect.Equal(t, nodes, 3, "the controller must count the nodes of the last view")
			expect.Equal(t, n, shards, "the controller must count the shards of its layout")
		})
	})

	t.Run("Stop", func(t *testing.T) {
		t.Parallel()

		t.Run("appends no Unwatch without a watch of its views", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			got, err := h.controller.Stop([]machine.Effect{marker})
			assert.NoError(t, err, "Stop must stop a controller before its acquisition")
			assert.Equal(t, got, []machine.Effect{marker}, "Stop must append nothing without a watch and a call")
		})

		t.Run("ends the watch of its views", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			watch := h.views
			h.stop(t)
			assert.Contains(t, h.unwatched, watch, "Stop must end the watch of the views")
		})

		t.Run("replies with ErrNotServing to a call that waits", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			p := proposalOf(t, 1, proposal.Drain, nodeA.Incarnation, proposal.NodeArgs{Node: nodeA.ID})
			call := h.submitCall(&p, operator)
			h.step(t, call)
			h.stop(t)
			assert.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrNotServing,
				"a proposal that waits for its commit must get ErrNotServing")
		})

		t.Run("takes no event after it has stopped", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.stop(t)
			out := []machine.Effect{marker}
			_, got, ok := h.controller.Step(h.report(&nodeA, 1), out)
			expect.False(t, ok, "a stopped controller must not take an event")
			expect.Equal(t, got, out, "a stopped controller must return out unchanged")
		})

		t.Run("returns fsm.ErrRejected for a second call", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.stop(t)
			out := []machine.Effect{marker}
			got, err := h.controller.Stop(out)
			expect.That(t, err).ErrorIs(fsm.ErrRejected, "the lifecycle must refuse a second stop")
			expect.Equal(t, got, out, "a refused stop must return out unchanged")
		})
	})

	t.Run("Stopped", func(t *testing.T) {
		t.Parallel()

		t.Run("appends no Unwatch", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			got, err := h.controller.Stopped([]machine.Effect{marker})
			assert.NoError(t, err, "Stopped must stop a controller that serves")
			assert.Equal(t, got, []machine.Effect{marker}, "Stopped must append nothing without a call that waits")
		})

		t.Run("replies with ErrNotServing to a call that waits", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			d := disruption(1, nodeA.ID)
			call := h.disruptCall(&d, operator)
			h.step(t, call)
			got, err := h.controller.Stopped(nil)
			assert.NoError(t, err, "Stopped must stop a controller that serves")
			h.route(got)
			assert.ErrorIs(t, replyTo[budget.Decision](t, h, call.Reply).Err, control.ErrNotServing,
				"a disruption that waits for its commit must get ErrNotServing")
		})

		t.Run("returns fsm.ErrRejected after Stop", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.stop(t)
			out := []machine.Effect{marker}
			got, err := h.controller.Stopped(out)
			expect.That(t, err).ErrorIs(fsm.ErrRejected, "the lifecycle must refuse a stop after Stop")
			expect.Equal(t, got, out, "a refused stop must return out unchanged")
		})
	})
}

// committedSpec is a spec that the acquisition of a test commits.
const committedSpec = `{"version":1}`

// The names of the instruments that the tests read.
const (
	transitionsInstrument  = "sharder.transitions.in_flight"
	unplacedInstrument     = "sharder.shards.unplaced"
	passInstrument         = "sharder.planner.pass.duration"
	movesInstrument        = "sharder.moves"
	deferredInstrument     = "sharder.budget.deferred"
	reservationsInstrument = "sharder.budget.reservations"
	unknownInstrument      = "sharder.commits.unknown"
	gapInstrument          = "sharder.writer.gap"
)

// The keys of the attributes that a controller binds to an instrument after the namespace and
// the cell.
const (
	keyProtocol = "protocol"
	keyPhase    = "phase"
	keyMode     = "mode"
	keyReason   = "reason"
	keyState    = "state"
)

// The instruments of a controller by the kind of their values. Each other instrument is a
// counter.
var (
	// gauges lists the instruments that are gauges.
	gauges = []string{transitionsInstrument, unplacedInstrument, reservationsInstrument}
	// histograms lists the instruments that are histograms.
	histograms = []string{passInstrument, gapInstrument}
)

// keys maps each instrument that binds attributes after the namespace and the cell to the
// keys of those attributes, in their order.
var keys = map[string][]string{
	transitionsInstrument:  {keyProtocol, keyPhase},
	passInstrument:         {keyMode},
	movesInstrument:        {keyReason},
	reservationsInstrument: {keyState},
	gapInstrument:          {keyReason},
}

// series returns the attribute set of the series of attrs of the instrument name: the
// namespace and the cell of the tests, and each value of attrs under its key in keys.
func series(name string, attrs []string) []telemetry.Attr {
	set := make([]telemetry.Attr, 0, 2+len(attrs))
	set = append(set,
		telemetry.AttrString(attrNamespaceKey, string(namespace)),
		telemetry.AttrString(attrCellKey, string(cellID)),
	)
	for i, v := range attrs {
		set = append(set, telemetry.AttrString(keys[name][i], v))
	}
	return set
}

// registry returns a registry with every built-in strategy of the support matrix.
func registry(tb assert.TB) *config.Registry {
	tb.Helper()
	r := config.NewRegistry(builtin.Support())
	assert.NoError(tb, builtin.Register(r), "the built-ins must register")
	return r
}

// completeConfig returns a Config that New accepts.
func completeConfig(tb assert.TB) control.Config {
	tb.Helper()
	var tokens machine.Token
	return control.Config{
		Clock:       fake.New(origin),
		Rand:        constant.NewCounter(half),
		Reporter:    noop.New(),
		Logger:      slog.New(slog.DiscardHandler),
		Registry:    registry(tb),
		Fits:        func(*store.Mutation) error { return nil },
		Acquisition: func(store.Head, *store.Mutation) shard.NamespaceIncarnation { return incA },
		Tokens:      &tokens,
		Scope:       cellScope,
		Node:        shard.Node{Address: controllerAddress},
		Term:        cellTerm,
		Parallelism: 1,
	}
}

// refuseRelease sets the bound of a commit to refuse each mutation that ends a
// reservation, as a store refuses a mutation that does not fit in one commit.
func refuseRelease(s *setup) {
	s.fits = func(m *store.Mutation) error {
		if len(m.Release) > 0 {
			return store.ErrTooLarge
		}
		return nil
	}
}

// call returns the token of the next inbound call.
func (h *harness) call() machine.ReplyToken {
	h.calls++
	return h.calls
}

// start starts the acquisition of the controller with the strategies of the setup, and
// runs the controller until it serves.
func (h *harness) start(tb assert.TB) {
	tb.Helper()
	out, err := h.controller.Acquire(h.spec, h.rng, []machine.Effect{marker})
	assert.NoError(tb, err, "Acquire must start the acquisition of a new controller")
	assert.NotEmpty(tb, out, "Acquire must keep the effects that out has")
	assert.Equal(tb, out[0], marker, "Acquire must keep the effects that out has")
	h.route(out[1:])
	h.run(tb)
}

// stop stops the controller with Stop, and keeps the effects of the stop.
func (h *harness) stop(tb assert.TB) {
	tb.Helper()
	out, err := h.controller.Stop([]machine.Effect{marker})
	assert.NoError(tb, err, "Stop must stop a controller that runs")
	h.route(out[1:])
}

// step passes ev to the controller with the marker in out, checks that the controller keeps
// out, and keeps the effects that the controller appends. It keeps the first error that a
// Result reports, and then stops the controller, as its machine does.
func (h *harness) step(tb assert.TB, ev machine.Event) {
	tb.Helper()
	res, got, _ := h.controller.Step(ev, []machine.Effect{marker})
	assert.NotEmpty(tb, got, "the controller must keep the effects that out has")
	assert.Equal(tb, got[0], marker, "the controller must keep the effects that out has")
	h.route(got[1:])
	if res.Acquired {
		h.acquired++
	}
	if res.Err != nil && h.err == nil {
		h.err = res.Err
		h.stop(tb)
	}
}

// route passes each effect of effects to observe, and keeps it in the list of its kind. It
// also keeps the token of each effect that has a token of its own.
func (h *harness) route(effects []machine.Effect) {
	for _, e := range effects {
		if h.observe != nil {
			h.observe(e)
		}
		switch e := e.(type) {
		case machine.Unwatch:
			h.unwatched = append(h.unwatched, e.Token)
			if h.views == e.Token {
				h.views = 0
			}
		case machine.WatchViews:
			h.tokens = append(h.tokens, e.Token)
			h.views = e.Token
		case machine.Timer:
			h.tokens = append(h.tokens, e.Token)
			h.timers = append(h.timers, e)
		case machine.Reply[struct{}], machine.Reply[protocol.Ack], machine.Reply[protocol.Receipt],
			machine.Reply[budget.Decision]:
			h.replies = append(h.replies, e)
		case machine.Callback:
			h.tokens = append(h.tokens, e.Token)
			h.sent = append(h.sent, e.Request)
			h.pending = append(h.pending, e)
		case machine.Plan:
			h.tokens = append(h.tokens, e.Token)
			h.plans = append(h.plans, e)
			h.pending = append(h.pending, e)
		case machine.Head:
			h.tokens = append(h.tokens, e.Token)
			h.pending = append(h.pending, e)
		case machine.Read:
			h.tokens = append(h.tokens, e.Token)
			h.pending = append(h.pending, e)
		case machine.Commit:
			h.tokens = append(h.tokens, e.Token)
			h.pending = append(h.pending, e)
		default:
			h.pending = append(h.pending, e)
		}
	}
}

// run performs the pending effects, the oldest first, and passes their results to the
// controller until no effect is pending. It leaves a Plan pending while paused is set, and
// each effect that pause reports.
func (h *harness) run(tb assert.TB) {
	tb.Helper()
	for range maxDeliveries {
		i := slices.IndexFunc(h.pending, func(e machine.Effect) bool {
			_, isPlan := e.(machine.Plan)
			return (!h.paused || !isPlan) && (h.pause == nil || !h.pause(e))
		})
		if i < 0 {
			return
		}
		e := h.pending[i]
		h.pending = slices.Delete(h.pending, i, i+1)
		h.step(tb, h.perform(tb, e))
	}
	tb.Fatalf("the controller still runs after %d deliveries", maxDeliveries)
}

// pass performs the pending Plan of the token tok, and runs the controller afterwards.
func (h *harness) pass(tb assert.TB, tok machine.Token) {
	tb.Helper()
	i := slices.IndexFunc(h.pending, func(e machine.Effect) bool {
		p, isPlan := e.(machine.Plan)
		return isPlan && p.Token == tok
	})
	assert.True(tb, i >= 0, "the Plan must be pending")
	e := h.pending[i]
	h.pending = slices.Delete(h.pending, i, i+1)
	h.step(tb, h.perform(tb, e))
	h.run(tb)
}

// perform performs e and returns the event of its result.
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
		if err == nil {
			h.commits = append(h.commits, e.Mutation)
		}
		return machine.Committed{Err: err, Token: e.Token, Generation: gen}
	case machine.Outcome:
		receipt, found, err := h.st.Outcome(ctx, e.Scope, e.ID)
		return machine.OutcomeRead{Err: err, Token: e.Token, Receipt: receipt, Found: found}
	case machine.Callback:
		var err error
		if h.answer != nil {
			err = h.answer(e.Request)
		}
		return machine.Called{Err: err, Token: e.Token}
	case machine.Plan:
		plan, err := e.Placer.Place(ctx, e.Input)
		var violations []placement.Violation
		if err == nil {
			violations, err = placement.Validate(e.Input, plan, e.Constraints)
		}
		h.planned = append(h.planned, plan)
		return machine.Planned{Err: err, Violations: violations, Plan: plan, Token: e.Token}
	default:
		tb.Fatalf("the controller returned a %T, which the harness does not perform", e)
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

// newer returns the strategies of the spec of the tests at the version v, after change
// changes it.
func (h *harness) newer(tb assert.TB, v epoch.Epoch, change func(*config.Spec)) *control.Strategies {
	tb.Helper()
	spec := testSpec(v)
	if change != nil {
		change(&spec)
	}
	res, err := config.Resolve(h.registry, &spec, provided)
	assert.NoError(tb, err, "the spec must resolve")
	s, err := control.Build(h.registry, res)
	assert.NoError(tb, err, "the strategies of the spec must build")
	return s
}

// accept passes the strategies of the spec of the tests at the version v, after change
// changes it, to Accept, and runs the controller afterwards.
func (h *harness) accept(tb assert.TB, v epoch.Epoch, change func(*config.Spec)) {
	tb.Helper()
	res, got := h.controller.Accept(h.newer(tb, v, change), []machine.Effect{marker})
	assert.NoError(tb, res.Err, "Accept must take a newer spec")
	assert.NotEmpty(tb, got, "Accept must keep the effects that out has")
	assert.Equal(tb, got[0], marker, "Accept must keep the effects that out has")
	h.route(got[1:])
	h.run(tb)
}

// view passes the view of nodes to the controller, and runs the controller afterwards.
func (h *harness) view(tb assert.TB, nodes ...shard.Node) {
	tb.Helper()
	assert.NotEqual(tb, h.views, machine.Token(0), "the controller must watch the views")
	h.revision++
	h.nodes = slices.Clone(nodes)
	v := membership.View{Scope: cellScope, Nodes: slices.Clone(nodes), Revision: h.revision, Incarnation: incA}
	h.step(tb, machine.Viewed{View: v, Token: h.views})
	h.run(tb)
}

// viewFails passes an item with the error err to the watch of the views, as a runtime does
// when the call of a watch fails. It then runs the controller.
func (h *harness) viewFails(tb assert.TB, err error) {
	tb.Helper()
	tok := h.views
	h.views = 0
	h.step(tb, machine.Viewed{Err: err, Token: tok})
	h.run(tb)
}

// elapse performs the oldest Timer whose Duration is d, and runs the controller afterwards.
func (h *harness) elapse(tb assert.TB, d time.Duration) {
	tb.Helper()
	i := slices.IndexFunc(h.timers, func(tm machine.Timer) bool { return tm.Duration == d })
	assert.True(tb, i >= 0, fmt.Sprintf("a Timer of %v must be armed", d))
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

// placements returns the placements of the scope of the cell by shard.
func (h *harness) placements(tb assert.TB) map[shard.ID]shard.Placement {
	tb.Helper()
	ctx := assert.Context(tb)
	_, cursor, err := h.st.Read(ctx, cellScope, incA, store.Query{}, page.Page{})
	assert.NoError(tb, err, "the store must read the scope of the cell")
	defer func() { assert.NoError(tb, cursor.Close(), "the cursor must close") }()
	got := make(map[shard.ID]shard.Placement)
	for p, err := range cursor.Seq(ctx) {
		assert.NoError(tb, err, "the cursor of the memory store must yield its placements")
		got[p.Shard] = p
	}
	return got
}

// writers counts the shards of the scope of the cell whose writer is on each node.
func (h *harness) writers(tb assert.TB) map[shard.NodeID]int {
	tb.Helper()
	counts := make(map[shard.NodeID]int)
	for _, p := range h.placements(tb) {
		for _, r := range p.Replicas {
			counts[r.Node]++
		}
	}
	return counts
}

// last returns the last reply of the controller, which must have the type R.
func last[R machine.Effect](tb assert.TB, h *harness) R {
	tb.Helper()
	assert.NotEmpty(tb, h.replies, "the controller must have replied")
	r, ok := h.replies[len(h.replies)-1].(R)
	assert.True(tb, ok, "the last reply must have the type that the test expects")
	return r
}

// replyTo returns the reply of the controller to the call to, which must have the type
// machine.Reply[T].
func replyTo[T machine.Result](tb assert.TB, h *harness, to machine.ReplyToken) machine.Reply[T] {
	tb.Helper()
	for _, e := range h.replies {
		if r, ok := e.(machine.Reply[T]); ok && r.To == to {
			return r
		}
	}
	tb.Fatalf("the controller has not replied to the call %d", to)
	return machine.Reply[T]{}
}

// shardIDs returns the IDs of the shards of the scope of the cell, in the order of the IDs.
func (h *harness) shardIDs(tb assert.TB) []shard.ID {
	tb.Helper()
	return slices.SortedFunc(maps.Keys(h.placements(tb)), shard.ID.Compare)
}

// exercise runs the steps of the case c over a controller that serves, as the agents, the
// membership, the operators and the store drive it. The steps change the view, restart
// nodes, report replicas, reply to the pending callbacks, end Timers, pause the passes and
// run them, lose commits and their replies, start and end outages of the store, ask for
// disruptions and end them, drain and undrain nodes, and propose moves. The model checks
// the safety rules of the core after every step:
//   - No step stops the controller with an error.
//   - The controller reserves the epoch of a callback before it sends the callback, and
//     reserves each epoch of a shard for one transition. The reserved epoch never falls.
//   - A placement has one writer at most, at an epoch that it has reserved.
//   - A node of an admitted disruption hosts no replica.
//   - A node hosts pairedMax shards at most, with the transitions that can end on it.
//   - The controller replies to each call once.
//
// The drain ends the faults, the drains and the disruptions, and replies to every callback
// with success. Every transition has then ended, the writer of every shard is on the live
// incarnation of a node of the view, and every Disrupt call has its reply.
func exercise(c *prop.Case) {
	m := newModel(c)
	stateful.Steps(c, stateful.Machine[struct{}]{
		Actions: []stateful.Action[struct{}]{
			{Name: "change the view", Run: func(c *prop.Case, _ int, _ any) { m.change(c) }},
			{Name: "restart a node", Run: func(c *prop.Case, _ int, _ any) { m.restart(c) }},
			{
				Name: "report", Weight: reports,
				Enabled: func(struct{}) bool { return !m.outage && slices.Contains(m.present, true) },
				Run:     func(c *prop.Case, _ int, _ any) { m.report(c) },
			},
			{
				Name: "answer", Weight: answers, Enabled: func(struct{}) bool { return len(m.callbacks()) > 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.answer(c) },
			},
			{
				Name: "elapse", Enabled: func(struct{}) bool { return len(m.h.timers) > 0 },
				Run: func(c *prop.Case, _ int, _ any) {
					m.expire(c, c.Draw(prop.Integer(0, len(m.h.timers)-1), "timer"))
				},
			},
			{Name: "pause the passes", Run: func(*prop.Case, int, any) { m.h.paused = !m.h.paused }},
			{
				Name: "plan",
				Enabled: func(struct{}) bool {
					return slices.ContainsFunc(m.h.pending, func(e machine.Effect) bool {
						_, ok := e.(machine.Plan)
						return ok
					})
				},
				Run: func(c *prop.Case, _ int, _ any) { m.plan(c) },
			},
			{Name: "lose a commit", Run: func(*prop.Case, int, any) { m.h.st.FailAt(memory.PointCommitApply) }},
			{
				Name: "lose the reply of a commit",
				Run: func(c *prop.Case, _ int, _ any) {
					c.Classify(labelLostReply)
					m.h.st.FailAt(memory.PointCommitAck)
				},
			},
			{Name: "start or end an outage", Run: func(c *prop.Case, _ int, _ any) { m.outages(c) }},
			{Name: "disrupt", Run: func(c *prop.Case, _ int, _ any) { m.disrupt(c) }},
			{
				Name: "end a disruption", Enabled: func(struct{}) bool { return len(m.disruptions) > 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.end(c) },
			},
			{Name: "drain", Run: func(c *prop.Case, _ int, _ any) { m.drain(c) }},
			{
				Name: "move", Enabled: func(struct{}) bool { return !m.outage },
				Run: func(c *prop.Case, _ int, _ any) { m.move(c) },
			},
			{
				Name: "complete", Drain: true, Enabled: func(struct{}) bool { return m.incomplete() },
				Run: func(c *prop.Case, _ int, _ any) { m.complete(c) },
			},
		},
		Invariant: func(c *prop.Case, _ struct{}) { m.invariant(c) },
		Settle:    func(c *prop.Case, _ struct{}) { m.settle(c) },
	})
}

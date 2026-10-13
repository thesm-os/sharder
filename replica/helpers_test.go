// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/rand/seeded"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The names and the identities of the tests.
const (
	// namespace is the namespace of the tests.
	namespace shard.Namespace = "orders"
	// slotsConfig is the configuration of fixed-slots in the spec of the tests.
	slotsConfig = `{"slots":4}`
	// controllerAddress is the address of the controller of the cell of the tests.
	controllerAddress = "controller-1:7000"
	// controllerIdentity is the identity of the controller of the cell of the tests.
	controllerIdentity shard.Identity = "controller-1"
	// otherNode is a node of the cell other than the node of the tests.
	otherNode shard.NodeID = "node-b"
)

// waitFor bounds each wait of a test. Inside a bubble of testing/synctest, the wait runs on
// the virtual time of the bubble.
const waitFor = time.Hour

// The epochs of the tests.
const (
	// rootTerm is the term of the root that writes the root scope.
	rootTerm epoch.Epoch = 1
	// cellTerm is the term of the controller of the cell of the tests.
	cellTerm epoch.Epoch = 3
	// firstVersion is the version of the spec of the tests.
	firstVersion epoch.Epoch = 1
)

// leaseTTL is the TTL of the lease of a session that a test joins, the default of the
// fenced exclusive ownership of the spec of the tests.
const leaseTTL = 10 * time.Second

// grace is the time between a Close and its deadline.
const grace = 30 * time.Second

// The node, the cell, the scopes and the incarnation of the tests.
var (
	// node is the node of the agent of the tests.
	node = shard.Node{ID: "node-a", Address: "node-a:7000", Identity: "agent-a"}
	// cellKey is the shard of the cell of the tests in the root scope.
	cellKey = shard.ID{ID128: id.ID128{0: 0x01, 15: 0xce}}
	// cellID is the cell of the tests: the text of the ULID of cellKey.
	cellID = shard.CellID(ulid.Format(cellKey.ID()))
	// rootScope is the root scope of the namespace of the tests.
	rootScope = shard.Scope{Namespace: namespace}
	// cellScope is the scope of the cell of the tests.
	cellScope = shard.Scope{Namespace: namespace, Cell: cellID}
	// incA is the namespace incarnation of the tests.
	incA = shard.NamespaceIncarnation{ID: id.New128([id.Size128]byte{0: 0x1a, 15: 0x0a})}
	// controllerMetadata is the metadata of the callbacks of the controller of the tests.
	controllerMetadata = protocol.Metadata{Caller: controllerIdentity, Version: protocol.Current}
	// lease is the lease of a session that a test joins.
	lease = ownership.Lease{
		TTL:          shard.Duration(leaseTTL),
		Renew:        shard.Duration(leaseTTL / 3),
		MaxDriftRate: 0.01,
		OnLoss:       ownership.ContinueFenced,
	}
)

// The shards of the tests.
var (
	shardA = shard.ID{ID128: id.ID128{0: 0x02, 15: 0x0a}}
	shardB = shard.ID{ID128: id.ID128{0: 0x02, 15: 0x0b}}
)

// The errors of the doubles of the tests.
var (
	// errBroken is an error of a handler that a retry does not fix.
	errBroken = errs.WithClass(errors.New("test: the handler is broken"), errs.Invalid)
	// errDisk is the cause of the faults of the tests.
	errDisk = errors.New("test: the disk fails")
	// errSubmit is the error of a proposal to the controller double, which takes none.
	errSubmit = errs.WithClass(errors.New("test: the controller takes no proposal"), errs.Unsupported)
)

// cleaner is an assert.TB that runs functions when its test ends, such as a *testing.T or
// a *prop.Case.
type cleaner interface {
	assert.TB
	// Cleanup registers f to run when the test ends.
	Cleanup(f func())
}

// controllerDouble is the controller of the cell of the tests, as the agent calls it. It
// accepts every report with the term of the controller, and decides each disruption with
// decide.
type controllerDouble struct {
	// decide returns the decision of the disruption d. With a nil decide, the controller
	// admits every disruption.
	decide func(d *budget.Disruption) budget.Decision
	// reports lists a copy of every report, in order.
	reports []*protocol.Report
	// disruptions lists a copy of every disruption, in order.
	disruptions []*budget.Disruption
	// dones lists every end of a reservation, in order.
	dones []protocol.DoneArgs
	// mu guards the fields above.
	mu sync.Mutex
}

var _ protocol.ControllerClient = (*controllerDouble)(nil)

// Report keeps a copy of r and accepts it.
func (c *controllerDouble) Report(_ context.Context, _ *shard.Cell, r *protocol.Report) (protocol.Ack, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reports = append(c.reports, r.CloneKanon())
	return protocol.Ack{Sequence: r.Sequence, ControllerEpoch: cellTerm}, nil
}

// Submit refuses every proposal.
func (*controllerDouble) Submit(context.Context, *shard.Cell, *proposal.Proposal) (protocol.Receipt, error) {
	return protocol.Receipt{}, errSubmit
}

// Disrupt keeps a copy of d, and returns the decision of decide.
func (c *controllerDouble) Disrupt(_ context.Context, _ *shard.Cell, d *budget.Disruption) (budget.Decision, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.disruptions = append(c.disruptions, d.CloneKanon())
	if c.decide == nil {
		return budget.Decision{Allowed: true}, nil
	}
	return c.decide(d), nil
}

// Done keeps the end of the reservation a.ID.
func (c *controllerDouble) Done(_ context.Context, _ *shard.Cell, a *protocol.DoneArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dones = append(c.dones, *a)
	return nil
}

// lastReport returns a copy of the last report of the agent.
func (c *controllerDouble) lastReport(tb assert.TB) *protocol.Report {
	tb.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	assert.NotEmpty(tb, c.reports, "the agent must have reported")
	return c.reports[len(c.reports)-1]
}

// guard is the fence of the tests. It keeps every token that the agent acquires, and admits
// every acquisition.
type guard struct {
	// acquired lists the token of every acquisition, in order.
	acquired []fence.Token
	// mu guards acquired.
	mu sync.Mutex
}

var _ fence.Acquirer = (*guard)(nil)

// Acquire keeps t and admits it.
func (g *guard) Acquire(_ context.Context, t fence.Token) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.acquired = append(g.acquired, t)
	return nil
}

// application is the handler of the application of the tests. It counts the calls of each
// key and keeps a copy of each callback. Each call returns the error of fail.
type application struct {
	// fail returns the error of the callback r, and nil for success. With a nil fail, every
	// callback succeeds.
	fail func(r *protocol.Request) error
	// effects maps the key of each callback to the number of calls with it.
	effects map[protocol.Key]int
	// calls lists a copy of every callback, in order.
	calls []*protocol.Request
	// adds lists every request of AddShard, as the agent passed it.
	adds []*replica.AddShardRequest
	// drops lists every request of DropShard, as the agent passed it.
	drops []*replica.DropShardRequest
	// mu guards the fields above.
	mu sync.Mutex
}

var _ replica.Handler = (*application)(nil)

// newApplication returns a handler whose callbacks succeed.
func newApplication() *application {
	return &application{effects: make(map[protocol.Key]int)}
}

// AddShard records the callback of req, and returns the error of fail.
func (a *application) AddShard(_ context.Context, req *replica.AddShardRequest) (replica.AddShardResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.adds = append(a.adds, req)
	return replica.AddShardResult{}, a.record(req.Request)
}

// DropShard records the callback of req, and returns the error of fail.
func (a *application) DropShard(_ context.Context, req *replica.DropShardRequest) (replica.DropShardResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.drops = append(a.drops, req)
	return replica.DropShardResult{}, a.record(req.Request)
}

// record counts the key of r and keeps a copy of r. It returns the error of fail. The caller
// has locked mu.
func (a *application) record(r *protocol.Request) error {
	a.effects[r.Key()]++
	a.calls = append(a.calls, r.CloneKanon())
	if a.fail == nil {
		return nil
	}
	return a.fail(r)
}

// called returns the number of calls with the key of r.
func (a *application) called(r *protocol.Request) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.effects[r.Key()]
}

// counts returns the number of calls of each key.
func (a *application) counts() map[protocol.Key]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return maps.Clone(a.effects)
}

// loaded is a handler of the tests that also reports the load of each replica.
type loaded struct {
	*application
	// load is the load of every replica.
	load shard.Load
}

var _ replica.LoadReporter = (*loaded)(nil)

// ShardLoad returns the load of every replica.
func (l *loaded) ShardLoad(shard.ID) shard.Load {
	return l.load
}

// fixture is the store, the clock, the registry and the doubles that the agent of a test
// uses. The test writes the root scope and the scope of the cell in place of the root and of
// the controller of the cell.
type fixture struct {
	// st is the store and the membership of the agent.
	st *memory.Store
	// clock is the clock of the store and of the agent.
	clock *fake.Clock
	// reg is the registry of the agent.
	reg *config.Registry
	// ctrl is the controller of the cell.
	ctrl *controllerDouble
	// guard is the fence of the agent.
	guard *guard
	// app is the handler of the agent.
	app *application
	// rootSeq and cellSeq are the Seq of the last commit to the root scope and to the scope
	// of the cell.
	rootSeq, cellSeq uint64
}

// newFixture returns a fixture whose root has created the namespace of the tests with its
// cell, and whose controller has acquired the scope of the cell at cellTerm. The store
// closes when tb ends.
func newFixture(tb cleaner) *fixture {
	tb.Helper()
	f := bare(tb)
	f.create(tb)
	f.commitCell(tb, &store.Mutation{Cell: &shard.Cell{
		ID:                 cellID,
		Controller:         controllerAddress,
		ControllerIdentity: controllerIdentity,
		Epoch:              cellTerm,
		SpecVersion:        firstVersion,
	}})
	return f
}

// bare returns a fixture of an empty store. The clock of the fixture starts at the time of
// the call, which a bubble of testing/synctest fixes, so the deadline of a context of the
// bubble and the clock of the agent agree. The store closes when tb ends.
func bare(tb cleaner) *fixture {
	tb.Helper()
	clk := fake.New(time.Now())
	st := memory.New(clk)
	tb.Cleanup(func() { assert.NoError(tb, st.Close(), "the store closes") })
	r := config.NewRegistry(builtin.Support())
	assert.NoError(tb, builtin.Register(r), "the built-ins register")
	return &fixture{st: st, clock: clk, reg: r, ctrl: &controllerDouble{}, guard: &guard{}, app: newApplication()}
}

// create creates the namespace of the tests in the root scope as the root does. Its commit
// has the spec of the tests with its mapping, and it puts the cell of the tests.
func (f *fixture) create(tb assert.TB) {
	tb.Helper()
	s := config.Spec{
		Namespace:    namespace,
		Preset:       builtin.ExclusiveWorker,
		Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
		Version:      firstVersion,
	}
	provided := map[config.Axis][]config.Capability{
		config.Store:      f.st.Provides(),
		config.Membership: f.st.Membership().Provides(),
	}
	res, err := config.Resolve(f.reg, &s, provided)
	assert.NoError(tb, err, "the spec of the tests resolves")
	raw, err := json.Marshal(&res.Spec)
	assert.NoError(tb, err, "the resolved spec encodes")
	f.commitRoot(tb, &store.Mutation{Spec: raw, Mapping: &res.Mapping, Put: []shard.Placement{{Shard: cellKey}}})
}

// config returns the Config of the agent of the node of the tests over f.
func (f *fixture) config() *replica.Config {
	return &replica.Config{
		Store:      f.st,
		Membership: f.st.Membership(),
		Controller: f.ctrl,
		Guard:      f.guard,
		Clock:      f.clock,
		Rand:       seeded.New(1),
		Reporter:   noop.New(),
		Registry:   f.reg,
		Logger:     slog.New(slog.DiscardHandler),
		Namespace:  namespace,
		Node:       node,
	}
}

// start starts the agent of the node of the tests with the handler of f, after each option
// changes its Config. The agent stops when tb ends.
func (f *fixture) start(tb cleaner, opts ...func(c *replica.Config)) *replica.Agent {
	tb.Helper()
	cfg := f.config()
	for _, opt := range opts {
		opt(cfg)
	}
	ctx, stop := context.WithCancel(assert.Context(tb))
	a, err := replica.Start(ctx, cfg, f.app)
	assert.NoError(tb, err, "Start starts the agent")
	tb.Cleanup(func() {
		stop()
		<-a.Done()
	})
	return a
}

// commitRoot commits m to the root scope as the root of the tests.
func (f *fixture) commitRoot(tb assert.TB, m *store.Mutation) {
	tb.Helper()
	ctx := assert.Context(tb)
	head, err := f.st.Head(ctx, rootScope)
	assert.NoError(tb, err, "the store returns the header of the root scope")
	f.rootSeq++
	m.ID = store.CommitID{Incarnation: incA, Term: rootTerm, Seq: f.rootSeq}
	_, err = f.st.Writer(rootScope, incA, rootTerm).Commit(ctx, head.Generation, m)
	assert.NoError(tb, err, "the root commits")
}

// commitCell commits m to the scope of the cell as the controller of the cell at its term.
func (f *fixture) commitCell(tb assert.TB, m *store.Mutation) {
	tb.Helper()
	assert.NoError(tb, f.commit(assert.Context(tb), m), "the controller of the cell commits")
}

// commit commits m to the scope of the cell as the controller of the cell at its term, and
// returns the error of the store. It does not assert, so a client of a history calls it.
func (f *fixture) commit(ctx context.Context, m *store.Mutation) error {
	head, err := f.st.Head(ctx, cellScope)
	if err != nil {
		return err
	}
	f.cellSeq++
	m.ID = store.CommitID{Incarnation: incA, Term: cellTerm, Seq: f.cellSeq}
	_, err = f.st.Writer(cellScope, incA, cellTerm).Commit(ctx, head.Generation, m)
	return err
}

// view returns the current view of the scope of the cell.
func (f *fixture) view(tb assert.TB) membership.View {
	tb.Helper()
	ctx, cancel := context.WithCancel(assert.Context(tb))
	cancel()
	for v, err := range f.st.Membership().Watch(ctx, cellScope, incA) {
		assert.NoError(tb, err, "the membership returns the view of the cell")
		return v
	}
	tb.Fatalf("the membership returns no view of the cell")
	return membership.View{}
}

// incarnation returns the incarnation of the session of the node of the tests in the cell.
func (f *fixture) incarnation(tb assert.TB) epoch.Epoch {
	tb.Helper()
	v := f.view(tb)
	n, ok := v.Node(node.ID)
	assert.True(tb, ok, "the node must have a session in the cell")
	return n.Incarnation
}

// place commits the placement of the shard sid with its writer on the node of the tests at
// the epoch e, and waits until the agent has read it.
func (f *fixture) place(tb assert.TB, sid shard.ID, e epoch.Epoch) {
	tb.Helper()
	p := placement(sid, e, node.ID)
	p.Incarnation = f.incarnation(tb)
	f.commitCell(tb, &store.Mutation{Put: []shard.Placement{p}})
	synctest.Wait()
}

// serve places the shard sid on the node of the tests at the epoch e, and passes the add of
// the operation e of the shard to the agent a. The add succeeds.
func (f *fixture) serve(tb assert.TB, a *replica.Agent, sid shard.ID, e epoch.Epoch) {
	tb.Helper()
	f.place(tb, sid, e)
	r := request(protocol.Add, sid, uint64(e), e, f.incarnation(tb))
	assert.NoError(tb, a.Do(assert.Context(tb), controllerMetadata, r), "the add of the shard must succeed")
}

// observed returns the entry of the replica of the shard sid in the last report of the
// agent.
func (f *fixture) observed(tb assert.TB, sid shard.ID) shard.Observed {
	tb.Helper()
	last := f.ctrl.lastReport(tb)
	i := slices.IndexFunc(last.Replicas, func(o shard.Observed) bool { return o.Shard == sid })
	assert.True(tb, i >= 0, "the last report must have the replica")
	return last.Replicas[i]
}

// unplace commits the placement of the shard sid with its writer on another node at the
// epoch e, and waits until the agent has read it.
func (f *fixture) unplace(tb assert.TB, sid shard.ID, e epoch.Epoch) {
	tb.Helper()
	f.commitCell(tb, &store.Mutation{Put: []shard.Placement{placement(sid, e, otherNode)}})
	synctest.Wait()
}

// request returns a callback of the controller on the forward path at step 0 of the
// operation n of the shard sid. The callback has the epoch e and the node of the tests at
// the incarnation inc.
func request(call protocol.Call, sid shard.ID, n uint64, e, inc epoch.Epoch) *protocol.Request {
	return &protocol.Request{
		Scope:           cellScope,
		Node:            node.ID,
		Range:           placement(sid, e, node.ID).Range,
		Operation:       shard.OperationID{Incarnation: incA, Shard: sid, Sequence: n},
		NodeIncarnation: inc,
		Epoch:           e,
		ControllerEpoch: cellTerm,
		Shard:           sid,
		Call:            call,
		Path:            protocol.Forward,
		Role:            shard.RolePrimary,
	}
}

// rejoin ends the session of the node of the tests with a session of its own, and waits
// until the agent has joined the cell again. It returns the incarnation of the new session
// of the agent.
func (f *fixture) rejoin(tb assert.TB) epoch.Epoch {
	tb.Helper()
	before := f.incarnation(tb)
	ctx, leave := context.WithCancel(assert.Context(tb))
	defer leave()
	n := node
	_, err := f.st.Membership().Join(ctx, cellScope, incA, &n, lease)
	assert.NoError(tb, err, "the test joins the cell with the node of the agent")
	synctest.Wait()
	for range 64 {
		if inc := f.incarnation(tb); inc > before+1 {
			return inc
		}
		f.clock.Advance(time.Second)
		synctest.Wait()
	}
	tb.Fatalf("the agent has not joined the cell again")
	return 0
}

// placement returns the placement of the shard sid with its writer on the node n at the
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

// closing returns the context of a Close, whose deadline is grace after the time of the
// clock of f. The context ends with cause at its deadline.
func (f *fixture) closing(tb assert.TB, cause error) (context.Context, context.CancelFunc) {
	tb.Helper()
	return context.WithDeadlineCause(assert.Context(tb), f.clock.Time().Add(grace), cause)
}

// received waits for the closing of ch, and fails tb when waitFor passes first.
func received(tb assert.TB, ch <-chan struct{}, what string) {
	tb.Helper()
	select {
	case <-ch:
	case <-time.After(waitFor):
		tb.Fatalf("%s did not happen within %v", what, waitFor)
	}
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package controller_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand/seeded"
	"go.thesmos.sh/core/telemetry"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/config/configtest"
	"go.thesmos.sh/sharder/controller"
	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/membership/membershiptest"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/local"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
	"go.thesmos.sh/sharder/store/storetest"
)

// The names and the identities of the tests.
const (
	// namespace is the namespace of the tests.
	namespace shard.Namespace = "orders"
	// otherNamespace is a second namespace, for a process that runs two.
	otherNamespace shard.Namespace = "payments"
	// slotsConfig is the configuration of fixed-slots in the spec of the tests.
	slotsConfig = `{"slots":4}`
	// rootAddress is the address in the record of the root of the tests.
	rootAddress = "root:7000"
	// rootIdentity is the identity of the root of the tests.
	rootIdentity shard.Identity = "root"
	// operator is the identity of the operator of the namespace of the tests.
	operator shard.Identity = "operator"
	// unplacedInstrument is the gauge that the controller of each scope binds to its cell.
	unplacedInstrument telemetry.InstrumentName = "sharder.shards.unplaced"
	// attrCell is the attribute of an instrument that has the cell of the scope.
	attrCell = "cell"
)

// waitFor bounds each wait of a test. Inside a bubble of testing/synctest, the wait runs on
// the virtual time of the bubble.
const waitFor = time.Hour

// The epochs of the placements of the cell of the tests.
const (
	// firstEpoch is the epoch of the first placement of the cell.
	firstEpoch epoch.Epoch = 2
	// secondEpoch is the epoch of the second placement of the cell.
	secondEpoch epoch.Epoch = 3
	// fencingTerm is a term above every term that the root of the tests grants.
	fencingTerm epoch.Epoch = 99
)

// The scopes, the processes and the clock of the tests.
var (
	// origin is the start of the clock of each test.
	origin = time.Date(2026, time.October, 12, 0, 0, 0, 0, time.UTC)
	// rootScope is the root scope of the namespace of the tests.
	rootScope = shard.Scope{Namespace: namespace}
	// procA is the node of the first controller process.
	procA = shard.Node{ID: "controller-a", Address: "controller-a:7000", Identity: "controller-a"}
	// procB is the node of the second controller process.
	procB = shard.Node{ID: "controller-b", Address: "controller-b:7000", Identity: "controller-b"}
	// cellKey is the shard of the cell of the tests in the root scope.
	cellKey = shard.ID{ID128: id.ID128{0: 0x01, 15: 0xce}}
	// cellID is the cell of the tests: the text of the ULID of cellKey.
	cellID = shard.CellID(ulid.Format(cellKey.ID()))
	// cellScope is the scope of the cell of the tests.
	cellScope = shard.Scope{Namespace: namespace, Cell: cellID}
	// rootMetadata is the metadata of the callbacks of the root of the tests.
	rootMetadata = protocol.Metadata{Caller: rootIdentity, Version: protocol.Current}
	// operatorMetadata is the metadata of the calls of the operator of the tests.
	operatorMetadata = protocol.Metadata{Caller: operator, Version: protocol.Current}
	// incA is the namespace incarnation that the root of the tests creates.
	incA = shard.NamespaceIncarnation{ID: id.New128([id.Size128]byte{0: 0x1a, 15: 0x0a})}
	// provided lists the capabilities of the store and the membership of the tests.
	provided = map[config.Axis][]config.Capability{
		config.Store:      {config.Leases},
		config.Membership: {config.Leases},
	}
)

// errBroken is the error of each commit to the scope of a cell while the store of a fixture
// is broken. A retry does not fix it.
var errBroken = errs.WithClass(errors.New("test: the store is broken"), errs.Invalid)

// cleaner is an assert.TB that runs functions when its test ends, such as a *testing.T or
// a *prop.Case.
type cleaner interface {
	assert.TB
	// Cleanup registers f to run when the test ends.
	Cleanup(f func())
}

// faults are the switches of the faults of the store and the membership of a fixture. A
// test flips them while the processes run.
type faults struct {
	// blocked is the channel that each new watch of the views of the scope of a cell waits
	// on once it ends. It is closed until block replaces it with an open channel, which
	// unblock closes.
	blocked chan struct{}
	// mu guards blocked.
	mu sync.Mutex
	// broken is true while the commits to the scope of a cell fail with errBroken.
	broken atomic.Bool
	// tight is true while no mutation of the scope of a cell fits one commit.
	tight atomic.Bool
	// stalled is true while the Leave of a session waits until its context ends.
	stalled atomic.Bool
}

// block makes each later watch of the views of the scope of a cell wait for unblock once it
// ends.
func (fs *faults) block() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.blocked = make(chan struct{})
}

// unblock ends the waits of the watches that started after block.
func (fs *faults) unblock() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	close(fs.blocked)
}

// providingStore is a store that implements config.Provider. It embeds the double of each
// interface.
type providingStore struct {
	*storetest.StoreStub
	*configtest.ProviderStub
}

// providingMembership is a membership that implements config.Provider. It embeds the double
// of each interface.
type providingMembership struct {
	*membershiptest.MembershipStub
	*configtest.ProviderStub
}

// newFaultyStore returns a double of mem that provides the capabilities of mem. A commit of
// a writer of the scope of a cell fails with errBroken while fs is broken. Fits returns an
// error that wraps store.ErrTooLarge for a mutation of the scope of a cell while fs is
// tight. The doubles record no call and report to no test, because the processes create the
// writers on their own goroutines.
func newFaultyStore(mem *memory.Store, fs *faults) *providingStore {
	writer := func(s shard.Scope, inc shard.NamespaceIncarnation, term epoch.Epoch) store.Writer {
		w := mem.Writer(s, inc, term)
		commit := func(ctx context.Context, expect epoch.Epoch, m *store.Mutation) (epoch.Epoch, error) {
			if s.Cell != "" && fs.broken.Load() {
				return 0, errBroken
			}
			return w.Commit(ctx, expect, m)
		}
		return storetest.NewWriterStub(nil, storetest.WriterStubBenchMode(), storetest.WriterStubDelegateTo(w),
			storetest.WithWriterCommit(commit))
	}
	fits := func(s shard.Scope, m *store.Mutation) error {
		if s.Cell != "" && fs.tight.Load() {
			return fmt.Errorf("test: the mutation of %v: %w", s, store.ErrTooLarge)
		}
		return mem.Fits(s, m)
	}
	return &providingStore{
		StoreStub: storetest.NewStoreStub(nil, storetest.StoreStubBenchMode(), storetest.StoreStubDelegateTo(mem),
			storetest.WithStoreWriter(writer), storetest.WithStoreFits(fits)),
		ProviderStub: configtest.NewProviderStub(nil, configtest.ProviderStubDelegateTo(mem)),
	}
}

// newFaultyMembership returns a double of m that provides the capabilities of m. While fs
// is stalled, the Leave of a session that Join returns waits until its context ends, and
// returns its cause. Once a watch of the views of the scope of a cell ends, it waits until
// fs is unblocked. The doubles record no call and report to no test, because the processes
// create the sessions on their own goroutines.
func newFaultyMembership(m *memory.Membership, fs *faults) *providingMembership {
	join := func(
		ctx context.Context,
		s shard.Scope,
		inc shard.NamespaceIncarnation,
		n *shard.Node,
		l ownership.Lease,
	) (membership.Session, error) {
		ss, err := m.Join(ctx, s, inc, n, l)
		if err != nil {
			return nil, fmt.Errorf("test: the join of %s: %w", n.ID, err)
		}
		leave := func(ctx context.Context) error {
			if fs.stalled.Load() {
				<-ctx.Done()
				return context.Cause(ctx)
			}
			return ss.Leave(ctx)
		}
		return membershiptest.NewSessionStub(nil, membershiptest.SessionStubBenchMode(),
			membershiptest.SessionStubDelegateTo(ss), membershiptest.WithSessionLeave(leave)), nil
	}
	watch := func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation) iter.Seq2[membership.View, error] {
		fs.mu.Lock()
		blocked := fs.blocked
		fs.mu.Unlock()
		inner := m.Watch(ctx, s, inc)
		return func(yield func(membership.View, error) bool) {
			for v, err := range inner {
				if !yield(v, err) {
					break
				}
			}
			if s.Cell != "" {
				<-blocked
			}
		}
	}
	return &providingMembership{
		MembershipStub: membershiptest.NewMembershipStub(nil, membershiptest.MembershipStubBenchMode(),
			membershiptest.MembershipStubDelegateTo(m), membershiptest.WithMembershipJoin(join),
			membershiptest.WithMembershipWatch(watch)),
		ProviderStub: configtest.NewProviderStub(nil, configtest.ProviderStubDelegateTo(m)),
	}
}

// cells is a telemetry.Reporter that counts the controllers of each cell of a process. The
// controller of a scope binds the gauge of its unplaced shards to its cell, and releases
// the gauge when it stops. Every other instrument discards its records.
type cells struct {
	// live maps each cell to the controllers of the cell that have bound the gauge and not
	// released it.
	live map[shard.CellID]int
	// bound maps each cell to the controllers of the cell that have bound the gauge.
	bound map[shard.CellID]int
	// mu guards live and bound.
	mu sync.Mutex
}

var _ telemetry.Reporter = (*cells)(nil)

// Counter returns a counter that discards its records.
func (*cells) Counter(spec telemetry.InstrumentSpec) telemetry.Counter {
	return noop.New().Counter(spec)
}

// Gauge returns the gauge of the unplaced shards, which counts its bindings, and a gauge
// that discards its records for every other spec.
func (c *cells) Gauge(spec telemetry.InstrumentSpec) telemetry.Gauge {
	if spec.Name != unplacedInstrument {
		return noop.New().Gauge(spec)
	}
	return &unplaced{cells: c}
}

// Histogram returns a histogram that discards its records.
func (*cells) Histogram(spec telemetry.InstrumentSpec) telemetry.Histogram {
	return noop.New().Histogram(spec)
}

// Tracer returns a tracer that discards every span.
func (*cells) Tracer(name telemetry.InstrumentName) telemetry.Tracer {
	return noop.New().Tracer(name)
}

// controllers returns the controllers of the cell id that run in the process.
func (c *cells) controllers(id shard.CellID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.live[id]
}

// started returns the controllers of the cell id that the process has started.
func (c *cells) started(id shard.CellID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bound[id]
}

// most returns the largest number of controllers that run in the process for one cell.
func (c *cells) most() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for cell, live := range c.live {
		if cell != "" {
			n = max(n, live)
		}
	}
	return n
}

// unplaced is the gauge of the unplaced shards of a cells reporter.
type unplaced struct {
	// cells is the reporter of the gauge.
	cells *cells
	// cell is the cell that the gauge is bound to.
	cell shard.CellID
}

var _ telemetry.Gauge = (*unplaced)(nil)

// Set discards value.
func (*unplaced) Set(context.Context, float64) {}

// Add discards delta.
func (*unplaced) Add(context.Context, float64) {}

// With returns the gauge bound to the cell of attrs, and counts the binding.
func (g *unplaced) With(attrs []telemetry.Attr) telemetry.Gauge {
	bound := &unplaced{cells: g.cells}
	for _, a := range attrs {
		if a.Key == attrCell {
			bound.cell = shard.CellID(a.Value.Str)
		}
	}
	g.cells.mu.Lock()
	defer g.cells.mu.Unlock()
	g.cells.live[bound.cell]++
	g.cells.bound[bound.cell]++
	return bound
}

// Release counts the end of the binding.
func (g *unplaced) Release() {
	g.cells.mu.Lock()
	defer g.cells.mu.Unlock()
	g.cells.live[g.cell]--
}

// fixture is the store, the network, the clock and the registry that the processes of a
// test share.
type fixture struct {
	// st is the store of the processes. It delegates to mem, and fails the calls that faults
	// switch on.
	st *providingStore
	// members is the membership of the processes. It delegates to the membership of mem, and
	// waits where faults switch it on.
	members *providingMembership
	// mem is the memory store under st and members.
	mem *memory.Store
	// faults are the switches of the faults of st and members.
	faults *faults
	// net connects the processes and the root of the tests.
	net *local.Network
	// clock is the clock of the store and of the processes.
	clock *fake.Clock
	// reg is the registry of the processes.
	reg *config.Registry
}

// newFixture returns a fixture with an empty store. The store closes when tb ends.
func newFixture(tb cleaner) *fixture {
	tb.Helper()
	clk := fake.New(origin)
	mem := memory.New(clk)
	tb.Cleanup(func() { assert.NoError(tb, mem.Close(), "the store closes") })
	fs := &faults{blocked: make(chan struct{})}
	close(fs.blocked)
	r := config.NewRegistry(builtin.Support())
	assert.NoError(tb, builtin.Register(r), "the built-ins register")
	return &fixture{
		st:      newFaultyStore(mem, fs),
		members: newFaultyMembership(mem.Membership(), fs),
		mem:     mem,
		faults:  fs,
		net:     local.NewNetwork(),
		clock:   clk,
		reg:     r,
	}
}

// config returns the Config of the controller process of node over f, whose instruments r
// reports.
func (f *fixture) config(node shard.Node, r telemetry.Reporter) *controller.Config {
	return &controller.Config{
		Store:      f.st,
		Membership: f.members,
		Agents:     f.net.Local(protocol.Metadata{Caller: node.Identity, Version: protocol.Current}),
		Clock:      f.clock,
		Rand:       seeded.New(1),
		Reporter:   r,
		Registry:   f.reg,
		Logger:     slog.New(slog.DiscardHandler),
		Operators:  map[shard.Namespace][]shard.Identity{namespace: {operator}},
		Node:       node,
		Volatile:   true,
	}
}

// process is a controller process of a test. It runs until the test halts it or ends.
type process struct {
	// ctrl is the controller of the process.
	ctrl *controller.Controller
	// cells counts the controllers of the cells of the process.
	cells *cells
	// stop ends the context of Run.
	stop context.CancelFunc
	// errc receives the result of Run.
	errc chan error
	// err is the result of Run, once halt has received it.
	err error
	// once receives the result of Run once.
	once sync.Once
}

// start starts the controller process of node over f, which runs the namespaces of specs.
// The process halts when tb ends.
func (f *fixture) start(tb cleaner, node shard.Node, specs ...config.Spec) *process {
	tb.Helper()
	r := &cells{live: make(map[shard.CellID]int), bound: make(map[shard.CellID]int)}
	p := f.launch(tb, f.config(node, r), specs...)
	p.cells = r
	return p
}

// launch starts the controller process of cfg, which runs the namespaces of specs. The
// network of f passes the callbacks of each root to the process. The process halts when tb
// ends.
func (f *fixture) launch(tb cleaner, cfg *controller.Config, specs ...config.Spec) *process {
	tb.Helper()
	ctrl, err := controller.New(cfg)
	assert.NoError(tb, err, "New accepts the configuration of the process")
	for i := range specs {
		f.net.AddAgent(specs[i].Namespace, cfg.Node.ID, ctrl)
	}
	ctx, stop := context.WithCancel(assert.Context(tb))
	p := &process{ctrl: ctrl, stop: stop, errc: make(chan error, 1)}
	go func() { p.errc <- ctrl.Run(ctx, specs...) }()
	tb.Cleanup(func() {
		// A test that checks the error of Run reads it with result. The cleanup only stops the
		// process.
		_ = p.halt(tb)
	})
	return p
}

// halt ends the context of Run, and returns the error of Run once Run returns.
func (p *process) halt(tb assert.TB) error {
	tb.Helper()
	p.stop()
	return p.result(tb)
}

// result waits until Run returns, and returns its error. A later call returns the same
// error.
func (p *process) result(tb assert.TB) error {
	tb.Helper()
	p.once.Do(func() { p.err = received(tb, p.errc, "Run returns") })
	return p.err
}

// testRoot is the root of the namespace of the tests, which a test writes in place of the
// root machines. It leads the election of the root scope for the whole test, so the root
// machine of each process campaigns and does not lead.
type testRoot struct {
	// f is the fixture of the root.
	f *fixture
	// term is the term of the root.
	term epoch.Epoch
	// seq is the Seq of the last commit of the root.
	seq uint64
}

// newRoot campaigns in the root scope, and creates the namespace of the tests with its spec
// and the record of the root.
func (f *fixture) newRoot(tb assert.TB) *testRoot {
	tb.Helper()
	lead, err := f.st.Campaign(assert.Context(tb), rootScope)
	assert.NoError(tb, err, "the root campaigns")
	s := spec(namespace)
	s.Version = 1
	res, err := config.Resolve(f.reg, &s, provided)
	assert.NoError(tb, err, "the spec of the tests resolves")
	raw, err := json.Marshal(&res.Spec)
	assert.NoError(tb, err, "the resolved spec encodes")
	r := &testRoot{f: f, term: lead.Term()}
	r.commit(tb, &store.Mutation{
		Spec:    raw,
		Mapping: &res.Mapping,
		Cell:    &shard.Cell{Controller: rootAddress, ControllerIdentity: rootIdentity, Epoch: r.term, SpecVersion: 1},
	})
	return r
}

// commit commits m to the root scope as the root of the tests.
func (r *testRoot) commit(tb assert.TB, m *store.Mutation) {
	tb.Helper()
	ctx := assert.Context(tb)
	head, err := r.f.st.Head(ctx, rootScope)
	assert.NoError(tb, err, "the store returns the header of the root scope")
	r.seq++
	m.ID = store.CommitID{Incarnation: incA, Term: r.term, Seq: r.seq}
	_, err = r.f.st.Writer(rootScope, incA, r.term).Commit(ctx, head.Generation, m)
	assert.NoError(tb, err, "the root commits")
}

// session returns the incarnation of the session of the process of node in the root scope.
func (r *testRoot) session(tb assert.TB, node shard.NodeID) epoch.Epoch {
	tb.Helper()
	v := view(tb, r.f, rootScope)
	n, ok := v.Node(node)
	assert.True(tb, ok, "the process has a session in the root scope")
	return n.Incarnation
}

// request returns the callback call of the root for the cell of the tests to the process of
// node, at the epoch e of the operation seq.
func (r *testRoot) request(
	tb assert.TB,
	call protocol.Call,
	node shard.NodeID,
	e epoch.Epoch,
	seq uint64,
) *protocol.Request {
	tb.Helper()
	return &protocol.Request{
		Scope:           rootScope,
		Node:            node,
		Operation:       shard.OperationID{Incarnation: incA, Shard: cellKey, Sequence: seq},
		NodeIncarnation: r.session(tb, node),
		Epoch:           e,
		ControllerEpoch: r.term,
		Shard:           cellKey,
		Call:            call,
		Path:            protocol.Forward,
		Role:            shard.RolePrimary,
	}
}

// place commits the placement of the cell of the tests on the process of node at the epoch
// e. The placement records the incarnation of the session of the process in the root scope.
func (r *testRoot) place(tb assert.TB, node shard.NodeID, e epoch.Epoch) {
	tb.Helper()
	inc := r.session(tb, node)
	r.commit(tb, &store.Mutation{Put: []shard.Placement{{
		Shard:       cellKey,
		Epoch:       e,
		Incarnation: inc,
		Reserved:    e,
		Replicas:    []shard.Replica{{Node: node, Role: shard.RolePrimary}},
	}}})
}

// host places the cell of the tests on the process p of node at the epoch e, and passes the
// add of the operation seq to p. It returns the error of Do.
func (r *testRoot) host(tb assert.TB, p *process, node shard.NodeID, e epoch.Epoch, seq uint64) error {
	tb.Helper()
	r.place(tb, node, e)
	return p.ctrl.Do(assert.Context(tb), rootMetadata, r.request(tb, protocol.Add, node, e, seq))
}

// fence acquires the scope of the cell of the tests at fencingTerm, as a newer controller of
// the cell does.
func (r *testRoot) fence(tb assert.TB) {
	tb.Helper()
	ctx := assert.Context(tb)
	head, err := r.f.st.Head(ctx, cellScope)
	assert.NoError(tb, err, "the store returns the header of the scope of the cell")
	m := &store.Mutation{
		ID:   store.CommitID{Incarnation: incA, Term: fencingTerm, Seq: 1},
		Cell: &shard.Cell{ID: cellID, Controller: rootAddress, ControllerIdentity: rootIdentity, Epoch: fencingTerm},
	}
	_, err = r.f.st.Writer(cellScope, incA, fencingTerm).Commit(ctx, head.Generation, m)
	assert.NoError(tb, err, "the newer controller acquires the scope of the cell")
}

// record returns the record of the controller of the scope s.
func record(tb assert.TB, f *fixture, s shard.Scope) shard.Cell {
	tb.Helper()
	ctx := assert.Context(tb)
	head, err := f.st.Head(ctx, s)
	assert.NoError(tb, err, "the store returns the header of the scope")
	state, cursor, err := f.st.Read(ctx, s, head.Incarnation, store.Query{}, page.Page{})
	assert.NoError(tb, err, "the store reads the scope")
	assert.NoError(tb, cursor.Close(), "the cursor closes")
	return state.Cell
}

// view returns the current view of the membership of the scope s.
func view(tb assert.TB, f *fixture, s shard.Scope) membership.View {
	tb.Helper()
	ctx, cancel := context.WithCancel(assert.Context(tb))
	cancel()
	for v, err := range f.members.Watch(ctx, s, incA) {
		assert.NoError(tb, err, "the membership returns the view of the scope")
		return v
	}
	tb.Fatalf("the membership returns no view of %v", s)
	return membership.View{}
}

// disruption returns a negotiable disruption of the node n in the scope of the cell of the
// tests, with the ID that the byte b ends.
func disruption(n shard.NodeID, b byte) *budget.Disruption {
	return &budget.Disruption{
		Start:       origin,
		End:         origin.Add(time.Hour),
		Scope:       cellScope,
		Nodes:       []shard.NodeID{n},
		ID:          budget.ID{ID: id.New128([id.Size128]byte{0: 0xd1, 15: b})},
		Incarnation: incA,
		Impact:      budget.ProcessRestart,
		Negotiable:  true,
	}
}

// spec returns the spec of the namespace ns.
func spec(ns shard.Namespace) config.Spec {
	return config.Spec{
		Namespace:    ns,
		Preset:       builtin.ExclusiveWorker,
		Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
	}
}

// received receives the value that ch delivers, and fails tb with msg when ch delivers
// nothing within waitFor.
func received[T any](tb assert.TB, ch <-chan T, msg string) T {
	tb.Helper()
	v, ok := streamtest.Receive(tb, ch, waitFor, msg)
	assert.True(tb, ok, msg)
	return v
}

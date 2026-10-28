// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package embedded_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/clock/hlc"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand/seeded"
	"go.thesmos.sh/core/telemetry"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/config/configtest"
	"go.thesmos.sh/sharder/embedded"
	"go.thesmos.sh/sharder/fence/fencetest"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/membership/membershiptest"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/local"
	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/replica/replicatest"
	"go.thesmos.sh/sharder/router"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
	"go.thesmos.sh/sharder/store/storetest"
)

// The namespaces of the tests.
const (
	// orders is the namespace of the tests whose process runs a router.
	orders shard.Namespace = "orders"
	// audits is the namespace of the tests whose process does not run a router.
	audits shard.Namespace = "audits"
)

// The strategies of the specs of the tests.
const (
	// slotsConfig is the configuration of fixed-slots in the specs of the tests.
	slotsConfig = `{"slots":4}`
	// slots is the number of the shards of each namespace under slotsConfig.
	slots = 4
	// churnConfig lets one pass of the planner move half of the shards of a namespace, so a
	// second process takes its shards in one pass.
	churnConfig = `{"churn":{"maxShardFraction":0.5,"perNode":4}}`
)

// The bounds of the waits of the tests. Inside a bubble of testing/synctest, they pass on the
// virtual time of the bubble.
const (
	// waitFor bounds each wait for a state of a process.
	waitFor = time.Hour
	// drainFor bounds the Close of a process.
	drainFor = time.Minute
)

// leaseTTL is the TTL of the lease of a session that a test joins.
const leaseTTL = 10 * time.Second

// key is the key of a request that a test routes.
const key = "tenant-1"

// lookupsName is the name of the counter of lookups that a router binds as it starts.
const lookupsName telemetry.InstrumentName = "sharder.router.lookups"

// The nodes of the processes of the tests.
var (
	nodeA = shard.Node{ID: "node-a", Address: "node-a:7000", Identity: "node-a"}
	nodeB = shard.Node{ID: "node-b", Address: "node-b:7000", Identity: "node-b"}
)

// spec is the spec of a namespace of the tests without its name.
var spec = config.Spec{
	Preset:       builtin.ExclusiveWorker,
	Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
	Placement:    config.Selection{Config: json.RawMessage(churnConfig)},
}

// lease is the lease of a session that a test joins.
var lease = ownership.Lease{
	TTL:          shard.Duration(leaseTTL),
	Renew:        shard.Duration(leaseTTL / 3),
	MaxDriftRate: 0.01,
	OnLoss:       ownership.ContinueFenced,
}

// The errors of the tests.
var (
	// errStop is the cause with which a test ends a context.
	errStop = errors.New("test: the test stops")
	// errRefused is the error of a call that a double of the tests refuses. A retry does not
	// fix it.
	errRefused = errs.WithClass(errors.New("test: the call is refused"), errs.Invalid)
)

// application is the application of one namespace of a process of the tests. It keeps the
// shards that the node serves, and signals each change of them.
type application struct {
	// handler is the handler of the namespace. Its AddShard and DropShard change shards.
	handler *replicatest.HandlerStub
	// shards is the set of the shards that the node serves.
	shards map[shard.ID]struct{}
	// changed is closed at the next change of shards.
	changed chan struct{}
	// mu guards shards and changed.
	mu sync.Mutex
}

// newApplication returns the application of a namespace whose node serves no shard. Its
// handler reports to tb.
func newApplication(tb assert.TB) *application {
	tb.Helper()
	a := &application{shards: make(map[shard.ID]struct{}), changed: make(chan struct{})}
	add := func(_ context.Context, req *replica.AddShardRequest) (replica.AddShardResult, error) {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.shards[req.Request.Shard] = struct{}{}
		close(a.changed)
		a.changed = make(chan struct{})
		return replica.AddShardResult{}, nil
	}
	drop := func(_ context.Context, req *replica.DropShardRequest) (replica.DropShardResult, error) {
		a.mu.Lock()
		defer a.mu.Unlock()
		delete(a.shards, req.Request.Shard)
		close(a.changed)
		a.changed = make(chan struct{})
		return replica.DropShardResult{}, nil
	}
	a.handler = replicatest.NewHandlerStub(tb, replicatest.WithHandlerAddShard(add),
		replicatest.WithHandlerDropShard(drop))
	return a
}

// count returns the number of the shards that the node serves.
func (a *application) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.shards)
}

// serves waits until the node serves n shards, and fails tb when waitFor passes first.
func (a *application) serves(tb assert.TB, n int) {
	tb.Helper()
	deadline := time.After(waitFor)
	for {
		a.mu.Lock()
		got, changed := len(a.shards), a.changed
		a.mu.Unlock()
		if got == n {
			return
		}
		select {
		case <-changed:
		case <-deadline:
			tb.Fatalf("the node serves %d shards after %v, and not %d", got, waitFor, n)
		}
	}
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

// reporter is the reporter of a process of a test. It calls bound when a router binds its
// counter of lookups, which the router does as it starts.
type reporter struct {
	telemetry.Reporter
	// bound runs when a router binds its counter of lookups.
	bound func()
}

var _ telemetry.Reporter = (*reporter)(nil)

// Counter calls bound for the counter of lookups of a router, and returns the counter of spec.
func (r *reporter) Counter(spec telemetry.InstrumentSpec) telemetry.Counter {
	if spec.Name == lookupsName {
		r.bound()
	}
	return r.Reporter.Counter(spec)
}

// process is a process of a test, with the handler of each of its namespaces.
type process struct {
	*embedded.Process
	// apps maps each namespace of the process to its handler.
	apps map[shard.Namespace]*application
}

// system is the store, the membership, the network and the registry that the processes of a
// test share.
type system struct {
	// st is the store and the membership that the doubles of the system delegate to.
	st *memory.Store
	// store is the store of the processes. Its Read returns errRefused for the scope that
	// refuseRead points to.
	store *providingStore
	// members is the membership of the processes. Its Join returns errRefused for the scope
	// that refuseJoin points to.
	members *providingMembership
	// net connects the processes.
	net *local.Network
	// reg is the registry of the processes.
	reg *config.Registry
	// refuseRead points to the scope whose reads fail, and is nil before the test refuses
	// one.
	refuseRead atomic.Pointer[shard.Scope]
	// refuseJoin points to the scope whose joins fail, and is nil before the test refuses
	// one.
	refuseJoin atomic.Pointer[shard.Scope]
}

// newSystem returns a system without processes. Its store closes when t ends. The doubles of
// the store and the membership record no call.
func newSystem(t *testing.T) *system {
	t.Helper()
	st := memory.New(hlc.New(1))
	t.Cleanup(func() { assert.NoError(t, st.Close(), "the store closes") })
	reg := config.NewRegistry(builtin.Support())
	assert.NoError(t, builtin.Register(reg), "the built-ins register")
	s := &system{st: st, net: local.NewNetwork(), reg: reg}
	read := func(
		ctx context.Context,
		sc shard.Scope,
		inc shard.NamespaceIncarnation,
		q store.Query,
		p page.Page,
	) (store.State, page.Cursor[shard.Placement], error) {
		if r := s.refuseRead.Load(); r != nil && *r == sc {
			return store.State{}, nil, errRefused
		}
		return st.Read(ctx, sc, inc, q, p)
	}
	s.store = &providingStore{
		StoreStub: storetest.NewStoreStub(t, storetest.StoreStubBenchMode(), storetest.StoreStubDelegateTo(st),
			storetest.WithStoreRead(read)),
		ProviderStub: configtest.NewProviderStub(t, configtest.ProviderStubDelegateTo(st)),
	}
	m := st.Membership()
	join := func(
		ctx context.Context,
		sc shard.Scope,
		inc shard.NamespaceIncarnation,
		n *shard.Node,
		l ownership.Lease,
	) (membership.Session, error) {
		if r := s.refuseJoin.Load(); r != nil && *r == sc {
			return nil, errRefused
		}
		return m.Join(ctx, sc, inc, n, l)
	}
	s.members = &providingMembership{
		MembershipStub: membershiptest.NewMembershipStub(t, membershiptest.MembershipStubBenchMode(),
			membershiptest.MembershipStubDelegateTo(m), membershiptest.WithMembershipJoin(join)),
		ProviderStub: configtest.NewProviderStub(t, configtest.ProviderStubDelegateTo(m)),
	}
	return s
}

// config returns the configuration of the process of the node n over s, with the namespace
// orders, which routes, and the namespace audits, which does not. Each namespace has an
// application of its own, which apps maps the namespace to. The doubles of the
// configuration report to tb.
func (s *system) config(tb assert.TB, n shard.Node) (cfg *embedded.Config, apps map[shard.Namespace]*application) {
	tb.Helper()
	apps = make(map[shard.Namespace]*application, 2)
	namespaces := make([]embedded.Namespace, 0, 2)
	for _, ns := range []shard.Namespace{orders, audits} {
		apps[ns] = newApplication(tb)
		sp := spec
		sp.Namespace = ns
		namespaces = append(namespaces, embedded.Namespace{
			Spec:    sp,
			Handler: apps[ns].handler,
			Guard:   fencetest.NewAcquirerStub(tb),
			Route:   ns == orders,
		})
	}
	return &embedded.Config{
		Namespaces: namespaces,
		Store:      s.store,
		Membership: s.members,
		Transport:  s.net.Local(protocol.Metadata{Caller: n.Identity, Version: protocol.Current}),
		Clock:      hlc.New(1),
		Rand:       seeded.New(1),
		Reporter:   noop.New(),
		Registry:   s.reg,
		Logger:     slog.New(slog.DiscardHandler),
		Node:       n,
		Volatile:   true,
	}, apps
}

// build returns the process of the node n over s, which the network of s serves, after each
// option changes its configuration. The process has not started.
func (s *system) build(t *testing.T, n shard.Node, opts ...func(cfg *embedded.Config)) *process {
	t.Helper()
	cfg, apps := s.config(t, n)
	for _, opt := range opts {
		opt(cfg)
	}
	p, err := embedded.New(cfg)
	assert.NoError(t, err, "New returns the process")
	for ns := range apps {
		s.net.AddAgent(ns, n.ID, p.AgentServer())
	}
	s.net.AddController(n.Address, p.ControllerServer())
	return &process{Process: p, apps: apps}
}

// start builds the process of the node n over s with opts, and starts it under the context of
// t. The process stops when t ends.
func (s *system) start(t *testing.T, n shard.Node, opts ...func(cfg *embedded.Config)) *process {
	t.Helper()
	p := s.build(t, n, opts...)
	assert.NoError(t, p.Start(t.Context()), "Start starts the process")
	t.Cleanup(func() { <-p.Done() })
	return p
}

// supersede makes the membership refuse each Join of the node n in the scope sc. It then
// ends the session of n in sc with a session of its own, so the component of n that joined
// sc joins again. The session of supersede ends when tb ends.
func (s *system) supersede(tb assert.TB, n shard.Node, sc shard.Scope, inc shard.NamespaceIncarnation) {
	tb.Helper()
	s.refuseJoin.Store(&sc)
	_, err := s.st.Membership().Join(assert.Context(tb), sc, inc, &n, lease)
	assert.NoError(tb, err, "the test joins the scope with the node of the process")
}

// route waits until the node of p serves every shard of orders and every goroutine of the
// bubble waits. It then returns the route of key from the router of orders.
func route(tb assert.TB, p *process) router.Route {
	tb.Helper()
	p.apps[orders].serves(tb, slots)
	synctest.Wait()
	w, ok := p.Router(orders)
	assert.True(tb, ok, "the process runs the router of orders")
	res, err := w.Route(assert.Context(tb), &router.RouteRequest{Key: []byte(key)})
	assert.NoError(tb, err, "the router routes the key")
	return res.Route
}

// closed reports whether ch is closed.
func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
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

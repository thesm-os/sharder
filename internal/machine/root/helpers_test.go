// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package root_test

import (
	"encoding/json"
	"log/slog"
	"slices"
	"time"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/coretest/telemetrytest"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand/constant"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/root"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/partition/fixedslots"
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
	// rootAddress is the address of the controller process of the root of the tests.
	rootAddress = "controller-1:7000"
	// rootIdentity is the identity of the controller process of the root of the tests.
	rootIdentity shard.Identity = "controller-1"
	// slotsConfig is the configuration of fixed-slots in the spec of the tests.
	slotsConfig = `{"slots":4}`
)

// maxDeliveries bounds the effects that run performs, so a machine that never settles
// fails its test instead of the timeout of go test.
const maxDeliveries = 10000

// half is the value that rand.Float64 returns for the source of randomness of the tests, so
// each backoff is half of its ceiling.
const half = 0.5

// marker is the effect that the harness passes in out with each event: an Unwatch without a
// watch, which the machine never returns itself.
var marker machine.Effect = machine.Unwatch{}

// The controller processes of the tests, each at its first incarnation and with the
// capacity of one cell.
var (
	procA = shard.Node{
		ID: "proc-a", Address: "proc-a:7000", Identity: "proc-a", Incarnation: 1,
		Capacity: map[string]float64{root.Capacity: 1},
	}
	procB = shard.Node{
		ID: "proc-b", Address: "proc-b:7000", Identity: "proc-b", Incarnation: 1,
		Capacity: map[string]float64{root.Capacity: 1},
	}
)

// The scope, the clock and the capabilities of the tests.
var (
	// rootScope is the root scope of the namespace of the tests.
	rootScope = shard.Scope{Namespace: namespace}
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
	// cfg is the Config of the machine. newHarness completes it.
	cfg root.Config
	// fits is the bound of a commit. A nil fits takes the bound of the store.
	fits func(m *store.Mutation) error
}

// option changes the setup of a harness.
type option func(*setup)

// harness is the runtime of the tests. It performs the effects of one root machine against
// a memory store, replies to the callbacks of the machine as the controller processes do,
// runs its passes and keeps its replies.
type harness struct {
	// answer returns the answer of a controller process to the request r, and nil for
	// success. With a nil answer, every process replies with success.
	answer func(r *protocol.Request) error
	// observe receives each effect that the root appends, before route keeps it, or is nil.
	observe func(e machine.Effect)
	// st is the store of the machine.
	st *memory.Store
	// machine is the machine under test.
	machine *root.Machine
	// logs keeps every record that the machine logs.
	logs *telemetrytest.Records
	// terms maps the token of each Campaign that the store granted to its term.
	terms map[machine.Token]store.Leadership
	// pending lists the effects that run performs, the oldest first.
	pending []machine.Effect
	// timers lists the Timers that the machine armed, the oldest first.
	timers []machine.Timer
	// replies lists every reply of the machine, in order.
	replies []machine.Effect
	// sent lists the request of every callback of the machine, in order.
	sent []*protocol.Request
	// campaigns lists the token of every Campaign of the machine, in order.
	campaigns []machine.Token
	// tokens lists the token of every effect of the machine that has a token of its own, in
	// order.
	tokens []machine.Token
	// resigned lists the token of every Resign of the machine, in order.
	resigned []machine.Token
	// unwatched lists the token of every Unwatch of the machine, in order.
	unwatched []machine.Token
	// views is the token of the WatchViews that runs, and zero without one.
	views machine.Token
	// incarnation is the namespace incarnation of the last WatchViews of the machine.
	incarnation shard.NamespaceIncarnation
	// revision is the revision of the last view that the harness passed.
	revision epoch.Epoch
	// calls is the token of the last inbound call of the harness.
	calls machine.ReplyToken
	// others is the Seq of the last commit of another root.
	others uint64
	// paused reports that run does not perform a Campaign, so a campaign remains in flight.
	paused bool
}

// newHarness returns a harness of a root machine of the namespace of the tests. Each
// option changes the setup before New.
func newHarness(tb testCase, opts ...option) *harness {
	tb.Helper()
	s := setup{cfg: root.Config{
		Rand:        constant.NewCounter(half),
		Reporter:    noop.New(),
		Provided:    provided,
		Spec:        namespaceSpec(),
		Operators:   []shard.Identity{operator},
		Node:        shard.Node{ID: "controller-1", Address: rootAddress, Identity: rootIdentity},
		Parallelism: 1,
	}}
	for _, opt := range opts {
		opt(&s)
	}
	h := &harness{
		st:    memory.New(fake.New(origin)),
		logs:  telemetrytest.NewRecords(slog.LevelDebug),
		terms: make(map[machine.Token]store.Leadership),
	}
	tb.Cleanup(func() { assert.NoError(tb, h.st.Close(), "the store must close") })
	s.cfg.Clock = fake.New(origin)
	s.cfg.Registry = registry(tb)
	s.cfg.Logger = slog.New(h.logs)
	s.cfg.Fits = s.fits
	if s.fits == nil {
		s.cfg.Fits = func(m *store.Mutation) error { return h.st.Fits(rootScope, m) }
	}
	var err error
	h.machine, err = root.New(&s.cfg)
	assert.NoError(tb, err, "New must return a machine of a complete config")
	return h
}

// call returns the token of the next inbound call.
func (h *harness) call() machine.ReplyToken {
	h.calls++
	return h.calls
}

// start passes Started to the machine and runs it until the controller of its first term
// serves.
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

// route passes each effect of effects to observe, and keeps it in the list of its kind. It
// also keeps the token of each effect that has a token of its own.
func (h *harness) route(effects []machine.Effect) {
	for _, e := range effects {
		if h.observe != nil {
			h.observe(e)
		}
		switch e := e.(type) {
		case machine.Campaign:
			h.tokens = append(h.tokens, e.Token)
			h.campaigns = append(h.campaigns, e.Token)
			h.pending = append(h.pending, e)
		case machine.Resign:
			h.resigned = append(h.resigned, e.Token)
		case machine.Unwatch:
			h.unwatched = append(h.unwatched, e.Token)
			if h.views == e.Token {
				h.views = 0
			}
		case machine.WatchViews:
			h.tokens = append(h.tokens, e.Token)
			h.views, h.incarnation = e.Token, e.Incarnation
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
		case machine.Head:
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
// machine until no effect is pending. It leaves a Campaign pending while paused is set.
func (h *harness) run(tb assert.TB) {
	tb.Helper()
	for range maxDeliveries {
		i := slices.IndexFunc(h.pending, func(e machine.Effect) bool {
			_, isCampaign := e.(machine.Campaign)
			return !h.paused || !isCampaign
		})
		if i < 0 {
			return
		}
		e := h.pending[i]
		h.pending = slices.Delete(h.pending, i, i+1)
		h.step(tb, h.perform(tb, e))
	}
	tb.Fatalf("the machine still runs after %d deliveries", maxDeliveries)
}

// perform performs e and returns the event of its result.
func (h *harness) perform(tb assert.TB, e machine.Effect) machine.Event {
	tb.Helper()
	ctx := assert.Context(tb)
	switch e := e.(type) {
	case machine.Campaign:
		l, err := h.st.Campaign(ctx, e.Scope)
		assert.NoError(tb, err, "the store must grant a term")
		h.terms[e.Token] = l
		return machine.Campaigned{Token: e.Token, Term: l.Term()}
	case machine.Head:
		head, err := h.st.Head(ctx, e.Scope)
		return machine.HeadRead{Err: err, Token: e.Token, Head: head}
	case machine.Read:
		state, cursor, err := h.st.Read(ctx, e.Scope, e.Incarnation, e.Query, e.Page)
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
	case machine.Commit:
		gen, err := h.st.Writer(e.Scope, e.Incarnation, e.Term).Commit(ctx, e.Expect, e.Mutation)
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
		return machine.Planned{Err: err, Violations: violations, Plan: plan, Token: e.Token}
	default:
		tb.Fatalf("the machine returned a %T, which the harness does not perform", e)
		return nil
	}
}

// view passes the view of the controller processes procs to the machine, and runs the
// machine afterwards. The view has the namespace incarnation of the WatchViews that runs. A
// runtime watches the membership in that incarnation.
func (h *harness) view(tb assert.TB, procs ...shard.Node) {
	tb.Helper()
	assert.NotEqual(tb, h.views, machine.Token(0), "the machine must watch the views")
	h.revision++
	v := membership.View{
		Scope:       rootScope,
		Nodes:       slices.Clone(procs),
		Revision:    h.revision,
		Incarnation: h.incarnation,
	}
	h.step(tb, machine.Viewed{View: v, Token: h.views})
	h.run(tb)
}

// state returns the state of the root scope.
func (h *harness) state(tb assert.TB) store.State {
	tb.Helper()
	state, cursor, err := h.st.Read(assert.Context(tb), rootScope, shard.NamespaceIncarnation{}, store.Query{},
		page.Page{})
	assert.NoError(tb, err, "the store must read the root scope")
	assert.NoError(tb, cursor.Close(), "the cursor must close")
	return state
}

// specProposal returns a spec proposal with the ID n and the base base, whose body is the
// spec of the namespace at the version v, in the incarnation of the root scope.
func (h *harness) specProposal(tb assert.TB, n byte, base, v epoch.Epoch) *proposal.Proposal {
	tb.Helper()
	spec := namespaceSpec()
	spec.Version = v
	body, err := json.Marshal(&spec)
	assert.NoError(tb, err, "the spec must encode")
	return &proposal.Proposal{
		Scope:       rootScope,
		Expires:     origin.Add(time.Hour),
		Body:        body,
		Base:        base,
		Incarnation: h.state(tb).Head.Incarnation,
		ID:          proposal.ID{ID: id.New128([16]byte{0: 0x9e, 15: n})},
		Kind:        proposal.SpecChange,
	}
}

// submitCall returns the Submit of p from caller as the next inbound call.
func (h *harness) submitCall(tb assert.TB, p *proposal.Proposal, caller shard.Identity) machine.Submit {
	tb.Helper()
	return machine.Submit{
		Proposal: p,
		Metadata: protocol.Metadata{Caller: caller, Version: protocol.Current},
		Reply:    h.call(),
	}
}

// namespaceSpec returns the spec of the namespace of the tests.
func namespaceSpec() config.Spec {
	return config.Spec{
		Namespace:    namespace,
		Preset:       builtin.ExclusiveWorker,
		Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
	}
}

// registry returns a registry with every built-in strategy of the support matrix.
func registry(tb assert.TB) *config.Registry {
	tb.Helper()
	r := config.NewRegistry(builtin.Support())
	assert.NoError(tb, builtin.Register(r), "the built-ins must register")
	return r
}

// completeConfig returns a Config that New accepts.
func completeConfig(tb assert.TB) root.Config {
	tb.Helper()
	return root.Config{
		Clock:       fake.New(origin),
		Rand:        constant.NewCounter(half),
		Reporter:    noop.New(),
		Provided:    provided,
		Logger:      slog.New(slog.DiscardHandler),
		Fits:        func(*store.Mutation) error { return nil },
		Registry:    registry(tb),
		Spec:        namespaceSpec(),
		Node:        shard.Node{Address: rootAddress},
		Parallelism: 1,
	}
}

// replyTo returns the reply of the machine to the call to, which must have the type
// machine.Reply[T].
func replyTo[T machine.Result](tb assert.TB, h *harness, to machine.ReplyToken) machine.Reply[T] {
	tb.Helper()
	for _, e := range h.replies {
		if r, ok := e.(machine.Reply[T]); ok && r.To == to {
			return r
		}
	}
	tb.Fatalf("the machine has not replied to the call %d", to)
	return machine.Reply[T]{}
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control

//go:generate go run golang.org/x/tools/cmd/stringer@v0.52.0 -type=state,event -linecomment -output=controller.string_gen.go

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"go.thesmos.sh/core/btree"
	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/fsm"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/engine"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/scope"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// Acquisition completes the mutation m that acquires the scope of a [Controller] at the
// header head of one attempt, and returns the namespace incarnation of the writer. m
// contains the record of the controller. For a head without an incarnation, the mutation
// creates the scope. The root of a new namespace then also puts the mapping and the spec of
// the namespace in m, and returns the incarnation that it generated. The root of an
// existing namespace returns the incarnation of head. The controller of a cell returns the
// incarnation that the root gave it with the cell.
type Acquisition func(head store.Head, m *store.Mutation) shard.NamespaceIncarnation

// Config is the configuration of a [Controller].
type Config struct {
	// Clock reads the time of the records of the scope: the start of each transition, each
	// drain and each admission, and the duration of each pass. It times the reservations
	// and the writer gaps.
	Clock clock.Clock
	// Rand is the source of the jitter of each backoff and of the IDs of new shards.
	Rand rand.Rand
	// Reporter reports the instruments of the controller.
	Reporter telemetry.Reporter
	// Logger logs the errors that do not stop the controller, such as a pass that fails.
	// The controller binds the namespace and the cell of its scope to it.
	Logger *slog.Logger
	// Registry builds the strategies of each spec that a spec proposal proposes.
	Registry *config.Registry
	// Provided maps the roles config.Store and config.Membership to the capabilities of
	// their adapters, which config.Resolve checks.
	Provided map[config.Axis][]config.Capability
	// Fits returns nil when one commit to the scope can apply the mutation m, as
	// store.Store.Fits does for the scope.
	Fits func(m *store.Mutation) error
	// Acquisition completes the mutation of each attempt of the acquisition.
	Acquisition Acquisition
	// Tokens is the counter of the tokens of the machine. The controller and its parts take
	// the token of each of their effects from it.
	Tokens *machine.Token
	// Load is the load of each shard without a reported load, or nil. The root gives each
	// cell a load, because no agent reports the load of a cell. With a nil Load, such a
	// shard has the mean load of the shards with one.
	Load shard.Load
	// Kinds lists the kinds of proposals that the controller applies.
	Kinds []proposal.OperationKind
	// Operators lists the identities that may submit a proposal, and request or end any
	// disruption.
	Operators []shard.Identity
	// Scope is the scope of the controller.
	Scope shard.Scope
	// Node is the description of the process of the controller. The record of the
	// controller contains its Address and its Identity.
	Node shard.Node
	// Term is the fence epoch of the controller.
	Term epoch.Epoch
	// Parallelism is the most evaluations that a pass runs at once, such as the CPU limit
	// of the process.
	Parallelism int
	// Unauthenticated makes the controller skip the identity of each caller, for a
	// transport that authenticates no caller. The checks of epochs and incarnations still
	// apply.
	Unauthenticated bool
	// Unreported is true for a scope whose nodes do not report, such as the root scope.
	// The controller then ends the writer gap of a shard at the commit that ends the
	// transition of the shard with a writer.
	Unreported bool
}

// validate returns an error that wraps ErrConfig for the first field of c that New refuses.
// scope.New checks Rand, Tokens, the namespace and Term.
func (c *Config) validate() error {
	if c.Clock == nil {
		return fmt.Errorf("%w: no clock", ErrConfig)
	}
	if c.Reporter == nil {
		return fmt.Errorf("%w: no reporter", ErrConfig)
	}
	if c.Logger == nil {
		return fmt.Errorf("%w: no logger", ErrConfig)
	}
	if c.Registry == nil {
		return fmt.Errorf("%w: no registry", ErrConfig)
	}
	if c.Fits == nil {
		return fmt.Errorf("%w: no bound of a commit", ErrConfig)
	}
	if c.Acquisition == nil {
		return fmt.Errorf("%w: no acquisition", ErrConfig)
	}
	if c.Node.Address == "" {
		return fmt.Errorf("%w: no address of the controller", ErrConfig)
	}
	if c.Parallelism < 1 {
		return fmt.Errorf("%w: the parallelism %d is below one", ErrConfig, c.Parallelism)
	}
	if i := slices.IndexFunc(c.Kinds, func(k proposal.OperationKind) bool { return !k.Valid() }); i >= 0 {
		return fmt.Errorf("%w: the kind %v is not valid", ErrConfig, c.Kinds[i])
	}
	return nil
}

// state is the state of a Controller in its lifecycle.
type state uint8

// The states of a Controller.
const (
	// stateIdle is the state before Acquire.
	stateIdle state = 1 // idle
	// stateAcquiring is the acquisition of the scope.
	stateAcquiring state = 2 // acquiring
	// stateServing is the state in which the controller runs its transitions and serves its
	// calls.
	stateServing state = 3 // serving
	// stateFailed is the state after an error, until the machine stops the controller.
	stateFailed state = 4 // failed
	// stateStopped is the state after Stop and Stopped.
	stateStopped state = 5 // stopped
)

// event is an event of the lifecycle of a Controller.
type event uint8

// The events of a Controller.
const (
	// eventAcquire is the call of Acquire.
	eventAcquire event = 1 // acquire
	// eventAcquired is the snapshot of the acquisition of the scope.
	eventAcquired event = 2 // acquired
	// eventFail is an error of a call of Step or Accept.
	eventFail event = 3 // fail
	// eventStop is a call of Stop or Stopped.
	eventStop event = 4 // stop
)

// lifecycle is the table of the states of a Controller. When the table has no edge for an
// event, the call that sent the event returns the error of fsm.Machine.Fire.
var lifecycle = fsm.Must(fsm.NewBuilder[state, event, struct{}](stateIdle).
	Edge(stateIdle, eventAcquire, stateAcquiring).
	Edge(stateAcquiring, eventAcquired, stateServing).
	Edges([]state{stateIdle, stateAcquiring, stateServing}, eventFail, stateFailed).
	Edges([]state{stateIdle, stateAcquiring, stateServing, stateFailed}, eventStop, stateStopped).
	Terminal(stateStopped).
	Build())

// call is the kind of an inbound call. The type of the reply depends on the kind.
type call uint8

// The kinds of inbound calls.
const (
	// callReport is a Report, with a Reply[protocol.Ack].
	callReport call = 1
	// callSubmit is a Submit, with a Reply[protocol.Receipt].
	callSubmit call = 2
	// callDisrupt is a Disrupt, with a Reply[budget.Decision].
	callDisrupt call = 3
	// callDone is a Done, with a Reply[struct{}].
	callDone call = 4
)

// followup is the reply to a Submit, a Disrupt or a Done that the controller sends once a
// commit has applied.
type followup struct {
	// result is the Result of the receipt of a Submit.
	result []byte
	// decision is the decision of a Disrupt.
	decision budget.Decision
	// proposal is the ID of the proposal of a Submit, and the zero ID for another call.
	proposal proposal.ID
	// to is the token of the call.
	to machine.ReplyToken
}

// pending is a commit in flight, with what the controller does once the commit has
// applied.
type pending struct {
	// mutation is the mutation of the commit.
	mutation *store.Mutation
	// followups lists the replies that wait for the commit.
	followups []followup
	// batch reports that the commit is a batch of the engine.
	batch bool
}

// Result is what one call of a [Controller] completed, for its machine.
//
// The zero Result is the result of a call that completed neither.
type Result struct {
	// Err is the error that stopped the Controller, or nil.
	Err error
	// Acquired reports that the call completed the acquisition of the scope. The
	// Controller serves from then on.
	Acquired bool
}

// Controller is the controller of one scope of a namespace, as a part of a machine. It
// acquires the scope at its term and continues the transitions of the scope. It plans the
// moves of the shards of the scope and runs them through its engine. It serves the reports
// of the agents, the proposals of operators and the disruptions of cluster managers. The
// machine passes each event to Step.
//
// The zero Controller is not usable. Construct one with [New].
//
// # Concurrency
//
// A Controller is not safe for concurrent use. Its machine calls it during Machine.Step.
type Controller struct {
	// budget is the budget of the scope.
	budget budgets
	// clock reads the time of the records of the scope.
	clock clock.Clock
	// rand is the source of the jitter of each backoff.
	rand rand.Rand
	// logger logs the errors that do not stop the controller.
	logger *slog.Logger
	// registry builds the strategies of each spec that a spec proposal proposes.
	registry *config.Registry
	// provided lists the capabilities of the adapters for config.Resolve.
	provided map[config.Axis][]config.Capability
	// acquisition completes the mutation of each attempt of the acquisition.
	acquisition Acquisition
	// fits bounds one commit to the scope.
	fits func(m *store.Mutation) error
	// tokens is the counter of the tokens of the machine.
	tokens *machine.Token
	// newer is a newer accepted spec that the next pass applies, or nil.
	newer *Strategies
	// spec is the spec that the controller applies, and nil before Acquire.
	spec *Strategies
	// ids generates the IDs of new shards.
	ids *ulid.Generator
	// metrics are the instruments of the controller.
	metrics *metrics
	// scope writes the scope.
	scope *scope.Scope
	// engine runs the transitions, and is nil before the acquisition.
	engine *engine.Engine
	// assignment is the committed state of the scope, and nil before the acquisition.
	assignment *assignment
	// commits maps the ID of each commit in flight to what follows it.
	commits map[store.CommitID]*pending
	// operators contains the identities of the operators.
	operators map[shard.Identity]bool
	// proposing contains the IDs of the proposals that the controller has taken and has not
	// finished.
	proposing map[proposal.ID]bool
	// applied maps the ID of each proposal that the scope has applied to its Expires, in
	// the order of the IDs.
	applied *btree.MapFunc[proposal.ID, time.Time]
	// gaps maps each shard whose writer gap runs to the gap.
	gaps map[shard.ID]gap
	// load is the load of each shard without a reported load, or nil.
	load shard.Load
	// kinds lists the kinds of proposals that the controller applies.
	kinds []proposal.OperationKind
	// accepted is the accepted spec of the namespace as JSON, with the commit of a spec
	// proposal in flight. It is empty in the scope of a cell.
	accepted []byte
	// scopeOf is the scope of the controller.
	scopeOf shard.Scope
	// rng is the range of positions of the scope, which Acquire takes.
	rng shard.Range
	// lc is the state of the controller in its lifecycle.
	lc fsm.Machine[state, event, struct{}]
	// node is the description of the process of the controller.
	node shard.Node
	// calls maps the token of each inbound call that waits for its reply to its kind, in
	// the order of the tokens.
	calls btree.Map[machine.ReplyToken, call]
	// passes is the state of the planning passes.
	passes passes
	// members is the membership of the scope.
	members members
	// term is the fence epoch of the controller.
	term epoch.Epoch
	// reports counts the reports that the controller accepted in its term.
	reports uint64
	// parallelism is the most evaluations that a pass runs at once.
	parallelism int
	// incarnation is the namespace incarnation of the writer, which the last attempt of
	// the acquisition returned.
	incarnation shard.NamespaceIncarnation
	// unauthenticated makes the controller skip the identity of callers.
	unauthenticated bool
	// unreported is true for a scope whose nodes do not report.
	unreported bool
}

// New returns the Controller of cfg before its acquisition.
//
// Error modes:
//   - an error that wraps ErrConfig for a nil cfg, and for a cfg without Clock, Reporter,
//     Logger, Registry, Fits, Acquisition or the address of Node, with a Parallelism below
//     one, or with a kind that is not Valid.
//   - an error that wraps ErrConfig and the error of scope.New for a cfg without Rand,
//     Tokens, the namespace of Scope, or Term.
func New(cfg *Config) (*Controller, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: no configuration", ErrConfig)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	byID := func(a, b proposal.ID) int { return a.Compare(b.ID) }
	c := &Controller{
		clock: cfg.Clock,
		rand:  cfg.Rand,
		logger: cfg.Logger.With(
			slog.String(attrNamespace, string(cfg.Scope.Namespace)),
			slog.String(attrCell, string(cfg.Scope.Cell)),
		),
		registry:        cfg.Registry,
		provided:        cfg.Provided,
		acquisition:     cfg.Acquisition,
		fits:            cfg.Fits,
		tokens:          cfg.Tokens,
		ids:             ulid.New(cfg.Clock, cfg.Rand),
		metrics:         newMetrics(cfg.Reporter, cfg.Scope),
		commits:         make(map[store.CommitID]*pending),
		operators:       make(map[shard.Identity]bool, len(cfg.Operators)),
		proposing:       make(map[proposal.ID]bool),
		applied:         btree.NewMapFunc[proposal.ID, time.Time](byID),
		gaps:            make(map[shard.ID]gap),
		load:            maps.Clone(cfg.Load),
		kinds:           slices.Clone(cfg.Kinds),
		budget:          newBudgets(),
		passes:          passes{plans: make(map[machine.Token]*plan), moving: make(map[shard.ID]moving)},
		lc:              lifecycle.Start(struct{}{}),
		node:            cfg.Node,
		scopeOf:         cfg.Scope,
		term:            cfg.Term,
		parallelism:     cfg.Parallelism,
		unauthenticated: cfg.Unauthenticated,
		unreported:      cfg.Unreported,
	}
	for _, o := range cfg.Operators {
		c.operators[o] = true
	}
	sc, err := scope.New(&scope.Config{
		Rand:        cfg.Rand,
		Unknown:     c.metrics.unknown,
		Acquisition: c.attempt,
		Tokens:      cfg.Tokens,
		Scope:       cfg.Scope,
		Term:        cfg.Term,
	})
	if err != nil {
		c.metrics.release()
		return nil, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	c.scope = sc
	return c, nil
}

// Acquire starts the acquisition of the scope, and appends the read of its header to out.
// The Controller applies the strategies s from then on, and rng is the range of positions
// of the scope, which the record of the controller contains.
//
// Error modes:
//   - an error that wraps fsm.ErrRejected after the first call, after an error and after
//     the Controller has stopped.
func (c *Controller) Acquire(s *Strategies, rng shard.Range, out []machine.Effect) ([]machine.Effect, error) {
	if err := c.fire(eventAcquire); err != nil {
		return out, err
	}
	c.spec, c.rng = s, rng
	return c.scope.Acquire(out), nil
}

// Step applies ev to the Controller when ev is an inbound call that the Controller serves,
// or the result of an effect of the Controller or of its parts. It appends the effects of
// ev to out, and the Result reports what ev completed. Step reports false for every other
// event and returns out unchanged. Such an event belongs to another part of the machine. A
// Controller takes no event after an error and after it has stopped.
//
// An error of the event fails the Controller. Step then drops the effects of the event,
// and Result.Err reports the error. The machine stops a failed Controller with Stop.
func (c *Controller) Step(ev machine.Event, out []machine.Effect) (Result, []machine.Effect, bool) {
	from, n := c.lc.State(), len(out)
	if from == stateFailed || from == stateStopped {
		return Result{}, out, false
	}
	next, ok, err := c.step(ev, out)
	if err != nil {
		return Result{Err: errors.Join(err, c.fire(eventFail))}, out[:n], true
	}
	return Result{Acquired: c.lc.State() != from}, next, ok
}

// Accept takes s, the strategies of an accepted spec of the namespace, and appends the
// effects of s to out. The next pass adopts s when its version is above the version of
// the spec that the Controller applies or that waits for a pass, and s then starts a pass.
// The Controller ignores any other s. A Controller returns out unchanged before Acquire,
// after an error and after it has stopped.
//
// An error of the pass that s starts fails the Controller, as an error of an event in Step
// does, and Result.Err reports the error.
func (c *Controller) Accept(s *Strategies, out []machine.Effect) (Result, []machine.Effect) {
	if st := c.lc.State(); st != stateAcquiring && st != stateServing {
		return Result{}, out
	}
	n := len(out)
	next, err := c.accept(s, out)
	if err != nil {
		return Result{Err: errors.Join(err, c.fire(eventFail))}, out[:n]
	}
	return Result{}, next
}

// Size returns the number of nodes in the last view of the scope, and the number of shards
// of its layout. Both are zero before the acquisition, because the Controller watches its
// views from the acquisition on.
func (c *Controller) Size() (int, int) {
	if c.assignment == nil {
		return 0, 0
	}
	return len(c.members.nodes), len(c.assignment.layout.Shards)
}

// Stop stops the Controller while the run of its machine continues, such as after an error
// of the Controller. It ends the watch of its views, replies with ErrNotServing to every
// call that waits for its reply, and releases its instruments. A stopped Controller takes
// no event.
//
// Error modes:
//   - an error that wraps fsm.ErrRejected for a Controller that has stopped.
func (c *Controller) Stop(out []machine.Effect) ([]machine.Effect, error) {
	if err := c.fire(eventStop); err != nil {
		return out, err
	}
	return c.halt(c.members.unwatch(out)), nil
}

// Stopped stops the Controller at the Stopped of the run of its machine. It replies with
// ErrNotServing to every call that waits for its reply and releases its instruments. It
// appends no other effect, because the runtime ends every watch of the run.
//
// Error modes:
//   - an error that wraps fsm.ErrRejected for a Controller that has stopped.
func (c *Controller) Stopped(out []machine.Effect) ([]machine.Effect, error) {
	if err := c.fire(eventStop); err != nil {
		return out, err
	}
	return c.halt(out), nil
}

// step passes ev to the handler of its type, or to the part of the Controller whose effect
// it is the result of. It reports whether ev is for the Controller, and returns the error
// that stops the Controller, or nil.
func (c *Controller) step(ev machine.Event, out []machine.Effect) ([]machine.Effect, bool, error) {
	var err error
	switch ev := ev.(type) {
	case machine.Report:
		out, err = c.report(&ev, out)
		return out, true, err
	case machine.Submit:
		out, err = c.submit(&ev, out)
		return out, true, err
	case machine.Disrupt:
		out, err = c.disrupt(&ev, out)
		return out, true, err
	case machine.Done:
		out, err = c.done(&ev, out)
		return out, true, err
	case machine.Cancelled:
		_, ok := c.calls.Delete(ev.Reply)
		return out, ok, nil
	case machine.Viewed:
		return c.viewed(&ev, out)
	case machine.Planned:
		return c.planned(&ev, out)
	}
	if res, next, ok := c.scope.Step(ev, out); ok {
		next, err = c.scoped(&res, next)
		return next, true, err
	}
	if c.engine != nil {
		if next, ok, err := c.engine.Step(ev, out); ok {
			next, err = c.batchAfter(next, err)
			return next, true, err
		}
	}
	if ev, ok := ev.(machine.Elapsed); ok {
		return c.elapsed(ev, out)
	}
	return out, false, nil
}

// fire delivers ev to the lifecycle of c. When the lifecycle refuses ev, fire returns the
// error of fsm.Machine.Fire with the event, the scope and the state of c.
func (c *Controller) fire(ev event) error {
	if st, err := c.lc.Fire(ev); err != nil {
		return fmt.Errorf("control: the event %v of %v in the state %v: %w", ev, c.scopeOf, st, err)
	}
	return nil
}

// attempt returns the incarnation and the mutation of one attempt of the acquisition at the
// header head: the record of the controller, which Config.Acquisition completes. The
// incarnation of the last attempt is the incarnation of the scope once the acquisition has
// applied.
func (c *Controller) attempt(head store.Head) (shard.NamespaceIncarnation, *store.Mutation) {
	m := &store.Mutation{Cell: c.record(c.spec.version)}
	c.incarnation = c.acquisition(head, m)
	return c.incarnation, m
}

// record returns the record of the controller under the spec of the version v: its
// address, its identity, the range of its scope and its term. The record of the root has
// no cell.
func (c *Controller) record(v epoch.Epoch) *shard.Cell {
	return &shard.Cell{
		ID:                 c.scopeOf.Cell,
		Controller:         c.node.Address,
		ControllerIdentity: c.node.Identity,
		Range:              c.rng,
		Epoch:              c.term,
		SpecVersion:        v,
	}
}

// accept takes s as the spec of the next pass when its version is above the version of the
// spec that c applies or that waits, and starts a pass.
func (c *Controller) accept(s *Strategies, out []machine.Effect) ([]machine.Effect, error) {
	latest := c.spec
	if c.newer != nil {
		latest = c.newer
	}
	if s.version <= latest.version {
		return out, nil
	}
	c.newer = s
	return c.trigger(out)
}

// scoped takes what the writer of the scope completed: its acquisition, a commit or its
// end.
func (c *Controller) scoped(res *scope.Result, out []machine.Effect) ([]machine.Effect, error) {
	switch {
	case res.Err != nil:
		return out, res.Err
	case res.Snapshot != nil:
		return c.acquired(res.Snapshot, out)
	case res.Commit != (store.CommitID{}):
		return c.apply(res.Commit, res.Generation, out)
	}
	return out, nil
}

// acquired takes the snapshot of the acquisition of the scope. The controller builds its
// engine, its assignment and its budget from it, continues every transition of the
// snapshot, and then serves: it watches the views of its membership and arms the timers of
// its reservations.
//
// Error modes:
//   - the error of the partitioner for a range that it does not divide.
//   - the error of newAssignment for a placement whose range starts where no span starts.
//   - the error of engine.New.
//   - the error of engine.Engine.Resume for a record that the engine does not continue.
func (c *Controller) acquired(snap *scope.Snapshot, out []machine.Effect) ([]machine.Effect, error) {
	spans, err := c.spec.partitioner.Initial(c.rng)
	if err == nil {
		c.assignment, err = newAssignment(snap, c.scopeOf.Cell, spans, c.ids, c.spec.writer)
	}
	if err == nil {
		c.engine, err = engine.New(&engine.Config{
			Settings:    c.spec.settings,
			Clock:       c.clock,
			Rand:        c.rand,
			Tokens:      c.tokens,
			Fits:        c.fits,
			Scope:       c.scopeOf,
			Term:        c.term,
			Incarnation: c.incarnation,
		})
	}
	for i := 0; err == nil && i < len(snap.State.Transitions); i++ {
		t := &snap.State.Transitions[i]
		p := c.assignment.placement(t.Operation.Shard)
		out, err = c.engine.Resume(t, &p, out)
	}
	if err == nil {
		err = c.fire(eventAcquired)
	}
	if err != nil {
		return out, err
	}
	c.budget.restore(&snap.State)
	for id, expires := range snap.State.Applied {
		c.applied.Set(id, expires)
	}
	c.accepted = snap.State.Spec
	out = c.watchViews(out)
	out = c.periodic(c.arm(out))
	c.gauge()
	return c.batchAfter(out, nil)
}

// elapsed takes the end of a Timer of the controller: the backoff of the watch of the
// views, the interval of the periodic passes, or a timer of a reservation. It reports
// whether the Timer is of the controller.
func (c *Controller) elapsed(ev machine.Elapsed, out []machine.Effect) ([]machine.Effect, bool, error) {
	switch ev.Token {
	case c.members.wait:
		return c.watchViews(out), true, nil
	case c.passes.interval:
		c.passes.interval = 0
		var err error
		out, err = c.trigger(c.periodic(out))
		return out, true, err
	}
	return c.expire(ev.Token, out)
}

// batchAfter commits the next batch of the engine. err is the error of the call of the
// engine before it, and batchAfter returns err joined with the error of the commit. The
// controller stops at an error and drops the effects of the step, so the commit of a batch
// after an error of the engine never runs.
func (c *Controller) batchAfter(out []machine.Effect, err error) ([]machine.Effect, error) {
	b := c.engine.Batch()
	if b == nil {
		return out, err
	}
	p := &pending{mutation: b, batch: true}
	for i := range b.Transitions {
		s := b.Transitions[i].Operation.Shard
		if w, ok := c.passes.moving[s]; ok {
			delete(c.passes.moving, s)
			b.Applied = append(b.Applied, w.applied)
			p.followups = append(p.followups, w.followup)
		}
	}
	out, cerr := c.commit(p, out)
	return out, errors.Join(err, cerr)
}

// commit commits the mutation of p after the commits in flight, and keeps p until the
// commit applies. The reservations, the drains and the undrains of the mutation take
// effect in the budget at once.
//
// Error modes:
//   - the error of Config.Fits for a mutation that one commit cannot apply.
//   - the error of scope.Scope.Commit.
func (c *Controller) commit(p *pending, out []machine.Effect) ([]machine.Effect, error) {
	err := c.fits(p.mutation)
	var commit store.CommitID
	if err == nil {
		commit, out, err = c.scope.Commit(p.mutation, out)
	}
	if err != nil {
		return out, fmt.Errorf("control: a commit to %v: %w", c.scopeOf, err)
	}
	c.commits[commit] = p
	c.budget.commit(p.mutation)
	return out, nil
}

// apply takes the commit id that created the generation gen:
//   - The controller starts the writer gaps of the commit, and applies the mutation of the
//     commit to its assignment.
//   - The reservations that the commit releases end.
//   - In a scope whose nodes do not report, the commit also ends writer gaps.
//   - The controller then replies to the calls that wait for the commit.
//   - After a batch of the engine, the controller continues the transitions.
//
// These commits start a pass:
//   - a commit that leaves a shard without a writer.
//   - a commit that adds or removes a drain.
//   - a commit that ends a transition while the bounds refused a move of the last plan.
func (c *Controller) apply(id store.CommitID, gen epoch.Epoch, out []machine.Effect) ([]machine.Effect, error) {
	p := c.commits[id]
	delete(c.commits, id)
	mu := p.mutation
	c.stops(mu)
	c.assignment.apply(mu, gen)
	if c.unreported {
		c.completed(mu)
	}
	c.budget.apply(mu)
	for i := range mu.Applied {
		a := &mu.Applied[i].Proposal
		c.applied.Set(a.ID, a.Expires)
	}
	for _, f := range mu.Forget {
		c.applied.Delete(f)
	}
	for i := range p.followups {
		out = c.follow(&p.followups[i], gen, out)
	}
	out = c.arm(out)
	c.gauge()
	var err error
	if p.batch {
		out, err = c.engine.Applied(out)
		out, err = c.batchAfter(out, err)
		var serr error
		out, serr = c.settle(out)
		err = errors.Join(err, serr)
	}
	if err != nil {
		return out, err
	}
	if c.lost(mu) || len(mu.Drain) > 0 || len(mu.Undrain) > 0 || len(mu.Done) > 0 && c.passes.refused {
		return c.trigger(out)
	}
	return out, nil
}

// follow sends the reply of f to its call after the commit that created the generation gen.
// The proposal of a Submit then has applied.
func (c *Controller) follow(f *followup, gen epoch.Epoch, out []machine.Effect) []machine.Effect {
	delete(c.proposing, f.proposal)
	kind, _ := c.calls.Get(f.to)
	switch kind {
	case callSubmit:
		receipt := protocol.Receipt{Result: f.result, Generation: gen, Incarnation: c.incarnation, Applied: true}
		return reply(c, f.to, receipt, nil, out)
	case callDisrupt:
		return reply(c, f.to, f.decision, nil, out)
	case callDone:
		return reply(c, f.to, struct{}{}, nil, out)
	case callReport:
		// The controller replies to a report in the step of the report.
	}
	return out
}

// lost reports whether the mutation mu leaves a shard without a writer, which the next pass
// places again.
func (c *Controller) lost(mu *store.Mutation) bool {
	for i := range mu.Put {
		if _, ok := writer(&mu.Put[i], c.spec.writer); !ok {
			return true
		}
	}
	return false
}

// serve checks that the controller serves the call md for the scope s and the namespace
// incarnation inc, and returns the error of the reply otherwise.
func (c *Controller) serve(md *protocol.Metadata, s shard.Scope, inc shard.NamespaceIncarnation) error {
	if c.lc.State() != stateServing {
		return ErrNotServing
	}
	if err := md.Check(); err != nil {
		return fmt.Errorf("control: the metadata of the call: %w", err)
	}
	if s != c.scopeOf {
		return fmt.Errorf("%w: the call is for the scope %v, and the controller has %v", ErrCall, s, c.scopeOf)
	}
	if inc != c.incarnation {
		return fmt.Errorf("%w: the call is for the namespace incarnation %v", shard.ErrIncarnation, inc.ID)
	}
	return nil
}

// halt replies with ErrNotServing to every call that waits for its reply, in the order of
// their tokens. It then releases the instruments of the controller.
func (c *Controller) halt(out []machine.Effect) []machine.Effect {
	for to, kind := range c.calls.All() {
		switch kind {
		case callReport:
			// The controller replies to each report in the step of the report, so no
			// report waits.
		case callSubmit:
			out = reply(c, to, protocol.Receipt{}, ErrNotServing, out)
		case callDisrupt:
			out = reply(c, to, budget.Decision{}, ErrNotServing, out)
		case callDone:
			out = reply(c, to, struct{}{}, ErrNotServing, out)
		}
	}
	c.metrics.release()
	return out
}

// next returns the next token of the machine.
func (c *Controller) next() machine.Token {
	*c.tokens++
	return *c.tokens
}

// reply appends the reply of the call to, with value and err, to out. It appends nothing
// for a call that has a reply or that the runtime has cancelled.
func reply[T machine.Result](c *Controller, to machine.ReplyToken, value T, err error,
	out []machine.Effect,
) []machine.Effect {
	if _, ok := c.calls.Delete(to); !ok {
		return out
	}
	return append(out, machine.Reply[T]{Value: value, Err: err, To: to})
}

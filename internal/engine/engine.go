// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/fsm"
	"go.thesmos.sh/core/idmap"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/resilience"

	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The retry policy of the callbacks of an Engine.
const (
	// Attempts is the number of attempts of a callback in one round, the first included.
	Attempts = 5
	// RetryBase is the ceiling of the backoff before the first retry of a callback. The
	// ceiling doubles with each later retry.
	RetryBase = 100 * time.Millisecond
	// RetryMax is the highest ceiling of a backoff. A step of an abort path whose round of
	// attempts failed runs again after RetryMax.
	RetryMax = 10 * time.Second
	// RetryBudget is the share of the callbacks of an Engine that can become retries within
	// BudgetWindow.
	RetryBudget = 0.1
	// MinRetries is the number of retries within BudgetWindow that the budget allows,
	// whatever RetryBudget allows.
	MinRetries = 10
	// BudgetWindow is the time over which the budget counts the callbacks and the retries.
	BudgetWindow = time.Minute
)

// Settings are the settings of an [Engine] that a new version of the spec of the namespace
// changes. [Engine.Configure] replaces them.
type Settings struct {
	// Protocol scripts the moves.
	Protocol handoff.Protocol
	// LockDelay is the wait before a drop on a node that has left the view completes.
	LockDelay time.Duration
	// MaxTransitions is the most transitions in flight at once.
	MaxTransitions int
	// MaxNodeTransitions is the most transitions in flight at once that count against one
	// node.
	MaxNodeTransitions int
	// NodeCallbacks is the most callbacks in flight at once to one node.
	NodeCallbacks int
}

// validate returns an error that wraps ErrConfig for the first field of s that New and
// Configure refuse.
func (s *Settings) validate() error {
	if s.Protocol == nil {
		return fmt.Errorf("%w: no protocol", ErrConfig)
	}
	if s.LockDelay < 0 {
		return fmt.Errorf("%w: the lock delay %v is below zero", ErrConfig, s.LockDelay)
	}
	if s.MaxTransitions < 1 {
		return fmt.Errorf("%w: the most transitions %d is below one", ErrConfig, s.MaxTransitions)
	}
	if s.MaxNodeTransitions < 1 {
		return fmt.Errorf("%w: the most transitions of a node %d is below one", ErrConfig, s.MaxNodeTransitions)
	}
	if s.NodeCallbacks < 1 {
		return fmt.Errorf("%w: the most callbacks of a node %d is below one", ErrConfig, s.NodeCallbacks)
	}
	return nil
}

// Config is the configuration of an [Engine].
type Config struct {
	// Clock times the records of the transitions and the window of the retry budget.
	Clock clock.Clock
	// Rand is the source of the jitter of each backoff.
	Rand rand.Rand
	// Tokens is the counter of the tokens of the machine. The Engine takes the token of each
	// of its effects from it. The other parts of the machine share it.
	Tokens *machine.Token
	// Fits returns nil when one commit can apply the mutation m, as store.Store.Fits does
	// for the scope of the transitions.
	Fits func(m *store.Mutation) error
	// Scope is the scope of the transitions.
	Scope shard.Scope
	// Settings are the settings that a new version of the spec changes.
	Settings
	// Term is the fence epoch of the controller. Each callback has it as its controller
	// epoch.
	Term epoch.Epoch
	// Incarnation is the namespace incarnation of the operations of the transitions.
	Incarnation shard.NamespaceIncarnation
}

// validate returns an error that wraps ErrConfig for the first field of c that New
// refuses. resilience.NewRetrier checks Clock and Rand.
func (c *Config) validate() error {
	if c.Tokens == nil {
		return fmt.Errorf("%w: no counter of tokens", ErrConfig)
	}
	if c.Fits == nil {
		return fmt.Errorf("%w: no bound of a commit", ErrConfig)
	}
	if c.Scope.Namespace == "" {
		return fmt.Errorf("%w: no namespace", ErrConfig)
	}
	if c.Incarnation.IsZero() {
		return fmt.Errorf("%w: no namespace incarnation", ErrConfig)
	}
	if c.Term == epoch.Zero {
		return fmt.Errorf("%w: no term", ErrConfig)
	}
	return c.Settings.validate()
}

// Move is a move of the replica of one shard. The machine passes it to [Engine.Start].
type Move struct {
	// From is the side of the node that the move takes the replica from. It is the zero
	// Endpoint for a move that only adds a replica.
	From store.Endpoint
	// To is the side of the node that the move gives the replica to.
	To store.Endpoint
	// Fallback is the side of the node that becomes the writer on the abort path. It is
	// the zero Endpoint when no node qualifies.
	Fallback store.Endpoint
	// Placement is the placement of the shard before the move. Its Epoch, Reserved and
	// Sequence give the epochs and the sequence of the move.
	Placement shard.Placement
	// SpecVersion is the version of the spec under which the machine planned the move.
	SpecVersion epoch.Epoch
}

// failure is the way in which the current step of a transition failed.
type failure uint8

// The failures of a step.
const (
	// exhausted is the failure of a step whose round of attempts ended without success.
	exhausted failure = 1
	// definite is the failure of a step that a retry cannot fix.
	definite failure = 2
)

// Engine runs the transitions of the scope of a controller. It runs the steps of each
// transition in order, and commits the changes of the transitions in batches. The machine
// passes each event to Step. Step takes the events of the callbacks and the Timers of the
// Engine.
//
// The zero Engine is not usable. Construct one with [New].
//
// # Concurrency
//
// An Engine is not safe for concurrent use. Its machine calls it during Machine.Step.
type Engine struct {
	// clock times the records of the transitions.
	clock clock.Clock
	// protocol scripts the moves.
	protocol handoff.Protocol
	// retrier decides each retry of a callback.
	retrier *resilience.Retrier
	// tokens is the counter of the tokens of the machine.
	tokens *machine.Token
	// fits returns nil when one commit can apply a mutation.
	fits func(m *store.Mutation) error
	// transitions maps the shard of each transition in flight to the transition.
	transitions *idmap.Map[shard.ID, *transition]
	// waits maps the token of each callback and Timer in flight to its transition.
	waits map[machine.Token]*transition
	// nodes maps each node that a callback has addressed to its permits and its queue.
	nodes map[shard.NodeID]*node
	// view maps each node of the last view to the node, and is nil before the first view.
	view map[shard.NodeID]*shard.Node
	// fault joins the error of each event that the phase or the wait of a transition
	// refused, and is nil while neither has refused an event.
	fault error
	// scope is the scope of the transitions.
	scope shard.Scope
	// order lists the transitions in flight in the order in which they joined the Engine.
	order []*transition
	// nodeOrder lists the nodes of nodes in the order in which they joined the Engine.
	nodeOrder []*node
	// dirty lists the transitions whose changes wait for the next batch, the oldest first.
	dirty []*transition
	// batch lists the transitions of the batch in flight, and is nil without one.
	batch []*transition
	// term is the controller epoch of each callback.
	term epoch.Epoch
	// lockDelay is the wait before a drop on a node that has left the view completes.
	lockDelay time.Duration
	// maxTransitions is the most transitions in flight at once.
	maxTransitions int
	// maxNodeTransitions is the most transitions in flight at once that count against one
	// node.
	maxNodeTransitions int
	// nodeCallbacks is the most callbacks in flight at once to one node.
	nodeCallbacks int
	// incarnation is the namespace incarnation of the operations.
	incarnation shard.NamespaceIncarnation
}

// New returns an Engine of cfg without transitions.
//
// Error modes:
//   - an error that wraps ErrConfig for a nil cfg, and for a cfg without Tokens,
//     Protocol, Fits, the namespace of Scope, Incarnation or Term.
//   - an error that wraps ErrConfig for a negative LockDelay, and for a MaxTransitions, a
//     MaxNodeTransitions or a NodeCallbacks below one.
//   - an error that wraps ErrConfig and the error of resilience.NewRetrier for a cfg
//     without Clock or Rand.
func New(cfg *Config) (*Engine, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: no configuration", ErrConfig)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	retrier, err := resilience.NewRetrier(resilience.RetryConfig{
		Clock:         cfg.Clock,
		Rand:          cfg.Rand,
		Attempts:      Attempts,
		Base:          RetryBase,
		Max:           RetryMax,
		MaxRetryAfter: RetryMax,
		Budget:        RetryBudget,
		MinRetries:    MinRetries,
		BudgetWindow:  BudgetWindow,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	return &Engine{
		clock:              cfg.Clock,
		protocol:           cfg.Protocol,
		retrier:            retrier,
		tokens:             cfg.Tokens,
		fits:               cfg.Fits,
		transitions:        idmap.NewTyped128[shard.ID, *transition](0),
		waits:              make(map[machine.Token]*transition),
		nodes:              make(map[shard.NodeID]*node),
		scope:              cfg.Scope,
		term:               cfg.Term,
		lockDelay:          cfg.LockDelay,
		maxTransitions:     cfg.MaxTransitions,
		maxNodeTransitions: cfg.MaxNodeTransitions,
		nodeCallbacks:      cfg.NodeCallbacks,
		incarnation:        cfg.Incarnation,
	}, nil
}

// Start starts the transition of m when the bounds admit it, and reports whether they
// did. The transition reserves the sequence after m.Placement.Sequence and the two epochs
// after m.Placement.Reserved. The next batch commits its record, with the placement and
// its new reserved epoch and sequence. A move that the bounds refuse does not change the
// Engine.
//
// Error modes:
//   - an error that wraps ErrMove for a move of a shard that has a transition in flight.
//   - an error that wraps ErrMove and the error of the protocol for a move that the
//     protocol cannot script, and for a script that handoff.Script.Validate refuses.
//   - an error that wraps fsm.ErrRejected when the phase or the wait of a transition
//     refuses an event.
func (e *Engine) Start(m *Move) (bool, error) {
	id := m.Placement.Shard
	if _, ok := e.transitions.Get(id); ok {
		return false, fmt.Errorf("%w: shard %v has a transition in flight", ErrMove, id)
	}
	if len(e.order) >= e.maxTransitions || e.full(m) {
		return false, nil
	}
	transfer := handoff.Transfer{From: m.From.Node, To: m.To.Node, Fallback: m.Fallback.Node}
	script, err := e.protocol.Script(transfer)
	if err == nil {
		err = script.Validate(transfer)
	}
	if err != nil {
		return false, fmt.Errorf("%w: shard %v: %w", ErrMove, id, err)
	}
	reserved := m.Placement.Reserved
	t := &transition{
		script:  script,
		phase:   handoff.Lifecycle.Start(struct{}{}),
		waiting: waitLifecycle.Start(struct{}{}),
		record: store.Transition{
			Started:  e.clock.Time(),
			Protocol: e.protocol.Name(),
			Fallback: m.Fallback,
			From:     m.From,
			To:       m.To,
			Operation: shard.OperationID{
				Incarnation: e.incarnation,
				Shard:       id,
				Sequence:    m.Placement.Sequence + 1,
			},
			SpecVersion:     m.SpecVersion,
			OldEpoch:        m.Placement.Epoch,
			NewEpoch:        reserved + 1,
			AbortEpoch:      reserved + 2,
			ProtocolVersion: e.protocol.Version(),
			Phase:           handoff.Lifecycle.Initial(),
		},
		placement: m.Placement,
		put:       true,
	}
	t.placement.Reserved, t.placement.Sequence = t.record.AbortEpoch, t.record.Operation.Sequence
	e.add(t)
	e.advance(t)
	return true, e.fault
}

// Resume continues the transition of the record rec of a snapshot at its committed phase
// and step, and appends the callback of its current step to out. p is the placement of
// the shard of rec. A transition that Resume continues counts against the bounds, also
// when it takes the Engine above them.
//
// Error modes:
//   - an error that wraps ErrProtocol for a record of another protocol, or of another
//     version of the protocol.
//   - an error that wraps ErrRecord for a record with a negative step.
//   - an error that wraps ErrProtocol and fsm.ErrState for a record whose phase
//     handoff.Lifecycle does not have.
//   - an error that wraps ErrRecord for a second record of one shard.
//   - an error that wraps ErrRecord for a record whose transfer the protocol cannot script.
//   - an error that wraps fsm.ErrRejected for a record in a terminal phase, because the
//     phase of a transition that has ended refuses Finish. The error also wraps
//     fsm.ErrRejected when the phase or the wait of a transition refuses another event.
func (e *Engine) Resume(rec *store.Transition, p *shard.Placement, out []machine.Effect) ([]machine.Effect, error) {
	id := rec.Operation.Shard
	if rec.Protocol != e.protocol.Name() || rec.ProtocolVersion != e.protocol.Version() {
		return out, fmt.Errorf("%w: shard %v has the protocol %s version %d, and the engine runs %s version %d",
			ErrProtocol, id, rec.Protocol, rec.ProtocolVersion, e.protocol.Name(), e.protocol.Version())
	}
	if rec.Step < 0 {
		return out, fmt.Errorf("%w: shard %v has the step %d", ErrRecord, id, rec.Step)
	}
	phase, err := handoff.Lifecycle.Resume(rec.Phase, struct{}{})
	if err != nil {
		return out, fmt.Errorf("%w: shard %v has the phase %v: %w", ErrProtocol, id, rec.Phase, err)
	}
	if _, ok := e.transitions.Get(id); ok {
		return out, fmt.Errorf("%w: shard %v has a second record", ErrRecord, id)
	}
	script, err := e.protocol.Script(
		handoff.Transfer{From: rec.From.Node, To: rec.To.Node, Fallback: rec.Fallback.Node},
	)
	if err != nil {
		return out, fmt.Errorf("%w: shard %v: %w", ErrRecord, id, err)
	}
	t := &transition{
		phase:     phase,
		waiting:   waitLifecycle.Start(struct{}{}),
		script:    script,
		placement: *p,
		record:    *rec,
	}
	e.add(t)
	if path := t.path(); t.record.Step < len(path) && path[t.record.Step].Action != handoff.Commit {
		return e.begin(t, out), e.fault
	}
	e.advance(t)
	return out, e.fault
}

// View replaces the view of e with the membership view v, and appends the callbacks that
// v lets e send to out. The current step of a transition ends once the incarnation of its
// node has ended in v. v shares memory with e, so the machine does not modify it.
//
// Error modes:
//   - an error that wraps fsm.ErrRejected when the phase or the wait of a transition
//     refuses an event.
func (e *Engine) View(v *membership.View, out []machine.Effect) ([]machine.Effect, error) {
	e.view = make(map[shard.NodeID]*shard.Node, len(v.Nodes))
	for i := range v.Nodes {
		e.view[v.Nodes[i].ID] = &v.Nodes[i]
	}
	for _, t := range e.order {
		switch t.waiting.State() {
		case waitPermit, waitCall, waitRetry, waitRound:
			// The callback of the current step has not completed.
			if e.ended(t.endpoint(t.step().Side)) {
				out = e.lapse(t, e.cancel(t, out))
			}
		case waitNone, waitBatch, waitLapse:
			// The current step has no callback in flight.
		}
	}
	for _, n := range e.nodeOrder {
		out = e.drain(n, out)
	}
	return out, e.fault
}

// Step takes ev when it is the result of a callback or a Timer of e, and appends the
// effects that follow ev to out. Step reports false for every other event and returns out
// unchanged. Such an event belongs to another part of the machine.
//
// Error modes:
//   - an error that wraps fsm.ErrRejected when the phase or the wait of a transition
//     refuses an event.
func (e *Engine) Step(ev machine.Event, out []machine.Effect) ([]machine.Effect, bool, error) {
	switch ev := ev.(type) {
	case machine.Called:
		if t, ok := e.waits[ev.Token]; ok {
			delete(e.waits, ev.Token)
			return e.called(t, ev.Err, out), true, e.fault
		}
	case machine.Elapsed:
		if t, ok := e.waits[ev.Token]; ok {
			delete(e.waits, ev.Token)
			return e.elapsed(t, out), true, e.fault
		}
	}
	return out, false, nil
}

// Batch returns the changes of the transitions that wait for a commit as the mutation of
// the next commit. It returns nil while a batch is in flight, and when no change waits. A
// batch takes the changes in the order in which they arose, while Config.Fits accepts the
// mutation. The first change always joins the batch.
func (e *Engine) Batch() *store.Mutation {
	if e.batch != nil || len(e.dirty) == 0 {
		return nil
	}
	m := &store.Mutation{}
	n := 0
	for _, t := range e.dirty {
		puts, transitions, done := len(m.Put), len(m.Transitions), len(m.Done)
		t.write(m)
		if n > 0 && e.fits(m) != nil {
			m.Put, m.Transitions, m.Done = m.Put[:puts], m.Transitions[:transitions], m.Done[:done]
			break
		}
		n++
	}
	e.batch = slices.Clone(e.dirty[:n])
	e.dirty = slices.Delete(e.dirty, 0, n)
	return m
}

// Configure replaces the settings of e with s, and appends the callbacks that s lets e
// send to out. A move that starts after the call has the protocol of s, and a transition in
// flight keeps the script that it has. A lower bound does not cancel a transition or a
// callback in flight. A higher NodeCallbacks sends the queued callbacks that the new
// permits allow, once e has a view.
//
// Error modes:
//   - an error that wraps ErrConfig for a nil s, and for an s that New refuses in a
//     Config.
//   - an error that wraps fsm.ErrRejected when the phase or the wait of a transition
//     refuses an event.
func (e *Engine) Configure(s *Settings, out []machine.Effect) ([]machine.Effect, error) {
	if s == nil {
		return out, fmt.Errorf("%w: no settings", ErrConfig)
	}
	if err := s.validate(); err != nil {
		return out, err
	}
	e.protocol, e.lockDelay = s.Protocol, s.LockDelay
	e.maxTransitions, e.maxNodeTransitions, e.nodeCallbacks = s.MaxTransitions, s.MaxNodeTransitions, s.NodeCallbacks
	if e.view == nil {
		return out, nil
	}
	for _, n := range e.nodeOrder {
		out = e.drain(n, out)
	}
	return out, e.fault
}

// Applied reports that the batch in flight has applied, and appends the callbacks of the
// steps that follow the batch to out. A transition that the batch ended leaves e.
//
// Error modes:
//   - an error that wraps fsm.ErrRejected when the phase or the wait of a transition
//     refuses an event.
func (e *Engine) Applied(out []machine.Effect) ([]machine.Effect, error) {
	batch := e.batch
	e.batch = nil
	for _, t := range batch {
		t.put = false
		if !t.ended {
			out = e.begin(t, out)
			continue
		}
		e.transitions.Delete(t.record.Operation.Shard)
		e.order = slices.DeleteFunc(e.order, func(o *transition) bool { return o == t })
	}
	return out, e.fault
}

// Inbound returns the number of transitions in flight that can end with a replica on the
// node n in a role of roles. Such a transition has its destination or its fallback on n in
// a role of roles. A transition is in flight until the batch that ends it has applied.
// Inbound costs O(t) for t transitions in flight and does not allocate.
func (e *Engine) Inbound(n shard.NodeID, roles []shard.Role) int {
	count := 0
	for _, t := range e.order {
		to, fallback := t.record.To, t.record.Fallback
		inbound := to.Node == n && slices.Contains(roles, to.Role)
		if inbound || fallback.Node == n && slices.Contains(roles, fallback.Role) {
			count++
		}
	}
	return count
}

// add adds t to the transitions in flight.
func (e *Engine) add(t *transition) {
	e.transitions.Set(t.record.Operation.Shard, t)
	e.order = append(e.order, t)
}

// full reports whether a node that m counts against has Config.MaxNodeTransitions
// transitions in flight.
func (e *Engine) full(m *Move) bool {
	for _, n := range [...]shard.NodeID{m.To.Node, m.Fallback.Node, e.source(m.From)} {
		if n != "" && e.load(n) >= e.maxNodeTransitions {
			return true
		}
	}
	return false
}

// load counts the transitions in flight that count against the node n. A transition
// counts against the node of its destination and of its fallback, and against the node of
// its source while the session of the source is in the view.
func (e *Engine) load(n shard.NodeID) int {
	count := 0
	for _, t := range e.order {
		if n == t.record.To.Node || n == t.record.Fallback.Node || n == e.source(t.record.From) {
			count++
		}
	}
	return count
}

// source returns the node of the source ep while the view has its session, and the empty
// node otherwise.
func (e *Engine) source(ep store.Endpoint) shard.NodeID {
	if n, ok := e.view[ep.Node]; ok && n.Incarnation == ep.Incarnation {
		return ep.Node
	}
	return ""
}

// ended reports whether the incarnation of ep has ended. It has ended when the view has a
// later incarnation of its node, or when the view does not have its node. ended reports
// false before the first view.
func (e *Engine) ended(ep store.Endpoint) bool {
	if e.view == nil {
		return false
	}
	n, ok := e.view[ep.Node]
	return !ok || n.Incarnation > ep.Incarnation
}

// begin starts a round of attempts of the callback of the current step of t, with a new
// request, and appends the callback to out when its node has a free permit.
func (e *Engine) begin(t *transition, out []machine.Effect) []machine.Effect {
	t.request, t.attempt = t.newRequest(e.scope, e.term), 0
	e.retrier.Start()
	return e.dispatch(t, out)
}

// dispatch sends the request of t to the node of its current step, and appends the
// callback to out. The request waits in the queue of the node before the first view, and
// while the node has no free permit. The step ends at once when the incarnation of the
// node has ended.
func (e *Engine) dispatch(t *transition, out []machine.Effect) []machine.Effect {
	step := t.step()
	ep := t.endpoint(step.Side)
	if e.ended(ep) {
		return e.lapse(t, out)
	}
	n := e.node(ep.Node)
	if e.view == nil || n.busy >= e.nodeCallbacks {
		n.queue = append(n.queue, t)
		e.fire(t, &t.waiting, eventQueue)
		return out
	}
	n.busy++
	t.token = e.next()
	e.fire(t, &t.waiting, eventSend)
	e.waits[t.token] = t
	return append(
		out,
		machine.Callback{Node: e.view[ep.Node], Request: t.request, Token: t.token, Timeout: step.Timeout},
	)
}

// called takes the result err of the callback of t, and appends the effects that follow
// it to out. A callback that runs past its timeout fails with the error of its context,
// which a retry can fix, so called classifies it as transient.
func (e *Engine) called(t *transition, err error, out []machine.Effect) []machine.Effect {
	out = e.release(t.endpoint(t.step().Side).Node, out)
	if err == nil {
		e.complete(t)
		return out
	}
	if errors.Is(err, context.DeadlineExceeded) {
		err = errs.WithClass(err, errs.Transient)
	}
	delay, refusal := e.retrier.Retry(t.attempt+1, err)
	if refusal == nil {
		t.attempt++
		return e.arm(t, eventBackoff, delay, out)
	}
	f := exhausted
	if !errs.Retryable(err) {
		f = definite
	}
	return e.fail(t, f, out)
}

// elapsed takes the Timer of t that has elapsed, and appends the effects that follow it
// to out. After a backoff, the callback runs its next attempt. After a round, a new round
// of attempts starts. After the lock delay, the drop completes.
func (e *Engine) elapsed(t *transition, out []machine.Effect) []machine.Effect {
	switch t.waiting.State() {
	case waitRetry:
		return e.dispatch(t, out)
	case waitRound:
		return e.begin(t, out)
	default:
		e.complete(t)
		return out
	}
}

// lapse ends the current step of t once the incarnation that the step addresses has ended.
// A drop on a node with a later incarnation completes at once. A drop on a node that has
// left the view completes after Config.LockDelay. Any other step fails definitively.
func (e *Engine) lapse(t *transition, out []machine.Effect) []machine.Effect {
	step := t.step()
	if step.Action != handoff.Drop {
		return e.fail(t, definite, out)
	}
	if _, ok := e.view[t.endpoint(step.Side).Node]; !ok && e.lockDelay > 0 {
		return e.arm(t, eventDelay, e.lockDelay, out)
	}
	e.complete(t)
	return out
}

// cancel ends the wait of t for a permit, a callback or a Timer. A callback in flight
// returns its permit, and the callbacks that the permit lets the queue send join out.
func (e *Engine) cancel(t *transition, out []machine.Effect) []machine.Effect {
	id, w := t.endpoint(t.step().Side).Node, t.waiting.State()
	if w == waitPermit {
		n := e.nodes[id]
		n.queue = slices.DeleteFunc(n.queue, func(q *transition) bool { return q == t })
		return out
	}
	delete(e.waits, t.token)
	if w == waitCall {
		return e.release(id, out)
	}
	return out
}

// complete ends the current step of t as completed, and continues t with its next step.
func (e *Engine) complete(t *transition) {
	t.record.Step++
	e.advance(t)
}

// fail ends the current step of t with the failure f, and appends the effects that follow
// it to out. Fail moves a running transition to its abort path, and keeps a transition on
// its abort path in its phase. A failed step of the script starts its abort path. A
// definite failure of a step of an abort path abandons the transition. A step of an abort
// path whose round of attempts failed runs again after RetryMax.
func (e *Engine) fail(t *transition, f failure, out []machine.Effect) []machine.Effect {
	from := t.phase.State()
	if phase := e.fire(t, &t.phase, handoff.Fail); phase != from {
		t.record.Phase, t.record.Failed, t.record.Step = phase, t.record.Step, 0
		e.advance(t)
		return out
	}
	if f == definite {
		e.end(t, handoff.Abandon)
		return out
	}
	return e.arm(t, eventRound, RetryMax, out)
}

// advance commits the commit steps of t from its current step on, and marks t for the
// next batch. After the last step of the path, it ends t with Finish.
func (e *Engine) advance(t *transition) {
	path := t.path()
	for t.record.Step < len(path) && path[t.record.Step].Action == handoff.Commit {
		t.commit(path[t.record.Step].Side)
		t.record.Step++
	}
	if t.record.Step >= len(path) {
		e.end(t, handoff.Finish)
		return
	}
	e.mark(t)
}

// end ends t on the outcome ev, which is Finish or Abandon, and marks t for the next
// batch. Abandon ends t without an assigned writer.
func (e *Engine) end(t *transition, ev handoff.Event) {
	e.fire(t, &t.phase, ev)
	if ev == handoff.Abandon {
		t.commit(noSide)
	}
	t.ended = true
	e.mark(t)
}

// mark makes t wait for the next batch, which commits its change.
func (e *Engine) mark(t *transition) {
	e.fire(t, &t.waiting, eventMark)
	e.dirty = append(e.dirty, t)
}

// arm appends a Timer of the duration d for t to out, and makes t wait for it with the
// event ev.
func (e *Engine) arm(t *transition, ev event, d time.Duration, out []machine.Effect) []machine.Effect {
	t.token = e.next()
	e.fire(t, &t.waiting, ev)
	e.waits[t.token] = t
	return append(out, machine.Timer{Token: t.token, Duration: d})
}

// fire delivers ev to m, the phase or the wait of t, and returns the state that m moves
// to. When m refuses ev, m keeps its state, and fire joins the error to the fault of e.
// Each method of e that can send an event returns the fault.
func (e *Engine) fire[S, E fsm.Enum](t *transition, m *fsm.Machine[S, E, struct{}], ev E) S {
	s, err := m.Fire(ev)
	if err != nil {
		err = fmt.Errorf("engine: the event %v of the transition of shard %v in the state %v: %w",
			ev, t.record.Operation.Shard, s, err)
		e.fault = errors.Join(e.fault, err)
	}
	return s
}

// next returns the next token of the machine.
func (e *Engine) next() machine.Token {
	*e.tokens++
	return *e.tokens
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package cell

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"

	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/fsm"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/internal/follow"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The keys of the attributes of the logs of a cell machine.
const (
	// attrCell is the cell.
	attrCell = "cell"
	// attrError is an error that does not stop the cell.
	attrError = "error"
)

// kinds lists the kinds of proposals that the controller of a cell applies.
var kinds = []proposal.OperationKind{proposal.Move, proposal.Drain, proposal.Undrain, proposal.Explain}

// Config is the configuration of a [Machine].
type Config struct {
	// Clock reads the time of the records of the cell: the start of each transition, each
	// drain and each admission, and the duration of each pass. It times the reservations.
	Clock clock.Clock
	// Rand is the source of the jitter of each backoff and of the IDs of new shards.
	Rand rand.Rand
	// Reporter reports the instruments of the cell.
	Reporter telemetry.Reporter
	// Provided maps the roles config.Store and config.Membership to the capabilities of
	// their adapters, which config.Resolve checks.
	Provided map[config.Axis][]config.Capability
	// Logger logs the errors that do not stop the cell, such as a pass that fails.
	Logger *slog.Logger
	// Fits returns nil when one commit to the scope of the cell can apply the mutation m, as
	// store.Store.Fits does for the scope.
	Fits func(m *store.Mutation) error
	// Registry builds the strategies of each accepted spec of the namespace.
	Registry *config.Registry
	// Scope is the scope of the cell.
	Scope shard.Scope
	// Operators lists the identities that may submit a proposal, and request or end any
	// disruption.
	Operators []shard.Identity
	// Node is the description of the process of the controller. The record of the cell
	// contains its Address and its Identity.
	Node shard.Node
	// Term is the fence epoch that the root granted to the controller of the cell.
	Term epoch.Epoch
	// Parallelism is the most evaluations that a pass runs at once, such as the CPU limit
	// of the process.
	Parallelism int
	// Incarnation is the namespace incarnation of the cell.
	Incarnation shard.NamespaceIncarnation
	// Unauthenticated makes the cell skip the identity of each caller, for a transport that
	// authenticates no caller. The checks of epochs and incarnations still apply.
	Unauthenticated bool
}

// state is the state of a cell machine in its lifecycle.
//
//dokimi:enum text=comment
type state uint8

// The states of a cell machine.
const (
	// stateIdle is the state before Started.
	stateIdle state = 1 // idle
	// stateFollowing is the read of the root scope for the first spec and the range of the
	// cell.
	stateFollowing state = 2 // following
	// stateControlling is the state after the first spec. The controller of the cell
	// acquires its scope, and then serves.
	stateControlling state = 3 // controlling
	// stateStopped is the state after Stopped, and after an error that stopped the cell.
	stateStopped state = 4 // stopped
)

// event is an event of the lifecycle of a cell machine.
//
//dokimi:enum text=comment
type event uint8

// The events of a cell machine.
const (
	// eventStart is the Started of the run.
	eventStart event = 1 // start
	// eventSpec is the first snapshot of the root scope, with the first spec.
	eventSpec event = 2 // spec
	// eventStop is the Stopped of the run, or an error that stops the cell.
	eventStop event = 3 // stop
)

// lifecycle is the table of the states of a cell machine. When the table has no edge for
// an event, the cell stops with the error of fsm.Machine.Fire.
var lifecycle = fsm.Must(fsm.NewBuilder[state, event, struct{}](stateIdle).
	Edge(stateIdle, eventStart, stateFollowing).
	Edge(stateFollowing, eventSpec, stateControlling).
	Edges([]state{stateIdle, stateFollowing, stateControlling}, eventStop, stateStopped).
	Terminal(stateStopped).
	Build())

// Machine is the controller of one cell of a namespace, as a state machine. It reads the
// accepted spec and the range of the cell from the root scope. Its controller then acquires
// the scope of the cell at the epoch that the root granted, continues the transitions of
// the scope, and serves the reports of the agents, the proposals of operators and the
// disruptions of cluster managers. The machine passes each newer accepted spec to its
// controller.
//
// The zero Machine is not usable. Construct one with [New].
//
// # Concurrency
//
// A Machine is not safe for concurrent use. A runtime calls Step from one goroutine at a
// time. [Machine.Err] is safe for concurrent use.
type Machine struct {
	// err is the error that stopped the cell, or nil.
	err atomic.Pointer[error]
	// provided lists the capabilities of the adapters for config.Resolve.
	provided map[config.Axis][]config.Capability
	// root reads the root scope.
	root *follow.Follower
	// control is the controller of the scope of the cell.
	control *control.Controller
	// registry builds the strategies of each spec.
	registry *config.Registry
	// logger logs the errors that do not stop the cell.
	logger *slog.Logger
	// rootRange is the range of the cell on the last page of the root scope that places the
	// cell, and nil before that page. A store keeps the placement of a shard once it has
	// one, so a snapshot that the follower reads again places the cell again.
	rootRange *shard.Range
	// scopeOf is the scope of the cell.
	scopeOf shard.Scope
	// started lists the tokens of the Started calls that wait for their reply, in their
	// order.
	started []machine.ReplyToken
	// lc is the state of the cell in its lifecycle.
	lc fsm.Machine[state, event, struct{}]
	// recorded is the mapping that the root recorded.
	recorded shard.Mapping
	// bounds are the cell bounds of the last spec that the cell resolved.
	bounds config.CellPolicy
	// tokens is the counter of the tokens of the effects of the machine, which its parts
	// share.
	tokens machine.Token
	// rootShard is the shard of the cell in the root scope. The ID of the cell is its text.
	rootShard shard.ID
	// above reports for each bound of the cell whether the last check found the cell above
	// it.
	above above
}

var _ machine.Machine = (*Machine)(nil)

// New returns the cell machine of cfg before Started.
//
// Error modes:
//   - an error that wraps ErrConfig for a nil cfg, and for a cfg without the cell of Scope
//     or Incarnation.
//   - an error that wraps ErrConfig and the error of shard.ID.UnmarshalText for a cell
//     that is not the text of a shard ID.
//   - an error that wraps ErrConfig and the errors of follow.New and control.New for a cfg
//     without Rand or without the namespace of Scope.
//   - an error that wraps ErrConfig and the error of control.New for every other field
//     that the controller of the cell needs.
func New(cfg *Config) (*Machine, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: no configuration", ErrConfig)
	}
	if cfg.Scope.Cell == "" {
		return nil, fmt.Errorf("%w: no cell", ErrConfig)
	}
	if cfg.Incarnation.IsZero() {
		return nil, fmt.Errorf("%w: no namespace incarnation", ErrConfig)
	}
	var rootShard shard.ID
	if err := rootShard.UnmarshalText([]byte(cfg.Scope.Cell)); err != nil {
		return nil, fmt.Errorf("%w: the cell %s: %w", ErrConfig, cfg.Scope.Cell, err)
	}
	m := &Machine{
		provided:  cfg.Provided,
		registry:  cfg.Registry,
		logger:    cfg.Logger,
		scopeOf:   cfg.Scope,
		lc:        lifecycle.Start(struct{}{}),
		rootShard: rootShard,
	}
	root, ferr := follow.New(&follow.Config{
		Rand:        cfg.Rand,
		Tokens:      &m.tokens,
		Scope:       shard.Scope{Namespace: cfg.Scope.Namespace},
		Incarnation: cfg.Incarnation,
	})
	inc := cfg.Incarnation
	ctl, cerr := control.New(&control.Config{
		Clock:           cfg.Clock,
		Rand:            cfg.Rand,
		Reporter:        cfg.Reporter,
		Logger:          cfg.Logger,
		Registry:        cfg.Registry,
		Provided:        cfg.Provided,
		Fits:            cfg.Fits,
		Acquisition:     func(store.Head, *store.Mutation) shard.NamespaceIncarnation { return inc },
		Tokens:          &m.tokens,
		Kinds:           kinds,
		Operators:       cfg.Operators,
		Scope:           cfg.Scope,
		Node:            cfg.Node,
		Term:            cfg.Term,
		Parallelism:     cfg.Parallelism,
		Unauthenticated: cfg.Unauthenticated,
	})
	if err := errors.Join(ferr, cerr); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	m.root, m.control = root, ctl
	return m, nil
}

// Err returns the error that stopped the cell, and nil while the cell runs or after a run
// that ended with Stopped.
func (m *Machine) Err() error {
	if err := m.err.Load(); err != nil {
		return *err
	}
	return nil
}

// Step applies ev to the cell and appends the effects of ev to out. It takes the inbound
// calls of a controller, the results of the effects of the cell and of its parts, and
// Started and Stopped. It ignores every other event, and every event after the cell has
// stopped.
//
// An error of the event stops the cell. The cell then drops the effects of the event,
// stops the reads of the root scope and its controller, and replies with the error to each
// Started that waits for its reply. Its controller ends its watches and replies to every
// other call that waits. [Machine.Err] then returns the error.
func (m *Machine) Step(ev machine.Event, out []machine.Effect) []machine.Effect {
	if m.lc.State() == stateStopped {
		return out
	}
	n := len(out)
	next, err := m.step(ev, out)
	if err != nil {
		return m.fail(err, out[:n])
	}
	return next
}

// step passes ev to the handler of its type, or to the part of the cell whose effect it is
// the result of. It returns the error that stops the cell, or nil.
func (m *Machine) step(ev machine.Event, out []machine.Effect) ([]machine.Effect, error) {
	switch ev := ev.(type) {
	case machine.Started:
		return m.start(ev, out)
	case machine.Stopped:
		var err error
		out, err = m.control.Stopped(out)
		return m.answer(control.ErrNotServing, out), errors.Join(err, m.fire(eventStop))
	case machine.Cancelled:
		if i := slices.Index(m.started, ev.Reply); i >= 0 {
			m.started = slices.Delete(m.started, i, i+1)
			return out, nil
		}
	}
	if res, next, ok := m.root.Step(ev, out); ok {
		return m.rooted(&res, next)
	}
	if res, next, ok := m.control.Step(ev, out); ok {
		return m.controlled(&res, next)
	}
	return out, nil
}

// start begins the run: the cell reads the root scope for its first spec.
func (m *Machine) start(ev machine.Started, out []machine.Effect) ([]machine.Effect, error) {
	m.started = append(m.started, ev.Reply)
	if err := m.fire(eventStart); err != nil {
		return out, err
	}
	return m.root.Start(out), nil
}

// fire delivers ev to the lifecycle of m. When the lifecycle refuses ev, fire returns the
// error of fsm.Machine.Fire with the event, the cell and the state of m.
func (m *Machine) fire(ev event) error {
	if st, err := m.lc.Fire(ev); err != nil {
		return fmt.Errorf("cell: the event %v of %v in the state %v: %w", ev, m.scopeOf, st, err)
	}
	return nil
}

// rooted takes what the follower of the root scope passed. A snapshot of the root scope
// gives the cell its range, the recorded mapping and the accepted spec, and a later
// generation can accept a newer spec.
func (m *Machine) rooted(res *follow.Result, out []machine.Effect) ([]machine.Effect, error) {
	if res.Err != nil {
		return out, fmt.Errorf("%w: %w", ErrRoot, res.Err)
	}
	if res.Page != nil {
		ps := res.Page.Placements
		if i := slices.IndexFunc(ps, func(p shard.Placement) bool { return p.Shard == m.rootShard }); i >= 0 {
			r := ps[i].Range
			m.rootRange = &r
		}
		if !res.Page.Last {
			return out, nil
		}
		return m.rootSnapshot(&res.Page.State, out)
	}
	if spec := res.Event.Mutation.Spec; spec != nil {
		return m.accept(spec, out)
	}
	return out, nil
}

// rootSnapshot takes the snapshot of the root scope with the state st. Before the first
// spec, it takes the range of the cell, the recorded mapping and the first spec, and its
// controller starts the acquisition of the scope of the cell. Later, it takes the spec as
// a newer spec.
//
// Error modes:
//   - an error that wraps ErrRoot for a root scope that does not place the cell.
//   - the error of resolve for a first spec that does not apply.
func (m *Machine) rootSnapshot(st *store.State, out []machine.Effect) ([]machine.Effect, error) {
	if m.lc.State() != stateFollowing {
		return m.accept(st.Spec, out)
	}
	if m.rootRange == nil {
		return out, fmt.Errorf("%w: the root has no cell %s", ErrRoot, m.scopeOf.Cell)
	}
	m.recorded = st.Mapping
	s, err := m.resolve(st.Spec)
	if err == nil {
		out, err = m.control.Acquire(s, *m.rootRange, out)
	}
	if err == nil {
		err = m.fire(eventSpec)
	}
	return out, err
}

// accept takes the accepted spec raw of the root scope, and passes its strategies to the
// controller of the cell. The cell logs a spec that does not apply, and keeps its own.
func (m *Machine) accept(raw []byte, out []machine.Effect) ([]machine.Effect, error) {
	s, err := m.resolve(raw)
	if err != nil {
		m.logger.LogAttrs(context.Background(), slog.LevelError, "cell: the accepted spec does not apply",
			slog.String(attrCell, string(m.scopeOf.Cell)), slog.Any(attrError, err))
		return out, nil
	}
	res, next := m.control.Accept(s, out)
	return m.controlled(&res, next)
}

// controlled takes what the controller of the cell completed. The cell replies to each
// Started that waits once the controller serves, and checks its bounds after each step of
// the controller.
//
// Error modes:
//   - the error that failed the controller.
func (m *Machine) controlled(res *control.Result, out []machine.Effect) ([]machine.Effect, error) {
	if res.Err != nil {
		return out, res.Err
	}
	if res.Acquired {
		out = m.answer(nil, out)
	}
	m.bound()
	return out, nil
}

// fail stops the cell with err. It stops the follower of the root scope and the
// controller, which end their watches, and replies to each Started that waits with err.
// Machine.Err then returns err.
func (m *Machine) fail(err error, out []machine.Effect) []machine.Effect {
	out, serr := m.control.Stop(m.root.Stop(out))
	err = errors.Join(err, serr, m.fire(eventStop))
	m.err.Store(&err)
	return m.answer(err, out)
}

// answer appends the reply with err to each Started that waits for its reply to out, in
// their order.
func (m *Machine) answer(err error, out []machine.Effect) []machine.Effect {
	for _, to := range m.started {
		out = append(out, machine.Reply[struct{}]{Err: err, To: to})
	}
	m.started = nil
	return out
}

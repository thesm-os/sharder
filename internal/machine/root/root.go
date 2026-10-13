// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package root

//go:generate go run golang.org/x/tools/cmd/stringer@v0.52.0 -type=state,event -linecomment -output=root.string_gen.go

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/fsm"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/resilience"
	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The bounds of the backoff before the root campaigns again after a campaign that ended
// with a transient error. resilience.Backoff returns each backoff as a draw of Config.Rand
// below its ceiling.
const (
	// CampaignRetryBase is the ceiling of the backoff before the first retry. The ceiling
	// doubles with each later retry.
	CampaignRetryBase = 100 * time.Millisecond
	// CampaignRetryMax is the highest ceiling of a backoff.
	CampaignRetryMax = 10 * time.Second
)

// The keys of the attributes of the logs of a root machine.
const (
	// attrNamespace is the namespace of the root.
	attrNamespace = "namespace"
	// attrTerm is the term of the root.
	attrTerm = "term"
	// attrError is the error of a term.
	attrError = "error"
)

// cellLoad is the load of each cell on the metric Capacity.
const cellLoad = 1

// kinds lists the kinds of proposals that the root applies.
var kinds = []proposal.OperationKind{proposal.SpecChange}

// Config is the configuration of a [Machine].
type Config struct {
	// Clock reads the time of the records of the root scope, and of the incarnation of a
	// namespace that the root creates.
	Clock clock.Clock
	// Rand is the source of the jitter of each backoff and of the incarnation of a
	// namespace that the root creates.
	Rand rand.Rand
	// Reporter reports the instruments of the controller of each term.
	Reporter telemetry.Reporter
	// Provided maps the roles config.Store and config.Membership to the capabilities of
	// their adapters, which config.Resolve checks.
	Provided map[config.Axis][]config.Capability
	// Logger logs the errors that do not stop the root, such as the error of a term.
	Logger *slog.Logger
	// Fits returns nil when one commit to the root scope can apply the mutation m, as
	// store.Store.Fits does for the scope.
	Fits func(m *store.Mutation) error
	// Registry builds the strategies of the root and of each spec of the namespace.
	Registry *config.Registry
	// Operators lists the identities that may submit a spec proposal.
	Operators []shard.Identity
	// Node is the description of the controller process. The record of the root contains
	// its Address and its Identity.
	Node shard.Node
	// Spec is the spec with which the root creates the namespace of Spec.Namespace, at
	// the first version. An existing namespace keeps its spec.
	Spec config.Spec
	// Parallelism is the most evaluations that a pass runs at once, such as the CPU limit
	// of the process.
	Parallelism int
	// Unauthenticated makes the root skip the identity of each caller, for a transport that
	// authenticates no caller. The checks of epochs and incarnations still apply.
	Unauthenticated bool
}

// validate returns an error that wraps ErrConfig for the first field of c that New refuses.
// New resolves the specs with the registry, and the controller of each term checks the
// other fields again.
func (c *Config) validate() error {
	if c.Clock == nil {
		return fmt.Errorf("%w: no clock", ErrConfig)
	}
	if c.Rand == nil {
		return fmt.Errorf("%w: no source of randomness", ErrConfig)
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
	if c.Node.Address == "" {
		return fmt.Errorf("%w: no address of the controller process", ErrConfig)
	}
	if c.Parallelism < 1 {
		return fmt.Errorf("%w: the parallelism %d is below one", ErrConfig, c.Parallelism)
	}
	return nil
}

// state is the state of a root machine in its lifecycle.
type state uint8

// The states of a root machine.
const (
	// stateIdle is the state before Started.
	stateIdle state = 1 // idle
	// stateCampaigning is the wait for the result of the last Campaign.
	stateCampaigning state = 2 // campaigning
	// stateWaiting is the backoff after a campaign that ended with a transient error.
	stateWaiting state = 3 // waiting
	// stateLeading is a term that the election granted. The controller of the term
	// acquires the root scope, and then serves.
	stateLeading state = 4 // leading
	// stateResigning is the wait for the end of a term that the root resigned after an
	// error of its controller.
	stateResigning state = 5 // resigning
	// stateStopped is the state after Stopped, and after an error that stopped the root.
	stateStopped state = 6 // stopped
)

// event is an event of the lifecycle of a root machine.
type event uint8

// The events of a root machine.
const (
	// eventStart is the Started of the run.
	eventStart event = 1 // start
	// eventGranted is a term that the election granted.
	eventGranted event = 2 // granted
	// eventLost is a campaign that ended with an error.
	eventLost event = 3 // lost
	// eventElapsed is the end of the backoff before the next campaign.
	eventElapsed event = 4 // elapsed
	// eventEnded is the end of a term.
	eventEnded event = 5 // ended
	// eventFault is an error of the controller of a term.
	eventFault event = 6 // fault
	// eventStop is the Stopped of the run, or an error that stops the root.
	eventStop event = 7 // stop
)

// lifecycle is the table of the states of a root machine. When the table has no edge for
// an event, the root stops with the error of fsm.Machine.Fire.
var lifecycle = fsm.Must(fsm.NewBuilder[state, event, struct{}](stateIdle).
	Edge(stateIdle, eventStart, stateCampaigning).
	Edge(stateCampaigning, eventGranted, stateLeading).
	Edge(stateCampaigning, eventLost, stateWaiting).
	Edge(stateWaiting, eventElapsed, stateCampaigning).
	Edge(stateLeading, eventEnded, stateCampaigning).
	Edge(stateLeading, eventFault, stateResigning).
	Edge(stateResigning, eventEnded, stateCampaigning).
	Edges([]state{stateIdle, stateCampaigning, stateWaiting, stateLeading, stateResigning}, eventStop, stateStopped).
	Terminal(stateStopped).
	Build())

// Machine is the root of one namespace, as a state machine. One root machine runs in each
// controller process for the life of the process. It campaigns in the election of the
// root scope, and the controller of each term that the election grants acquires the root
// scope and places the cells of the namespace on the controller processes. The root
// creates the namespace when the root scope has no commit. It applies the spec proposals
// of operators. At the end of a term, the root stops the controller of the term and
// campaigns again.
//
// The zero Machine is not usable. Construct one with [New].
//
// # Concurrency
//
// A Machine is not safe for concurrent use, so a runtime calls Step from one goroutine at a
// time. [Machine.Err] is safe for concurrent use.
type Machine struct {
	// rand is the source of the jitter of each backoff.
	rand rand.Rand
	// ids generates the incarnation of a namespace that the root creates.
	ids *ulid.Generator
	// logger logs the errors that do not stop the root.
	logger *slog.Logger
	// control is the controller of the term, and nil outside a term.
	control *control.Controller
	// strategies are the strategies of the root.
	strategies *control.Strategies
	// err is the error that stopped the root, or nil.
	err atomic.Pointer[error]
	// scopeOf is the root scope of the namespace.
	scopeOf shard.Scope
	// lc is the state of the root in its lifecycle.
	lc fsm.Machine[state, event, struct{}]
	// initial is the spec of a namespace that the root creates, as JSON.
	initial []byte
	// started lists the tokens of the Started calls that wait for their reply, in their
	// order.
	started []machine.ReplyToken
	// mapping is the mapping of a namespace that the root creates.
	mapping shard.Mapping
	// cfg is the configuration of the controller of each term, without its term.
	cfg control.Config
	// tokens is the counter of the tokens of the effects of the machine, which its parts
	// share.
	tokens machine.Token
	// campaign is the token of the last Campaign.
	campaign machine.Token
	// wait is the token of the last Timer of a backoff.
	wait machine.Token
	// term is the fence epoch of the last term that the election granted.
	term epoch.Epoch
	// attempt counts the campaigns that ended with a transient error in a row.
	attempt int
}

var _ machine.Machine = (*Machine)(nil)

// New returns the root machine of cfg before Started.
//
// Error modes:
//   - an error that wraps ErrConfig for a nil cfg, and for a cfg without Clock, Rand,
//     Reporter, Logger, Registry, Fits or the address of Node, or with a Parallelism
//     below one.
//   - an error that wraps ErrConfig and the error of config.Resolve or control.Build for a
//     Spec that does not apply, and for a Registry that does not build the strategies of
//     the root.
func New(cfg *Config) (*Machine, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: no configuration", ErrConfig)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	mapping, initial, err := namespace(cfg.Registry, cfg.Provided, &cfg.Spec)
	if err != nil {
		return nil, err
	}
	s, err := strategies(cfg.Registry, cfg.Provided, cfg.Spec.Namespace)
	if err != nil {
		return nil, err
	}
	m := &Machine{
		rand:       cfg.Rand,
		ids:        ulid.New(cfg.Clock, cfg.Rand),
		logger:     cfg.Logger,
		strategies: s,
		initial:    initial,
		mapping:    mapping,
		scopeOf:    shard.Scope{Namespace: cfg.Spec.Namespace},
		lc:         lifecycle.Start(struct{}{}),
	}
	m.cfg = control.Config{
		Clock:           cfg.Clock,
		Rand:            cfg.Rand,
		Reporter:        cfg.Reporter,
		Logger:          cfg.Logger,
		Registry:        cfg.Registry,
		Provided:        cfg.Provided,
		Fits:            cfg.Fits,
		Acquisition:     m.acquisition,
		Tokens:          &m.tokens,
		Load:            shard.Load{Capacity: cellLoad},
		Kinds:           kinds,
		Operators:       cfg.Operators,
		Scope:           m.scopeOf,
		Node:            cfg.Node,
		Parallelism:     cfg.Parallelism,
		Unauthenticated: cfg.Unauthenticated,
		Unreported:      true,
	}
	return m, nil
}

// Err returns the error that stopped the root, and nil while the root runs or after a run
// that ended with Stopped.
func (m *Machine) Err() error {
	if err := m.err.Load(); err != nil {
		return *err
	}
	return nil
}

// Step applies ev to the root and appends the effects of ev to out. It takes the inbound
// calls of a controller, the results of the effects of the root and of the controller of
// its term, and Started and Stopped. It ignores every other event, and every event after
// the root has stopped.
//
// An error of the controller of a term ends the term: the root logs the error, stops the
// controller and resigns the term. Any other error of the event stops the root. The root
// then takes these steps:
//   - It drops the effects of the event.
//   - It stops the controller of its term.
//   - It resigns the term, or ends the campaign that it waits for.
//   - It publishes the error in [Machine.Err].
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

// step passes ev to the handler of its type, or to the controller of the term. It returns
// the error that stops the root, or nil.
func (m *Machine) step(ev machine.Event, out []machine.Effect) ([]machine.Effect, error) {
	switch ev := ev.(type) {
	case machine.Started:
		return m.start(ev, out)
	case machine.Stopped:
		return m.stopped(out)
	case machine.Campaigned:
		if ev.Token == m.campaign {
			return m.campaigned(&ev, out)
		}
		return out, nil
	case machine.TermEnded:
		if ev.Token == m.campaign {
			return m.ended(out)
		}
		return out, nil
	case machine.Elapsed:
		if ev.Token == m.wait {
			return m.elect(out), m.fire(eventElapsed)
		}
	case machine.Report:
		return append(out, machine.Reply[protocol.Ack]{Err: errCellCall, To: ev.Reply}), nil
	case machine.Disrupt:
		return append(out, machine.Reply[budget.Decision]{Err: errCellCall, To: ev.Reply}), nil
	case machine.Done:
		return append(out, machine.Reply[struct{}]{Err: errCellCall, To: ev.Reply}), nil
	}
	if m.control != nil {
		if res, next, ok := m.control.Step(ev, out); ok {
			return m.controlled(&res, next)
		}
	}
	if ev, ok := ev.(machine.Submit); ok {
		return append(out, machine.Reply[protocol.Receipt]{Err: control.ErrNotServing, To: ev.Reply}), nil
	}
	return out, nil
}

// start begins the run: the root campaigns, and replies to Started.
func (m *Machine) start(ev machine.Started, out []machine.Effect) ([]machine.Effect, error) {
	m.started = append(m.started, ev.Reply)
	if err := m.fire(eventStart); err != nil {
		return out, err
	}
	return m.answer(nil, m.elect(out)), nil
}

// stopped takes the Stopped of the run. The controller of the term replies to the calls
// that wait for their reply.
func (m *Machine) stopped(out []machine.Effect) ([]machine.Effect, error) {
	var err error
	if m.control != nil {
		out, err = m.control.Stopped(out)
	}
	return out, errors.Join(err, m.fire(eventStop))
}

// elect appends the next Campaign of the root to out.
func (m *Machine) elect(out []machine.Effect) []machine.Effect {
	m.tokens++
	m.campaign = m.tokens
	return append(out, machine.Campaign{Scope: m.scopeOf, Token: m.campaign})
}

// campaigned takes the result of a campaign. A term that the election granted starts the
// controller of the term, which acquires the root scope. The root records the grant in its
// lifecycle before it creates the controller, so a grant outside a campaign creates no
// controller. It keeps the controller before the acquisition, so the stop of a root whose
// acquisition fails also stops the controller. A campaign that ended with a transient error
// runs again after a backoff.
//
// Error modes:
//   - an error that wraps the error of the campaign for an error that is not transient.
//   - an error that wraps fsm.ErrRejected for a grant outside a campaign.
//   - an error that wraps control.ErrConfig for a term that control.New refuses, such as
//     the zero term.
func (m *Machine) campaigned(ev *machine.Campaigned, out []machine.Effect) ([]machine.Effect, error) {
	if ev.Err != nil {
		lost := m.fire(eventLost)
		if !errs.Retryable(ev.Err) {
			return out, errors.Join(fmt.Errorf("root: the campaign of %v: %w", m.scopeOf, ev.Err), lost)
		}
		m.attempt++
		m.tokens++
		m.wait = m.tokens
		delay := resilience.Backoff(m.rand, m.attempt, CampaignRetryBase, CampaignRetryMax)
		return append(out, machine.Timer{Token: m.wait, Duration: delay}), lost
	}
	err := m.fire(eventGranted)
	var ctl *control.Controller
	if err == nil {
		m.attempt, m.term = 0, ev.Term
		cfg := m.cfg
		cfg.Term = ev.Term
		ctl, err = control.New(&cfg)
	}
	if err == nil {
		m.control = ctl
		out, err = ctl.Acquire(m.strategies, shard.Range{}, out)
	}
	if err != nil {
		return out, fmt.Errorf("root: the term %d of %v: %w", ev.Term, m.scopeOf, err)
	}
	return out, nil
}

// ended takes the end of a term. The root stops the controller of a term that the store
// ended, and campaigns again.
func (m *Machine) ended(out []machine.Effect) ([]machine.Effect, error) {
	var err error
	if m.control != nil {
		out, err = m.control.Stop(out)
		m.control = nil
	}
	return m.elect(out), errors.Join(err, m.fire(eventEnded))
}

// controlled takes what the controller of the term completed. An error of the controller
// ends the term: the root logs the error, stops the controller and resigns the term.
func (m *Machine) controlled(res *control.Result, out []machine.Effect) ([]machine.Effect, error) {
	if res.Err == nil {
		return out, nil
	}
	m.logger.LogAttrs(context.Background(), slog.LevelError, "root: the term failed",
		slog.String(attrNamespace, string(m.scopeOf.Namespace)), slog.Uint64(attrTerm, uint64(m.term)),
		slog.Any(attrError, res.Err))
	out, err := m.control.Stop(out)
	m.control = nil
	return append(out, machine.Resign{Token: m.campaign}), errors.Join(err, m.fire(eventFault))
}

// fire delivers ev to the lifecycle of m. When the lifecycle refuses ev, fire returns the
// error of fsm.Machine.Fire with the event, the root scope and the state of m.
func (m *Machine) fire(ev event) error {
	if st, err := m.lc.Fire(ev); err != nil {
		return fmt.Errorf("root: the event %v of %v in the state %v: %w", ev, m.scopeOf, st, err)
	}
	return nil
}

// fail stops the root with err. It stops the controller of the term, which ends its
// watches and replies to the calls that wait. It resigns the term, or ends the campaign
// that the root waits for, and replies to each Started that waits with err. Machine.Err
// then returns err.
func (m *Machine) fail(err error, out []machine.Effect) []machine.Effect {
	var serr error
	if m.control != nil {
		out, serr = m.control.Stop(out)
	}
	if st := m.lc.State(); st == stateCampaigning || st == stateLeading {
		out = append(out, machine.Resign{Token: m.campaign})
	}
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

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"go.thesmos.sh/core/fsm"
	"go.thesmos.sh/core/pool"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/shard"
)

// The bounds of the backoff before a watch of views runs again after a transient error.
// resilience.Backoff returns each backoff as a draw of Config.Rand below its ceiling.
const (
	// RetryBase is the ceiling of the backoff before the first retry. The ceiling doubles
	// with each later retry.
	RetryBase = 100 * time.Millisecond
	// RetryMax is the highest ceiling of a backoff.
	RetryMax = 10 * time.Second
)

// Config is the configuration of a [Machine].
type Config struct {
	// Rand is the source of the jitter of each backoff.
	Rand rand.Rand
	// Reporter reports the instruments of the router.
	Reporter telemetry.Reporter
	// Logger logs the errors that do not stop the router, such as a namespace whose
	// positions the router cannot compute after a rediscovery.
	Logger *slog.Logger
	// Registry resolves the spec of the namespace and builds its partitioner.
	Registry *config.Registry
	// Provided maps the roles config.Store and config.Membership to the capabilities of
	// their adapters, which config.Resolve checks.
	Provided map[config.Axis][]config.Capability
	// Namespace is the namespace of the router.
	Namespace shard.Namespace
}

// validate returns an error that wraps ErrConfig for the first field of c that New refuses.
// follow.New checks Rand and Namespace.
func (c *Config) validate() error {
	if c.Reporter == nil {
		return fmt.Errorf("%w: no reporter", ErrConfig)
	}
	if c.Logger == nil {
		return fmt.Errorf("%w: no logger", ErrConfig)
	}
	if c.Registry == nil {
		return fmt.Errorf("%w: no registry", ErrConfig)
	}
	return nil
}

// state is the state of a router in its lifecycle.
//
//dokimi:enum text=comment
type state uint8

// The states of a router.
const (
	// stateIdle is the state before Started.
	stateIdle state = 1 // idle
	// stateReading is the first read of the namespace.
	stateReading state = 2 // reading
	// stateServing is the state in which the routes of an installed read serve.
	stateServing state = 3 // serving
	// stateRediscovering is a read of the namespace again, while the routes of the
	// installed read serve.
	stateRediscovering state = 4 // rediscovering
	// stateStopped is the state after Stopped, and after an error that stopped the router.
	stateStopped state = 5 // stopped
)

// event is an event of the lifecycle of a router.
//
//dokimi:enum text=comment
type event uint8

// The events of a router.
const (
	// eventStart is the Started of the run.
	eventStart event = 1 // start
	// eventRead is the end of a read of the namespace: its install, or a rediscovery that
	// found the installed incarnation.
	eventRead event = 2 // read
	// eventRediscover is the start of a read of the namespace while the routes of the
	// installed read serve.
	eventRediscover event = 3 // rediscover
	// eventStop is the Stopped of the run, or an error that stops the router.
	eventStop event = 4 // stop
)

// lifecycle is the table of the states of a router. When the table has no edge for an
// event, the router stops with the error of fsm.Machine.Fire.
var lifecycle = fsm.Must(fsm.NewBuilder[state, event, struct{}](stateIdle).
	Edge(stateIdle, eventStart, stateReading).
	Edges([]state{stateReading, stateRediscovering}, eventRead, stateServing).
	Edge(stateServing, eventRediscover, stateRediscovering).
	Edges([]state{stateIdle, stateReading, stateServing, stateRediscovering}, eventStop, stateStopped).
	Terminal(stateStopped).
	Build())

// Machine is the router of one namespace in watch mode, as a state machine:
//   - It reads the spec, the mapping and the cell map of the namespace from the root scope,
//     and builds the partitioner of the spec.
//   - It reads the scope and watches the views of each cell, and keeps the route of each
//     shard.
//   - It publishes the routes. [Machine.Route] reads them while Step runs.
//   - It replies to each Refresh once it has applied the generation that the Refresh waits
//     for.
//   - It reads the namespace again when a read reports another namespace incarnation or a
//     Refresh is of another incarnation.
//
// The zero Machine is not usable. Construct one with [New].
//
// # Concurrency
//
// A Machine is not safe for concurrent use. Its runtime calls Step from one goroutine at a
// time. [Machine.Route] and [Machine.Err] are safe for concurrent use, also while Step
// runs.
type Machine struct {
	// rand is the source of the jitter of each backoff.
	rand rand.Rand
	// reporter reports the instruments of each cell.
	reporter telemetry.Reporter
	// metrics are the instruments of the router that are not bound to a cell.
	metrics *metrics
	// current is the installed read of the namespace, and nil before the first install.
	current *discovery
	// err is the error that stopped the router, or nil.
	err atomic.Pointer[error]
	// logger logs the errors that do not stop the router.
	logger *slog.Logger
	// registry resolves the spec of the namespace.
	registry *config.Registry
	// provided lists the capabilities of the adapters for config.Resolve.
	provided map[config.Axis][]config.Capability
	// published is the table of the routes that Route reads.
	published atomic.Pointer[table]
	// positions lends Route the buffer of a position.
	positions *pool.Pool[*[positionSize]byte]
	// incoming is the read of the namespace that runs, and nil without one.
	incoming *discovery
	// namespace is the namespace of the router.
	namespace shard.Namespace
	// lc is the state of the router in its lifecycle.
	lc fsm.Machine[state, event, struct{}]
	// waits lists the Refresh calls that wait for their reply in the order of the calls.
	waits []wait
	// started lists the tokens of the Started calls that wait for their reply in the order
	// of the calls.
	started []machine.ReplyToken
	// tokens is the counter of the tokens of the effects of the machine. The parts of the
	// machine share the counter.
	tokens machine.Token
}

var _ machine.Machine = (*Machine)(nil)

// New returns the router of cfg before Started. Route returns ErrNoRoute until the router
// has read the namespace.
//
// Error modes:
//   - an error that wraps ErrConfig for a nil cfg, and for a cfg without Reporter, Logger
//     or Registry.
//   - an error that wraps ErrConfig and the error of follow.New for a cfg without Rand or
//     Namespace.
func New(cfg *Config) (*Machine, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: no configuration", ErrConfig)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	m := &Machine{
		positions: pool.NewPool(func() *[positionSize]byte { return new([positionSize]byte) }),
		rand:      cfg.Rand,
		reporter:  cfg.Reporter,
		logger:    cfg.Logger.With(slog.String(attrNamespace, string(cfg.Namespace))),
		registry:  cfg.Registry,
		provided:  cfg.Provided,
		namespace: cfg.Namespace,
		lc:        lifecycle.Start(struct{}{}),
	}
	d, err := m.newDiscovery()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	m.incoming = d
	m.metrics = newMetrics(cfg.Reporter, cfg.Namespace)
	m.published.Store(&table{err: ErrNoRoute})
	return m, nil
}

// Err returns the error that stopped the router, and nil while the router runs or after a
// run that ended with Stopped.
func (m *Machine) Err() error {
	if err := m.err.Load(); err != nil {
		return *err
	}
	return nil
}

// Step applies ev to the router and appends the effects of ev to out. It takes Started,
// Stopped, the Refresh calls of clients and their Cancelled, and the results of the effects
// of the router and of its followers. It ignores every other event, and every event after
// the router has stopped.
//
// An error of the event stops the router. The router then drops the effects of the event,
// ends its reads, and replies with the error to each Started that waits for its reply. Each
// Refresh that waits gets ErrClosed, Route returns ErrClosed, and [Machine.Err] returns the
// error.
//
// At Stopped, each call that waits gets ErrClosed. The router appends no other effect,
// because the runtime ends every read of the run.
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

// step passes ev to the handler of its type, or to the follower whose effect it is the
// result of. It returns the error that stops the router, or nil.
func (m *Machine) step(ev machine.Event, out []machine.Effect) ([]machine.Effect, error) {
	switch ev := ev.(type) {
	case machine.Started:
		return m.start(ev, out)
	case machine.Stopped:
		return m.halt(ErrClosed, out), m.fire(eventStop)
	case machine.Refresh:
		return m.refresh(ev, out)
	case machine.Cancelled:
		m.cancelled(ev)
		return out, nil
	case machine.Viewed:
		return m.viewed(&ev, out)
	}
	for _, d := range [...]*discovery{m.current, m.incoming} {
		if d == nil {
			continue
		}
		if res, next, ok := d.root.Step(ev, out); ok {
			return m.rooted(d, &res, next)
		}
		for _, c := range d.cells {
			if res, next, ok := c.follower.Step(ev, out); ok {
				return m.scoped(d, c, &res, next)
			}
		}
	}
	if ev, ok := ev.(machine.Elapsed); ok {
		return m.elapsed(ev, out), nil
	}
	return out, nil
}

// start begins the run: the router reads the root scope.
func (m *Machine) start(ev machine.Started, out []machine.Effect) ([]machine.Effect, error) {
	m.started = append(m.started, ev.Reply)
	if err := m.fire(eventStart); err != nil {
		return out, err
	}
	return m.incoming.root.Start(out), nil
}

// cancelled drops the call of ev, whose caller no longer waits: a Started or a Refresh. It
// then sets the highest generation of each cell of the installed discovery to the highest
// that a Refresh of the cell in its incarnation waits for, so a cancelled Refresh does not
// count in the lag. cancelled costs O(c·w) for the c cells and the w Refresh calls that wait.
func (m *Machine) cancelled(ev machine.Cancelled) {
	m.started = slices.DeleteFunc(m.started, func(to machine.ReplyToken) bool { return to == ev.Reply })
	m.waits = slices.DeleteFunc(m.waits, func(w wait) bool { return w.reply == ev.Reply })
	d := m.current
	if d == nil {
		return
	}
	for _, c := range d.cells {
		c.highest = 0
		for _, w := range m.waits {
			if w.cell == c.scope.Cell && w.incarnation == d.incarnation {
				c.highest = max(c.highest, w.min)
			}
		}
		c.report()
	}
}

// fire delivers ev to the lifecycle of m. When the lifecycle refuses ev, fire returns the
// error of fsm.Machine.Fire with the event, the namespace and the state of m.
func (m *Machine) fire(ev event) error {
	if st, err := m.lc.Fire(ev); err != nil {
		return fmt.Errorf("routes: the event %v of namespace %s in the state %v: %w", ev, m.namespace, st, err)
	}
	return nil
}

// fail stops the router with err, and Machine.Err then returns err. The router ends the
// reads of its discoveries, and halts with err.
func (m *Machine) fail(err error, out []machine.Effect) []machine.Effect {
	for _, d := range [...]*discovery{m.current, m.incoming} {
		if d != nil {
			out = d.stop(out)
		}
	}
	err = errors.Join(err, m.fire(eventStop))
	m.err.Store(&err)
	return m.halt(err, out)
}

// halt ends the work of the router:
//   - It replies with err to each Started that waits, and with ErrClosed to each Refresh
//     that waits.
//   - Route returns ErrClosed from then on.
//   - It releases the instruments of the router and of its cells.
func (m *Machine) halt(err error, out []machine.Effect) []machine.Effect {
	out = m.answer(err, out)
	for _, w := range m.waits {
		out = append(out, machine.Reply[struct{}]{Err: ErrClosed, To: w.reply})
	}
	m.published.Store(&table{err: ErrClosed})
	for _, d := range [...]*discovery{m.current, m.incoming} {
		if d != nil {
			d.release()
		}
	}
	m.metrics.release()
	return out
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

// next returns the next token of the machine.
func (m *Machine) next() machine.Token {
	m.tokens++
	return m.tokens
}

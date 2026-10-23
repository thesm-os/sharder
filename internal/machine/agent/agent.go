// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

//go:generate go run golang.org/x/tools/cmd/stringer@v0.52.0 -type=state,event -linecomment -output=agent.string_gen.go

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"go.thesmos.sh/core/btree"
	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/fsm"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/idmap"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/follow"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/shard"
)

// The bounds of the backoff before each retry of a join, a report, a cleanup and a call of
// Close. resilience.Backoff returns each backoff as a draw of Config.Rand below its ceiling.
const (
	// RetryBase is the ceiling of the backoff before the first retry. The ceiling doubles
	// with each later retry.
	RetryBase = 100 * time.Millisecond
	// RetryMax is the highest ceiling of a backoff.
	RetryMax = 10 * time.Second
)

// CallTimeout is the longest time that a call of the agent to the controller of its cell
// takes: a report, a disruption and the end of a disruption.
const CallTimeout = 10 * time.Second

// Config is the configuration of a [Machine].
type Config struct {
	// Clock reads the time of each report and of the disruption of a Close.
	Clock clock.Clock
	// Rand is the source of the jitter of each backoff and of the ID of the disruption of a
	// Close.
	Rand rand.Rand
	// Reporter reports the instruments of the agent.
	Reporter telemetry.Reporter
	// Provided maps the roles config.Store and config.Membership to the capabilities of
	// their adapters, which config.Resolve checks.
	Provided map[config.Axis][]config.Capability
	// Logger logs the errors that do not stop the agent, such as a report that fails.
	Logger *slog.Logger
	// Registry resolves the spec of the namespace.
	Registry *config.Registry
	// Namespace is the namespace of the agent.
	Namespace shard.Namespace
	// Node is the description of the node. Node.Cell selects the cell that the node joins,
	// and a node without a cell joins the only cell of the namespace. The membership gives
	// the node its incarnation.
	Node shard.Node
	// Unauthenticated makes the agent skip the identity of each caller, for a transport
	// that authenticates no caller. The checks of epochs and incarnations still apply.
	Unauthenticated bool
	// Root makes the machine the agent of a controller process in the root scope. That agent
	// takes the callbacks of the root that place cells on the process. Node.Cell is empty for
	// the agent of the root.
	Root bool
}

// validate returns an error that wraps ErrConfig for the first field of c that New refuses.
// follow.New checks Rand and Namespace.
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
	if c.Node.ID == "" {
		return fmt.Errorf("%w: no ID of the node", ErrConfig)
	}
	if c.Node.Address == "" {
		return fmt.Errorf("%w: no address of the node", ErrConfig)
	}
	if c.Root && c.Node.Cell != "" {
		return fmt.Errorf("%w: node %s of the root has the cell %s", ErrConfig, c.Node.ID, c.Node.Cell)
	}
	return nil
}

// state is the state of an agent in its lifecycle.
type state uint8

// The states of an agent.
const (
	// stateIdle is the state before Started.
	stateIdle state = 1 // idle
	// stateRooting is the read of the root scope for the spec and the cell of the node.
	stateRooting state = 2 // rooting
	// stateJoining is the wait for the first session of the node and for the first snapshot
	// of the scope of its cell.
	stateJoining state = 3 // joining
	// stateServing is the state in which the node has a session and the agent serves.
	stateServing state = 4 // serving
	// stateRejoining is the wait for the next session after the session of the node ended.
	stateRejoining state = 5 // rejoining
	// stateClosing is the run of a Close.
	stateClosing state = 6 // closing
	// stateStopped is the state after Stopped, after a Close, and after an error that
	// stopped the agent.
	stateStopped state = 7 // stopped
)

// event is an event of the lifecycle of an agent.
type event uint8

// The events of an agent.
const (
	// eventStart is the Started of the run.
	eventStart event = 1 // start
	// eventRooted is the read of the spec and the cell of the node from the root scope.
	eventRooted event = 2 // rooted
	// eventServed is the first session of the node together with the first snapshot of
	// the scope of its cell.
	eventServed event = 3 // served
	// eventLost is the end of the session of the node.
	eventLost event = 4 // lost
	// eventJoined is the next session of the node.
	eventJoined event = 5 // joined
	// eventClose is a Close.
	eventClose event = 6 // close
	// eventStop is the Stopped of the run, the end of a Close, or an error that stops the
	// agent.
	eventStop event = 7 // stop
)

// lifecycle is the table of the states of an agent. When the table has no edge for an
// event, the agent stops with the error of fsm.Machine.Fire.
var lifecycle = fsm.Must(fsm.NewBuilder[state, event, struct{}](stateIdle).
	Edge(stateIdle, eventStart, stateRooting).
	Edge(stateRooting, eventRooted, stateJoining).
	Edge(stateJoining, eventServed, stateServing).
	Edge(stateServing, eventLost, stateRejoining).
	Edge(stateRejoining, eventJoined, stateServing).
	Edges([]state{stateServing, stateRejoining}, eventClose, stateClosing).
	Edges([]state{stateIdle, stateRooting, stateJoining, stateServing, stateRejoining, stateClosing}, eventStop,
		stateStopped).
	Terminal(stateStopped).
	Build())

// Machine is the agent of one node of a namespace, as a state machine:
//   - It reads the spec and the cell of the node from the root scope.
//   - It joins the cell, and watches the placements of the node and the record of the
//     controller of the cell.
//   - It checks each callback of the controller, and runs the callbacks of each shard one at
//     a time through the handler of the application.
//   - It cleans up each tenure that the watched placements no longer list.
//   - It reports the replicas of the node to the controller.
//
// The zero Machine is not usable. Construct one with [New].
//
// # Concurrency
//
// A Machine is not safe for concurrent use. The runtime calls Step from one goroutine at a
// time. [Machine.Err], [Machine.Token] and [Machine.Observed] are safe for concurrent use.
type Machine struct {
	// clock reads the time of each report and of the disruption of a Close.
	clock clock.Clock
	// rand is the source of the jitter of each backoff.
	rand rand.Rand
	// registry resolves the spec of the namespace.
	registry *config.Registry
	// published is the table of the fencing tokens that Token reads, a copy of grants.
	published atomic.Pointer[btree.MapFunc[shard.ID, grant]]
	// served is the cell and the namespace incarnation of the observation of the node. It is
	// nil before the agent serves and after it stops.
	served atomic.Pointer[observedCell]
	// logger logs the errors that do not stop the agent.
	logger *slog.Logger
	// metrics are the instruments of the agent.
	metrics *metrics
	// err is the error that stopped the agent, or nil.
	err atomic.Pointer[error]
	// provided lists the capabilities of the adapters for config.Resolve.
	provided map[config.Axis][]config.Capability
	// root reads the root scope.
	root *follow.Follower
	// cell reads the scope of the cell for the node, and is nil before the agent knows its
	// cell.
	cell *follow.Follower
	// ids generates the ID of the disruption of a Close.
	ids *ulid.Generator
	// grants is the table of the fencing tokens of the tenures of the node, in the order of
	// the shards.
	grants *btree.MapFunc[shard.ID, grant]
	// shards maps each shard that the agent has seen to its state, in the order of the
	// shards.
	shards *btree.MapFunc[shard.ID, *shardState]
	// effects maps the token of each Acquire, Handle, Revoke and Timer of a callback, a cleanup
	// or a revocation to the work that waits for it.
	effects map[machine.Token]*work
	// cellScope is the scope that the node joins: the scope of its cell, or the root scope
	// for the agent of the root. It is empty before the agent knows it.
	cellScope shard.Scope
	// rootScope is the root scope of the namespace.
	rootScope shard.Scope
	// lc is the state of the agent in its lifecycle.
	lc fsm.Machine[state, event, struct{}]
	// started lists the tokens of the Started calls that wait for their reply, in their
	// order.
	started []machine.ReplyToken
	// node is the description of the node.
	node shard.Node
	// watched is the state of the scope of the cell that concerns the node.
	watched watched
	// ns is what the agent has read from the root scope.
	ns namespace
	// closing is the state of a Close.
	closing closing
	// settings are the parts of the spec that the agent applies.
	settings settings
	// reports is the state of the reports of the node.
	reports reports
	// session is the membership session of the node.
	session session
	// epoch is the highest controller epoch that the agent has seen.
	epoch epoch.Epoch
	// tokens is the counter of the tokens of the effects of the machine, which its parts
	// share.
	tokens machine.Token
	// applied is the last generation of the scope of the cell that the agent has applied, for
	// the observation of the node.
	applied atomic.Uint64
	// incarnation is the namespace incarnation, and zero before the agent knows it.
	incarnation shard.NamespaceIncarnation
	// want is the shard of the cell of Config.Node in the root scope, and the zero ID for a
	// node without a cell.
	want shard.ID
	// unauthenticated makes the agent skip the identity of callers.
	unauthenticated bool
	// inRoot is true for the agent of a controller process in the root scope.
	inRoot bool
}

var _ machine.Machine = (*Machine)(nil)

// New returns the agent of cfg before Started.
//
// Error modes:
//   - an error that wraps ErrConfig for a nil cfg, and for a cfg without Clock, Reporter,
//     Logger, Registry, or the ID or the address of Node.
//   - an error that wraps ErrConfig for a cfg of the agent of the root whose node has a cell.
//   - an error that wraps ErrConfig and the error of shard.ID.UnmarshalText for a Node
//     whose cell is not the text of a shard ID.
//   - an error that wraps ErrConfig and the error of follow.New for a cfg without Rand or
//     Namespace.
func New(cfg *Config) (*Machine, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: no configuration", ErrConfig)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	var want shard.ID
	if err := want.UnmarshalText([]byte(cfg.Node.Cell)); err != nil {
		return nil, fmt.Errorf("%w: the cell %s of node %s: %w", ErrConfig, cfg.Node.Cell, cfg.Node.ID, err)
	}
	m := &Machine{
		clock: cfg.Clock,
		rand:  cfg.Rand,
		logger: cfg.Logger.With(
			slog.String(attrNamespace, string(cfg.Namespace)),
			slog.String(attrNode, string(cfg.Node.ID)),
		),
		registry:        cfg.Registry,
		provided:        cfg.Provided,
		ids:             ulid.New(cfg.Clock, cfg.Rand),
		grants:          btree.NewMapFunc[shard.ID, grant](shard.ID.Compare),
		shards:          btree.NewMapFunc[shard.ID, *shardState](shard.ID.Compare),
		effects:         make(map[machine.Token]*work),
		watched:         watched{placements: idmap.NewTyped128[shard.ID, shard.Placement](0)},
		node:            cfg.Node,
		rootScope:       shard.Scope{Namespace: cfg.Namespace},
		want:            want,
		lc:              lifecycle.Start(struct{}{}),
		unauthenticated: cfg.Unauthenticated,
		inRoot:          cfg.Root,
	}
	m.published.Store(m.grants.Clone())
	root, err := follow.New(&follow.Config{Rand: cfg.Rand, Tokens: &m.tokens, Scope: m.rootScope})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	m.root = root
	m.metrics = newMetrics(cfg.Reporter, cfg.Namespace, cfg.Node.ID)
	return m, nil
}

// Err returns the error that stopped the agent, and nil while the agent runs or after a run
// that ended with Stopped or a Close.
func (m *Machine) Err() error {
	if err := m.err.Load(); err != nil {
		return *err
	}
	return nil
}

// Step applies ev to the agent and appends the effects of ev to out. It takes the callbacks
// of the controller of the cell, the calls of the application, the results of the effects of
// the agent and of its readers, and Started and Stopped. It ignores every other event, and
// every event after the agent has stopped.
//
// An error of the event stops the agent. The agent then drops the effects of the event,
// ends the reads of its scopes and the session of the node, and replies with the error to
// each Started that waits for its reply. Every other call that waits gets ErrNotServing.
// Machine.Token then returns an error for every shard, and [Machine.Err] returns the error.
//
// At Stopped, every call that waits gets ErrNotServing. The agent appends no other effect,
// because the runtime ends every read and every session of the run.
func (m *Machine) Step(ev machine.Event, out []machine.Effect) []machine.Effect {
	if m.lc.State() == stateStopped {
		return out
	}
	n := len(out)
	next, err := m.step(ev, out)
	if err != nil {
		next = m.fail(err, out[:n])
	}
	return next
}

// step passes ev to the handler of its type, or to the part of the agent whose effect it is
// the result of. It returns the error that stops the agent, or nil.
func (m *Machine) step(ev machine.Event, out []machine.Effect) ([]machine.Effect, error) {
	switch ev := ev.(type) {
	case machine.Started:
		return m.start(ev, out)
	case machine.Stopped:
		return m.answer(ErrNotServing, m.halt(out)), m.fire(eventStop)
	case machine.Cancelled:
		return m.cancelled(ev, out)
	case machine.Do:
		return m.do(&ev, out), nil
	case machine.ReportFault:
		return m.fault(&ev, out), nil
	case machine.Recovered:
		return m.recovered(ev, out), nil
	case machine.Close:
		return m.close(ev, out)
	case machine.Joined:
		return m.joined(&ev, out)
	case machine.SessionEnded:
		return m.ended(ev, out)
	case machine.Left:
		return m.departed(ev, out)
	case machine.Acquired:
		return m.acquired(ev, out), nil
	case machine.Handled:
		return m.handled(ev, out), nil
	case machine.Revoked:
		return m.revoked(ev, out), nil
	case machine.Reported:
		return m.reported(&ev, out), nil
	case machine.Decided:
		return m.decided(&ev, out), nil
	case machine.Ended:
		return m.released(ev, out)
	}
	if res, next, ok := m.root.Step(ev, out); ok {
		return m.rooted(&res, next)
	}
	if m.cell != nil {
		if res, next, ok := m.cell.Step(ev, out); ok {
			return m.watch(&res, next)
		}
	}
	if ev, ok := ev.(machine.Elapsed); ok {
		return m.elapsed(ev, out), nil
	}
	return out, nil
}

// start begins the run: the agent reads the root scope for the spec and the cell of the
// node.
func (m *Machine) start(ev machine.Started, out []machine.Effect) ([]machine.Effect, error) {
	m.started = append(m.started, ev.Reply)
	if err := m.fire(eventStart); err != nil {
		return out, err
	}
	return m.root.Start(out), nil
}

// elapsed takes the end of a Timer of the agent: the backoff before a join, the window of
// the reports, the backoff of a report, the backoff of a cleanup, or the wait of a Close
// before its next call.
func (m *Machine) elapsed(ev machine.Elapsed, out []machine.Effect) []machine.Effect {
	switch ev.Token {
	case m.session.wait:
		return m.join(out)
	case m.reports.window:
		return m.report(out)
	case m.reports.retry:
		m.reports.retry = 0
		return m.report(out)
	case m.closing.retry:
		return m.again(out)
	}
	return m.retried(ev.Token, out)
}

// cancelled drops the call of ev, whose caller no longer waits. A callback that waits for
// its turn or for its placement ends without a caller. The agent completes a callback that
// runs, and keeps its result. A Close whose caller no longer waits ends the session of the
// node and stops the agent. cancelled costs O(n) for the n shards that the agent has seen.
func (m *Machine) cancelled(ev machine.Cancelled, out []machine.Effect) ([]machine.Effect, error) {
	if i := slices.Index(m.started, ev.Reply); i >= 0 {
		m.started = slices.Delete(m.started, i, i+1)
		return out, nil
	}
	if ev.Reply == m.closing.reply {
		return m.abandon(out)
	}
	for _, st := range m.shards.All() {
		for _, w := range st.waiting() {
			if i := slices.Index(w.waiters, ev.Reply); i >= 0 {
				w.waiters = slices.Delete(w.waiters, i, i+1)
				if len(w.waiters) == 0 && !w.running {
					st.forget(w)
				}
				return out, nil
			}
		}
	}
	return out, nil
}

// fire delivers ev to the lifecycle of m. When the lifecycle refuses ev, fire returns the
// error of fsm.Machine.Fire with the event, the node and the state of m.
func (m *Machine) fire(ev event) error {
	if st, err := m.lc.Fire(ev); err != nil {
		return fmt.Errorf("agent: the event %v of node %s in the state %v: %w", ev, m.node.ID, st, err)
	}
	return nil
}

// fail stops the agent with err, and Machine.Err then returns err. The agent ends the reads
// of the scopes and the session of the node. Each Started that waits gets err, and every
// other call that waits gets ErrNotServing.
func (m *Machine) fail(err error, out []machine.Effect) []machine.Effect {
	out = m.halt(m.unwatch(out))
	if m.session.join != 0 {
		out = append(out, machine.Leave{Token: m.session.join})
	}
	err = errors.Join(err, m.fire(eventStop))
	m.err.Store(&err)
	return m.answer(err, out)
}

// unwatch ends the reads of the scopes when the agent stops itself. At Stopped, the runtime
// ends every read of the run.
func (m *Machine) unwatch(out []machine.Effect) []machine.Effect {
	out = m.root.Stop(out)
	if m.cell != nil {
		out = m.cell.Stop(out)
	}
	return out
}

// halt ends the work of the agent:
//   - It replies with ErrNotServing to every callback and to the Close that wait for their
//     reply.
//   - It revokes every fencing token.
//   - It withdraws the observation of the node.
//   - It releases the instruments of the agent.
func (m *Machine) halt(out []machine.Effect) []machine.Effect {
	for _, st := range m.shards.All() {
		for _, w := range st.waiting() {
			out = reply(w, ErrNotServing, out)
		}
	}
	if m.closing.reply != 0 {
		out = append(out, machine.Reply[struct{}]{Err: ErrNotServing, To: m.closing.reply})
	}
	m.revoke()
	m.served.Store(nil)
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

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The calls that the errors of an agent name.
const (
	// callCallback is a callback of the controller.
	callCallback = "the callback"
	// callFault is a report of a fault.
	callFault = "the report of the fault"
	// callRecovery is a report of a recovery.
	callRecovery = "the report of the recovery"
	// callClose is a Close.
	callClose = "the close"
)

// incomparable is the first field of each request and each result of the package. The
// compiler then refuses == on the type, so a field that the type gains later does not break
// a comparison. As the first field it does not change the size of the type.
type incomparable [0]func()

// Config is the configuration of an agent. [New] refuses a Config without one of its
// required fields.
type Config struct {
	// Store reads the root scope of the namespace and the scope of the cell of the node. It
	// is required. The capabilities of a store that implements config.Provider decide which
	// strategies the spec of the namespace may select.
	Store store.Reader
	// Membership keeps the session of the node in its cell. It is required. The capabilities
	// of a membership that implements config.Provider decide which strategies the spec of
	// the namespace may select.
	Membership membership.Membership
	// Controller sends the reports of the node, the disruption of a Close and the end of its
	// reservation to the controller of the cell. It is required.
	Controller protocol.ControllerClient
	// Guard acquires the fence of each tenure before the agent calls Handler.AddShard. It is
	// required, because every namespace of the support matrix is exclusive and fenced.
	Guard fence.Acquirer
	// Clock reads the time of each report, renewal, timer and deadline of the agent. It is
	// required.
	Clock clock.Clock
	// Rand is the source of the jitter of each backoff and of the ID of the disruption of a
	// Close. It is required.
	Rand rand.Rand
	// Reporter reports the instruments of the agent. It is required.
	Reporter telemetry.Reporter
	// Registry resolves the spec of the namespace, which the agent reads from the root
	// scope. It is required.
	Registry *config.Registry
	// Logger logs the errors that do not stop the agent, such as a report that fails. It is
	// required.
	Logger *slog.Logger
	// Namespace is the namespace of the node. It is required.
	Namespace shard.Namespace
	// Node is the description of the node. It requires an ID and an address. Node.Cell
	// selects the cell that the node joins, and a node without a cell joins the only cell of
	// the namespace.
	Node shard.Node
	// Unauthenticated makes the agent skip the identity of each caller, for a transport that
	// authenticates no caller. The checks of epochs and incarnations still apply.
	Unauthenticated bool
}

// validate returns an error that wraps ErrConfig for the first field of c that New requires
// and that neither the machine nor the driver of the agent requires. The machine and the
// driver refuse a Config without one of the other required fields.
func (c *Config) validate() error {
	if c.Controller == nil {
		return fmt.Errorf("%w: no controller client", ErrConfig)
	}
	if c.Guard == nil {
		return fmt.Errorf("%w: no fence guard", ErrConfig)
	}
	return nil
}

// Agent hosts the replicas that the controller of the cell of the node places on the node. It
// serves the callbacks of the controller as a protocol.AgentServer, which the embedder serves
// through a transport adapter.
//
// The zero Agent is not usable. Construct one with [New], and start it with [Agent.Start].
//
// # Concurrency
//
// An Agent is safe for concurrent use. Start runs once.
type Agent struct {
	// err is the error that ended the run, or nil after a Close. The run sets it before it
	// closes done.
	err error
	// machine is the machine of the agent.
	machine *agent.Machine
	// driver runs the machine.
	driver *driver.Driver
	// runner is the handler of the agent when the handler is a Runner, and nil otherwise.
	runner *Runner
	// logger logs the work of a tenure that fails.
	logger *slog.Logger
	// cancel ends the context of the run. It is nil until Start has derived that context.
	cancel atomic.Pointer[context.CancelFunc]
	// done is closed once the run has ended.
	done chan struct{}
	// node is the ID of the node of the agent.
	node shard.NodeID
	// started is true once Start has been called.
	started atomic.Bool
	// closed is true once a Close has passed its call to the machine.
	closed atomic.Bool
}

var _ protocol.AgentServer = (*Agent)(nil)

// New returns the agent of cfg.Node, which calls h to start and stop its replicas. The agent
// does not run, read or start a goroutine before [Agent.Start], so the embedder serves it
// through its transport first. Until the node serves, Do, ReportFault, Recovered and Close
// return an error that wraps ErrNotServing. When h implements [LoadReporter], each report of
// the agent has the loads that h reports. When h is a [Runner], New binds it to the agent.
//
// Error modes:
//   - an error that wraps ErrConfig for a nil cfg or a nil h, and for a cfg that lacks a
//     required field or whose Node.Cell is not the text of a shard ID.
//   - an error that wraps ErrConfig for a Runner that another agent uses.
func New(cfg *Config, h Handler) (*Agent, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: no configuration", ErrConfig)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if h == nil {
		return nil, fmt.Errorf("%w: no handler", ErrConfig)
	}
	m, merr := agent.New(&agent.Config{
		Clock:    cfg.Clock,
		Rand:     cfg.Rand,
		Reporter: cfg.Reporter,
		Provided: map[config.Axis][]config.Capability{
			config.Store:      provides(cfg.Store),
			config.Membership: provides(cfg.Membership),
		},
		Logger:          cfg.Logger,
		Registry:        cfg.Registry,
		Namespace:       cfg.Namespace,
		Node:            cfg.Node,
		Unauthenticated: cfg.Unauthenticated,
	})
	dc := driver.Config{
		Machine:     m,
		Reader:      cfg.Store,
		Membership:  cfg.Membership,
		Controllers: cfg.Controller,
		Handler:     newApplication(h),
		Guard:       cfg.Guard,
		Clock:       cfg.Clock,
		Logger:      cfg.Logger,
	}
	if loads, ok := h.(LoadReporter); ok {
		dc.Loads = loads
	}
	d, derr := driver.New(&dc)
	if err := errors.Join(merr, derr); err != nil {
		return nil, fmt.Errorf("%w: node %s: %w", ErrConfig, cfg.Node.ID, err)
	}
	a := &Agent{machine: m, driver: d, logger: cfg.Logger, node: cfg.Node.ID, done: make(chan struct{})}
	if run, ok := h.(*Runner); ok {
		if !run.agent.CompareAndSwap(nil, a) {
			return nil, fmt.Errorf("%w: node %s: the runner serves another agent", ErrConfig, cfg.Node.ID)
		}
		a.runner = run
	}
	return a, nil
}

// Start starts the agent. The agent reads the spec of the namespace and the cell of the node
// from the root scope, joins the cell, and reads the placements of the node from the scope of
// the cell. In the support matrix every replica is a writer, and a node that starts restores
// none. Start returns once the node has a session and the agent has read the scope of its
// cell. The agent then runs until ctx ends or a Close returns.
//
// Error modes:
//   - ErrStarted for a second call.
//   - an error that wraps the error with which the agent stops before it serves, such as an
//     error of class errs.Invalid for a spec that the registry does not resolve or a cell
//     that the namespace does not have.
//   - an error that wraps the cause of ctx when ctx ends before the agent serves.
//
// Start returns its error once every goroutine that it started has returned.
func (a *Agent) Start(ctx context.Context) error {
	if a.started.Swap(true) {
		return ErrStarted
	}
	run, cancel := context.WithCancel(ctx)
	a.cancel.Store(&cancel)
	go a.run(run)
	if err := a.driver.Ready(ctx); err != nil {
		//dokimi:mutate-skip sbr-delete: the run has ended or ends by itself once Ready fails, so cancel only releases the context of the run
		cancel()
		<-a.done
		return fmt.Errorf("replica: the start of node %s: %w", a.node, err)
	}
	return nil
}

// Do runs r, a callback of the controller of the cell. It checks md and r before it looks up
// the key of r or calls the handler. A callback that starts a tenure, such as an add, runs
// once the placements that the agent watches list the tenure at the epoch of r. A repeated
// key returns the kept result. Do does not modify or keep r.
//
// Error modes:
//   - an error of class errs.Transient when the controller epoch of r is above the record of
//     the controller that the agent has read, and when the deadline of ctx passes before r
//     runs.
//   - an error of class errs.Denied when md.Caller is not the identity of that record.
//   - an error of class errs.Conflict for r of another namespace incarnation, another node
//     or node incarnation, a lower epoch, controller epoch or operation than the highest the
//     node has seen, a forward callback of an operation whose abort path the node has seen,
//     and a callback of an operation that the node has seen end.
//   - an error of class errs.Invalid for a key that the node has seen with another payload,
//     and for a request on the path protocol.Cleanup, which only the agent builds.
//   - the error of the handler, or of the acquisition of the fence of an add. Such an error
//     that wraps context.DeadlineExceeded classifies as errs.Transient. The agent does not
//     keep it as the result of the key.
//   - an error that wraps ErrNotServing before the node serves and once the run of the agent
//     has ended.
func (a *Agent) Do(ctx context.Context, md protocol.Metadata, r *protocol.Request) error {
	req := r.CloneKanon()
	err := a.call(ctx, callCallback, func(tok machine.ReplyToken) machine.Event {
		return machine.Do{Request: req, Metadata: md, Reply: tok}
	})
	return machine.ClassifyAttempt(err)
}

// Close moves the replicas of the node away before the process stops. It sends the
// controller a disruption of the node that ends at the deadline of ctx, and repeats it after
// each deferral until the controller admits it, which the controller does once the replicas
// of the node have moved away. Close then leaves the cell and ends the reservation of the
// disruption. It returns once the run of the agent has ended. When ctx ends first, Close
// ends the run without the end of the reservation, and the replicas of the node fail over
// as after a crash: the session of the node ends at the latest when its lease expires, and
// the controller then ends the reservation. Agent.Err returns nil after a Close that ended
// the run.
//
// Error modes:
//   - an error that wraps ErrConfig for a ctx without a deadline.
//   - an error that wraps the cause of ctx when ctx ends before the admission.
//   - an error that wraps ErrNotServing before Start, and when the run of the agent has ended
//     before the Close.
func (a *Agent) Close(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("%w: the context of the close has no deadline", ErrConfig)
	}
	cancel := a.cancel.Load()
	if cancel == nil {
		return fmt.Errorf("%w: %s before the start of node %s", ErrNotServing, callClose, a.node)
	}
	err := a.call(ctx, callClose, func(tok machine.ReplyToken) machine.Event {
		return machine.Close{Deadline: deadline, Reply: tok}
	})
	a.closed.Store(true)
	(*cancel)()
	<-a.done
	return err
}

// Done returns a channel that is closed once the run of the agent has ended and every
// goroutine of the agent has returned. The run ends when the context of Start ends, when a
// Close returns, and when the agent stops itself, such as after the root has created the
// namespace again.
func (a *Agent) Done() <-chan struct{} {
	return a.done
}

// Err returns nil while the agent runs and after a Close that ended the run. After any other
// end of the run, it returns an error that wraps the error of the run: the cause of the
// context of Start, or the error with which the agent stopped itself.
func (a *Agent) Err() error {
	select {
	case <-a.done:
		return a.err
	default:
		return nil
	}
}

// run runs the driver of a until ctx ends or the machine stops itself. It then ends the work
// of every tenure of the runner of a, and closes done. It keeps the error of the run unless a
// Close ended the run.
func (a *Agent) run(ctx context.Context) {
	defer close(a.done)
	err := a.driver.Run(ctx)
	if a.runner != nil {
		a.runner.stop()
	}
	if !a.closed.Load() {
		a.err = fmt.Errorf("replica: the run of the agent: %w", err)
	}
}

// provides returns the capabilities of an adapter that implements config.Provider, and nil
// for any other adapter.
func provides(adapter any) []config.Capability {
	if p, ok := adapter.(config.Provider); ok {
		return p.Provides()
	}
	return nil
}

// call passes the inbound call that event returns to the machine of a, and returns the error
// of the reply of the machine. Every call of an agent replies with an error alone. The errors
// of call contain the text what.
//
// Error modes:
//   - an error that wraps ErrNotServing before Start.
//   - an error that wraps ErrNotServing and driver.ErrStopped once the run of a has ended.
//     It classifies as errs.Transient, the class of ErrNotServing.
//   - an error that wraps the error of the reply, and the cause of ctx when ctx ends first.
func (a *Agent) call(ctx context.Context, what string, event func(machine.ReplyToken) machine.Event) error {
	if a.cancel.Load() == nil {
		return fmt.Errorf("%w: %s before the start of node %s", ErrNotServing, what, a.node)
	}
	_, err := a.driver.Call[struct{}](ctx, event)
	if errors.Is(err, driver.ErrStopped) {
		return errs.WithClass(fmt.Errorf("%w: %s: %w", ErrNotServing, what, err), errs.Transient)
	}
	if err != nil {
		return fmt.Errorf("replica: %s: %w", what, err)
	}
	return nil
}

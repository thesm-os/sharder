// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package embedded

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"

	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/task"
	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/controller"
	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/router"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// Transport is the client of a transport adapter. It sends the callbacks of the controller of
// a process and the calls of its agents to the other processes.
type Transport interface {
	protocol.AgentClient
	protocol.ControllerClient
}

// Config is the configuration of a Process. New refuses a Config without one of its required
// fields.
type Config struct {
	// Namespaces lists the namespaces of the process. It requires at least one, and no two
	// with one name.
	Namespaces []Namespace
	// Store keeps the scopes of each namespace. It is required.
	Store store.Store
	// Membership keeps the sessions of the process and of its nodes. It is required.
	Membership membership.Membership
	// Transport sends the calls of the process. It is required.
	Transport Transport
	// Clock reads the time of every component of the process. It is required.
	Clock clock.Clock
	// Rand is the source of randomness of every component of the process. It is safe for
	// concurrent use, as rand/seeded and rand/crypto are. It is required.
	Rand rand.Rand
	// Reporter reports the instruments of every component of the process. It is required.
	Reporter telemetry.Reporter
	// Registry builds the strategies of each namespace from its spec. It is required.
	Registry *config.Registry
	// Logger logs the errors that do not stop a component of the process. It is required.
	Logger *slog.Logger
	// Operators maps each namespace to the identities that may submit a proposal, as in
	// controller.Config.
	Operators map[shard.Namespace][]shard.Identity
	// Node describes the process. The controller and every agent of the process share it. It
	// requires an ID and an address, and it has no cell.
	Node shard.Node
	// CellCapacity is the most cells that the root of a namespace places on the process, as in
	// controller.Config.
	CellCapacity int
	// Volatile makes the process take a store that does not provide config.Durable, as in
	// controller.Config.
	Volatile bool
	// Unauthenticated makes the controller and every agent of the process skip the identity
	// of each caller, for a transport that authenticates no caller.
	Unauthenticated bool
}

// Namespace is one namespace of a process.
type Namespace struct {
	// Handler starts and stops the replicas of the node. It is required.
	Handler replica.Handler
	// Guard acquires the fence of each tenure of the node. It is required.
	Guard fence.Acquirer
	// Spec is the initial spec of the namespace. The root creates the namespace from it when
	// the root scope has no commit.
	Spec config.Spec
	// Route makes the process run a router of the namespace.
	Route bool
}

// part is one component of a process, such as the agent of a namespace. The run of the
// process starts each part in a task of its own.
type part struct {
	// start starts the component under ctx, and returns once the component serves.
	start func(ctx context.Context) error
	// wait returns once the component has ended. A component ends when ctx ends, and when it
	// stops itself. wait returns the error with which the component stopped itself.
	wait func(ctx context.Context) error
}

// Process is one application process of the topology embedded. It runs the controller of the
// process, the agent of each namespace, and the router of each namespace that routes. It
// runs once.
//
// The zero Process is not usable. Construct one with [New].
//
// # Concurrency
//
// A Process is safe for concurrent use.
type Process struct {
	// err is the error that ended the run, or nil after a Close. The run sets it before it
	// closes done.
	err error
	// controller is the controller of the process.
	controller *controller.Controller
	// server passes each callback to the controller or to the agent of its namespace.
	server protocol.AgentServer
	// agents maps each namespace to its agent.
	agents map[shard.Namespace]*replica.Agent
	// routers maps each namespace that routes to its router once the router serves, and to
	// nil when the router has not started.
	routers map[shard.Namespace]*router.Watcher
	// cancel ends the context of the run. It is nil until Start has derived that context.
	cancel context.CancelFunc
	// done is closed once the run has ended and every goroutine of the process has returned,
	// and by a Close before Start.
	done chan struct{}
	// cfg is the configuration of the process.
	cfg Config
	// mu guards routers, cancel, started and closed.
	mu sync.Mutex
	// started is true once Start or Close has been called.
	started bool
	// closed is true once Close has been called.
	closed bool
}

// New returns the process of cfg. It builds the controller of the process and the agent of
// each namespace, and the server of the callbacks of the process. It does not start a
// goroutine or read the store, so the embedder serves the process through its transport
// before Start.
//
// Error modes:
//   - an error that wraps ErrConfig when cfg is nil, has no namespace or has two namespaces
//     with one name.
//   - an error that wraps ErrConfig and every error of controller.New and replica.New.
func New(cfg *Config) (*Process, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: no configuration", ErrConfig)
	}
	if len(cfg.Namespaces) == 0 {
		return nil, fmt.Errorf("%w: no namespace", ErrConfig)
	}
	seen := make(map[shard.Namespace]bool, len(cfg.Namespaces))
	for i := range cfg.Namespaces {
		ns := cfg.Namespaces[i].Spec.Namespace
		if seen[ns] {
			return nil, fmt.Errorf("%w: two namespaces %s", ErrConfig, ns)
		}
		seen[ns] = true
	}
	ctrl, err := controller.New(&controller.Config{
		Store:           cfg.Store,
		Membership:      cfg.Membership,
		Agents:          cfg.Transport,
		Clock:           cfg.Clock,
		Rand:            cfg.Rand,
		Reporter:        cfg.Reporter,
		Registry:        cfg.Registry,
		Logger:          cfg.Logger,
		Operators:       cfg.Operators,
		Node:            cfg.Node,
		CellCapacity:    cfg.CellCapacity,
		Volatile:        cfg.Volatile,
		Unauthenticated: cfg.Unauthenticated,
	})
	agents := make(map[shard.Namespace]*replica.Agent, len(cfg.Namespaces))
	servers := make(map[shard.Namespace]protocol.AgentServer, len(cfg.Namespaces))
	for i := range cfg.Namespaces {
		n := &cfg.Namespaces[i]
		ns := n.Spec.Namespace
		a, aerr := replica.New(&replica.Config{
			Store:           cfg.Store,
			Membership:      cfg.Membership,
			Controller:      cfg.Transport,
			Guard:           n.Guard,
			Clock:           cfg.Clock,
			Rand:            cfg.Rand,
			Reporter:        cfg.Reporter,
			Registry:        cfg.Registry,
			Logger:          cfg.Logger,
			Namespace:       ns,
			Node:            cfg.Node,
			Unauthenticated: cfg.Unauthenticated,
		}, n.Handler)
		err = errors.Join(err, aerr)
		agents[ns], servers[ns] = a, a
	}
	if err != nil {
		return nil, fmt.Errorf("%w: process %s: %w", ErrConfig, cfg.Node.ID, err)
	}
	return &Process{
		controller: ctrl,
		server:     protocol.Dispatch(ctrl, servers),
		agents:     agents,
		routers:    make(map[shard.Namespace]*router.Watcher),
		done:       make(chan struct{}),
		cfg:        *cfg,
	}, nil
}

// AgentServer returns the server of the callbacks of the process: protocol.Dispatch over the
// controller and the agent of each namespace. The embedder serves it through its transport.
// A callback that arrives before the transport serves the process fails as unreachable, and
// the controller that sent it sends it again.
func (p *Process) AgentServer() protocol.AgentServer {
	return p.server
}

// ControllerServer returns the controller of the process, which the embedder serves at the
// address of the node.
func (p *Process) ControllerServer() protocol.ControllerServer {
	return p.controller
}

// Start runs the controller of the process, then starts the agent of each namespace, then the
// router of each namespace whose Route is set. It returns once every agent serves and every
// router has read its namespace. The process then runs until Close, until ctx ends, or until
// a component stops itself. The end of one component stops every component.
//
// Error modes:
//   - ErrStarted for a second call, and for a call after Close.
//   - an error that wraps the error of Agent.Start or router.New, such as the cause of ctx
//     when ctx ends before the process serves.
//
// Start returns its error once every goroutine that it started has returned.
func (p *Process) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return ErrStarted
	}
	run, cancel := context.WithCancel(ctx)
	p.started, p.cancel = true, cancel
	p.mu.Unlock()
	ready := make(chan error, 1)
	go p.run(run, cancel, ready)
	if err := <-ready; err != nil {
		<-p.done
		return fmt.Errorf("embedded: the start of process %s: %w", p.cfg.Node.ID, err)
	}
	return nil
}

// Agent returns the agent of the namespace ns, and reports whether the process has one.
func (p *Process) Agent(ns shard.Namespace) (*replica.Agent, bool) {
	a, ok := p.agents[ns]
	return a, ok
}

// Router returns the router of the namespace ns, and reports whether the process runs one.
// The router of a namespace whose Route is set runs once Start has returned nil.
func (p *Process) Router(ns shard.Namespace) (*router.Watcher, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	w := p.routers[ns]
	return w, w != nil
}

// Close moves the replicas of the node away through Agent.Close of every namespace at once,
// then closes every router and stops the controller. The controller stops last, because it
// can host the controller of the cell that admits the drains. ctx bounds the moves, and
// requires a deadline. When ctx ends first, the replicas of the node fail over as after a
// crash. A Close before Start stops nothing, and a later Start returns ErrStarted. A Close
// after the end of the run returns nil.
//
// Error modes:
//   - an error that wraps the error of Agent.Close of each namespace, such as replica.ErrConfig
//     for a ctx without a deadline, or the cause of ctx when ctx ends before the replicas have
//     moved.
//
// Close returns once every goroutine of the process has returned.
func (p *Process) Close(ctx context.Context) error {
	p.mu.Lock()
	started := p.started
	p.started, p.closed = true, true
	cancel := p.cancel
	if !started {
		close(p.done)
	}
	p.mu.Unlock()
	select {
	case <-p.done:
		return nil
	default:
	}
	drains := make([]error, len(p.cfg.Namespaces))
	var wg sync.WaitGroup
	for i := range p.cfg.Namespaces {
		a := p.agents[p.cfg.Namespaces[i].Spec.Namespace]
		wg.Go(func() { drains[i] = a.Close(ctx) })
	}
	wg.Wait()
	cancel()
	<-p.done
	if err := errors.Join(drains...); err != nil {
		return fmt.Errorf("embedded: the close of process %s: %w", p.cfg.Node.ID, err)
	}
	return nil
}

// Done returns a channel that is closed once the run of the process has ended and every
// goroutine of the process has returned. A Close before Start closes it too.
func (p *Process) Done() <-chan struct{} {
	return p.done
}

// Err returns nil while the process runs, and after a Close that ended the run. After any
// other end of the run, it returns the error of the component that stopped itself, or the
// cause of the context of Start.
func (p *Process) Err() error {
	select {
	case <-p.done:
		return p.err
	default:
		return nil
	}
}

// run runs the parts of the process under ctx until the first of them ends, and then calls
// cancel and closes done. It reports the result of the starts on ready. A part that ends
// because ctx ended returns nil, and the end of any other part stops the run with the error
// of the part.
func (p *Process) run(ctx context.Context, cancel context.CancelFunc, ready chan<- error) {
	defer close(p.done)
	//dokimi:mutate-skip sbr-delete: cancel releases the context of a run that has ended, which no caller reads
	defer cancel()
	parts := p.parts()
	err := task.Run(ctx, math.MaxInt, func(ctx context.Context, g *task.Group) error {
		// The body returns nil. The task of a part that fails to start returns the error, and a
		// refused task records the cause of ctx. When ctx ends first, the run takes its cause.
		ready <- start(ctx, g, parts)
		return nil
	})
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if err == nil && !closed {
		err = context.Cause(ctx)
	}
	p.err = err
}

// parts returns the parts of the process in the order of their starts: the controller, the
// agent of each namespace, and then the router of each namespace that routes.
func (p *Process) parts() []part {
	specs := make([]config.Spec, 0, len(p.cfg.Namespaces))
	for i := range p.cfg.Namespaces {
		specs = append(specs, p.cfg.Namespaces[i].Spec)
	}
	parts := []part{{
		start: func(context.Context) error { return nil },
		wait: func(ctx context.Context) error {
			return p.controller.Run(ctx, specs...)
		},
	}}
	for i := range p.cfg.Namespaces {
		a := p.agents[p.cfg.Namespaces[i].Spec.Namespace]
		parts = append(parts, part{
			start: a.Start,
			wait: func(context.Context) error {
				<-a.Done()
				return a.Err()
			},
		})
	}
	for i := range p.cfg.Namespaces {
		if !p.cfg.Namespaces[i].Route {
			continue
		}
		cfg := router.Config{
			Store:      p.cfg.Store,
			Membership: p.cfg.Membership,
			Clock:      p.cfg.Clock,
			Rand:       p.cfg.Rand,
			Reporter:   p.cfg.Reporter,
			Registry:   p.cfg.Registry,
			Logger:     p.cfg.Logger,
			Namespace:  p.cfg.Namespaces[i].Spec.Namespace,
		}
		var w *router.Watcher
		parts = append(parts, part{
			start: func(ctx context.Context) error {
				var err error
				w, err = router.New(ctx, &cfg)
				p.mu.Lock()
				p.routers[cfg.Namespace] = w
				p.mu.Unlock()
				//nolint:wrapcheck // Start wraps the error of the start of the process.
				return err
			},
			wait: func(ctx context.Context) error {
				select {
				case <-w.Done():
				case <-ctx.Done():
				}
				return errors.Join(w.Err(), w.Close())
			},
		})
	}
	return parts
}

// start starts each part of parts in a task of g, in order, and waits until the part serves
// before it starts the next. The task of a part then waits for the end of the part. start
// returns nil once every part serves. It returns the error of the first part that fails to
// start, and the cause of ctx when ctx ends first, and starts nothing after it.
func start(ctx context.Context, g *task.Group, parts []part) error {
	served := make(chan struct{}, 1)
	for _, c := range parts {
		err := g.Go(func(ctx context.Context) error {
			if err := c.start(ctx); err != nil {
				return err
			}
			served <- struct{}{}
			err := c.wait(ctx)
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		})
		//dokimi:mutate-skip ror-false,sbr-delete: Go refuses a task only once ctx has ended, and the select then returns the cause of ctx
		if err != nil {
			//nolint:wrapcheck // Start wraps the error of the start of the process.
			return err
		}
		select {
		case <-served:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return nil
}

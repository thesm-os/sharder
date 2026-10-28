// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/routes"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// Config is the configuration of a router in watch mode. [New] refuses a Config without one
// of its required fields.
type Config struct {
	// Store reads the root scope of the namespace and the scope of each cell. It is required.
	// The capabilities of a store that implements config.Provider decide which strategies the
	// spec of the namespace may select.
	Store store.Reader
	// Membership watches the views of each cell. Each route takes the addresses of its nodes
	// from the views. It is required. The capabilities of a membership that implements
	// config.Provider decide which strategies the spec of the namespace may select.
	Membership membership.Membership
	// Clock times the backoff before a read of the router runs again. It is required.
	Clock clock.Clock
	// Rand is the source of the jitter of each backoff. It is required.
	Rand rand.Rand
	// Reporter reports the instruments of the router. It is required.
	Reporter telemetry.Reporter
	// Registry resolves the spec of the namespace and builds its partitioner. The router reads
	// the spec from the root scope. It is required.
	Registry *config.Registry
	// Logger logs the errors that do not stop the router, such as a namespace whose positions
	// the router cannot compute after the root has created it again. It is required.
	Logger *slog.Logger
	// Namespace is the namespace of the router. It is required.
	Namespace shard.Namespace
}

// Watcher is the router of one namespace in watch mode. The support matrix has no other
// routing source. Watcher implements [Router].
//
// A Watcher reads the spec, the mapping and the cell map of the namespace from the root
// scope, and builds the partitioner of the spec. It reads the scope and watches the views of
// every cell of the cell map, and keeps the route of each shard. It reads the namespace again
// when a read reports another namespace incarnation, and when a Refresh is of another
// incarnation than the installed one. The routes of the installed incarnation serve until the
// read of the new one is complete.
//
// The zero Watcher is not usable. Construct one with [New].
//
// # Concurrency
//
// A Watcher is safe for concurrent use.
//
//dokimi:suite
//dokimi:out path=routertest/
//dokimi:subject Watcher new=newWatcher
type Watcher struct {
	// err is the error that ended the run, or nil after a Close. The run sets it before it
	// closes done.
	err error
	// machine is the machine of the router.
	machine *routes.Machine
	// driver runs the machine.
	driver *driver.Driver
	// cancel ends the context of the run.
	cancel context.CancelFunc
	// done is closed once the run has ended.
	done chan struct{}
	// closed is true once a Close has begun.
	closed atomic.Bool
}

var _ Router = (*Watcher)(nil)

// New starts the router of cfg.Namespace in watch mode. It returns once the root has created
// the namespace and the router has read the scope and the first view of every cell. ctx
// bounds the wait of New alone. The router then runs until Close or until a read fails with
// an error that a retry does not fix.
//
// Error modes:
//   - an error that wraps ErrConfig for a nil cfg, and for a cfg that lacks a required field.
//   - an error that wraps ErrMapping for a namespace whose positions the router cannot
//     compute, such as a spec that cfg.Registry does not resolve with the capabilities of
//     cfg.Store and cfg.Membership.
//   - an error that wraps the error of a read that stops the router before it serves.
//   - an error that wraps the cause of ctx when ctx ends before the router serves.
//
// New returns its error once every goroutine that it started has returned.
func New(ctx context.Context, cfg *Config) (*Watcher, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: no configuration", ErrConfig)
	}
	m, merr := routes.New(&routes.Config{
		Rand:     cfg.Rand,
		Reporter: cfg.Reporter,
		Logger:   cfg.Logger,
		Registry: cfg.Registry,
		Provided: map[config.Axis][]config.Capability{
			config.Store:      provides(cfg.Store),
			config.Membership: provides(cfg.Membership),
		},
		Namespace: cfg.Namespace,
	})
	d, derr := driver.New(&driver.Config{
		Machine:    m,
		Reader:     cfg.Store,
		Membership: cfg.Membership,
		Clock:      cfg.Clock,
		Logger:     cfg.Logger,
	})
	if err := errors.Join(merr, derr); err != nil {
		return nil, fmt.Errorf("%w: namespace %s: %w", ErrConfig, cfg.Namespace, err)
	}
	run, cancel := context.WithCancel(context.WithoutCancel(ctx))
	w := &Watcher{machine: m, driver: d, cancel: cancel, done: make(chan struct{})}
	go w.run(run)
	if err := d.Ready(ctx); err != nil {
		cancel()
		<-w.done
		return nil, fmt.Errorf("router: the start of namespace %s: %w", cfg.Namespace, err)
	}
	return w, nil
}

// Route returns the route of the shard of req.Key. It computes the position of the key, finds
// the cell of the position in the cell map, and loads the route of its slot from the cache of
// the cell. Route reads the routes that the router published last, so it does not wait for
// the router. It counts the hit or the miss of the key in the instruments of the router with
// ctx.
//
// Error modes:
//   - ErrNoRoute for a key whose shard has no route.
//   - an error that wraps ErrMapping for every key of a namespace whose positions the router
//     cannot compute.
//   - ErrClosed once the run of the router has ended.
//
// # Allocation contract
//
// Zero alloc. A request in a local variable of the caller remains on the stack of the caller.
//
//dokimi:budget allocs=0
//dokimi:mixin concurrent
func (w *Watcher) Route(ctx context.Context, req *RouteRequest) (RouteResult, error) {
	r, err := w.machine.Route(ctx, req.Key)
	//nolint:wrapcheck // Route returns the sentinels of the router as they are, so errors.Is matches them without an allocation.
	return RouteResult{Route: r}, err
}

// Refresh returns once the router has applied generation req.Min of the cell req.Cell in the
// namespace incarnation req.Incarnation, or a later generation of the cell. A Refresh of
// another incarnation than the installed one makes the router read the root scope again.
//
// Error modes:
//   - an error that wraps ErrConfig for a request that lacks a namespace incarnation or a
//     cell.
//   - an error that wraps shard.ErrIncarnation when the router has read the root scope again
//     and req.Incarnation is not the incarnation of the namespace.
//   - an error that wraps ErrCell for a cell that the cell map does not have.
//   - an error that wraps ErrMapping for a namespace whose positions the router cannot
//     compute.
//   - an error that wraps ErrClosed once the run of the router has ended, also for a call
//     that waits when the run ends.
//   - an error that wraps the cause of ctx when ctx ends first.
//
//dokimi:mixin waits
func (w *Watcher) Refresh(ctx context.Context, req *RefreshRequest) (RefreshResult, error) {
	if req.Incarnation.IsZero() {
		return RefreshResult{}, fmt.Errorf("%w: a refresh without a namespace incarnation", ErrConfig)
	}
	if req.Cell == "" {
		return RefreshResult{}, fmt.Errorf("%w: a refresh without a cell", ErrConfig)
	}
	ev := machine.Refresh{Cell: req.Cell, Min: req.Min, Incarnation: req.Incarnation}
	_, err := w.driver.Call[struct{}](ctx, func(tok machine.ReplyToken) machine.Event {
		ev.Reply = tok
		return ev
	})
	if errors.Is(err, driver.ErrStopped) {
		return RefreshResult{}, fmt.Errorf("%w: the refresh of cell %s: %w", ErrClosed, req.Cell, err)
	}
	if err != nil {
		return RefreshResult{}, fmt.Errorf("router: the refresh of cell %s: %w", req.Cell, err)
	}
	return RefreshResult{}, nil
}

// Close stops the router and returns once every goroutine of the router has returned. Route
// and Refresh return ErrClosed after Close. Close always returns nil.
func (w *Watcher) Close() error {
	w.closed.Store(true)
	w.cancel()
	<-w.done
	return nil
}

// Done returns a channel that is closed once the run of the router has ended and every
// goroutine of the router has returned. The run ends when Close stops it, and when the
// router stops itself after a read that fails with an error that a retry does not fix.
func (w *Watcher) Done() <-chan struct{} {
	return w.done
}

// Err returns nil while the router runs and after a Close that ended the run. After the
// router has stopped itself, it returns an error that wraps the error of the read that
// stopped it.
func (w *Watcher) Err() error {
	select {
	case <-w.done:
		return w.err
	default:
		return nil
	}
}

// run runs the driver of w until ctx ends or the machine stops itself, and then closes done.
// It keeps the error of the run unless a Close ended the run.
func (w *Watcher) run(ctx context.Context) {
	defer close(w.done)
	err := w.driver.Run(ctx)
	if !w.closed.Load() {
		w.err = fmt.Errorf("router: the run of the router: %w", err)
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

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"

	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/pool"
	"go.thesmos.sh/core/task"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// Handler is the handler of the application. The driver calls AddShard for a
// machine.Handle of a request whose Call is protocol.Add, and DropShard for a request whose
// Call is protocol.Drop. AddShard and DropShard do not modify the request or the token, and
// do not keep them after they return.
type Handler interface {
	// AddShard starts serving r.Shard in r.Role. t is the fencing token of the tenure that r
	// starts.
	AddShard(ctx context.Context, r *protocol.Request, t *fence.Token) error
	// DropShard stops serving r.Shard, and releases its local resources.
	DropShard(ctx context.Context, r *protocol.Request) error
}

// Revoker is a Handler that ends the tenure of a fencing token. The driver calls RevokeShard
// for a machine.Revoke when its Handler implements Revoker. RevokeShard does not modify the
// token, and does not keep it after it returns.
type Revoker interface {
	// RevokeShard ends the work of the tenure of t, whose session has ended.
	RevokeShard(ctx context.Context, t *fence.Token) error
}

// LoadReporter reports the load of the replicas of an application. The driver sends each
// report of an agent with the loads that it reports.
type LoadReporter interface {
	// ShardLoad returns the load of the replica of the shard id, or nil for a replica
	// without a load.
	ShardLoad(id shard.ID) shard.Load
}

// Config configures a [Driver]. [New] refuses a Config without one of the required fields.
// An optional field serves only the machines that return its effects, such as the Handler
// of the agent. An effect whose dependency the Config does not have gets a result with an
// error that wraps [ErrConfig].
type Config struct {
	// Machine is the machine that the driver runs. It is required.
	Machine machine.Machine
	// Reader performs Head, Read and Watch. It is required.
	Reader store.Reader
	// Store performs Commit, Outcome and Campaign. The root and the controller of a cell
	// have it, and the agent and the router do not.
	Store store.Store
	// Membership performs Join and WatchViews. It is required.
	Membership membership.Membership
	// Agents performs Callback. The root and the controller of a cell have it.
	Agents protocol.AgentClient
	// Controllers performs SendReport, SendDisrupt and SendDone. The agent has it.
	Controllers protocol.ControllerClient
	// Handler performs Handle. The agent has it.
	Handler Handler
	// Loads reports the loads of the replicas of each SendReport. Without it, the driver
	// sends each report as the machine built it.
	Loads LoadReporter
	// Guard performs Acquire. The agent of an exclusive namespace has it.
	Guard fence.Acquirer
	// Clock times each Timer. It is required.
	Clock clock.Clock
	// Logger receives the failures of Leave and Resign, which have no result of their own.
	// It is required.
	Logger *slog.Logger
}

// Driver runs one machine in production. It passes the events of the machine one at a
// time, and performs the effects of each step on goroutines of core's task.Run.
//
// The zero Driver is not usable. Construct one with [New].
//
// # Concurrency
//
// A Driver is safe for concurrent use. Run runs once, and any number of goroutines call
// Ready and Call.
type Driver struct {
	// machine is the machine of the run.
	machine machine.Machine
	// reader performs Head, Read and Watch.
	reader store.Reader
	// store performs Commit, Outcome and Campaign, or is nil.
	store store.Store
	// membership performs Join and WatchViews.
	membership membership.Membership
	// agents performs Callback, or is nil.
	agents protocol.AgentClient
	// controllers performs SendReport, SendDisrupt and SendDone, or is nil.
	controllers protocol.ControllerClient
	// handler performs Handle, or is nil.
	handler Handler
	// loads reports the loads of each SendReport, or is nil.
	loads LoadReporter
	// guard performs Acquire, or is nil.
	guard fence.Acquirer
	// clock times each Timer.
	clock clock.Clock
	// logger receives the failures of Leave and Resign.
	logger *slog.Logger
	// calls receives each inbound call of Call.
	calls chan call
	// replies keeps the channels of the replies of calls that have received their reply.
	replies *pool.Pool[chan machine.Effect]
	// cancels receives the token of each call whose context ended before its reply.
	cancels chan machine.ReplyToken
	// results receives the result of each effect.
	results chan machine.Event
	// stopping is closed when the run stops, before the run passes Stopped.
	stopping chan struct{}
	// started is closed once the machine has replied to Started, or once the run has
	// stopped without that reply.
	started chan struct{}
	// startErr is the error of the reply to Started, or ErrStopped for a run that stopped
	// without that reply. The run sets it before it closes started.
	startErr error
	// tokens counts the ReplyTokens of the run.
	tokens atomic.Uint64
	// ran reports that Run has been called.
	ran atomic.Bool
}

// New returns a Driver of cfg that has not run.
//
// Error modes:
//   - an error that wraps ErrConfig for a nil cfg, and for a cfg without a Machine, a
//     Reader, a Membership, a Clock or a Logger.
func New(cfg *Config) (*Driver, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: no configuration", ErrConfig)
	}
	if cfg.Machine == nil {
		return nil, fmt.Errorf("%w: no machine", ErrConfig)
	}
	if cfg.Reader == nil {
		return nil, fmt.Errorf("%w: no store reader", ErrConfig)
	}
	if cfg.Membership == nil {
		return nil, fmt.Errorf("%w: no membership", ErrConfig)
	}
	if cfg.Clock == nil {
		return nil, fmt.Errorf("%w: no clock", ErrConfig)
	}
	if cfg.Logger == nil {
		return nil, fmt.Errorf("%w: no logger", ErrConfig)
	}
	return &Driver{
		machine:     cfg.Machine,
		reader:      cfg.Reader,
		store:       cfg.Store,
		membership:  cfg.Membership,
		agents:      cfg.Agents,
		controllers: cfg.Controllers,
		handler:     cfg.Handler,
		loads:       cfg.Loads,
		guard:       cfg.Guard,
		clock:       cfg.Clock,
		logger:      cfg.Logger,
		calls:       make(chan call),
		replies:     pool.NewPool(func() chan machine.Effect { return make(chan machine.Effect, 1) }),
		cancels:     make(chan machine.ReplyToken),
		results:     make(chan machine.Event),
		stopping:    make(chan struct{}),
		started:     make(chan struct{}),
	}, nil
}

// Run runs the machine until ctx ends, and returns once every goroutine of the run has
// returned. It passes Started first. It then passes one event at a time, and each event is
// one of these:
//   - the result of an effect.
//   - the event of a call of Call.
//   - the Cancelled of a call whose context ended before its reply.
//
// Run performs the effects of each step as tasks of core's task.Run, and passes each Reply
// of the machine to the call that waits for it.
//
// Once ctx ends, Run stops the machine. It cancels every effect in flight, passes Stopped,
// and performs only the replies of the effects of Stopped. It stops the machine the same
// way in these cases:
//   - The machine replies to Started with an error.
//   - The machine has stopped itself with an error.
//   - The machine returns an effect that the driver does not perform.
//
// The machine bounds the effects in flight, such as the callbacks to each node, so the
// group of the run has no limit of its own.
//
// Run always returns an error, because only an error stops a run.
//
// Error modes:
//   - ErrRun when Run has been called before.
//   - an error that wraps the error of the reply to Started, and an error that wraps
//     ErrReply for a reply to Started of another type than Reply[struct{}].
//   - an error that wraps the error of Machine.Err once the machine has stopped itself.
//   - an error that wraps ErrEffect for an effect of a type that the driver does not
//     perform.
//   - an error that wraps the cause of ctx once ctx ends.
func (d *Driver) Run(ctx context.Context) error {
	if d.ran.Swap(true) {
		return ErrRun
	}
	err := task.Run(ctx, math.MaxInt, func(ctx context.Context, g *task.Group) error {
		r := &run{
			d:       d,
			group:   g,
			pending: make(map[machine.ReplyToken]chan<- machine.Effect),
			ends:    make(map[machine.Token]ending),
			start:   machine.ReplyToken(d.tokens.Add(1)),
		}
		return r.loop(ctx)
	})
	return fmt.Errorf("driver: the run of the machine stopped: %w", err)
}

// Ready waits until the machine has replied to Started, and returns the error of the
// reply. Any number of goroutines call Ready, and each gets the same result.
//
// Error modes:
//   - the error of the reply to Started.
//   - the cause of ctx when ctx ends before the reply.
//   - ErrStopped when Run returns without a reply to Started.
func (d *Driver) Ready(ctx context.Context) error {
	select {
	case <-d.started:
		return d.startErr
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

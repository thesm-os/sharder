// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/pool"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// The attributes of the log of a runner.
const (
	// attrShard is the attribute of the shard of a tenure.
	attrShard = "shard"
	// attrError is the attribute of the error of the work of a tenure.
	attrError = "error"
	// attrReport is the attribute of the error of the report of a fault, or nil.
	attrReport = "report"
)

// Opener opens the tenures of the shards of a node. The application implements it, and a
// Runner calls it.
type Opener interface {
	// Open starts serving the tenure of req. It reads the data of the shard only from a state
	// that contains the acquisition of the fence, as Handler.AddShard does. It records the
	// key of req.Request in the transaction of each effect that must not happen twice. It
	// returns once the replica serves, with the work of the tenure. Open does not modify req,
	// and does not keep it after it returns.
	Open(ctx context.Context, req *OpenRequest) (OpenResult, error)
}

// OpenRequest is the request of Opener.Open.
type OpenRequest struct {
	_ incomparable
	// Request is the add that starts the tenure. Request.Key identifies the add across its
	// deliveries.
	Request *protocol.Request
	// Token is the fencing token of the tenure.
	Token fence.Token
}

// OpenResult is the result of Opener.Open. The runner keeps its Run until the work of the
// tenure has ended.
type OpenResult struct {
	_ incomparable
	// Run runs the work of the tenure until its context ends. A nil Run means a tenure
	// without work of its own, such as a replica that serves the requests of a router.
	Run func(ctx context.Context) error
}

// running is the work of one tenure of a shard on the node of a runner.
type running struct {
	// stop ends the context of the work.
	stop context.CancelFunc
	// done is closed once the work has returned.
	done chan struct{}
	// epoch is the epoch of the tenure.
	epoch epoch.Epoch
}

// end ends the work of t, and returns once the work has returned. A nil t has no work.
func (t *running) end() {
	if t == nil {
		return
	}
	t.stop()
	<-t.done
}

// Runner is a Handler that opens each tenure of the node through an Opener, and runs the work
// of the tenure until the drop of the tenure.
//
// An add of the tenure that runs returns nil and leaves its work running. An add of a newer
// tenure of the shard ends the work of the older tenure, waits for it, and then opens the
// newer tenure. A drop ends the work of the tenure, and returns once the work has returned.
// A work that returns an error before its context ends makes the runner report a permanent
// fault of the replica, and log the error. An error that wraps epoch.ErrFenced means that a
// newer tenure has acquired the fence, and the runner does not report it.
//
// New binds a Runner to its agent, and a Runner serves one agent. The agent ends the work of
// every tenure before it closes Done.
//
// The zero Runner is not usable. Construct one with [NewRunner].
//
// # Concurrency
//
// A Runner is safe for concurrent use. The agent calls one method at a time for each shard.
//
// # Allocation contract
//
// An open takes its request from a pool. A tenure allocates its context, its channel and its
// goroutine.
type Runner struct {
	// opener opens the tenures.
	opener Opener
	// agent is the agent that the runner serves. New sets it.
	agent atomic.Pointer[Agent]
	// requests keeps the requests of Open.
	requests *pool.Pool[*OpenRequest]
	// tenures maps each shard with a tenure on the node to the work of the tenure.
	tenures map[shard.ID]*running
	// mu guards tenures.
	mu sync.Mutex
}

var _ Handler = (*Runner)(nil)

// NewRunner returns the runner of o.
func NewRunner(o Opener) *Runner {
	return &Runner{
		opener:   o,
		requests: pool.NewPool(func() *OpenRequest { return new(OpenRequest) }),
		tenures:  make(map[shard.ID]*running),
	}
}

// AddShard opens the tenure of req through the Opener, and starts the work that Open returns.
// It returns nil for an add of the tenure that runs. It ends the work of an older tenure of
// the shard before it opens the tenure of req.
//
// Error modes:
//   - an error that wraps the error of Open. No work starts.
func (r *Runner) AddShard(ctx context.Context, req *AddShardRequest) (AddShardResult, error) {
	sid, e := req.Request.Shard, req.Token.Epoch
	r.mu.Lock()
	older := r.tenures[sid]
	r.mu.Unlock()
	if older != nil && older.epoch == e {
		return AddShardResult{}, nil
	}
	older.end()
	open := r.requests.Get()
	open.Request, open.Token = req.Request, req.Token
	res, err := r.opener.Open(ctx, open)
	open.Request, open.Token = nil, fence.Token{}
	//dokimi:mutate-skip sbr-delete: a request that the pool does not get back costs one allocation of a later open, which no test observes
	r.requests.Put(open)
	if err != nil {
		return AddShardResult{}, fmt.Errorf("replica: the open of shard %v at epoch %d: %w", sid, e, err)
	}
	work, stop := context.WithCancel(context.WithoutCancel(ctx))
	t := &running{stop: stop, done: make(chan struct{}), epoch: e}
	r.mu.Lock()
	r.tenures[sid] = t
	r.mu.Unlock()
	go r.run(work, t, sid, res.Run)
	return AddShardResult{}, nil
}

// DropShard ends the work of the tenure of the shard of req, and returns once the work has
// returned. A drop of a shard without a tenure returns nil.
func (r *Runner) DropShard(_ context.Context, req *DropShardRequest) (DropShardResult, error) {
	r.mu.Lock()
	t := r.tenures[req.Request.Shard]
	delete(r.tenures, req.Request.Shard)
	r.mu.Unlock()
	t.end()
	return DropShardResult{}, nil
}

// run runs work, the work of the tenure t of the shard sid, until ctx ends, and closes t.done
// once work has returned. A nil work returns at once. When work returns an error before ctx
// ends, run reports a permanent fault of the replica through the agent of r, unless the error
// wraps epoch.ErrFenced, and logs the error with the error of the report.
func (r *Runner) run(ctx context.Context, t *running, sid shard.ID, work func(ctx context.Context) error) {
	defer close(t.done)
	if work == nil {
		return
	}
	err := work(ctx)
	if err == nil || ctx.Err() != nil || errors.Is(err, epoch.ErrFenced) {
		return
	}
	a := r.agent.Load()
	_, ferr := a.ReportFault(
		context.WithoutCancel(ctx),
		&ReportFaultRequest{Cause: err, Shard: sid, Fault: shard.FaultPermanent},
	)
	a.logger.LogAttrs(ctx, slog.LevelWarn, "replica: the work of a tenure failed",
		slog.String(attrShard, sid.String()), slog.Any(attrError, err), slog.Any(attrReport, ferr))
}

// stop ends the work of every tenure of r, and returns once the work has returned. The agent
// calls it once its run has ended.
func (r *Runner) stop() {
	r.mu.Lock()
	tenures := r.tenures
	r.tenures = make(map[shard.ID]*running)
	r.mu.Unlock()
	for _, t := range tenures {
		t.end()
	}
}

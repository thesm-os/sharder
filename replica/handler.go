// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica

import (
	"context"
	"fmt"

	"go.thesmos.sh/core/pool"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// Handler is the handler of the application. The agent calls AddShard to start a replica of
// the node, and DropShard to stop one.
//
// Callbacks arrive at least once, and the agent calls one method at a time for each shard.
// The agent refuses a stale callback, and returns its kept result for a repeated key, before
// it calls the handler. A handler whose callback has an effect that must not happen twice
// records the key of the callback in the same transaction as the effect, and returns the
// recorded result for a key that it finds. It may delete a recorded key once it has returned
// nil for a callback of a later operation of the shard. The context ends when the timeout of
// the step passes, and a handler that ignores the end keeps the next callback of the shard
// waiting.
//
// The agent hands out each request from a pool. A handler does not modify a request, and
// does not keep it after it returns.
//
//dokimi:stub
//dokimi:out path=replicatest/
type Handler interface {
	// AddShard starts serving the shard of the callback in its role. The agent acquires the
	// fence of the callback at its epoch before it calls AddShard. AddShard discards every
	// state that it derived from an earlier tenure of the shard. It reads the data of the
	// shard only from a state that contains the acquisition: a new transaction on the
	// authoritative data source, or a replica that has applied the acquisition and every
	// fenced write before it. It returns when the replica serves.
	AddShard(ctx context.Context, req *AddShardRequest) (AddShardResult, error)
	// DropShard stops serving the shard of the callback, and releases its local resources.
	// The Path of the callback is protocol.Cleanup when the agent drops a removed tenure by
	// itself.
	DropShard(ctx context.Context, req *DropShardRequest) (DropShardResult, error)
}

// AddShardRequest is the request of Handler.AddShard.
type AddShardRequest struct {
	_ incomparable
	// Request is the callback that starts the replica. Request.Key identifies the callback
	// across its deliveries.
	Request *protocol.Request
	// Token is the fencing token of the tenure that the add starts. The agent acquires the
	// fence with it before it calls AddShard.
	Token fence.Token
}

// AddShardResult is the result of Handler.AddShard.
type AddShardResult struct {
	_ incomparable
}

// DropShardRequest is the request of Handler.DropShard.
type DropShardRequest struct {
	_ incomparable
	// Request is the callback that stops the replica. Request.Key identifies the callback
	// across its deliveries.
	Request *protocol.Request
}

// DropShardResult is the result of Handler.DropShard.
type DropShardResult struct {
	_ incomparable
}

// Revoker is a Handler that ends the work of a tenure whose session has ended. When the
// Handler of an agent implements Revoker, the agent calls RevokeShard at the end of the
// session of the node under ownership.Stop, once for each tenure whose token Agent.Token
// returned until the end. The agent calls RevokeShard in the turn of the shard, one call at a
// time for each shard, as it calls AddShard and DropShard. A Handler without RevokeShard
// learns of the end of the session from Agent.Token alone.
//
//dokimi:stub
//dokimi:out path=replicatest/
type Revoker interface {
	// RevokeShard ends the work of the tenure of the token of req, and returns once the work
	// has ended. The fence admits the writes of the tenure until a newer tenure acquires it.
	// The agent also revokes a tenure whose AddShard failed, so RevokeShard returns nil for a
	// tenure that the handler does not run.
	RevokeShard(ctx context.Context, req *RevokeShardRequest) (RevokeShardResult, error)
}

// RevokeShardRequest is the request of Revoker.RevokeShard.
type RevokeShardRequest struct {
	_ incomparable
	// Token is the fencing token of the tenure whose session has ended.
	Token fence.Token
}

// RevokeShardResult is the result of Revoker.RevokeShard.
type RevokeShardResult struct {
	_ incomparable
}

// LoadReporter reports the load of the replicas of an application. When the Handler that
// Start receives implements LoadReporter, the agent sends each report with the loads that it
// returns. Without it, each replica reports only the count metric.
//
//dokimi:stub
//dokimi:out path=replicatest/
type LoadReporter interface {
	// ShardLoad returns the load of the replica of the shard id, or nil for a replica without
	// a load. It does not perform I/O, and the agent calls it for each replica of each
	// report.
	ShardLoad(id shard.ID) shard.Load
}

// application is the handler of the driver of an agent. It passes each callback to the
// Handler of the application in a request from a pool of its own.
//
// # Concurrency
//
// An application is safe for concurrent use. The agent calls it from a goroutine of the
// driver for each callback.
//
// # Allocation contract
//
// A callback takes its request from a pool, so a call allocates only when the pool is empty.
type application struct {
	// handler is the Handler of the application.
	handler Handler
	// adds keeps the requests of AddShard.
	adds *pool.Pool[*AddShardRequest]
	// drops keeps the requests of DropShard.
	drops *pool.Pool[*DropShardRequest]
	// revokes keeps the requests of RevokeShard.
	revokes *pool.Pool[*RevokeShardRequest]
}

var (
	_ driver.Handler = (*application)(nil)
	_ driver.Revoker = (*application)(nil)
)

// newApplication returns the handler of the driver of an agent, which passes each callback to
// h.
func newApplication(h Handler) *application {
	return &application{
		handler: h,
		adds:    pool.NewPool(func() *AddShardRequest { return new(AddShardRequest) }),
		drops:   pool.NewPool(func() *DropShardRequest { return new(DropShardRequest) }),
		revokes: pool.NewPool(func() *RevokeShardRequest { return new(RevokeShardRequest) }),
	}
}

// AddShard passes the callback r and the fencing token t of its tenure to Handler.AddShard,
// and returns an error that wraps its error. It clears the request before it returns the
// request to its pool.
func (a *application) AddShard(ctx context.Context, r *protocol.Request, t *fence.Token) error {
	req := a.adds.Get()
	req.Request, req.Token = r, *t
	_, err := a.handler.AddShard(ctx, req)
	req.Request, req.Token = nil, fence.Token{}
	//dokimi:mutate-skip sbr-delete: a request that the pool does not get back costs one allocation of a later callback, which the allocations of the driver for each callback hide from a test of the agent
	a.adds.Put(req)
	if err != nil {
		return fmt.Errorf("replica: the add of shard %v: %w", r.Shard, err)
	}
	return nil
}

// DropShard passes the callback r to Handler.DropShard, and returns an error that wraps its
// error. It clears the request before it returns the request to its pool.
func (a *application) DropShard(ctx context.Context, r *protocol.Request) error {
	req := a.drops.Get()
	req.Request = r
	_, err := a.handler.DropShard(ctx, req)
	req.Request = nil
	//dokimi:mutate-skip sbr-delete: a request that the pool does not get back costs one allocation of a later callback, which the allocations of the driver for each callback hide from a test of the agent
	a.drops.Put(req)
	if err != nil {
		return fmt.Errorf("replica: the drop of shard %v: %w", r.Shard, err)
	}
	return nil
}

// RevokeShard passes the fencing token t of a tenure whose session has ended to
// Revoker.RevokeShard, and returns an error that wraps its error. It returns nil for a Handler
// without RevokeShard. It clears the request before it returns the request to its pool.
func (a *application) RevokeShard(ctx context.Context, t *fence.Token) error {
	rv, ok := a.handler.(Revoker)
	if !ok {
		return nil
	}
	req := a.revokes.Get()
	req.Token = *t
	_, err := rv.RevokeShard(ctx, req)
	req.Token = fence.Token{}
	//dokimi:mutate-skip sbr-delete: a request that the pool does not get back costs one allocation of a later revocation, which no test observes
	a.revokes.Put(req)
	if err != nil {
		return fmt.Errorf("replica: the revocation of shard %v: %w", t.Shard, err)
	}
	return nil
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package router

import (
	"context"
	"errors"
	"fmt"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/pool"

	"go.thesmos.sh/sharder/internal/machine/routes"
	"go.thesmos.sh/sharder/shard"
)

// callStates lends Do its call states.
var callStates = pool.NewResetPool(func() *callState { return new(callState) })

// incomparable is the first field of each request and each result of the package. The
// compiler then refuses == on the type, so a field that the type gains later does not break
// a comparison. As the first field it does not change the size of the type.
type incomparable [0]func()

// Route lists the replicas of one shard at one epoch. The writer comes first. A Route has
// these fields:
//   - Incarnation is the namespace incarnation of the route.
//   - Cell is the cell of the shard.
//   - Shard is the shard.
//   - Range is the range of positions of the shard.
//   - Epoch is the epoch of the placement of the shard.
//   - Generation is the generation of the scope of the cell from which the router took the
//     placement.
//   - Replicas lists an [Endpoint] for each replica whose node the latest view of the cell
//     lists. The endpoints are in the order of the replicas of the placement.
//
// A Route that a [Watcher] returns shares the bytes of Range and the elements of Replicas
// with the router, so the caller does not modify them.
type Route = routes.Route

// Endpoint is one replica that a client can call. An Endpoint has these fields:
//   - Node is the node of the replica.
//   - Address is the address of the node in the latest view of its cell.
//   - Role is the role of the replica.
type Endpoint = routes.Endpoint

// Router maps keys to the routes of their shards. An implementation is safe for concurrent
// use.
//
//dokimi:stub
//dokimi:out path=routertest/
type Router interface {
	// Route returns the route of the shard of req.Key. It returns an error of class
	// errs.Transient when the router has no route for the key, such as for a shard without a
	// placement.
	Route(ctx context.Context, req *RouteRequest) (RouteResult, error)
	// Refresh returns once the router has applied generation req.Min of the cell req.Cell in
	// the namespace incarnation req.Incarnation, or a later generation of the cell. It returns
	// an error that wraps shard.ErrIncarnation when the router has read the root scope again
	// and req.Incarnation is not the incarnation of the namespace. It returns the cause of ctx
	// when ctx ends first.
	Refresh(ctx context.Context, req *RefreshRequest) (RefreshResult, error)
}

// RouteRequest is the request of Router.Route.
type RouteRequest struct {
	_ incomparable
	// Key is the key that the caller routes.
	Key []byte
}

// RouteResult is the result of Router.Route.
type RouteResult struct {
	_ incomparable
	// Route is the route of the shard of the key.
	Route Route
}

// RefreshRequest is the request of Router.Refresh. Do builds it from the fields of a
// [NotOwnerError].
type RefreshRequest struct {
	_ incomparable
	// Cell is the cell of the generation. It is required.
	Cell shard.CellID
	// Min is the generation that the caller waits for. A later generation of the cell ends the
	// wait as well.
	Min epoch.Epoch
	// Incarnation is the namespace incarnation of the generation. It is required.
	Incarnation shard.NamespaceIncarnation
}

// RefreshResult is the result of Router.Refresh.
type RefreshResult struct {
	_ incomparable
}

// callState contains the requests that Do passes to its Router. Do takes one from callStates
// for each call, so a request does not move to the heap on each call.
type callState struct {
	// route is the request of each Route.
	route RouteRequest
	// refresh is the request of each Refresh.
	refresh RefreshRequest
}

// Reset clears s, so a call state in the pool does not keep the key of a caller.
func (s *callState) Reset() {
	*s = callState{}
}

// Do calls call with the route of key and returns the error of call. After a call that
// returns a *NotOwnerError, Do calls r.Refresh with the namespace incarnation, the cell and
// the generation of the error. It routes key again when Refresh returns nil or an error that
// wraps shard.ErrIncarnation. It then calls call with the new route. Do calls call at most
// attempts times. It does not refresh the router after the last call. Do finds a
// *NotOwnerError that call wraps.
//
// Do returns a lost reply, a timeout and every other error of call at once. A request that
// fails after the server started it may have taken effect, so the application retries it
// with an idempotency of its own.
//
// Error modes:
//   - an error that wraps ErrConfig for attempts below 1.
//   - an error that wraps the error of r.Route, such as ErrNoRoute.
//   - an error that wraps an error of r.Refresh other than shard.ErrIncarnation, such as the
//     cause of ctx.
//   - the error of the last call of call, unchanged.
//
// # Concurrency
//
// Do is safe for concurrent use when r is.
//
// # Allocation contract
//
// Zero alloc, apart from the allocations of call and of the errors that Do wraps. Do takes
// the requests that it passes to r from a pool.
func Do(ctx context.Context, r Router, key []byte, attempts int, call func(Route) error) error {
	if attempts < 1 {
		return fmt.Errorf("%w: %d attempts", ErrConfig, attempts)
	}
	s := callStates.Get()
	//dokimi:mutate-skip sbr-delete: a call state that the pool does not get back costs one allocation of a later call, and a mutation run checks no allocation ceiling
	defer callStates.Put(s)
	s.route.Key = key
	for n := 1; ; n++ {
		res, err := r.Route(ctx, &s.route)
		if err != nil {
			return fmt.Errorf("router: the route of the key: %w", err)
		}
		err = call(res.Route)
		moved, ok := errors.AsType[*NotOwnerError](err)
		if !ok || n == attempts {
			return err
		}
		s.refresh.Incarnation, s.refresh.Cell, s.refresh.Min = moved.Incarnation, moved.Cell, moved.Generation
		if _, err = r.Refresh(ctx, &s.refresh); err != nil && !errors.Is(err, shard.ErrIncarnation) {
			return fmt.Errorf("router: the refresh after a redirect: %w", err)
		}
	}
}

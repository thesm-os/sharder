// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package router_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"testing"
	"testing/synctest"
	"weak"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/router"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// scriptKey is the key that the tests of Do route through a script.
const scriptKey = "key-a"

// released is the size of a key whose release a test checks. A key of more than 16 bytes is
// an allocation of its own, so the collector frees it alone.
const released = 64

// errScript is the error of a script that has no result left for a call.
var errScript = errs.WithClass(errors.New("test: the script has no result left"), errs.Invalid)

// The routes and the redirect of the scripts.
var (
	// first is the route of the shard of scriptKey on node A.
	first = router.Route{
		Cell:        cellID,
		Replicas:    []router.Endpoint{{Node: nodeA.ID, Address: nodeA.Address, Role: shard.RolePrimary}},
		Epoch:       1,
		Generation:  2,
		Incarnation: incA,
		Shard:       shards[0],
	}
	// second is the route of the shard of scriptKey on node B, after the move that redirect
	// reports.
	second = router.Route{
		Cell:        cellID,
		Replicas:    []router.Endpoint{{Node: nodeB.ID, Address: nodeB.Address, Role: shard.RolePrimary}},
		Epoch:       2,
		Generation:  5,
		Incarnation: incA,
		Shard:       shards[0],
	}
	// redirect is the error of a server that does not serve the shard of first.
	redirect = &router.NotOwnerError{
		Cell:        cellID,
		Hint:        nodeB.ID,
		Shard:       shards[0],
		Epoch:       1,
		Generation:  5,
		Incarnation: incA,
	}
	// errTransport is the error of a call whose reply the transport lost.
	errTransport = errs.WithClass(errors.New("test: the reply is lost"), errs.Transient)
)

// routed is one result of Route of a script.
type routed struct {
	// err is the error of the call.
	err error
	// route is the route of the call.
	route router.Route
}

// script is a router of the tests. Route returns the next result of routes, and Refresh
// returns the next error of refreshes. A call without a result left returns errScript. The
// script records a copy of the key of each Route and each request of Refresh.
type script struct {
	// routes lists the result of each call of Route, in order.
	routes []routed
	// refreshes lists the error of each call of Refresh, in order.
	refreshes []error
	// keys lists a copy of the key of each call of Route, in order.
	keys [][]byte
	// refreshed lists the request of each call of Refresh, in order.
	refreshed []router.RefreshRequest
}

var _ router.Router = (*script)(nil)

// Route records a copy of req.Key, and returns the next result of routes.
func (s *script) Route(_ context.Context, req *router.RouteRequest) (router.RouteResult, error) {
	n := len(s.keys)
	s.keys = append(s.keys, slices.Clone(req.Key))
	if n >= len(s.routes) {
		return router.RouteResult{}, errScript
	}
	return router.RouteResult{Route: s.routes[n].route}, s.routes[n].err
}

// Refresh records req, and returns the next error of refreshes.
func (s *script) Refresh(_ context.Context, req *router.RefreshRequest) (router.RefreshResult, error) {
	n := len(s.refreshed)
	s.refreshed = append(s.refreshed, *req)
	if n >= len(s.refreshes) {
		return router.RefreshResult{}, errScript
	}
	return router.RefreshResult{}, s.refreshes[n]
}

// server is a server of the tests that serves only the routes for which current reports
// true. It returns the redirect of moved for any other route. It records the route of each
// call.
type server struct {
	// current reports whether the server serves the route r.
	current func(r router.Route) bool
	// moved returns the redirect of the route r.
	moved func(r router.Route) *router.NotOwnerError
	// calls lists the route of each call, in order.
	calls []router.Route
}

// call serves r. It returns the redirect of moved for a route that is not current.
func (s *server) call(r router.Route) error {
	s.calls = append(s.calls, r)
	if s.current(r) {
		return nil
	}
	return s.moved(r)
}

// TestRouter checks Do.
func TestRouter(t *testing.T) {
	t.Parallel()

	t.Run("Do", func(t *testing.T) {
		t.Parallel()

		t.Run("calls call with the route of the key", func(t *testing.T) {
			t.Parallel()
			s := &script{routes: []routed{{route: first}}}
			var got []router.Route
			err := router.Do(t.Context(), s, []byte(scriptKey), 1, func(r router.Route) error {
				got = append(got, r)
				return nil
			})
			assert.NoError(t, err, "Do must succeed when the callback succeeds")
			expect.Equal(t, got, []router.Route{first}, "Do must pass the route of the key to the callback")
			expect.Equal(t, s.keys, [][]byte{[]byte(scriptKey)}, "Do must route the key")
		})

		t.Run("returns ErrConfig for fewer than one attempt", func(t *testing.T) {
			t.Parallel()
			s := &script{routes: []routed{{route: first}}}
			err := router.Do(t.Context(), s, []byte(scriptKey), 0, func(router.Route) error { return nil })
			assert.ErrorIs(t, err, router.ErrConfig, "Do must refuse fewer than one attempt")
			assert.Empty(t, s.keys, "Do must not route a key without an attempt")
		})

		t.Run("returns an error that wraps the error of Route", func(t *testing.T) {
			t.Parallel()
			s := &script{routes: []routed{{err: router.ErrNoRoute}}}
			calls := 0
			err := router.Do(t.Context(), s, []byte(scriptKey), 2, func(router.Route) error {
				calls++
				return nil
			})
			assert.ErrorIs(t, err, router.ErrNoRoute, "Do must return the error of Route")
			assert.Equal(t, calls, 0, "Do must not run the callback without a route")
		})

		t.Run("returns the error of call when the error is not a NotOwnerError", func(t *testing.T) {
			t.Parallel()
			s := &script{routes: []routed{{route: first}, {route: second}}}
			err := router.Do(t.Context(), s, []byte(scriptKey), 3, func(router.Route) error { return errTransport })
			assert.Equal(t, err, errTransport, "Do must return the error of the callback unchanged")
			expect.Length(t, s.keys, 1, "Do must not route the key again after a lost reply")
			expect.Empty(t, s.refreshed, "Do must not refresh the router after a lost reply")
		})

		t.Run("refreshes the router to the generation of a NotOwnerError", func(t *testing.T) {
			t.Parallel()
			s := &script{routes: []routed{{route: first}, {route: second}}, refreshes: []error{nil}}
			srv := &server{
				current: func(r router.Route) bool { return r.Epoch == second.Epoch },
				moved:   func(router.Route) *router.NotOwnerError { return redirect },
			}
			assert.NoError(t, router.Do(t.Context(), s, []byte(scriptKey), 2, srv.call),
				"Do must return the result of the call after the redirect")
			assert.Equal(t, s.refreshed, []router.RefreshRequest{{
				Cell:        redirect.Cell,
				Min:         redirect.Generation,
				Incarnation: redirect.Incarnation,
			}}, "Do must wait for the generation of the redirect")
		})

		t.Run("routes the key again after a NotOwnerError", func(t *testing.T) {
			t.Parallel()
			s := &script{routes: []routed{{route: first}, {route: second}}, refreshes: []error{nil}}
			srv := &server{
				current: func(r router.Route) bool { return r.Epoch == second.Epoch },
				moved:   func(router.Route) *router.NotOwnerError { return redirect },
			}
			assert.NoError(t, router.Do(t.Context(), s, []byte(scriptKey), 2, srv.call),
				"Do must return the result of the call after the redirect")
			expect.Equal(t, srv.calls, []router.Route{first, second},
				"Do must pass the route after the redirect to the callback")
			expect.Equal(t, s.keys, [][]byte{[]byte(scriptKey), []byte(scriptKey)}, "Do must route the key again")
		})

		t.Run("routes the key again after a NotOwnerError that call wraps", func(t *testing.T) {
			t.Parallel()
			s := &script{routes: []routed{{route: first}, {route: second}}, refreshes: []error{nil}}
			calls := 0
			err := router.Do(t.Context(), s, []byte(scriptKey), 2, func(router.Route) error {
				calls++
				if calls == 1 {
					return fmt.Errorf("test: the call: %w", redirect)
				}
				return nil
			})
			assert.NoError(t, err, "Do must return the result of the call after the redirect")
			assert.Equal(t, calls, 2, "Do must find the redirect that the callback wraps")
		})

		t.Run("routes the key again when Refresh returns an error that wraps shard.ErrIncarnation",
			func(t *testing.T) {
				t.Parallel()
				s := &script{
					routes:    []routed{{route: first}, {route: second}},
					refreshes: []error{fmt.Errorf("test: the refresh: %w", shard.ErrIncarnation)},
				}
				srv := &server{
					current: func(r router.Route) bool { return r.Epoch == second.Epoch },
					moved:   func(router.Route) *router.NotOwnerError { return redirect },
				}
				assert.NoError(t, router.Do(t.Context(), s, []byte(scriptKey), 2, srv.call),
					"Do must route the key again in the incarnation that the router installed")
				assert.Equal(t, srv.calls, []router.Route{first, second},
					"Do must pass the route after the refresh to the callback")
			})

		t.Run("returns an error that wraps the error of Refresh", func(t *testing.T) {
			t.Parallel()
			s := &script{routes: []routed{{route: first}, {route: second}}, refreshes: []error{errStop}}
			srv := &server{
				current: func(r router.Route) bool { return r.Epoch == second.Epoch },
				moved:   func(router.Route) *router.NotOwnerError { return redirect },
			}
			assert.ErrorIs(t, router.Do(t.Context(), s, []byte(scriptKey), 2, srv.call), errStop,
				"Do must return the error of Refresh")
			assert.Length(t, srv.calls, 1, "Do must not run the callback again after a failed Refresh")
		})

		t.Run("returns the NotOwnerError of the last attempt", func(t *testing.T) {
			t.Parallel()
			s := &script{routes: []routed{{route: first}, {route: first}}, refreshes: []error{nil}}
			srv := &server{
				current: func(router.Route) bool { return false },
				moved:   func(router.Route) *router.NotOwnerError { return redirect },
			}
			err := router.Do(t.Context(), s, []byte(scriptKey), 2, srv.call)
			got := assert.ErrorAs[*router.NotOwnerError](t, err, "Do must return the redirect of the last attempt")
			expect.Equal(t, got, redirect, "Do must return the redirect of the last attempt", assert.ByIdentity())
			expect.Length(t, srv.calls, 2, "Do must run the callback once per attempt")
			expect.Length(t, s.refreshed, 1, "Do must not refresh the router after the last attempt")
		})

		t.Run("keeps no reference to the key after it returns", func(t *testing.T) {
			t.Parallel()
			s := &script{routes: []routed{{err: router.ErrNoRoute}}}
			gone := func() weak.Pointer[[released]byte] {
				key := new([released]byte)
				err := router.Do(t.Context(), s, key[:], 1, func(router.Route) error { return nil })
				assert.ErrorIs(t, err, router.ErrNoRoute, "Do must return the error of Route")
				return weak.Make(key)
			}()
			runtime.GC()
			assert.Nil(t, gone.Value(), "Do must release the key of its call")
			runtime.KeepAlive(s)
		})

		t.Run("routes the key to the placement of a later generation after a redirect", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				w := f.start(t, func(c *router.Config) { c.Store = newGated(f.st) })
				p := f.placement(0, nodeB.ID, 2)
				gen := f.write(t, cellScope, &store.Mutation{Put: []shard.Placement{p}})
				srv := &server{
					current: func(r router.Route) bool { return r.Epoch == p.Epoch },
					moved: func(r router.Route) *router.NotOwnerError {
						return &router.NotOwnerError{
							Cell:        r.Cell,
							Hint:        nodeB.ID,
							Shard:       r.Shard,
							Epoch:       r.Epoch,
							Generation:  gen,
							Incarnation: r.Incarnation,
						}
					},
				}
				assert.NoError(t, router.Do(t.Context(), w, f.keys[0], 2, srv.call),
					"Do must call the server of the later placement")
				assert.Length(t, srv.calls, 2, "Do must call the server again after the redirect")
				assert.Equal(t, srv.calls[1].Replicas[0].Node, nodeB.ID, "Do must call the node of the later placement")
			})
		})

		t.Run("routes the key in the namespace that the root creates again", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				w := f.start(t, func(c *router.Config) { c.Store = newGated(f.st) })
				f.recreate(t)
				srv := &server{
					current: func(r router.Route) bool { return r.Incarnation == incB },
					moved: func(r router.Route) *router.NotOwnerError {
						return &router.NotOwnerError{
							Cell:        otherCell,
							Shard:       r.Shard,
							Epoch:       r.Epoch,
							Generation:  f.placed,
							Incarnation: incB,
						}
					},
				}
				assert.NoError(t, router.Do(t.Context(), w, f.keys[0], 2, srv.call),
					"Do must call the server of the namespace that the root created again")
				assert.Length(t, srv.calls, 2, "Do must call the server again after the redirect")
				expect.Equal(t, srv.calls[1].Incarnation, incB, "Do must route the key in the new incarnation")
				expect.Equal(
					t,
					srv.calls[1].Cell,
					otherCell,
					"Do must route the key to the cell of the new incarnation",
				)
			})
		})
	})
}

// TestRouterAllocs checks the allocation contract of Do. MaxAllocs counts the allocations of
// the whole process, so the test does not run in parallel. Do takes its call state from a
// pool, so the test empties the pool first: the warm-up call then allocates the call state,
// and the measured calls reuse it only when Do returns it to the pool.
func TestRouterAllocs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		w := f.start(t)
		ctx, key := t.Context(), f.keys[0]
		served := func(router.Route) error { return nil }
		var err error
		runtime.GC()
		runtime.GC()
		expect.MaxAllocs(t, func() { err = router.Do(ctx, w, key, 1, served) }, 0, "Do must not allocate")
		assert.NoError(t, err, "the test must measure a call that the server serves")
	})
}

// BenchmarkRouter measures Do for the key of a placed shard and a call that succeeds.
func BenchmarkRouter(b *testing.B) {
	f := newFixture(b)
	w := f.start(b)
	ctx, key := b.Context(), f.keys[0]
	served := func(router.Route) error { return nil }

	b.Run("Do", func(b *testing.B) {
		var err error
		c := bench.Start(b).MaxAllocs(0)
		defer c.End()
		for c.Loop() {
			err = router.Do(ctx, w, key, 1, served)
		}
		assert.NoError(b, err, "the benchmark must measure a call that the server serves")
	})
}

// recreate deletes the root scope, and creates the namespace again in incB with the same spec
// and the cell otherCell. The scope of the old cell keeps its placements and its sessions.
// The controller of the new cell places every slot on node A, and the nodes A and B join the
// new cell. placed is the generation of the commit of the placements from then on.
func (f *fixture) recreate(tb cleaner) {
	tb.Helper()
	f.st.Delete(rootScope)
	f.incarnation = incB
	clear(f.seqs)
	f.write(tb, rootScope, &store.Mutation{
		Spec:    f.spec,
		Mapping: &f.mapping,
		Put:     []shard.Placement{{Shard: shard.ID{ID128: otherKey}}},
	})
	f.cell = shard.Scope{Namespace: namespace, Cell: otherCell}
	f.placed = f.write(tb, f.cell, &store.Mutation{Put: f.placements(nodeA.ID, firstEpoch)})
	f.join(tb, f.cell, nodeA, nodeB)
}

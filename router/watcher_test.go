// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package router_test

import (
	"context"
	"errors"
	"iter"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/history"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/router"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The operations of the history of the route of a shard.
const (
	// opCommit is a commit of a placement of the shard, whose argument is its epoch.
	opCommit = "commit"
	// opRoute is a call of Route, whose output is the epoch of the route, or zero for an
	// error.
	opRoute = "route"
)

// The size of the history of the route of a shard.
const (
	// moves is the number of commits that move the shard to another node.
	moves = 4
	// readers is the number of clients that call Route while the commits run.
	readers = 2
	// pause is the time between two calls of Route of one reader.
	pause = time.Millisecond
	// tenancy is the time between two commits of client 0, so the readers call Route between
	// the commits.
	tenancy = 5 * pause
)

// errBroken is an error of a store that a retry does not fix.
var errBroken = errs.WithClass(errors.New("test: the store is broken"), errs.Invalid)

// routing is the state of the sequential spec of the route of one shard.
type routing struct {
	// returned is the epoch of the route that the last call of Route returned.
	returned epoch.Epoch
	// committed is the epoch of the last commit of a placement of the shard.
	committed epoch.Epoch
}

// routings is the sequential spec of the route of one shard. A commit raises the committed
// epoch. Route returns an epoch from the epoch that the last Route returned up to the
// committed epoch. So the epoch of the route does not fall, and it does not pass the epoch of
// the last commit that has begun.
var routings = history.Spec[routing]{
	Initial: func() routing { return routing{returned: firstEpoch, committed: firstEpoch} },
	Next: func(s routing, op history.Operation) []routing {
		if op.Name == opCommit {
			e, _ := op.Args[0].(epoch.Epoch)
			return []routing{{returned: s.returned, committed: e}}
		}
		if !op.Known {
			return []routing{s}
		}
		e, _ := op.Output.(epoch.Epoch)
		if e < s.returned || e > s.committed {
			return nil
		}
		return []routing{{returned: e, committed: s.committed}}
	},
}

// faulty is a store whose watches pass errBroken in place of their next item once broken
// is true. It embeds a memory.Store, so it provides the capabilities of that store.
type faulty struct {
	*memory.Store
	// broken is true once the watches fail.
	broken atomic.Bool
}

// Watch yields the items of the watch of the memory store until broken is true, and then
// yields errBroken in place of the next item.
func (s *faulty) Watch(ctx context.Context, sc shard.Scope, inc shard.NamespaceIncarnation, from epoch.Epoch,
	n shard.NodeID,
) iter.Seq2[store.Event, error] {
	return func(yield func(store.Event, error) bool) {
		for ev, err := range s.Store.Watch(ctx, sc, inc, from, n) {
			if s.broken.Load() {
				yield(store.Event{}, errBroken)
				return
			}
			if !yield(ev, err) {
				return
			}
		}
	}
}

// plainMembership is a membership that does not provide a capability. Its method set has the
// methods of membership.Membership alone.
type plainMembership struct {
	membership.Membership
}

// TestWatcher checks the start of a router, its routes, its Refresh, its Close and the end of
// its run.
func TestWatcher(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a nil Config", func(t *testing.T) {
			t.Parallel()
			_, err := router.New(t.Context(), nil)
			assert.ErrorIs(t, err, router.ErrConfig, "New must refuse a nil Config")
		})

		tests := []struct {
			give func(c *router.Config)
			name string
		}{
			{name: "returns ErrConfig for a Config without a store", give: func(c *router.Config) { c.Store = nil }},
			{
				name: "returns ErrConfig for a Config without a membership",
				give: func(c *router.Config) { c.Membership = nil },
			},
			{name: "returns ErrConfig for a Config without a clock", give: func(c *router.Config) { c.Clock = nil }},
			{
				name: "returns ErrConfig for a Config without a source of randomness",
				give: func(c *router.Config) { c.Rand = nil },
			},
			{
				name: "returns ErrConfig for a Config without a reporter",
				give: func(c *router.Config) { c.Reporter = nil },
			},
			{
				name: "returns ErrConfig for a Config without a registry",
				give: func(c *router.Config) { c.Registry = nil },
			},
			{name: "returns ErrConfig for a Config without a logger", give: func(c *router.Config) { c.Logger = nil }},
			{
				name: "returns ErrConfig for a Config without a namespace",
				give: func(c *router.Config) { c.Namespace = "" },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				cfg := bare(t).config()
				tt.give(cfg)
				_, err := router.New(t.Context(), cfg)
				assert.ErrorIs(t, err, router.ErrConfig, "New must refuse the Config")
			})
		}

		t.Run("returns once the router has the route of each shard", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				w := f.start(t)
				assert.Total(t, func(key []byte) error {
					_, err := w.Route(t.Context(), &router.RouteRequest{Key: key})
					return err
				}, f.keys, "Route must return the route of each shard once New returns")
			})
		})

		t.Run("returns an error that wraps ErrMapping for a membership without the capabilities that the spec "+
			"requires", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				cfg := f.config()
				cfg.Membership = plainMembership{Membership: f.st.Membership()}
				_, err := router.New(t.Context(), cfg)
				assert.ErrorIs(t, err, router.ErrMapping,
					"New must refuse a membership that does not provide the capabilities of the spec")
			})
		})

		t.Run("returns the cause of its context when the context ends before the root creates the namespace",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := bare(t)
					ctx, cancel := context.WithTimeoutCause(t.Context(), time.Minute, errStop)
					defer cancel()
					_, err := router.New(ctx, f.config())
					assert.ErrorIs(t, err, errStop, "New must return the cause of its context")
				})
			})

		t.Run("returns its error once every goroutine of the router has returned", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := bare(t)
				g := newGated(f.st)
				cfg := f.config()
				cfg.Store = g
				ctx, cancel := context.WithTimeoutCause(t.Context(), time.Minute, errStop)
				defer cancel()
				_, err := router.New(ctx, cfg)
				assert.ErrorIs(t, err, errStop, "New must return the cause of its context")
				assert.True(t, g.opened.Load(), "New must return once the watches of the router have returned")
			})
		})

		t.Run("runs the router after its context ends", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				ctx, cancel := context.WithCancel(t.Context())
				w, err := router.New(ctx, f.config())
				assert.NoError(t, err, "New must start the router")
				t.Cleanup(func() { assert.NoError(t, w.Close(), "Close must stop the router") })
				cancel()
				gen := f.write(t, cellScope, &store.Mutation{Put: []shard.Placement{f.placement(0, nodeB.ID, 2)}})
				_, err = w.Refresh(t.Context(), &router.RefreshRequest{Incarnation: incA, Cell: cellID, Min: gen})
				assert.NoError(t, err, "the router must apply a commit after the context of New has ended")
			})
		})
	})

	t.Run("Route", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the route of the shard of the key", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				w := f.start(t)
				res, err := w.Route(t.Context(), &router.RouteRequest{Key: f.keys[1]})
				assert.NoError(t, err, "Route must return the route of a placed shard")
				assert.Equal(t, res.Route, router.Route{
					Cell:        cellID,
					Range:       f.spans[1].Range,
					Replicas:    []router.Endpoint{{Node: nodeA.ID, Address: nodeA.Address, Role: shard.RolePrimary}},
					Epoch:       firstEpoch,
					Generation:  f.placed,
					Incarnation: incA,
					Shard:       shards[1],
				}, "Route must return the placement of the shard of the key with the address of its node")
			})
		})

		t.Run("returns ErrNoRoute for a key whose shard has no replica", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.write(t, cellScope, &store.Mutation{Put: []shard.Placement{f.placement(0, "", 2)}})
				w := f.start(t)
				_, err := w.Route(t.Context(), &router.RouteRequest{Key: f.keys[0]})
				assert.ErrorIs(t, err, router.ErrNoRoute, "Route must refuse a key whose shard has no replica")
			})
		})

		t.Run("returns ErrClosed after Close", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				w := f.start(t)
				assert.NoError(t, w.Close(), "Close must stop the router")
				_, err := w.Route(t.Context(), &router.RouteRequest{Key: f.keys[0]})
				assert.ErrorIs(t, err, router.ErrClosed, "Route must refuse every key after Close")
			})
		})

		t.Run("returns the routes that the commits of the shard leave while they run", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				w := f.start(t)
				h := history.New()
				done := make(chan struct{})
				var moved atomic.Int64
				outcomes := history.Concurrently(readers+1, waitFor, func(client int) (any, error) {
					if client == 0 {
						defer close(done)
						return nil, move(t.Context(), f, h)
					}
					for {
						if read(t.Context(), w, h, client, f.keys[0]) > firstEpoch {
							moved.Add(1)
						}
						select {
						case <-done:
							read(t.Context(), w, h, client, f.keys[0])
							return nil, nil
						case <-time.After(pause):
						}
					}
				})
				for _, o := range outcomes {
					expect.True(t, o.Finished, "every client must finish")
					expect.NoError(t, o.Error, "the commits of every client must succeed")
				}
				expect.True(t, moved.Load() > 0, "the readers must route to a placement that a later commit put")
				history.Linearizable(t, h, routings,
					"Route returns the placements that the commits of the shard leave, and never an older one")
			})
		})
	})

	t.Run("Refresh", func(t *testing.T) {
		t.Parallel()

		t.Run("returns once the router has applied the generation of the cell", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				g := newGated(f.st)
				w := f.start(t, func(c *router.Config) { c.Store = g })
				gen := f.write(t, cellScope, &store.Mutation{Put: []shard.Placement{f.placement(0, nodeB.ID, 2)}})
				_, err := w.Refresh(t.Context(), &router.RefreshRequest{Incarnation: incA, Cell: cellID, Min: gen})
				assert.NoError(t, err, "Refresh must return once the router has applied the generation")
				assert.True(t, g.opened.Load(), "Refresh must wait until the watch of the cell passes the generation")
			})
		})

		t.Run("returns at once for a generation that the router has applied", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				g := newGated(f.st)
				w := f.start(t, func(c *router.Config) { c.Store = g })
				_, err := w.Refresh(t.Context(), &router.RefreshRequest{Incarnation: incA, Cell: cellID, Min: f.placed})
				assert.NoError(t, err, "Refresh must return for a generation that the router has applied")
				assert.False(t, g.opened.Load(), "Refresh must not wait for a generation that the router has applied")
			})
		})

		t.Run("returns ErrConfig for a request without a namespace incarnation", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				w := f.start(t)
				_, err := w.Refresh(t.Context(), &router.RefreshRequest{Cell: cellID, Min: f.placed})
				assert.ErrorIs(t, err, router.ErrConfig, "Refresh must refuse a request without an incarnation")
			})
		})

		t.Run("returns ErrConfig for a request without a cell", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				w := f.start(t)
				_, err := w.Refresh(t.Context(), &router.RefreshRequest{Incarnation: incA, Min: f.placed})
				assert.ErrorIs(t, err, router.ErrConfig, "Refresh must refuse a request without a cell")
			})
		})

		t.Run("returns an error that wraps shard.ErrIncarnation for an incarnation that the root has not created",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					w := f.start(t)
					_, err := w.Refresh(t.Context(), &router.RefreshRequest{Incarnation: incB, Cell: cellID, Min: 1})
					assert.ErrorIs(t, err, shard.ErrIncarnation,
						"Refresh must refuse an incarnation that the root has not created")
				})
			})

		t.Run("returns an error that wraps ErrCell for a cell that the cell map does not have", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				w := f.start(t)
				_, err := w.Refresh(t.Context(), &router.RefreshRequest{Incarnation: incA, Cell: otherCell, Min: 1})
				assert.ErrorIs(t, err, router.ErrCell, "Refresh must refuse a cell that the cell map does not have")
			})
		})

		t.Run("returns the cause of its context when the context ends first", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				w := f.start(t, func(c *router.Config) { c.Store = newGated(f.st) })
				gen := f.write(t, cellScope, &store.Mutation{Put: []shard.Placement{f.placement(0, nodeB.ID, 2)}})
				ctx, cancel := context.WithTimeoutCause(t.Context(), time.Minute, errStop)
				defer cancel()
				_, err := w.Refresh(ctx, &router.RefreshRequest{Incarnation: incA, Cell: cellID, Min: gen})
				assert.ErrorIs(t, err, errStop, "Refresh must return the cause of its context")
			})
		})

		t.Run("returns ErrClosed after Close", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				w := f.start(t)
				assert.NoError(t, w.Close(), "Close must stop the router")
				_, err := w.Refresh(t.Context(), &router.RefreshRequest{Incarnation: incA, Cell: cellID, Min: f.placed})
				expect.That(t, err).ErrorIs(router.ErrClosed, "Refresh must refuse a call after Close")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "a Refresh after Close must be invalid")
			})
		})

		t.Run("returns ErrClosed to a call that waits when Close stops the router", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				w := f.start(t, func(c *router.Config) { c.Store = newGated(f.st) })
				gen := f.write(t, cellScope, &store.Mutation{Put: []shard.Placement{f.placement(0, nodeB.ID, 2)}})
				refreshed := make(chan error, 1)
				go func() {
					_, err := w.Refresh(t.Context(), &router.RefreshRequest{Incarnation: incA, Cell: cellID, Min: gen})
					refreshed <- err
				}()
				synctest.Wait()
				assert.NoError(t, w.Close(), "Close must stop the router")
				assert.ErrorIs(
					t,
					<-refreshed,
					router.ErrClosed,
					"a Refresh that waits must end when Close stops the router",
				)
			})
		})
	})

	t.Run("Close", func(t *testing.T) {
		t.Parallel()

		t.Run("returns once every goroutine of the router has returned", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				g := newGated(f.st)
				w := f.start(t, func(c *router.Config) { c.Store = g })
				assert.NoError(t, w.Close(), "Close must stop the router")
				assert.True(t, g.opened.Load(), "Close must return once the watches of the router have returned")
			})
		})

		t.Run("returns nil for a router that Close has stopped", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				w := newFixture(t).start(t)
				assert.NoError(t, w.Close(), "Close must stop the router")
				assert.NoError(t, w.Close(), "Close must return nil for a router that has stopped")
			})
		})
	})

	t.Run("Done", func(t *testing.T) {
		t.Parallel()

		t.Run("closes once Close has stopped the router", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				w := newFixture(t).start(t)
				assert.NoError(t, w.Close(), "Close must stop the router")
				received(t, w.Done(), "the end of the run")
			})
		})

		t.Run("closes once the router stops itself", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				s := &faulty{Store: f.st}
				w := f.start(t, func(c *router.Config) { c.Store = s })
				s.broken.Store(true)
				f.write(t, cellScope, &store.Mutation{Put: []shard.Placement{f.placement(0, nodeB.ID, 2)}})
				received(t, w.Done(), "the end of the run")
			})
		})
	})

	t.Run("Err", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil while the router runs", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				assert.Nil(t, newFixture(t).start(t).Err(), "Err must be nil while the router runs")
			})
		})

		t.Run("returns nil after Close", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				w := newFixture(t).start(t)
				assert.NoError(t, w.Close(), "Close must stop the router")
				assert.Nil(t, w.Err(), "Err must be nil after Close")
			})
		})

		t.Run("returns the error with which the router stopped itself", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				s := &faulty{Store: f.st}
				w := f.start(t, func(c *router.Config) { c.Store = s })
				s.broken.Store(true)
				f.write(t, cellScope, &store.Mutation{Put: []shard.Placement{f.placement(0, nodeB.ID, 2)}})
				received(t, w.Done(), "the end of the run")
				assert.ErrorIs(t, w.Err(), errBroken, "Err must return the error of the store that stopped the router")
			})
		})
	})
}

// TestWatcherAllocs checks the allocation contract of Route. MaxAllocs counts the allocations
// of the whole process, so the test does not run in parallel.
func TestWatcherAllocs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		w := f.start(t)
		ctx, key := t.Context(), f.keys[0]
		var res router.RouteResult
		var err error
		expect.MaxAllocs(t, func() {
			req := router.RouteRequest{Key: key}
			res, err = w.Route(ctx, &req)
		}, 0, "Route must not allocate")
		assert.NoError(t, err, "the test must measure the route of a placed shard")
		assert.Equal(t, res.Route.Epoch, firstEpoch, "the test must measure the route of a placed shard")
	})
}

// BenchmarkWatcher measures Route for the key of a placed shard.
func BenchmarkWatcher(b *testing.B) {
	f := newFixture(b)
	w := f.start(b)
	ctx, key := b.Context(), f.keys[0]

	b.Run("Route", func(b *testing.B) {
		b.Run("serial", func(b *testing.B) {
			var res router.RouteResult
			var err error
			c := bench.Start(b).MaxAllocs(0)
			defer c.End()
			for c.Loop() {
				req := router.RouteRequest{Key: key}
				res, err = w.Route(ctx, &req)
			}
			assert.NoError(b, err, "the benchmark must measure the route of a placed shard")
			assert.Equal(b, res.Route.Epoch, firstEpoch, "the benchmark must measure the route of a placed shard")
		})

		b.Run("parallel", func(b *testing.B) {
			c := bench.Start(b).MaxAllocs(0)
			defer c.End()
			c.RunParallel(func(pb *bench.PB) {
				var res router.RouteResult
				var err error
				routed := 0
				for pb.Next() {
					req := router.RouteRequest{Key: key}
					res, err = w.Route(ctx, &req)
					routed++
				}
				if routed > 0 {
					expect.NoError(b, err, "the benchmark must measure the route of a placed shard")
					expect.Equal(
						b,
						res.Route.Epoch,
						firstEpoch,
						"the benchmark must measure the route of a placed shard",
					)
				}
			})
		})
	})
}

// move commits a placement of slot 0 at each epoch after firstEpoch, moves times. A placement
// of an even epoch is on node B, and a placement of an odd epoch is on node A. move records
// each commit in h as client 0, and waits tenancy after each commit, so the readers call Route
// between the commits. It returns the first error of a commit.
func move(ctx context.Context, f *fixture, h *history.History) error {
	for e := firstEpoch + 1; e <= firstEpoch+moves; e++ {
		n := nodeB.ID
		if e%2 == 1 {
			n = nodeA.ID
		}
		op := h.Invoke(0, opCommit, []any{e})
		if _, err := f.commit(
			ctx,
			cellScope,
			&store.Mutation{Put: []shard.Placement{f.placement(0, n, e)}},
		); err != nil {
			op.Fail(err)
			return err
		}
		op.OK(nil)
		<-time.After(tenancy)
	}
	return nil
}

// read calls Route for key on w and records the call in h as client. It returns the output of
// the call: the epoch of the route, and zero for an error.
func read(ctx context.Context, w *router.Watcher, h *history.History, client int, key []byte) epoch.Epoch {
	call := h.Invoke(client, opRoute, nil)
	res, err := w.Route(ctx, &router.RouteRequest{Key: key})
	got := epoch.Zero
	if err == nil {
		got = res.Route.Epoch
	}
	call.OK(got)
	return got
}

// received waits for the closing of ch, and fails tb when waitFor passes first.
func received(tb assert.TB, ch <-chan struct{}, what string) {
	tb.Helper()
	select {
	case <-ch:
	case <-time.After(waitFor):
		tb.Fatalf("%s did not happen within %v", what, waitFor)
	}
}

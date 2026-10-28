// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver_test

import (
	"context"
	"iter"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/version"

	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
	"go.thesmos.sh/sharder/store/storetest"
)

// lateWatches is the number of watches of the test that drops the items of ended watches.
const lateWatches = 32

// allNodes is the Node of a watch that yields every change of its scope.
const allNodes shard.NodeID = ""

// The placements of the commits of the tests.
var (
	first = shard.Placement{
		Shard: shard.ID{ID128: id.ID128{15: 1}},
		Range: shard.Range{End: []byte{1}},
	}
	second = shard.Placement{
		Shard: shard.ID{ID128: id.ID128{15: 2}},
		Range: shard.Range{Start: []byte{1}},
	}
)

// cursor is a page.Cursor whose iteration yields its placements and then its error, when it
// has one. Its Close records the call and returns closeErr.
type cursor struct {
	err      error
	closeErr error
	items    []shard.Placement
	closed   atomic.Bool
}

var _ page.Cursor[shard.Placement] = (*cursor)(nil)

// Seq yields the placements of c, and then the error of c when it has one.
func (c *cursor) Seq(context.Context) iter.Seq2[shard.Placement, error] {
	return func(yield func(shard.Placement, error) bool) {
		for _, p := range c.items {
			if !yield(p, nil) {
				return
			}
		}
		if c.err != nil {
			yield(shard.Placement{}, c.err)
		}
	}
}

// NextPage returns no token.
func (*cursor) NextPage() string {
	return ""
}

// Close records the call and returns the error of Close of c.
func (c *cursor) Close() error {
	c.closed.Store(true)
	return c.closeErr
}

// TestStore checks the effects of the store that Run performs: Head, Read, Watch, Unwatch,
// Commit and Outcome.
func TestStore(t *testing.T) {
	t.Parallel()

	t.Run("Run", func(t *testing.T) {
		t.Parallel()

		t.Run("passes the header that the reader returns for a Head", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Head{Scope: scope, Token: 1}))
				commit(t, f.store, 1)
				want, err := f.store.Head(t.Context(), scope)
				assert.NoError(t, err, "Head reads the scope")
				assert.Equal(t, f.results(t, 1)[0], machine.Event(machine.HeadRead{Head: want, Token: 1}),
					"the run must pass the header of the scope")
			})
		})

		t.Run("passes the error of the reader for a Head", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Head{Scope: scope, Token: 1}))
				f.store.StartOutage()
				got, ok := f.results(t, 1)[0].(machine.HeadRead)
				assert.True(t, ok, "the run must pass a HeadRead")
				expect.That(t, got.Err).ErrorIs(memory.ErrOutage, "the HeadRead must have the error of the reader")
				expect.Equal(t, got.Token, machine.Token(1), "the HeadRead must have the token of the Head")
			})
		})

		t.Run("passes the state, the placements and the token of the next page of a Read", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				read := machine.Read{Scope: scope, Page: page.Page{Limit: 1}, Token: 1, Incarnation: inc}
				f := newFixture(t, performing(read))
				commit(t, f.store, 1, first, second)
				st, cur, err := f.store.Read(t.Context(), scope, inc, store.Query{}, read.Page)
				assert.NoError(t, err, "Read reads the scope")
				var placements []shard.Placement
				for p, err := range cur.Seq(t.Context()) {
					assert.NoError(t, err, "the cursor yields the page")
					placements = append(placements, p)
				}
				want := machine.PageRead{State: st, Placements: placements, Next: cur.NextPage(), Token: 1}
				assert.NoError(t, cur.Close(), "the cursor closes")
				assert.Equal(
					t,
					f.results(t, 1)[0],
					machine.Event(want),
					"the run must pass the page that the reader returns",
				)
			})
		})

		t.Run("passes the error of the reader for a Read", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Read{Scope: scope, Token: 1}))
				f.store.StartOutage()
				got, ok := f.results(t, 1)[0].(machine.PageRead)
				assert.True(t, ok, "the run must pass a PageRead")
				expect.That(t, got.Err).ErrorIs(memory.ErrOutage, "the PageRead must have the error of the reader")
				expect.Equal(t, got.Token, machine.Token(1), "the PageRead must have the token of the Read")
			})
		})

		t.Run("passes the error of the cursor of a Read", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Read{Scope: scope, Token: 1}))
				c := &cursor{items: []shard.Placement{first}, err: errBusy}
				r := storetest.NewReaderStub(t)
				r.OnRead.Returns(store.State{}, c, nil)
				f.cfg.Reader = r
				expect.Equal(t, f.results(t, 1)[0], machine.Event(machine.PageRead{Err: errBusy, Token: 1}),
					"the PageRead must have only the error of the cursor")
				expect.True(t, c.closed.Load(), "the run must close the cursor of a page that fails")
			})
		})

		t.Run("passes the error of the Close of the cursor of a Read", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Read{Scope: scope, Token: 1}))
				r := storetest.NewReaderStub(t)
				r.OnRead.Returns(store.State{}, &cursor{items: []shard.Placement{first}, closeErr: errBroken}, nil)
				f.cfg.Reader = r
				got, ok := f.results(t, 1)[0].(machine.PageRead)
				assert.True(t, ok, "the run must pass a PageRead")
				expect.That(t, got.Err).
					ErrorIs(errBroken, "the PageRead must have the error of the Close of the cursor")
				expect.Equal(t, got, machine.PageRead{Err: got.Err, Token: 1}, "the PageRead must have only the error")
			})
		})

		t.Run("closes the cursor of a Read", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Read{Scope: scope, Token: 1}))
				c := &cursor{items: []shard.Placement{first, second}}
				r := storetest.NewReaderStub(t)
				r.OnRead.Returns(store.State{}, c, nil)
				f.cfg.Reader = r
				assert.Equal(t, f.results(t, 1)[0],
					machine.Event(machine.PageRead{Placements: []shard.Placement{first, second}, Token: 1}),
					"the run must pass the placements of the page")
				assert.True(t, c.closed.Load(), "the run must close the cursor of the page")
			})
		})

		t.Run("passes each generation of a Watch in order", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Watch{Scope: scope, Token: 1, Incarnation: inc}))
				commit(t, f.store, 1, first)
				commit(t, f.store, 2, second)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				items := streamtest.Follow(ctx, f.store.Watch(ctx, scope, inc, 0, allNodes))
				want := make([]machine.Event, 0, 2)
				for range 2 {
					item := received(t, items, "the store yields a generation")
					want = append(want, machine.Watched{Event: item.Value, Err: item.Err, Token: 1})
				}
				assert.Equal(t, f.results(t, 2), want, "the run must pass the generations of the watch in order")
			})
		})

		t.Run("passes the error that ends a Watch and no item after it", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Watch{Scope: scope, Token: 1}))
				f.cfg.Reader = storetest.NewReaderStub(t, storetest.WithReaderWatch(func(
					context.Context, shard.Scope, shard.NamespaceIncarnation, epoch.Epoch, shard.NodeID,
				) iter.Seq2[store.Event, error] {
					return func(yield func(store.Event, error) bool) {
						if yield(store.Event{}, errBusy) {
							yield(store.Event{Generation: 1}, nil)
						}
					}
				}))
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.events, "the run passes Started")
				assert.Equal(t, received(t, f.events, "the run passes the item of the watch"),
					machine.Event(machine.Watched{Err: errBusy, Token: 1}), "the run must pass the error of the watch")
				synctest.Wait()
				streamtest.Blocks(t, f.events, "the run must not pass an item after the error of the watch")
				stopped(t, cancel, errc)
			})
		})

		t.Run("passes ErrEnded when the iteration of a Watch ends while the watch continues", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Watch{Scope: scope, Token: 1}))
				f.cfg.Reader = storetest.NewReaderStub(t, storetest.WithReaderWatch(func(
					context.Context, shard.Scope, shard.NamespaceIncarnation, epoch.Epoch, shard.NodeID,
				) iter.Seq2[store.Event, error] {
					return func(func(store.Event, error) bool) {}
				}))
				got, ok := f.results(t, 1)[0].(machine.Watched)
				assert.True(t, ok, "the run must pass a Watched")
				expect.That(t, got.Err).ErrorIs(driver.ErrEnded, "the item must have ErrEnded")
				expect.Equal(t, got.Token, machine.Token(1), "the item must have the token of the Watch")
			})
		})

		t.Run("ends a Watch at its Unwatch while a later Watch continues", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{
							machine.Reply[struct{}]{To: ev.Reply},
							machine.Watch{Scope: scope, Token: 1, From: 1},
							machine.Watch{Scope: scope, Token: 2, From: 2},
						}
					case machine.Watched:
						if ev.Token == 1 {
							return []machine.Effect{machine.Unwatch{Token: 1}}
						}
					}
					return nil
				})
				ctxs := map[epoch.Epoch]chan context.Context{
					1: make(chan context.Context, 1),
					2: make(chan context.Context, 1),
				}
				f.cfg.Reader = storetest.NewReaderStub(t, storetest.WithReaderWatch(func(
					ctx context.Context, _ shard.Scope, _ shard.NamespaceIncarnation, from epoch.Epoch, _ shard.NodeID,
				) iter.Seq2[store.Event, error] {
					ctxs[from] <- ctx
					return func(yield func(store.Event, error) bool) {
						if yield(store.Event{Generation: from}, nil) {
							<-ctx.Done()
						}
					}
				}))
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.events, "the run passes Started")
				for range 2 {
					_, ok := received(t, f.events, "the run passes the item of a watch").(machine.Watched)
					assert.True(t, ok, "the run must pass a Watched")
				}
				synctest.Wait()
				expect.That(t, received(t, ctxs[1], "the run starts the first watch").Err()).
					ErrorIs(context.Canceled, "the Unwatch must end the first watch")
				expect.Nil(t, received(t, ctxs[2], "the run starts the second watch").Err(),
					"the Unwatch of the first watch must not end the second")
				stopped(t, cancel, errc)
			})
		})

		t.Run("ends the context of a Watch once an error ends the watch", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Watch{Scope: scope, Token: 1}))
				ended := make(chan struct{}, 1)
				f.cfg.Reader = storetest.NewReaderStub(t, storetest.WithReaderWatch(func(
					ctx context.Context, _ shard.Scope, _ shard.NamespaceIncarnation, _ epoch.Epoch, _ shard.NodeID,
				) iter.Seq2[store.Event, error] {
					context.AfterFunc(ctx, func() { ended <- struct{}{} })
					return func(yield func(store.Event, error) bool) {
						yield(store.Event{}, errBusy)
					}
				}))
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.events, "the run passes Started")
				received(t, f.events, "the run passes the error of the watch")
				received(t, ended, "the run must end the context of a watch that an error has ended")
				stopped(t, cancel, errc)
			})
		})

		t.Run("passes no item of a Watch after its Unwatch", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				watches := make([]machine.Effect, 0, lateWatches)
				for tok := range machine.Token(lateWatches) {
					watches = append(watches, machine.Watch{Scope: scope, Token: tok + 1})
				}
				script := performing(watches...)
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					if w, ok := ev.(machine.Watched); ok {
						return []machine.Effect{machine.Unwatch{Token: w.Token}}
					}
					return script(ev)
				})
				f.cfg.Reader = storetest.NewReaderStub(t, storetest.WithReaderWatch(func(
					ctx context.Context, _ shard.Scope, _ shard.NamespaceIncarnation, _ epoch.Epoch, _ shard.NodeID,
				) iter.Seq2[store.Event, error] {
					return func(yield func(store.Event, error) bool) {
						if yield(store.Event{Generation: 1}, nil) {
							<-ctx.Done()
							yield(store.Event{Generation: 2}, nil)
						}
					}
				}))
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.events, "the run passes Started")
				for range lateWatches {
					got, ok := received(t, f.events, "the run passes the first item of a watch").(machine.Watched)
					assert.True(t, ok, "the run must pass a Watched")
					assert.Equal(
						t,
						got.Event.Generation,
						epoch.Epoch(1),
						"the first item of each watch is the first generation",
					)
				}
				synctest.Wait()
				streamtest.Blocks(
					t,
					f.events,
					"the run must not pass an item that a watch yields after its Unwatch",
				)
				stopped(t, cancel, errc)
			})
		})

		t.Run("commits the mutation of a Commit", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Commit{
					Mutation:    mutation(1, first),
					Scope:       scope,
					Token:       1,
					Term:        term,
					Incarnation: inc,
				}))
				assert.Equal(t, f.results(t, 1)[0], machine.Event(machine.Committed{Generation: 1, Token: 1}),
					"the run must pass the generation of the commit")
				head, err := f.store.Head(t.Context(), scope)
				assert.NoError(t, err, "Head reads the scope")
				assert.Equal(t, head.Generation, epoch.Epoch(1), "the store must have the generation of the commit")
			})
		})

		t.Run("passes the error of the writer for a Commit", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Commit{
					Mutation:    mutation(1),
					Scope:       scope,
					Token:       1,
					Term:        term,
					Expect:      3,
					Incarnation: inc,
				}))
				got, ok := f.results(t, 1)[0].(machine.Committed)
				assert.True(t, ok, "the run must pass a Committed")
				expect.That(t, got.Err).ErrorIs(version.ErrMismatch, "the Committed must have the error of the writer")
				expect.Equal(t, got.Token, machine.Token(1), "the Committed must have the token of the Commit")
			})
		})

		t.Run("passes ErrConfig for a Commit without a store", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Commit{Mutation: &store.Mutation{}, Scope: scope, Token: 1}))
				f.cfg.Store = nil
				got, ok := f.results(t, 1)[0].(machine.Committed)
				assert.True(t, ok, "the run must pass a Committed")
				expect.That(t, got.Err).ErrorIs(driver.ErrConfig, "the Committed must have ErrConfig")
				expect.Equal(t, got.Token, machine.Token(1), "the Committed must have the token of the Commit")
			})
		})

		t.Run("passes the receipt that the store returns for an Outcome", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				id := store.CommitID{Incarnation: inc, Term: term, Seq: 1}
				f := newFixture(t, performing(machine.Outcome{Scope: scope, Token: 1, ID: id}))
				commit(t, f.store, 1)
				receipt, found, err := f.store.Outcome(t.Context(), scope, id)
				assert.NoError(t, err, "Outcome reads the scope")
				assert.True(t, found, "the scope has the receipt of the commit")
				assert.Equal(
					t,
					f.results(t, 1)[0],
					machine.Event(machine.OutcomeRead{Receipt: receipt, Found: true, Token: 1}),
					"the run must pass the receipt of the commit",
				)
			})
		})

		t.Run(
			"passes an OutcomeRead without a receipt for a commit that the scope does not record",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t, performing(machine.Outcome{
						Scope: scope,
						Token: 1,
						ID:    store.CommitID{Incarnation: inc, Term: term, Seq: 9},
					}))
					commit(t, f.store, 1)
					assert.Equal(t, f.results(t, 1)[0], machine.Event(machine.OutcomeRead{Token: 1}),
						"the run must pass an OutcomeRead without a receipt")
				})
			},
		)

		t.Run("passes the error of the store for an Outcome", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Outcome{Scope: scope, Token: 1}))
				f.store.StartOutage()
				got, ok := f.results(t, 1)[0].(machine.OutcomeRead)
				assert.True(t, ok, "the run must pass an OutcomeRead")
				expect.That(t, got.Err).ErrorIs(memory.ErrOutage, "the OutcomeRead must have the error of the store")
				expect.Equal(t, got.Token, machine.Token(1), "the OutcomeRead must have the token of the Outcome")
			})
		})

		t.Run("passes ErrConfig for an Outcome without a store", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Outcome{Scope: scope, Token: 1}))
				f.cfg.Store = nil
				got, ok := f.results(t, 1)[0].(machine.OutcomeRead)
				assert.True(t, ok, "the run must pass an OutcomeRead")
				expect.That(t, got.Err).ErrorIs(driver.ErrConfig, "the OutcomeRead must have ErrConfig")
				expect.Equal(t, got.Token, machine.Token(1), "the OutcomeRead must have the token of the Outcome")
			})
		})
	})
}

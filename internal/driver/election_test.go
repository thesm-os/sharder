// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver_test

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// leadership is a store.Leadership whose Resign calls its function.
type leadership struct {
	done   chan struct{}
	resign func(ctx context.Context) error
	term   epoch.Epoch
}

var _ store.Leadership = (*leadership)(nil)

// Term returns the term of l.
func (l *leadership) Term() epoch.Epoch {
	return l.term
}

// Done returns the channel of l that closes when the term ends.
func (l *leadership) Done() <-chan struct{} {
	return l.done
}

// Resign calls the function of l.
func (l *leadership) Resign(ctx context.Context) error {
	return l.resign(ctx)
}

// elector is a store.Store whose Campaign calls its function. Its other methods are those
// of Store.
type elector struct {
	store.Store
	campaign func(ctx context.Context) (store.Leadership, error)
}

var _ store.Store = (*elector)(nil)

// Campaign calls the function of e.
func (e *elector) Campaign(ctx context.Context, _ shard.Scope) (store.Leadership, error) {
	return e.campaign(ctx)
}

// grantor is a store.Store whose Campaign grants the term 1, which ends when the test closes
// done. It passes the context of each campaign to campaigns, and counts the calls of Resign
// in resigns. Its other methods are those of Store.
type grantor struct {
	store.Store
	campaigns chan context.Context
	done      chan struct{}
	resigns   atomic.Int64
}

var _ store.Store = (*grantor)(nil)

// Campaign passes ctx to the campaigns of g, and returns the term 1 of g.
func (g *grantor) Campaign(ctx context.Context, _ shard.Scope) (store.Leadership, error) {
	g.campaigns <- ctx
	return &leadership{term: 1, done: g.done, resign: func(context.Context) error {
		g.resigns.Add(1)
		return nil
	}}, nil
}

// resigning is the script of a probe that campaigns at Started, and resigns the campaign at
// the event of a call.
func resigning(ev machine.Event) []machine.Effect {
	switch ev := ev.(type) {
	case machine.Started:
		return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}, machine.Campaign{Scope: scope, Token: 1}}
	case machine.Recovered:
		return []machine.Effect{machine.Resign{Token: 1}, machine.Reply[struct{}]{To: ev.Reply}}
	}
	return nil
}

// TestElection checks the effects of the election that Run performs: Campaign and Resign.
func TestElection(t *testing.T) {
	t.Parallel()

	t.Run("Run", func(t *testing.T) {
		t.Parallel()

		t.Run("passes the term that the store grants for a Campaign", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Campaign{Scope: scope, Token: 1}))
				assert.Equal(t, f.results(t, 1)[0], machine.Event(machine.Campaigned{Term: 1, Token: 1}),
					"the run must pass the term that the store grants")
			})
		})

		t.Run("passes the error of the store for a Campaign", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Campaign{Scope: scope, Token: 1}))
				f.store.StartOutage()
				got, ok := f.results(t, 1)[0].(machine.Campaigned)
				assert.True(t, ok, "the run must pass a Campaigned")
				expect.That(t, got.Err).ErrorIs(memory.ErrOutage, "the Campaigned must have the error of the store")
				expect.Equal(t, got.Token, machine.Token(1), "the Campaigned must have the token of the Campaign")
			})
		})

		t.Run("passes ErrConfig for a Campaign without a store", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Campaign{Scope: scope, Token: 1}))
				f.cfg.Store = nil
				got, ok := f.results(t, 1)[0].(machine.Campaigned)
				assert.True(t, ok, "the run must pass a Campaigned")
				expect.That(t, got.Err).ErrorIs(driver.ErrConfig, "the Campaigned must have ErrConfig")
				expect.Equal(t, got.Token, machine.Token(1), "the Campaigned must have the token of the Campaign")
			})
		})

		t.Run("passes TermEnded once the store ends the term", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var f *fixture
				f = newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{
							machine.Reply[struct{}]{To: ev.Reply},
							machine.Campaign{Scope: scope, Token: 1},
						}
					case machine.Campaigned:
						f.store.Delete(scope)
					}
					return nil
				})
				assert.Equal(t, f.results(t, 2), []machine.Event{
					machine.Campaigned{Term: 1, Token: 1},
					machine.TermEnded{Token: 1},
				}, "the run must pass TermEnded once the store ends the term")
			})
		})

		t.Run("cancels the context of a Campaign once the store ends its term", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Campaign{Scope: scope, Token: 1}))
				g := &grantor{Store: f.store, campaigns: make(chan context.Context, 1), done: make(chan struct{})}
				f.cfg.Store = g
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.probe.events, "the run passes Started")
				received(t, f.probe.events, "the run passes the term")
				close(g.done)
				received(t, f.probe.events, "the run passes the end of the term")
				synctest.Wait()
				assert.ErrorIs(
					t,
					received(t, g.campaigns, "the store receives the campaign").Err(),
					context.Canceled,
					"the run must cancel the context of the campaign once the store ends its term",
				)
				stopped(t, cancel, errc)
			})
		})

		t.Run("does not resign a term that the store ends", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Campaign{Scope: scope, Token: 1}))
				g := &grantor{Store: f.store, campaigns: make(chan context.Context, 1), done: make(chan struct{})}
				f.cfg.Store = g
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.probe.events, "the run passes Started")
				received(t, f.probe.events, "the run passes the term")
				close(g.done)
				received(t, f.probe.events, "the run passes the end of the term")
				synctest.Wait()
				assert.Equal(t, g.resigns.Load(), int64(0), "the run must not resign a term that the store has ended")
				stopped(t, cancel, errc)
			})
		})

		t.Run("does not resign a term when the run stops", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Campaign{Scope: scope, Token: 1}))
				g := &grantor{Store: f.store, campaigns: make(chan context.Context, 1), done: make(chan struct{})}
				f.cfg.Store = g
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.probe.events, "the run passes Started")
				received(t, f.probe.events, "the run passes the term")
				stopped(t, cancel, errc)
				assert.Equal(t, g.resigns.Load(), int64(0), "the run must not resign a term when it stops")
			})
		})

		t.Run("resigns the term of a Campaign at its Resign", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{
							machine.Reply[struct{}]{To: ev.Reply},
							machine.Campaign{Scope: scope, Token: 1},
						}
					case machine.Campaigned:
						return []machine.Effect{machine.Resign{Token: 1}}
					}
					return nil
				})
				var resigned atomic.Bool
				done := make(chan struct{})
				f.cfg.Store = &elector{Store: f.store, campaign: func(context.Context) (store.Leadership, error) {
					return &leadership{term: 1, done: done, resign: func(ctx context.Context) error {
						resigned.Store(ctx.Err() == nil)
						close(done)
						return nil
					}}, nil
				}}
				assert.Equal(t, f.results(t, 2), []machine.Event{
					machine.Campaigned{Term: 1, Token: 1},
					machine.TermEnded{Token: 1},
				}, "the run must pass TermEnded once it has resigned the term")
				assert.True(t, resigned.Load(), "the run must resign the term before it cancels the campaign")
			})
		})

		t.Run("logs a Resign that fails and ends the term with the context of the campaign", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{
							machine.Reply[struct{}]{To: ev.Reply},
							machine.Campaign{Scope: scope, Token: 1},
						}
					case machine.Campaigned:
						return []machine.Effect{machine.Resign{Token: 1}}
					}
					return nil
				})
				f.cfg.Store = &elector{Store: f.store, campaign: func(ctx context.Context) (store.Leadership, error) {
					done := make(chan struct{})
					context.AfterFunc(ctx, func() { close(done) })
					return &leadership{
						term:   1,
						done:   done,
						resign: func(context.Context) error { return errBroken },
					}, nil
				}}
				assert.Equal(t, f.results(t, 2), []machine.Event{
					machine.Campaigned{Term: 1, Token: 1},
					machine.TermEnded{Token: 1},
				}, "the run must end the term with the context of the campaign")
				levels, errs := logged(f.logs)
				assert.Equal(
					t,
					levels,
					[]slog.Level{slog.LevelWarn},
					"the run must log the failed resignation once as a warning",
				)
				assert.Length(t, errs, 1, "the record must have the error of Resign")
				assert.ErrorIs(t, errs[0], errBroken, "the record must have the error of Resign")
			})
		})

		t.Run("ends the wait of a Campaign at a Resign before the store grants a term", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, resigning)
				_, err := f.store.Campaign(t.Context(), scope)
				assert.NoError(t, err, "the test leads the election")
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				received(t, f.probe.events, "the run passes Started")
				synctest.Wait()
				_, err = d.Call[struct{}](t.Context(), func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				assert.NoError(t, err, "the call returns its reply")
				received(t, f.probe.events, "the run passes the event of the call")
				got, ok := received(t, f.probe.events, "the run passes the result of the campaign").(machine.Campaigned)
				assert.True(t, ok, "the run must pass a Campaigned")
				expect.That(t, got.Err).
					ErrorIs(context.Canceled, "the Campaigned must have the error of the ended wait")
				expect.Equal(t, got.Token, machine.Token(1), "the Campaigned must have the token of the Campaign")
				stopped(t, cancel, errc)
			})
		})

		t.Run("ends a term that the store grants as a Resign ends the wait", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, resigning)
				var resigned atomic.Bool
				f.cfg.Store = &elector{Store: f.store, campaign: func(ctx context.Context) (store.Leadership, error) {
					<-ctx.Done()
					done := make(chan struct{})
					close(done)
					return &leadership{term: 1, done: done, resign: func(context.Context) error {
						resigned.Store(true)
						return nil
					}}, nil
				}}
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				received(t, f.probe.events, "the run passes Started")
				_, err := d.Call[struct{}](t.Context(), func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				assert.NoError(t, err, "the call returns its reply")
				received(t, f.probe.events, "the run passes the event of the call")
				assert.Equal(
					t,
					received(t, f.probe.events, "the run passes the term"),
					machine.Event(
						machine.Campaigned{Term: 1, Token: 1},
					),
					"the run must pass the term that the store granted",
				)
				assert.Equal(t, received(t, f.probe.events, "the run passes the end of the term"),
					machine.Event(machine.TermEnded{Token: 1}), "the run must pass the end of the term")
				assert.True(t, resigned.Load(), "the run must resign the term")
				stopped(t, cancel, errc)
			})
		})
	})
}

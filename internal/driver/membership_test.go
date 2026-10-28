// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver_test

import (
	"context"
	"iter"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/membership/membershiptest"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store/memory"
)

// The lease and the incarnations of the sessions of the tests.
var (
	// lease renews every 3 seconds, with a validity of 5 seconds and a TTL of 10 seconds.
	lease = ownership.Lease{
		TTL:          shard.Duration(10 * time.Second),
		Renew:        shard.Duration(3 * time.Second),
		MaxDriftRate: 0.5,
		OnLoss:       ownership.Stop,
	}
	// otherInc is a namespace incarnation that did not create the scope of the tests.
	otherInc = shard.NamespaceIncarnation{ID: id.New128([id.Size128]byte{0: 0x1a, 15: 0x0b})}
	// otherNode is a second node of the views of the tests.
	otherNode = shard.Node{ID: "node-b", Address: "node-b:7000"}
)

// join is the Join of the node of the tests in the scope of the tests.
var join = machine.Join{Node: &node, Scope: scope, Token: 1, Lease: lease, Incarnation: inc}

// leaving is the script of a machine that joins at Started, and leaves at the event of a
// call.
func leaving(ev machine.Event) []machine.Effect {
	switch ev := ev.(type) {
	case machine.Started:
		return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}, join}
	case machine.Recovered:
		return []machine.Effect{machine.Leave{Token: 1}, machine.Reply[struct{}]{To: ev.Reply}}
	}
	return nil
}

// TestMembership checks the effects of the membership that Run performs: Join, Leave,
// WatchViews and the Unwatch of a WatchViews.
func TestMembership(t *testing.T) {
	t.Parallel()

	t.Run("Run", func(t *testing.T) {
		t.Parallel()

		t.Run("passes the incarnation of the session of a Join", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(join))
				assert.Equal(t, f.results(t, 1)[0], machine.Event(machine.Joined{Incarnation: 1, Token: 1}),
					"the run must pass the incarnation of the session")
			})
		})

		t.Run("passes the error of the membership for a Join", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(join))
				f.store.StartOutage()
				got, ok := f.results(t, 1)[0].(machine.Joined)
				assert.True(t, ok, "the run must pass a Joined")
				expect.That(t, got.Err).ErrorIs(memory.ErrOutage, "the Joined must have the error of the membership")
				expect.Equal(t, got.Token, machine.Token(1), "the Joined must have the token of the Join")
			})
		})

		t.Run("passes SessionEnded once the session ends", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var f *fixture
				f = newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}, join}
					case machine.Joined:
						f.store.Delete(scope)
					}
					return nil
				})
				assert.Equal(t, f.results(t, 2), []machine.Event{
					machine.Joined{Incarnation: 1, Token: 1},
					machine.SessionEnded{Token: 1},
				}, "the run must pass SessionEnded once the session ends")
			})
		})

		t.Run("cancels the context of a Join once its session ends", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(join))
				done := make(chan struct{})
				m := membershiptest.NewMembershipStub(t)
				m.OnJoin.Returns(session(t, 1, done), nil)
				f.cfg.Membership = m
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.events, "the run passes Started")
				received(t, f.events, "the run passes the session")
				close(done)
				received(t, f.events, "the run passes the end of the session")
				synctest.Wait()
				assert.ErrorIs(
					t,
					m.OnJoin.AssertCalledOnce(t, "the membership receives one join").Ctx.Err(),
					context.Canceled,
					"the run must cancel the context of the join once its session ends",
				)
				stopped(t, cancel, errc)
			})
		})

		t.Run("leaves the session of a Join at its Leave", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}, join}
					case machine.Joined:
						return []machine.Effect{machine.Leave{Token: 1}}
					}
					return nil
				})
				assert.Equal(t, f.results(t, 3), []machine.Event{
					machine.Joined{Incarnation: 1, Token: 1},
					machine.Left{Token: 1},
					machine.SessionEnded{Token: 1},
				}, "the run must pass the result of the Leave and then the end of the session")
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				view := received(
					t,
					streamtest.Follow(ctx, f.store.Membership().Watch(ctx, scope, inc)),
					"the membership yields its view",
				)
				assert.NoError(t, view.Err, "the membership yields its view")
				assert.Empty(t, view.Value.Nodes, "the view must not list a node that has left")
			})
		})

		t.Run("logs a Leave that fails and ends the session at its deadline", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var f *fixture
				f = newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}, join}
					case machine.Joined:
						f.store.StartOutage()
						return []machine.Effect{machine.Leave{Token: 1}}
					}
					return nil
				})
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.events, "the run passes Started")
				assert.Equal(
					t,
					received(t, f.events, "the run passes Joined"),
					machine.Event(
						machine.Joined{Incarnation: 1, Token: 1},
					),
					"the run must pass the session of the Join",
				)
				received(t, f.events, "the run passes the result of the Leave")
				synctest.Wait()
				f.clock.Advance(time.Duration(lease.TTL))
				assert.Equal(
					t,
					received(t, f.events, "the run passes the end of the session"),
					machine.Event(
						machine.SessionEnded{Token: 1},
					),
					"the run must pass SessionEnded once the session has lost its lease",
				)
				levels, errs := logged(f.logs)
				assert.Equal(
					t,
					levels,
					[]slog.Level{slog.LevelWarn},
					"the run must log the failed Leave once as a warning",
				)
				assert.Length(t, errs, 1, "the record must have the error of Leave")
				assert.ErrorIs(t, errs[0], memory.ErrOutage, "the record must have the error of Leave")
				stopped(t, cancel, errc)
			})
		})

		t.Run("passes the error of a Leave that fails in Left", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var f *fixture
				f = newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}, join}
					case machine.Joined:
						f.store.StartOutage()
						return []machine.Effect{machine.Leave{Token: 1}}
					}
					return nil
				})
				got, ok := f.results(t, 2)[1].(machine.Left)
				assert.True(t, ok, "the run must pass a Left before the session ends")
				expect.That(t, got.Err).ErrorIs(memory.ErrOutage, "the Left must have the error of Leave")
				expect.Equal(t, got.Token, machine.Token(1), "the Left must have the token of the Join")
			})
		})

		t.Run("ends the wait of a Join at a Leave before the membership returns a session", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, leaving)
				f.cfg.Membership = membershiptest.NewMembershipStub(t, membershiptest.WithMembershipJoin(func(
					ctx context.Context,
					_ shard.Scope,
					_ shard.NamespaceIncarnation,
					_ *shard.Node,
					_ ownership.Lease,
				) (membership.Session, error) {
					<-ctx.Done()
					return nil, context.Cause(ctx)
				}))
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				received(t, f.events, "the run passes Started")
				_, err := d.Call[struct{}](t.Context(), func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				assert.NoError(t, err, "the call returns its reply")
				received(t, f.events, "the run passes the event of the call")
				got, ok := received(t, f.events, "the run passes the result of the join").(machine.Joined)
				assert.True(t, ok, "the run must pass a Joined")
				expect.That(t, got.Err).ErrorIs(context.Canceled, "the Joined must have the error of the ended wait")
				expect.Equal(t, got.Token, machine.Token(1), "the Joined must have the token of the Join")
				stopped(t, cancel, errc)
			})
		})

		t.Run("leaves a session that the membership returns as a Leave ends the wait", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, leaving)
				done := make(chan struct{})
				s := session(t, 3, done)
				s.OnLeave.Func(func(context.Context) error {
					close(done)
					return nil
				})
				f.cfg.Membership = membershiptest.NewMembershipStub(t, membershiptest.WithMembershipJoin(func(
					ctx context.Context,
					_ shard.Scope,
					_ shard.NamespaceIncarnation,
					_ *shard.Node,
					_ ownership.Lease,
				) (membership.Session, error) {
					<-ctx.Done()
					return s, nil
				}))
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				received(t, f.events, "the run passes Started")
				_, err := d.Call[struct{}](t.Context(), func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				assert.NoError(t, err, "the call returns its reply")
				received(t, f.events, "the run passes the event of the call")
				assert.Equal(
					t,
					received(t, f.events, "the run passes the session"),
					machine.Event(
						machine.Joined{Incarnation: 3, Token: 1},
					),
					"the run must pass the session that the membership returned",
				)
				assert.Equal(t, received(t, f.events, "the run passes the result of the Leave"),
					machine.Event(machine.Left{Token: 1}), "the run must pass the result of the Leave")
				assert.Equal(t, received(t, f.events, "the run passes the end of the session"),
					machine.Event(machine.SessionEnded{Token: 1}), "the run must pass the end of the session")
				s.OnLeave.AssertCalledOnce(t, "the run must leave the session")
				stopped(t, cancel, errc)
			})
		})

		t.Run("passes the current view of the scope for a WatchViews", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.WatchViews{Scope: scope, Token: 1, Incarnation: inc}))
				_, err := f.store.Membership().Join(t.Context(), scope, inc, &node, lease)
				assert.NoError(t, err, "the node joins the scope")
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				view := received(
					t,
					streamtest.Follow(ctx, f.store.Membership().Watch(ctx, scope, inc)),
					"the membership yields its view",
				)
				assert.Equal(
					t,
					f.results(t, 1)[0],
					machine.Event(machine.Viewed{View: view.Value, Err: view.Err, Token: 1}),
					"the run must pass the view that the membership yields",
				)
			})
		})

		t.Run("passes the error that ends a WatchViews", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.WatchViews{Scope: scope, Token: 1, Incarnation: otherInc}))
				commit(t, f.store, 1)
				got, ok := f.results(t, 1)[0].(machine.Viewed)
				assert.True(t, ok, "the run must pass a Viewed")
				expect.That(t, got.Err).ErrorIs(shard.ErrIncarnation, "the Viewed must have the error of the watch")
				expect.Equal(t, got.Token, machine.Token(1), "the Viewed must have the token of the WatchViews")
			})
		})

		t.Run("passes ErrEnded when the iteration of a WatchViews ends while the watch continues", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.WatchViews{Scope: scope, Token: 1}))
				f.cfg.Membership = membershiptest.NewMembershipStub(t, membershiptest.WithMembershipWatch(
					func(context.Context, shard.Scope, shard.NamespaceIncarnation) iter.Seq2[membership.View, error] {
						return func(func(membership.View, error) bool) {}
					}))
				got, ok := f.results(t, 1)[0].(machine.Viewed)
				assert.True(t, ok, "the run must pass a Viewed")
				expect.That(t, got.Err).ErrorIs(driver.ErrEnded, "the Viewed must have ErrEnded")
				expect.Equal(t, got.Token, machine.Token(1), "the Viewed must have the token of the WatchViews")
			})
		})

		t.Run("passes no view of a WatchViews after its Unwatch", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{
							machine.Reply[struct{}]{To: ev.Reply},
							machine.WatchViews{Scope: scope, Token: 1, Incarnation: inc},
						}
					case machine.Viewed:
						return []machine.Effect{machine.Unwatch{Token: 1}}
					}
					return nil
				})
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.events, "the run passes Started")
				_, ok := received(t, f.events, "the run passes the current view").(machine.Viewed)
				assert.True(t, ok, "the run must pass the current view")
				synctest.Wait()
				_, err := f.store.Membership().Join(t.Context(), scope, inc, &otherNode, lease)
				assert.NoError(t, err, "a node joins the scope")
				synctest.Wait()
				streamtest.Blocks(t, f.events, "the run must not pass a view after the Unwatch of the watch")
				stopped(t, cancel, errc)
			})
		})
	})
}

// session returns a double of a session of the incarnation inc, which ends when done closes.
func session(tb assert.TB, inc epoch.Epoch, done <-chan struct{}) *membershiptest.SessionStub {
	tb.Helper()
	return membershiptest.NewSessionStub(tb,
		membershiptest.WithSessionIncarnation(func() epoch.Epoch { return inc }),
		membershiptest.WithSessionDone(func() <-chan struct{} { return done }))
}

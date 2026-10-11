// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package memory_test

import (
	"context"
	"iter"
	"runtime"
	"testing"
	"time"
	"weak"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/clock/fake"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/membership/membershiptest"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// renewalPoints maps each loss of the laws of package membership to the point that loses a
// renewal there.
var renewalPoints = map[membershiptest.Loss]memory.Point{
	membershiptest.LoseRequest: memory.PointRenewApply,
	membershiptest.LoseReply:   memory.PointRenewAck,
}

// TestSession checks the laws of package membership, and the capabilities, the joins, the
// watches and the sessions of a Membership.
func TestSession(t *testing.T) {
	t.Parallel()

	t.Run("Membership", func(t *testing.T) {
		t.Parallel()

		t.Run("passes the laws of package membership", func(t *testing.T) {
			t.Parallel()
			membershiptest.AssertMembership(t,
				func(_ assert.TB, clk clock.Clock) (membership.Membership, store.Store) {
					s := memory.New(clk)
					m := s.Membership()
					return m, s
				},
				membershiptest.WithLostRenewals(
					func(tb assert.TB, _ membership.Membership, s store.Store, l membershiptest.Loss) {
						memoryOf(tb, s).FailAt(renewalPoints[l])
					}),
			)
		})

		t.Run("Provides", func(t *testing.T) {
			t.Parallel()

			t.Run("returns the capability leases", func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, open(t).Membership().Provides(), []config.Capability{config.Leases},
					"a membership provides leases")
			})
		})

		t.Run("Join", func(t *testing.T) {
			t.Parallel()

			t.Run("returns ErrClosed after Close", func(t *testing.T) {
				t.Parallel()
				s := open(t)
				n := shard.Node{ID: node}
				assert.FailsAfterClose(t, s.Close, func() error {
					_, err := s.Membership().Join(t.Context(), scope, incA, &n, lease)
					return err
				}, memory.ErrClosed, "Join returns ErrClosed after Close")
			})

			t.Run("ends the live session of a node that joins again", func(t *testing.T) {
				t.Parallel()
				s := open(t)
				n := shard.Node{ID: node}
				before, err := s.Membership().Join(t.Context(), scope, incA, &n, lease)
				assert.NoError(t, err, "Join registers the node")
				after, err := s.Membership().Join(t.Context(), scope, incA, &n, lease)
				assert.NoError(t, err, "Join registers the node again")
				streamtest.Receive(
					t,
					before.Done(),
					waitFor,
					"Done of the session before is closed once the node joins again",
				)
				assert.True(t, after.Incarnation() > before.Incarnation(),
					"the session of a node that joins again has a higher incarnation")
			})

			t.Run("ends the sessions of another incarnation once a commit creates the scope", func(t *testing.T) {
				t.Parallel()
				s := open(t)
				n := shard.Node{ID: node}
				ss, err := s.Membership().Join(t.Context(), scope, incB, &n, lease)
				assert.NoError(t, err, "Join registers the node in a scope before its first commit")
				commit(t, s, incA, &store.Mutation{})
				streamtest.Receive(t, ss.Done(), waitFor,
					"Done of a session is closed once another incarnation creates its scope")
			})

			t.Run("keeps the sessions of the incarnation of the commit that creates the scope", func(t *testing.T) {
				t.Parallel()
				s := open(t)
				n := shard.Node{ID: node}
				ss, err := s.Membership().Join(t.Context(), scope, incA, &n, lease)
				assert.NoError(t, err, "Join registers the node in a scope before its first commit")
				commit(t, s, incA, &store.Mutation{})
				streamtest.Blocks(t, ss.Done(), "Done of a session is open after its incarnation creates its scope")
			})

			t.Run(
				"returns ErrIncarnation for the zero incarnation in a scope that a commit created",
				func(t *testing.T) {
					t.Parallel()
					s := open(t)
					commit(t, s, incA, &store.Mutation{})
					n := shard.Node{ID: node}
					_, err := s.Membership().Join(t.Context(), scope, shard.NamespaceIncarnation{}, &n, lease)
					assert.ErrorIs(t, err, shard.ErrIncarnation,
						"Join returns ErrIncarnation for the zero incarnation in a scope that a commit created")
				},
			)

			t.Run("expires the leases of two joins at one instant in the order of the joins", func(t *testing.T) {
				t.Parallel()
				clk := fake.New(origin)
				s := memory.New(clk)
				t.Cleanup(func() { assert.NoError(t, s.Close(), "Close returns nil") })
				next, stop := iter.Pull2(s.Membership().Watch(t.Context(), scope, incA))
				defer stop()
				pull(t, next)
				ended, cancel := context.WithCancel(t.Context())
				cancel()
				joinEach(ended, t, s)
				clk.Advance(time.Duration(lease.TTL))
				pull(t, next)
				pull(t, next)
				assert.Equal(t, pull(t, next).Nodes, []shard.Node{{ID: otherNode, Incarnation: 1}},
					"the lease of the earlier join expires first", assert.EquateEmpty())
			})
		})

		t.Run("Watch", func(t *testing.T) {
			t.Parallel()

			t.Run("yields ErrClosed after Close", func(t *testing.T) {
				t.Parallel()
				s := open(t)
				assert.FailsAfterClose(t, s.Close, func() error {
					for _, err := range s.Membership().Watch(t.Context(), scope, incA) {
						return err
					}
					return nil
				}, memory.ErrClosed, "Watch yields ErrClosed after Close")
			})

			t.Run("lists the sessions of the incarnation of the watch", func(t *testing.T) {
				t.Parallel()
				s := open(t)
				joins := [...]struct {
					node shard.NodeID
					inc  shard.NamespaceIncarnation
				}{{node: node, inc: incA}, {node: otherNode, inc: incB}}
				for _, j := range joins {
					n := shard.Node{ID: j.node}
					_, err := s.Membership().Join(t.Context(), scope, j.inc, &n, lease)
					assert.NoError(t, err, "Join registers the node in a scope before its first commit")
				}
				next, stop := iter.Pull2(s.Membership().Watch(t.Context(), scope, incA))
				defer stop()
				assert.Equal(t, pull(t, next).Nodes, []shard.Node{{ID: node, Incarnation: 1}},
					"a view lists the nodes of the sessions of the incarnation of the watch", assert.EquateEmpty())
			})

			t.Run("yields each view to a watch that lags, in the order of their revisions", func(t *testing.T) {
				t.Parallel()
				s := open(t)
				next, stop := iter.Pull2(s.Membership().Watch(t.Context(), scope, incA))
				defer stop()
				first := pull(t, next)
				joinEach(t.Context(), t, s)
				a, b := pull(t, next), pull(t, next)
				assert.Equal(t, a.Revision, first.Revision+1, "the watch yields the view after the first join next")
				assert.Equal(t, a.Nodes, []shard.Node{{ID: node, Incarnation: 1}},
					"the view after the first join lists its node", assert.EquateEmpty())
				assert.Equal(t, b.Revision, first.Revision+2, "the watch yields the view after the second join last")
			})

			t.Run("yields the current view first while another watch lags", func(t *testing.T) {
				t.Parallel()
				s := open(t)
				lagging, stopLagging := iter.Pull2(s.Membership().Watch(t.Context(), scope, incA))
				defer stopLagging()
				pull(t, lagging)
				joinEach(t.Context(), t, s)
				next, stop := iter.Pull2(s.Membership().Watch(t.Context(), scope, incA))
				defer stop()
				assert.Length(t, pull(t, next).Nodes, 2, "a new watch yields the view after the last join first")
			})

			t.Run("releases a session whose views the watch has read", func(t *testing.T) {
				t.Parallel()
				clk := fake.New(origin)
				s := memory.New(clk)
				t.Cleanup(func() { assert.NoError(t, s.Close(), "Close returns nil") })
				next, stop := iter.Pull2(s.Membership().Watch(t.Context(), scope, incA))
				defer stop()
				pull(t, next)
				gone := leftSession(t, s, clk)
				pull(t, next)
				runtime.GC()
				assert.Nil(t, gone.Value(), "the store releases a session once each watch has read its views")
				runtime.KeepAlive(s)
			})

			t.Run("releases the views that it has not read once it ends", func(t *testing.T) {
				t.Parallel()
				clk := fake.New(origin)
				s := memory.New(clk)
				t.Cleanup(func() { assert.NoError(t, s.Close(), "Close returns nil") })
				next, stop := iter.Pull2(s.Membership().Watch(t.Context(), scope, incA))
				pull(t, next)
				gone := leftSession(t, s, clk)
				stop()
				runtime.GC()
				assert.Nil(t, gone.Value(), "the store releases a session once the watches that lag end")
				runtime.KeepAlive(s)
			})
		})
	})

	t.Run("Session", func(t *testing.T) {
		t.Parallel()

		t.Run("Leave", func(t *testing.T) {
			t.Parallel()

			t.Run("returns ErrClosed after Close", func(t *testing.T) {
				t.Parallel()
				s := open(t)
				n := shard.Node{ID: node}
				ss, err := s.Membership().Join(t.Context(), scope, incA, &n, lease)
				assert.NoError(t, err, "Join registers the node")
				assert.FailsAfterClose(t, s.Close, func() error { return ss.Leave(t.Context()) }, memory.ErrClosed,
					"Leave returns ErrClosed after Close")
			})

			t.Run("returns nil for a session that has ended", func(t *testing.T) {
				t.Parallel()
				s := open(t)
				n := shard.Node{ID: node}
				ss, err := s.Membership().Join(t.Context(), scope, incA, &n, lease)
				assert.NoError(t, err, "Join registers the node")
				assert.NoError(t, ss.Leave(t.Context()), "Leave returns nil")
				assert.NoError(t, ss.Leave(t.Context()), "Leave returns nil for a session that has ended")
			})

			t.Run("keeps the node of the session that replaced the session", func(t *testing.T) {
				t.Parallel()
				s := open(t)
				n := shard.Node{ID: node}
				before, err := s.Membership().Join(t.Context(), scope, incA, &n, lease)
				assert.NoError(t, err, "Join registers the node")
				_, err = s.Membership().Join(t.Context(), scope, incA, &n, lease)
				assert.NoError(t, err, "Join registers the node again")
				assert.NoError(t, before.Leave(t.Context()), "Leave returns nil for a session that has ended")
				next, stop := iter.Pull2(s.Membership().Watch(t.Context(), scope, incA))
				defer stop()
				assert.Equal(
					t,
					pull(t, next).Nodes,
					[]shard.Node{{ID: node, Incarnation: 2}},
					"the view lists the node of the later session after the earlier session leaves",
					assert.EquateEmpty(),
				)
			})

			t.Run("releases the session when no watch lags", func(t *testing.T) {
				t.Parallel()
				clk := fake.New(origin)
				s := memory.New(clk)
				t.Cleanup(func() { assert.NoError(t, s.Close(), "Close returns nil") })
				gone := leftSession(t, s, clk)
				runtime.GC()
				assert.Nil(t, gone.Value(), "the store releases a session that left")
				runtime.KeepAlive(s)
			})
		})
	})
}

// pull returns the next view of a watch that iter.Pull2 turned into next. It fails t when
// the watch yields an error or ends.
func pull(t *testing.T, next func() (membership.View, error, bool)) membership.View {
	t.Helper()
	v, err, ok := next()
	assert.True(t, ok, "Watch yields a view before it ends")
	assert.NoError(t, err, "Watch yields the views of the scope")
	return v
}

// joinEach joins the node and then the other node to the scope of s in the incarnation A,
// each with the context ctx.
func joinEach(ctx context.Context, t *testing.T, s *memory.Store) {
	t.Helper()
	for _, id := range [...]shard.NodeID{node, otherNode} {
		n := shard.Node{ID: id}
		_, err := s.Membership().Join(ctx, scope, incA, &n, lease)
		assert.NoError(t, err, "Join registers the node")
	}
}

// leftSession joins the node to the scope of s with a context of its own, and leaves the
// session. It advances clk past the expiry of the lease, and then the store has applied
// each event of the session. It returns a weak pointer to the context of the join.
func leftSession(t *testing.T, s *memory.Store, clk *fake.Clock) weak.Pointer[tracked] {
	t.Helper()
	ctx := &tracked{Context: t.Context()}
	n := shard.Node{ID: node}
	ss, err := s.Membership().Join(ctx, scope, incA, &n, lease)
	assert.NoError(t, err, "Join registers the node")
	assert.NoError(t, ss.Leave(t.Context()), "Leave returns nil")
	clk.Advance(time.Duration(lease.TTL))
	_, err = s.Head(t.Context(), scope)
	assert.NoError(t, err, "Head returns the header of the scope")
	return weak.Make(ctx)
}

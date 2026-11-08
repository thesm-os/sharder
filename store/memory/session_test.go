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
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/membership/membershiptest"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// renewalPoints maps each loss of the suite of membership.Membership to the point that
// loses a renewal there.
var renewalPoints = map[membershiptest.Loss]memory.Point{
	membershiptest.LoseRequest: memory.PointRenewApply,
	membershiptest.LoseReply:   memory.PointRenewAck,
}

// backedMembership is the Membership of a Store, with the store and the fake clock of its
// leases. It loses the renewals of the losses of the suite of membership.Membership.
type backedMembership struct {
	*memory.Membership
	// s is the store of the membership.
	s *memory.Store
	// clk is the clock of the leases of s.
	clk *fake.Clock
}

// newBackedMembership returns the Membership of an empty Store whose leases run on a fake
// clock, the subject of the suite of membership.Membership, and closes the store when the
// check of h ends.
func newBackedMembership(h *membershiptest.MembershipHandle) backedMembership {
	clk := fake.New(origin)
	s := memory.New(clk)
	h.Cleanup(func() { assert.NoError(h, s.Close(), "Close returns nil") })
	return backedMembership{Membership: s.Membership(), s: s, clk: clk}
}

// Store returns the store of the membership.
func (m backedMembership) Store() store.Store {
	return m.s
}

// Advance moves the clock of the leases forward by d.
func (m backedMembership) Advance(d time.Duration) {
	m.clk.Advance(d)
}

// LoseRenewals arms the fault of the store at the point of l.
func (m backedMembership) LoseRenewals(l membershiptest.Loss) {
	m.s.FailAt(renewalPoints[l])
}

// TestSession checks the capabilities, the joins, the renewals, the watches, the readers of
// the views and the sessions of a Membership.
func TestSession(t *testing.T) {
	t.Parallel()

	t.Run("Membership", func(t *testing.T) {
		t.Parallel()

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

			t.Run("records one view once a commit of another incarnation ends the sessions", func(t *testing.T) {
				t.Parallel()
				s := manual(t, fake.New(origin))
				for _, id := range [...]shard.NodeID{node, otherNode} {
					n := shard.Node{ID: id}
					_, err := s.Membership().Join(t.Context(), scope, incB, &n, lease)
					assert.NoError(t, err, "Join registers the node in a scope before its first commit")
				}
				r := s.Membership().Views(scope, incA)
				t.Cleanup(r.Close)
				_, _, err := r.Next()
				assert.NoError(t, err, "Next returns the views of the scope")
				commit(t, s, incA, &store.Mutation{})
				views, _, err := r.Next()
				assert.NoError(t, err, "Next returns the views after the commit")
				assert.Length(t, views, 1, "a commit that creates the scope ends the other sessions in one view")
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

		t.Run("Renew", func(t *testing.T) {
			t.Parallel()

			t.Run("moves the expiry of the lease", func(t *testing.T) {
				t.Parallel()
				clk := fake.New(origin)
				s := manual(t, clk)
				ss := joinNode(t, s)
				r := s.Membership().Views(scope, incA)
				t.Cleanup(r.Close)
				clk.Advance(time.Duration(lease.Renew))
				assert.NoError(t, s.Membership().Renew(ss), "Renew renews the lease")
				clk.Advance(time.Duration(lease.TTL - lease.Renew))
				views, _, err := r.Next()
				assert.NoError(t, err, "Next returns the views of the scope")
				assert.NotEmpty(t, views, "Next returns the views since the creation of the reader")
				assert.Equal(t, views[len(views)-1].Nodes, []shard.Node{{ID: node, Incarnation: 1}},
					"the view lists the node at the TTL after its join after a renewal", assert.EquateEmpty())
			})

			t.Run("returns ErrClosed after Close", func(t *testing.T) {
				t.Parallel()
				s := manual(t, fake.New(origin))
				ss := joinNode(t, s)
				assert.FailsAfterClose(t, s.Close, func() error { return s.Membership().Renew(ss) }, memory.ErrClosed,
					"Renew returns ErrClosed after Close")
			})

			tests := []struct {
				name  string
				point memory.Point
			}{
				{name: "returns ErrFault at a fault of PointRenewApply", point: memory.PointRenewApply},
				{name: "returns ErrFault at a fault of PointRenewAck", point: memory.PointRenewAck},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					s := manual(t, fake.New(origin))
					ss := joinNode(t, s)
					s.FailAt(tt.point)
					assert.ErrorIs(t, s.Membership().Renew(ss), memory.ErrFault,
						"Renew returns ErrFault at a fault of its point")
				})
			}

			t.Run("moves the expiry of the lease at a fault of PointRenewAck", func(t *testing.T) {
				t.Parallel()
				clk := fake.New(origin)
				s := manual(t, clk)
				ss := joinNode(t, s)
				r := s.Membership().Views(scope, incA)
				t.Cleanup(r.Close)
				clk.Advance(time.Duration(lease.Renew))
				s.FailAt(memory.PointRenewAck)
				assert.ErrorIs(t, s.Membership().Renew(ss), memory.ErrFault, "Renew returns ErrFault at the fault")
				clk.Advance(time.Duration(lease.TTL - lease.Renew))
				views, _, err := r.Next()
				assert.NoError(t, err, "Next returns the views of the scope")
				assert.NotEmpty(t, views, "Next returns the views since the creation of the reader")
				assert.Equal(t, views[len(views)-1].Nodes, []shard.Node{{ID: node, Incarnation: 1}},
					"the view lists the node at the TTL after its join after a renewal whose reply is lost",
					assert.EquateEmpty())
			})

			t.Run("keeps the expiry of the lease at a fault of PointRenewApply", func(t *testing.T) {
				t.Parallel()
				clk := fake.New(origin)
				s := manual(t, clk)
				ss := joinNode(t, s)
				r := s.Membership().Views(scope, incA)
				t.Cleanup(r.Close)
				clk.Advance(time.Duration(lease.Renew))
				s.FailAt(memory.PointRenewApply)
				assert.ErrorIs(t, s.Membership().Renew(ss), memory.ErrFault, "Renew returns ErrFault at the fault")
				clk.Advance(time.Duration(lease.TTL - lease.Renew))
				views, _, err := r.Next()
				assert.NoError(t, err, "Next returns the views of the scope")
				assert.NotEmpty(t, views, "Next returns the views since the creation of the reader")
				assert.Empty(t, views[len(views)-1].Nodes,
					"the view lists no node at the TTL after the join after a renewal that the store lost")
			})

			t.Run("returns nil for a session whose Done is closed", func(t *testing.T) {
				t.Parallel()
				s := manual(t, fake.New(origin))
				ss := joinNode(t, s)
				assert.NoError(t, ss.Leave(t.Context()), "Leave returns nil")
				assert.NoError(t, s.Membership().Renew(ss), "Renew returns nil for a session that has ended")
			})

			t.Run("keeps the expiry of a session whose Done is closed", func(t *testing.T) {
				t.Parallel()
				clk := fake.New(origin)
				s := manual(t, clk)
				ss := joinNode(t, s)
				r := s.Membership().Views(scope, incA)
				t.Cleanup(r.Close)
				assert.NoError(t, s.Membership().Lose(ss), "Lose ends the session at its node")
				streamtest.Receive(t, ss.Done(), waitFor, "Done is closed once Lose ends the session")
				clk.Advance(time.Duration(lease.Renew))
				assert.NoError(t, s.Membership().Renew(ss), "Renew returns nil for a session that has ended")
				clk.Advance(time.Duration(lease.TTL - lease.Renew))
				views, _, err := r.Next()
				assert.NoError(t, err, "Next returns the views of the scope")
				assert.NotEmpty(t, views, "Next returns the views since the reader read last")
				assert.Empty(t, views[len(views)-1].Nodes,
					"the view lists no node at the TTL after the join of a session that Renew did not renew")
			})

			t.Run("keeps the expiry of a session whose join context has ended", func(t *testing.T) {
				t.Parallel()
				clk := fake.New(origin)
				s := manual(t, clk)
				ended, cancel := context.WithCancel(t.Context())
				cancel()
				n := shard.Node{ID: node}
				ss, err := s.Membership().Join(ended, scope, incA, &n, lease)
				assert.NoError(t, err, "Join registers the node")
				r := s.Membership().Views(scope, incA)
				t.Cleanup(r.Close)
				clk.Advance(time.Duration(lease.Renew))
				assert.NoError(t, s.Membership().Renew(ss),
					"Renew returns nil for a session whose join context has ended")
				clk.Advance(time.Duration(lease.TTL - lease.Renew))
				views, _, err := r.Next()
				assert.NoError(t, err, "Next returns the views of the scope")
				assert.NotEmpty(t, views, "Next returns the views since the creation of the reader")
				assert.Empty(t, views[len(views)-1].Nodes,
					"the view lists no node at the TTL after a join whose context ended before the renewal")
			})

			t.Run("returns an error of class errs.Invalid for a session of another store", func(t *testing.T) {
				t.Parallel()
				ss := joinNode(t, manual(t, fake.New(origin)))
				err := manual(t, fake.New(origin)).Membership().Renew(ss)
				assert.Equal(t, errs.Classify(err), errs.Invalid,
					"Renew refuses a session of another store as an invalid request")
			})

			t.Run("returns an error of class errs.Invalid for a session that no store created", func(t *testing.T) {
				t.Parallel()
				err := manual(t, fake.New(origin)).Membership().Renew(membershiptest.NewSessionStub(t))
				assert.Equal(t, errs.Classify(err), errs.Invalid,
					"Renew refuses a session that no store created as an invalid request")
			})
		})

		t.Run("Lose", func(t *testing.T) {
			t.Parallel()

			t.Run("closes Done of the session", func(t *testing.T) {
				t.Parallel()
				s := manual(t, fake.New(origin))
				ss := joinNode(t, s)
				assert.NoError(t, s.Membership().Lose(ss), "Lose ends the session at its node")
				streamtest.Receive(t, ss.Done(), waitFor, "Done is closed once Lose ends the session")
			})

			t.Run("keeps the node in the view until the expiry of the lease", func(t *testing.T) {
				t.Parallel()
				clk := fake.New(origin)
				s := manual(t, clk)
				ss := joinNode(t, s)
				r := s.Membership().Views(scope, incA)
				t.Cleanup(r.Close)
				assert.NoError(t, s.Membership().Lose(ss), "Lose ends the session at its node")
				views, _, err := r.Next()
				assert.NoError(t, err, "Next returns the views of the scope")
				assert.NotEmpty(t, views, "Next returns the views since the creation of the reader")
				assert.Equal(t, views[len(views)-1].Nodes, []shard.Node{{ID: node, Incarnation: 1}},
					"the view lists the node of a lost session before the expiry of its lease", assert.EquateEmpty())
				clk.Advance(time.Duration(lease.TTL))
				views, _, err = r.Next()
				assert.NoError(t, err, "Next returns the views of the scope")
				assert.NotEmpty(t, views, "Next returns the view after the expiry")
				assert.Empty(t, views[len(views)-1].Nodes, "the view lists no node after the expiry of the lease")
			})

			t.Run("returns an error of class errs.Invalid for a session of another store", func(t *testing.T) {
				t.Parallel()
				ss := joinNode(t, manual(t, fake.New(origin)))
				err := manual(t, fake.New(origin)).Membership().Lose(ss)
				assert.Equal(t, errs.Classify(err), errs.Invalid,
					"Lose refuses a session of another store as an invalid request")
			})

			t.Run("returns an error of class errs.Invalid for a session that no store created", func(t *testing.T) {
				t.Parallel()
				err := manual(t, fake.New(origin)).Membership().Lose(membershiptest.NewSessionStub(t))
				assert.Equal(t, errs.Classify(err), errs.Invalid,
					"Lose refuses a session that no store created as an invalid request")
			})
		})

		t.Run("Views", func(t *testing.T) {
			t.Parallel()

			t.Run("returns a reader that keeps every view from its creation", func(t *testing.T) {
				t.Parallel()
				s := manual(t, fake.New(origin))
				r := s.Membership().Views(scope, incA)
				t.Cleanup(r.Close)
				joinEach(t.Context(), t, s)
				views, _, err := r.Next()
				assert.NoError(t, err, "Next returns the views of the scope")
				assert.Length(t, views, 3, "the reader returns the view at its creation and the view after each join")
			})
		})
	})

	t.Run("ViewReader", func(t *testing.T) {
		t.Parallel()

		t.Run("Next", func(t *testing.T) {
			t.Parallel()

			t.Run("returns ErrClosed after Close of the store", func(t *testing.T) {
				t.Parallel()
				s := manual(t, fake.New(origin))
				r := s.Membership().Views(scope, incA)
				t.Cleanup(r.Close)
				assert.FailsAfterClose(t, s.Close, func() error {
					_, _, err := r.Next()
					return err
				}, memory.ErrClosed, "Next returns ErrClosed after Close of the store")
			})

			t.Run("returns ErrClosed after Close of the reader", func(t *testing.T) {
				t.Parallel()
				r := manual(t, fake.New(origin)).Membership().Views(scope, incA)
				assert.FailsAfterClose(t, func() error {
					r.Close()
					return nil
				}, func() error {
					_, _, err := r.Next()
					return err
				}, memory.ErrClosed, "Next returns ErrClosed after Close of the reader")
			})

			t.Run("returns ErrIncarnation for a scope that a commit of another incarnation created",
				func(t *testing.T) {
					t.Parallel()
					s := manual(t, fake.New(origin))
					commit(t, s, incA, &store.Mutation{})
					r := s.Membership().Views(scope, incB)
					t.Cleanup(r.Close)
					_, _, err := r.Next()
					assert.ErrorIs(t, err, shard.ErrIncarnation,
						"Next returns ErrIncarnation for a scope that a commit of another incarnation created")
				})

			t.Run("returns the views that the reader has not read", func(t *testing.T) {
				t.Parallel()
				s := manual(t, fake.New(origin))
				r := s.Membership().Views(scope, incA)
				t.Cleanup(r.Close)
				first, _, err := r.Next()
				assert.NoError(t, err, "Next returns the views of the scope")
				assert.Length(t, first, 1, "the first Next returns the view at the creation of the reader")
				joinNode(t, s)
				views, _, err := r.Next()
				assert.NoError(t, err, "Next returns the views of the scope")
				assert.Length(t, views, 1, "Next returns the one view that the reader has not read")
				assert.Equal(t, views[0].Revision, first[0].Revision+1, "Next returns the view after the join")
			})

			t.Run("returns the channel that the next change of the scope closes", func(t *testing.T) {
				t.Parallel()
				s := manual(t, fake.New(origin))
				r := s.Membership().Views(scope, incA)
				t.Cleanup(r.Close)
				_, changed, err := r.Next()
				assert.NoError(t, err, "Next returns the views of the scope")
				streamtest.Blocks(t, changed, "the channel is open while the scope does not change")
				joinNode(t, s)
				streamtest.Receive(t, changed, waitFor, "the channel is closed once a join changes the scope")
			})
		})

		t.Run("Close", func(t *testing.T) {
			t.Parallel()

			t.Run("releases the views that the reader has not read", func(t *testing.T) {
				t.Parallel()
				clk := fake.New(origin)
				s := manual(t, clk)
				r := s.Membership().Views(scope, incA)
				_, _, err := r.Next()
				assert.NoError(t, err, "Next returns the views of the scope")
				gone := leftSession(t, s, clk)
				r.Close()
				runtime.GC()
				assert.Nil(t, gone.Value(), "the store releases a session once the reader that lags closes")
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

// joinNode joins the node to the scope of s in the incarnation A with the lease of the
// tests, and returns its session. It fails t when the join fails.
func joinNode(t *testing.T, s *memory.Store) membership.Session {
	t.Helper()
	n := shard.Node{ID: node}
	ss, err := s.Membership().Join(t.Context(), scope, incA, &n, lease)
	assert.NoError(t, err, "Join registers the node")
	return ss
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

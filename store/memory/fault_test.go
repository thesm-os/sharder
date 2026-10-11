// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package memory_test

import (
	"context"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/version"

	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// waitFor bounds each wait of the tests for an item, an event or a closed channel that the
// store delivers at once.
const waitFor = 10 * time.Second

// otherPoint is a point that is not one of Points.
const otherPoint memory.Point = "elsewhere"

// lease is the lease of the sessions of the tests. It renews every 3 seconds, with a
// validity of 5 seconds and a TTL of 10 seconds.
var lease = ownership.Lease{
	TTL:          shard.Duration(10 * time.Second),
	Renew:        shard.Duration(3 * time.Second),
	MaxDriftRate: 0.5,
	OnLoss:       ownership.Stop,
}

// TestFault checks the fault points, the outages and the compaction of a Store.
func TestFault(t *testing.T) {
	t.Parallel()

	t.Run("Points", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the four points", func(t *testing.T) {
			t.Parallel()
			assert.Equal(
				t,
				open(t).Points(),
				[]memory.Point{
					memory.PointCommitApply,
					memory.PointCommitAck,
					memory.PointRenewApply,
					memory.PointRenewAck,
				},
				"Points returns each point at which FailAt interrupts a call",
			)
		})
	})

	t.Run("FailAt", func(t *testing.T) {
		t.Parallel()

		t.Run("fails no commit for a point that is not one of Points", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			s.FailAt(otherPoint)
			assert.Equal(t, commit(t, s, incA, &store.Mutation{}), 1, "a commit applies after FailAt of another point")
		})
	})

	t.Run("StartOutage", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			call func(ctx context.Context, s *memory.Store) error
		}{
			{name: "makes Head return ErrOutage", call: func(ctx context.Context, s *memory.Store) error {
				_, err := s.Head(ctx, scope)
				return err
			}},
			{name: "makes Read return ErrOutage", call: func(ctx context.Context, s *memory.Store) error {
				_, _, err := s.Read(ctx, scope, incA, store.Query{}, page.Page{})
				return err
			}},
			{name: "makes Outcome return ErrOutage", call: func(ctx context.Context, s *memory.Store) error {
				_, _, err := s.Outcome(ctx, scope, store.CommitID{Incarnation: incA, Term: term, Seq: 1})
				return err
			}},
			{name: "makes Campaign return ErrOutage", call: func(ctx context.Context, s *memory.Store) error {
				_, err := s.Campaign(ctx, scope)
				return err
			}},
			{name: "makes Join return ErrOutage", call: func(ctx context.Context, s *memory.Store) error {
				n := shard.Node{ID: node}
				_, err := s.Membership().Join(ctx, scope, incA, &n, lease)
				return err
			}},
			{name: "makes Watch yield ErrOutage", call: func(ctx context.Context, s *memory.Store) error {
				for _, err := range s.Watch(ctx, scope, incA, 0, "") {
					return err
				}
				return nil
			}},
			{
				name: "makes Watch of the membership yield ErrOutage",
				call: func(ctx context.Context, s *memory.Store) error {
					for _, err := range s.Membership().Watch(ctx, scope, incA) {
						return err
					}
					return nil
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				s := open(t)
				s.StartOutage()
				assert.ErrorIs(
					t,
					tt.call(t.Context(), s),
					memory.ErrOutage,
					"a call during an outage returns ErrOutage",
				)
			})
		}

		t.Run("makes Commit return ErrOutcomeUnknown", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			s.StartOutage()
			m := store.Mutation{ID: store.CommitID{Incarnation: incA, Term: term, Seq: 1}}
			_, err := s.Writer(scope, incA, term).Commit(t.Context(), 0, &m)
			expect.That(t, err).ErrorIs(version.ErrOutcomeUnknown, "a commit during an outage has an unknown outcome").
				ErrorIs(memory.ErrOutage, "a commit during an outage returns ErrOutage")
		})

		t.Run("ends a watch with ErrOutage", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			commit(t, s, incA, &store.Mutation{})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			items := streamtest.Follow(ctx, s.Watch(ctx, scope, incA, 0, ""))
			_, ok := streamtest.Receive(t, items, waitFor, "Watch yields the generation of the scope at once")
			assert.True(t, ok, "Watch yields the generation of the scope")
			s.StartOutage()
			it, ok := streamtest.Receive(t, items, waitFor, "Watch yields an error at once when the outage starts")
			assert.True(t, ok, "Watch yields an error before it ends")
			assert.ErrorIs(t, it.Err, memory.ErrOutage, "a watch ends with ErrOutage once an outage starts")
		})

		t.Run("makes Leave return ErrOutage", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			n := shard.Node{ID: node}
			ss, err := s.Membership().Join(t.Context(), scope, incA, &n, lease)
			assert.NoError(t, err, "Join registers the node")
			s.StartOutage()
			assert.ErrorIs(t, ss.Leave(t.Context()), memory.ErrOutage, "Leave returns ErrOutage during an outage")
		})

		t.Run("makes Resign return ErrOutage", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			l, err := s.Campaign(t.Context(), scope)
			assert.NoError(t, err, "Campaign grants a term")
			s.StartOutage()
			assert.ErrorIs(t, l.Resign(t.Context()), memory.ErrOutage, "Resign returns ErrOutage during an outage")
		})

		t.Run("fails the renewals of a session", func(t *testing.T) {
			t.Parallel()
			clk := fake.New(origin)
			s := memory.New(clk)
			t.Cleanup(func() { assert.NoError(t, s.Close(), "Close returns nil") })
			n := shard.Node{ID: node}
			ss, err := s.Membership().Join(t.Context(), scope, incA, &n, lease)
			assert.NoError(t, err, "Join registers the node")
			s.StartOutage()
			clk.Advance(lease.Validity())
			streamtest.Receive(t, ss.Done(), waitFor,
				"Done is closed at the validity of the lease after the join when no renewal applies")
		})
	})

	t.Run("EndOutage", func(t *testing.T) {
		t.Parallel()

		t.Run("lets the calls after it succeed", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			s.StartOutage()
			s.EndOutage()
			_, err := s.Head(t.Context(), scope)
			assert.NoError(t, err, "Head returns the header of the scope after the outage")
		})
	})

	t.Run("Compact", func(t *testing.T) {
		t.Parallel()

		t.Run("keeps a scope before its first commit", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			_, err := s.Head(t.Context(), scope)
			assert.NoError(t, err, "Head returns the header of a scope before its first commit")
			assert.NotPanics(t, s.Compact, "Compact keeps a scope before its first commit")
		})
	})
}

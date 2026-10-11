// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package memory_test

import (
	"context"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/version"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// otherCell is the cell of a scope without commits.
const otherCell shard.CellID = "cell-2"

// protocol is the handoff protocol of the transitions of the tests.
const protocol shard.StrategyName = "memory_test"

// The identifiers of the proposals and the disruptions of the tests.
var (
	proposalID   = proposal.ID{ID: id.New128([16]byte{0: tagProposal, 15: 0x01})}
	disruptionID = budget.ID{ID: id.New128([16]byte{0: tagDisruption, 15: 0x01})}
)

// everyPart is a mutation that changes each part of a state that a later commit can share.
// Those parts are the applied proposals, the transitions, the reservations and the drains.
var everyPart = store.Mutation{
	Applied: []store.Applied{{Caller: caller, Proposal: proposal.Proposal{
		Scope:       scope,
		Expires:     origin.Add(time.Hour),
		Body:        []byte(`{}`),
		Base:        1,
		Incarnation: incA,
		ID:          proposalID,
		Kind:        proposal.Move,
	}}},
	Transitions: []store.Transition{{
		Started:         origin,
		Protocol:        protocol,
		Operation:       shard.OperationID{Incarnation: incA, Shard: placements[0].Shard, Sequence: 1},
		NewEpoch:        1,
		ProtocolVersion: 1,
		Phase:           shard.PhaseRunning,
	}},
	Reserve: []budget.Reservation{{
		Owner: caller,
		Disruption: budget.Disruption{
			Start:       origin,
			End:         origin.Add(time.Hour),
			Scope:       scope,
			Nodes:       []shard.NodeID{node},
			ID:          disruptionID,
			Incarnation: incA,
			Impact:      budget.ProcessRestart,
		},
	}},
	Drain: []store.Drain{{Since: origin, Node: node, Caller: caller}},
}

// TestScope checks the calls of a Store on one scope after Close, with an ended context, at
// the end of a watch and after Delete, and the parts of the state that generations share.
func TestScope(t *testing.T) {
	t.Parallel()

	t.Run("Writer", func(t *testing.T) {
		t.Parallel()

		t.Run("Commit", func(t *testing.T) {
			t.Parallel()

			t.Run("returns ErrOutcomeUnknown for an ended context", func(t *testing.T) {
				t.Parallel()
				s := open(t)
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				m := store.Mutation{ID: store.CommitID{Incarnation: incA, Term: term, Seq: 1}}
				_, err := s.Writer(scope, incA, term).Commit(ctx, 0, &m)
				expect.That(t, err).
					ErrorIs(version.ErrOutcomeUnknown, "a commit with an ended context has an unknown outcome").
					ErrorIs(context.Canceled, "a commit with an ended context returns the error of the context")
			})

			t.Run("returns ErrClosed after Close", func(t *testing.T) {
				t.Parallel()
				s := open(t)
				m := store.Mutation{ID: store.CommitID{Incarnation: incA, Term: term, Seq: 1}}
				assert.FailsAfterClose(t, s.Close, func() error {
					_, err := s.Writer(scope, incA, term).Commit(t.Context(), 0, &m)
					return err
				}, memory.ErrClosed, "Commit returns ErrClosed after Close")
			})

			t.Run("shares the parts of the state that it does not change with the generation before it",
				func(t *testing.T) {
					t.Parallel()
					s := open(t)
					m := everyPart
					commit(t, s, incA, &m)
					before := state(t, s)
					commit(t, s, incA, &store.Mutation{Spec: []byte(`{}`)})
					after := state(t, s)
					expect.Equal(t, after.Applied, before.Applied,
						"a commit without proposals shares the applied proposals of the state before it",
						assert.ByIdentity())
					expect.Equal(
						t,
						after.Transitions,
						before.Transitions,
						"a commit without transitions shares the transitions of the state before it",
						assert.ByIdentity(),
					)
					expect.Equal(t, after.Reservations, before.Reservations,
						"a commit without reservations shares the reservations of the state before it",
						assert.ByIdentity())
					expect.Equal(t, after.Drains, before.Drains,
						"a commit without drains shares the drains of the state before it", assert.ByIdentity())
				})
		})
	})

	t.Run("Head", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrClosed after Close", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			assert.FailsAfterClose(t, s.Close, func() error {
				_, err := s.Head(t.Context(), scope)
				return err
			}, memory.ErrClosed, "Head returns ErrClosed after Close")
		})
	})

	t.Run("Read", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrClosed after Close", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			assert.FailsAfterClose(t, s.Close, func() error {
				_, _, err := s.Read(t.Context(), scope, incA, store.Query{}, page.Page{})
				return err
			}, memory.ErrClosed, "Read returns ErrClosed after Close")
		})

		t.Run("returns ErrCompacted for a page token of a generation after the current one", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			commit(t, s, incA, &store.Mutation{Put: placements})
			commit(t, s, incA, &store.Mutation{})
			_, cur, err := s.Read(t.Context(), scope, incA, store.Query{}, page.Page{Limit: 1})
			assert.NoError(t, err, "Read returns the first page of the scope")
			token := cur.NextPage()
			s.Delete(scope)
			commit(t, s, incA, &store.Mutation{})
			_, _, err = s.Read(t.Context(), scope, incA, store.Query{}, page.Page{Token: token, Limit: 1})
			assert.ErrorIs(t, err, store.ErrCompacted,
				"Read returns ErrCompacted for a page token of a generation that the scope does not have")
		})
	})

	t.Run("Outcome", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrClosed after Close", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			assert.FailsAfterClose(t, s.Close, func() error {
				_, _, err := s.Outcome(t.Context(), scope, store.CommitID{Incarnation: incA, Term: term, Seq: 1})
				return err
			}, memory.ErrClosed, "Outcome returns ErrClosed after Close")
		})
	})

	t.Run("Watch", func(t *testing.T) {
		t.Parallel()

		t.Run("yields ErrClosed after Close", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			assert.FailsAfterClose(t, s.Close, func() error {
				for _, err := range s.Watch(t.Context(), scope, incA, 0, "") {
					return err
				}
				return nil
			}, memory.ErrClosed, "Watch yields ErrClosed after Close")
		})

		t.Run("returns when the caller stops ranging before the last kept generation", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			commit(t, s, incA, &store.Mutation{})
			commit(t, s, incA, &store.Mutation{})
			var got []epoch.Epoch
			for e, err := range s.Watch(t.Context(), scope, incA, 0, "") {
				assert.NoError(t, err, "Watch yields the generations of the scope")
				got = append(got, e.Generation)
				break
			}
			assert.Equal(
				t,
				got,
				[]epoch.Epoch{1},
				"Watch yields the first generation, and returns when the caller stops",
			)
		})

		t.Run("returns without an event for an ended context and a scope before its first commit",
			func(t *testing.T) {
				t.Parallel()
				s := open(t)
				ended, cancel := context.WithCancel(t.Context())
				cancel()
				var got []store.Event
				for e, err := range s.Watch(ended, scope, incA, 0, "") {
					assert.NoError(t, err, "Watch yields the generations of the scope")
					got = append(got, e)
				}
				assert.Empty(t, got, "Watch does not yield an event for a scope before its first commit")
			})
	})

	t.Run("Delete", func(t *testing.T) {
		t.Parallel()

		t.Run("leaves every scope for a scope that no call named", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			commit(t, s, incA, &store.Mutation{})
			s.Delete(shard.Scope{Namespace: namespace, Cell: otherCell})
			h, err := s.Head(t.Context(), scope)
			assert.NoError(t, err, "Head returns the header of the scope")
			assert.Equal(t, h.Generation, 1, "a Delete of another scope leaves the scope")
		})

		t.Run("ends the term of the scope", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			l, err := s.Campaign(t.Context(), scope)
			assert.NoError(t, err, "Campaign grants a term")
			s.Delete(scope)
			streamtest.Receive(t, l.Done(), waitFor, "Done is closed once Delete removes the scope")
		})

		t.Run("ends the sessions of the scope", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			n := shard.Node{ID: node}
			ss, err := s.Membership().Join(t.Context(), scope, incA, &n, lease)
			assert.NoError(t, err, "Join registers the node")
			s.Delete(scope)
			streamtest.Receive(t, ss.Done(), waitFor, "Done is closed once Delete removes the scope")
		})

		t.Run("discards the generations of the scope", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			commit(t, s, incA, &store.Mutation{Put: placements[:1]})
			commit(t, s, incA, &store.Mutation{})
			s.Delete(scope)
			commit(t, s, incA, &store.Mutation{Put: placements[1:]})
			_, cur, err := s.Read(t.Context(), scope, incA, store.Query{}, page.Page{})
			assert.NoError(t, err, "Read returns the first page of the scope")
			var got []shard.Placement
			for p, err := range cur.Seq(t.Context()) {
				assert.NoError(t, err, "the cursor yields the placements of its page")
				got = append(got, p)
			}
			assert.Equal(t, got, placements[1:], "Read returns the placements of the scope that a commit created again")
		})
	})
}

// state returns the state of the scope of s that Read returns.
func state(t *testing.T, s *memory.Store) store.State {
	t.Helper()
	st, cur, err := s.Read(t.Context(), scope, incA, store.Query{}, page.Page{})
	assert.NoError(t, err, "Read returns the state of the scope")
	assert.NoError(t, cur.Close(), "Close of a cursor returns nil")
	return st
}

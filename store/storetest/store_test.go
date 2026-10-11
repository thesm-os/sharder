// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package storetest_test

import (
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/clock/fake"

	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
	"go.thesmos.sh/sharder/store/storetest"
)

// limits are the limits of the stores of the test. The law of Fits generates mutations on
// both sides of them, and every other law commits mutations below them.
var limits = store.Limits{MaxPlacements: 8, MaxBytes: 4096}

// origin is the start of the clock of each store of the test.
var origin = time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC)

// points maps each loss of the suite to the point of the memory store that loses a commit
// there.
var points = map[storetest.Loss]memory.Point{
	storetest.LoseRequest: memory.PointCommitApply,
	storetest.LoseReply:   memory.PointCommitAck,
}

// TestStore runs the suite against the memory store with every option, and checks Valid
// and String of every Loss.
func TestStore(t *testing.T) {
	t.Parallel()

	t.Run("AssertStore", func(t *testing.T) {
		t.Parallel()

		// The memory store keeps nothing beyond its process, so the reopen returns the same
		// store. It runs the law of reopens without a second store.
		t.Run("passes the memory store with every option", func(t *testing.T) {
			t.Parallel()
			storetest.AssertStore(t,
				func(assert.TB) store.Store { return memory.New(fake.New(origin), memory.WithLimits(limits)) },
				storetest.WithReopen(func(_ assert.TB, s store.Store) store.Store { return s }),
				storetest.WithLostCommits(func(tb assert.TB, s store.Store, l storetest.Loss) {
					memoryOf(tb, s).FailAt(points[l])
				}),
				storetest.WithDelete(func(tb assert.TB, s store.Store, sc shard.Scope) { memoryOf(tb, s).Delete(sc) }),
				storetest.WithCompact(func(tb assert.TB, s store.Store) { memoryOf(tb, s).Compact() }),
			)
		})
	})

	t.Run("Loss", func(t *testing.T) {
		t.Parallel()

		t.Run("Valid", func(t *testing.T) {
			t.Parallel()

			t.Run("reports true for LoseRequest and LoseReply", func(t *testing.T) {
				t.Parallel()
				prop.True(t, storetest.Loss.Valid, "every constant of Loss must be Valid",
					prop.Using(prop.SampledFrom(storetest.LoseRequest, storetest.LoseReply)))
			})

			t.Run("reports false for the zero Loss", func(t *testing.T) {
				t.Parallel()
				assert.False(t, storetest.Loss(0).Valid(), "the zero Loss must not be Valid")
			})

			t.Run("reports false for a value above LoseReply", func(t *testing.T) {
				t.Parallel()
				prop.False(t, storetest.Loss.Valid, "a value above the last constant must not be Valid",
					prop.Using(prop.Integer[storetest.Loss](storetest.LoseReply+1, 255)))
			})
		})

		t.Run("String", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name string
				give storetest.Loss
				want string
			}{
				{name: "returns request for LoseRequest", give: storetest.LoseRequest, want: "request"},
				{name: "returns reply for LoseReply", give: storetest.LoseReply, want: "reply"},
				{name: "returns the number of the zero Loss", give: 0, want: "Loss(0)"},
				{name: "returns the number of a value above LoseReply", give: storetest.LoseReply + 1, want: "Loss(3)"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					assert.Equal(t, tt.give.String(), tt.want, "String must return the spelling of the loss")
				})
			}
		})
	})
}

// memoryOf returns the memory store s, which the suite passes to an option, and fails tb
// when s is another store.
func memoryOf(tb assert.TB, s store.Store) *memory.Store {
	tb.Helper()
	m, ok := s.(*memory.Store)
	assert.True(tb, ok, "the suite passes the store that newStore returned")
	return m
}

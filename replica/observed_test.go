// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica_test

import (
	"context"
	"testing"
	"testing/synctest"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// TestObserved checks the observation of its cell that an agent returns.
func TestObserved(t *testing.T) {
	t.Parallel()

	t.Run("Observed", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrNotServing before the agent serves", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a, err := replica.New(f.config(), f.app)
				assert.NoError(t, err, "New returns the agent")
				_, err = a.Observed(t.Context(), &replica.ObservedRequest{})
				assert.ErrorIs(t, err, replica.ErrNotServing, "an agent that does not serve must have no observation")
			})
		})

		t.Run("returns the cell and the namespace incarnation of the node", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				res, err := a.Observed(t.Context(), &replica.ObservedRequest{})
				assert.NoError(t, err, "a serving agent must have an observation")
				expect.Equal(t, res.Cell, cellID, "the observation must name the cell of the node")
				expect.Equal(t, res.Incarnation, incA, "the observation must name the namespace incarnation")
			})
		})

		t.Run("returns the last generation of the scope of the cell that the agent has applied", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				f.commitCell(t, &store.Mutation{Put: []shard.Placement{placement(shardB, 1, otherNode)}})
				synctest.Wait()
				head, err := f.st.Head(t.Context(), cellScope)
				assert.NoError(t, err, "the store returns the header of the scope of the cell")
				res, err := a.Observed(t.Context(), &replica.ObservedRequest{})
				assert.NoError(t, err, "a serving agent must have an observation")
				assert.Equal(t, res.Generation, head.Generation,
					"the observation must have the generation of the last commit")
			})
		})

		t.Run("returns ErrNotServing after the run of the agent has ended", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				ctx, cancel := context.WithCancel(t.Context())
				a := f.run(t, ctx, f.app)
				cancel()
				<-a.Done()
				_, err := a.Observed(t.Context(), &replica.ObservedRequest{})
				assert.ErrorIs(t, err, replica.ErrNotServing, "an agent whose run has ended must have no observation")
			})
		})
	})
}

// TestObservedAllocs checks that Observed does not allocate.
func TestObservedAllocs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := newFixture(t).start(t)
		ctx := t.Context()
		var res replica.ObservedResult
		var err error
		expect.MaxAllocs(t, func() {
			req := replica.ObservedRequest{}
			res, err = a.Observed(ctx, &req)
		}, 0, "Observed must not allocate")
		assert.NoError(t, err, "the test must measure the observation of a serving agent")
		assert.Equal(t, res.Cell, cellID, "the test must measure the observation of a serving agent")
	})
}

// BenchmarkObserved measures Observed in a loop and in parallel.
func BenchmarkObserved(b *testing.B) {
	f := newFixture(b)
	a := f.start(b)
	ctx := b.Context()

	b.Run("Observed", func(b *testing.B) {
		b.Run("serial", func(b *testing.B) {
			var res replica.ObservedResult
			var err error
			c := bench.Start(b).MaxAllocs(0)
			defer c.End()
			for c.Loop() {
				req := replica.ObservedRequest{}
				res, err = a.Observed(ctx, &req)
			}
			assert.NoError(b, err, "the benchmark must measure a serving agent")
			assert.Equal(b, res.Cell, cellID, "the benchmark must return the cell of the node")
		})

		b.Run("parallel", func(b *testing.B) {
			c := bench.Start(b).MaxAllocs(0)
			defer c.End()
			c.RunParallel(func(pb *bench.PB) {
				var res replica.ObservedResult
				var err error
				read := 0
				for pb.Next() {
					req := replica.ObservedRequest{}
					res, err = a.Observed(ctx, &req)
					read++
				}
				if read > 0 {
					expect.NoError(b, err, "the benchmark must measure a serving agent")
					expect.Equal(b, res.Cell, cellID, "the benchmark must return the cell of the node")
				}
			})
		})
	})
}

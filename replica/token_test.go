// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/history"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The operations of the history of the tokens of a shard.
const (
	// opAdd is an add of a tenure, whose argument is its epoch.
	opAdd = "add"
	// opDrop is a drop of the active tenure.
	opDrop = "drop"
	// opToken is a call of Token, whose output is the epoch of the token, or zero for an
	// error.
	opToken = "token"
)

// The size of the history of the tokens of a shard.
const (
	// tenures is the number of tenures that the callbacks of the history start and end.
	tenures = 3
	// readers is the number of clients that call Token while the callbacks run.
	readers = 2
	// pause is the time between two calls of Token of one reader.
	pause = time.Millisecond
	// tenancy is the time that client 0 keeps each tenure before its drop, so the readers
	// call Token during the tenure.
	tenancy = 5 * pause
)

// tokens is the sequential spec of the token of one shard. Its state is the epoch of the
// active tenure, and zero without one. An add makes its epoch the state, a drop makes the
// state zero, and Token returns the state.
var tokens = history.Spec[epoch.Epoch]{
	Initial: func() epoch.Epoch { return epoch.Zero },
	Next: func(s epoch.Epoch, op history.Operation) []epoch.Epoch {
		switch op.Name {
		case opAdd:
			e, _ := op.Args[0].(epoch.Epoch)
			return []epoch.Epoch{e}
		case opDrop:
			return []epoch.Epoch{epoch.Zero}
		}
		if op.Returned(s) {
			return []epoch.Epoch{s}
		}
		return nil
	},
}

// TestToken checks the fencing tokens that an agent returns.
func TestToken(t *testing.T) {
	t.Parallel()

	t.Run("Token", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the token of the tenure of an add", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				f.serve(t, a, shardA, 2)
				res, err := a.Token(t.Context(), &replica.TokenRequest{Shard: shardA})
				assert.NoError(t, err, "Token must return the token of the tenure")
				assert.Equal(t, res.Token,
					fence.Token{Namespace: namespace, Cell: cellID, Incarnation: incA, Shard: shardA, Epoch: 2},
					"the token must name the shard and the epoch of the tenure")
			})
		})

		t.Run("returns ErrNotWriter for a shard without a tenure on the node", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				a := newFixture(t).start(t)
				_, err := a.Token(t.Context(), &replica.TokenRequest{Shard: shardA})
				assert.ErrorIs(t, err, replica.ErrNotWriter, "Token must refuse a shard without a tenure")
			})
		})

		t.Run("returns an error that wraps epoch.ErrFenced once the run of the agent has ended", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, f.app)
				f.serve(t, a, shardA, 1)
				stop()
				received(t, a.Done(), "the end of the run")
				_, err := a.Token(t.Context(), &replica.TokenRequest{Shard: shardA})
				assert.ErrorIs(t, err, epoch.ErrFenced, "Token must revoke the tokens once the run has ended")
			})
		})

		t.Run("returns the token that the callbacks of the shard leave while they run", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				inc := f.incarnation(t)
				h := history.New()
				done := make(chan struct{})
				var active atomic.Int64
				outcomes := history.Concurrently(readers+1, waitFor, func(client int) (any, error) {
					if client == 0 {
						defer close(done)
						return nil, cycle(t.Context(), f, a, h, inc)
					}
					for {
						if read(t.Context(), a, h, client) != epoch.Zero {
							active.Add(1)
						}
						select {
						case <-done:
							read(t.Context(), a, h, client)
							return nil, nil
						case <-time.After(pause):
						}
					}
				})
				for _, o := range outcomes {
					expect.True(t, o.Finished, "every client must finish")
					expect.NoError(t, o.Error, "the callbacks of every client must succeed")
				}
				expect.True(t, active.Load() > 0, "the readers must call Token during a tenure")
				history.Linearizable(
					t,
					h,
					tokens,
					"Token returns the epoch of the tenure that the callbacks of the shard leave",
				)
			})
		})
	})
}

// TestTokenAllocs checks the allocation contract of Token. MaxAllocs counts the allocations
// of the whole process, so the test does not run in parallel.
func TestTokenAllocs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		a := f.start(t)
		f.serve(t, a, shardA, 1)
		ctx := t.Context()
		var res replica.TokenResult
		var err error
		expect.MaxAllocs(t, func() {
			req := replica.TokenRequest{Shard: shardA}
			res, err = a.Token(ctx, &req)
		}, 0, "Token must not allocate")
		assert.NoError(t, err, "the test must measure the token of a tenure")
		assert.Equal(t, res.Token.Epoch, epoch.Epoch(1), "the test must measure the token of a tenure")
	})
}

// BenchmarkToken measures Token for the tenure of one shard.
func BenchmarkToken(b *testing.B) {
	f := newFixture(b)
	a := f.start(b)
	ctx := b.Context()
	p := placement(shardA, 1, node.ID)
	p.Incarnation = f.incarnation(b)
	f.commitCell(b, &store.Mutation{Put: []shard.Placement{p}})
	assert.NoError(b, a.Do(ctx, controllerMetadata, request(protocol.Add, shardA, 1, 1, p.Incarnation)),
		"the add of the shard must succeed")

	b.Run("Token", func(b *testing.B) {
		b.Run("serial", func(b *testing.B) {
			var res replica.TokenResult
			var err error
			c := bench.Start(b).MaxAllocs(0)
			defer c.End()
			for c.Loop() {
				req := replica.TokenRequest{Shard: shardA}
				res, err = a.Token(ctx, &req)
			}
			assert.NoError(b, err, "the benchmark must measure the token of a tenure")
			assert.Equal(b, res.Token.Epoch, epoch.Epoch(1), "the benchmark must measure the token of a tenure")
		})

		b.Run("parallel", func(b *testing.B) {
			c := bench.Start(b).MaxAllocs(0)
			defer c.End()
			c.RunParallel(func(pb *bench.PB) {
				var res replica.TokenResult
				var err error
				read := 0
				for pb.Next() {
					req := replica.TokenRequest{Shard: shardA}
					res, err = a.Token(ctx, &req)
					read++
				}
				if read > 0 {
					expect.NoError(b, err, "the benchmark must measure the token of a tenure")
					expect.Equal(b, res.Token.Epoch, epoch.Epoch(1), "the benchmark must measure the token of a tenure")
				}
			})
		})
	})
}

// cycle starts and ends tenures of shardA on the agent a as the controller of the cell does,
// and records each add and each drop in h as client 0. It places the shard on the node at
// the incarnation inc before each add, and keeps each tenure for tenancy. It returns the first
// error of a commit or a callback.
func cycle(ctx context.Context, f *fixture, a *replica.Agent, h *history.History, inc epoch.Epoch) error {
	for e := epoch.Epoch(1); e <= tenures; e++ {
		p := placement(shardA, e, node.ID)
		p.Incarnation = inc
		if err := f.commit(ctx, &store.Mutation{Put: []shard.Placement{p}}); err != nil {
			return err
		}
		add := h.Invoke(0, opAdd, []any{e})
		if err := a.Do(ctx, controllerMetadata, request(protocol.Add, shardA, uint64(2*e-1), e, inc)); err != nil {
			add.Fail(err)
			return err
		}
		add.OK(nil)
		<-time.After(tenancy)
		drop := h.Invoke(0, opDrop, nil)
		if err := a.Do(ctx, controllerMetadata, request(protocol.Drop, shardA, uint64(2*e), e, inc)); err != nil {
			drop.Fail(err)
			return err
		}
		drop.OK(nil)
	}
	return nil
}

// read calls Token for shardA on the agent a, records the call in h as client, and returns
// its output: the epoch of the token, and zero for an error.
func read(ctx context.Context, a *replica.Agent, h *history.History, client int) epoch.Epoch {
	call := h.Invoke(client, opToken, nil)
	res, err := a.Token(ctx, &replica.TokenRequest{Shard: shardA})
	got := epoch.Zero
	if err == nil {
		got = res.Token.Epoch
	}
	call.OK(got)
	return got
}

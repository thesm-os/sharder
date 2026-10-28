// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/replica/replicatest"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store/memory"
)

// lostReplies is the number of the renewals of a lease whose replies a test loses. It covers
// every renewal before the deadline of the lease.
const lostReplies = 3

// work is the work of one tenure of the tests. It records its start and its end.
type work struct {
	// err is the error that the work returns.
	err error
	// started is closed once the work runs.
	started chan struct{}
	// ended is closed once the work has returned.
	ended chan struct{}
	// early makes the work return at once, in place of at the end of its context.
	early bool
}

// run runs w. It returns w.err at once when w.early is set, and once ctx ends otherwise.
func (w *work) run(ctx context.Context) error {
	close(w.started)
	defer close(w.ended)
	if !w.early {
		<-ctx.Done()
	}
	return w.err
}

// opened is a copy of one request of Open.
type opened struct {
	// request is a copy of the add of the request.
	request *protocol.Request
	// token is the token of the request.
	token fence.Token
}

// opening is the behaviour of the opener of a test.
type opening struct {
	// fail is the error of every Open, or nil.
	fail error
	// err is the error that the work of each tenure returns.
	err error
	// gate makes each Open wait until gate is closed, or is nil.
	gate chan struct{}
	// idle makes Open return a tenure without work.
	idle bool
	// early makes the work of each tenure return at once.
	early bool
}

// opener is the Opener of the tests. Its double keeps a copy of each request of Open, and
// returns new work for each tenure. The double records each request of Open as the runner
// passed it.
type opener struct {
	// OpenerStub is the double that the runner calls.
	*replicatest.OpenerStub
	opening
	// opens lists a copy of each request of Open, in order.
	opens []opened
	// works lists the work of each tenure that Open returned, in order.
	works []*work
	// mu guards opens and works.
	mu sync.Mutex
}

// newOpener returns an opener with the behaviour b. Its Open waits for gate, keeps a copy of
// the request, and returns the error fail, a tenure without work, or new work. Its double
// reports to tb.
func newOpener(tb assert.TB, b opening) *opener {
	tb.Helper()
	o := &opener{opening: b}
	open := func(_ context.Context, req *replica.OpenRequest) (replica.OpenResult, error) {
		if o.gate != nil {
			<-o.gate
		}
		o.mu.Lock()
		defer o.mu.Unlock()
		o.opens = append(o.opens, opened{request: req.Request.CloneKanon(), token: req.Token})
		if o.fail != nil {
			return replica.OpenResult{}, o.fail
		}
		if o.idle {
			return replica.OpenResult{}, nil
		}
		w := &work{err: o.err, early: o.early, started: make(chan struct{}), ended: make(chan struct{})}
		o.works = append(o.works, w)
		return replica.OpenResult{Run: w.run}, nil
	}
	o.OpenerStub = replicatest.NewOpenerStub(tb, replicatest.WithOpenerOpen(open))
	return o
}

// calls returns the number of the calls of Open.
func (o *opener) calls() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.opens)
}

// last returns the work of the last tenure that Open returned.
func (o *opener) last(tb assert.TB) *work {
	tb.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	assert.NotEmpty(tb, o.works, "Open must have returned work")
	return o.works[len(o.works)-1]
}

// TestRunner checks the tenures that a runner opens and the work that it runs.
func TestRunner(t *testing.T) {
	t.Parallel()

	t.Run("AddShard", func(t *testing.T) {
		t.Parallel()

		t.Run("opens the tenure of the add with the token of the tenure", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				o := newOpener(t, opening{idle: true})
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, replica.NewRunner(o))
				t.Cleanup(func() {
					stop()
					<-a.Done()
				})
				f.serve(t, a, shardA, 1)
				assert.Length(t, o.opens, 1, "the runner must open the tenure once")
				expect.Equal(t, o.opens[0].request.Call, protocol.Add, "the runner must open the tenure with its add")
				expect.Equal(t, o.opens[0].token, f.guard.OnAcquire.Calls()[0].T,
					"the runner must open the tenure with its token")
			})
		})

		t.Run("runs the work of the tenure until the drop of the tenure", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				o := newOpener(t, opening{})
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, replica.NewRunner(o))
				t.Cleanup(func() {
					stop()
					<-a.Done()
				})
				f.serve(t, a, shardA, 1)
				w := o.last(t)
				<-w.started
				r := request(protocol.Drop, shardA, 2, 1, f.incarnation(t))
				assert.NoError(t, a.Do(t.Context(), controllerMetadata, r), "the drop must succeed")
				select {
				case <-w.ended:
				default:
					t.Fatal("the drop must return once the work has returned")
				}
			})
		})

		t.Run("keeps the work of the tenure for an add of the tenure that runs", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				o := newOpener(t, opening{})
				run := replica.NewRunner(o)
				add := request(protocol.Add, shardA, 1, 1, 1)
				token := fence.Token{Shard: shardA, Epoch: 1}
				_, err := run.AddShard(t.Context(), &replica.AddShardRequest{Request: add, Token: token})
				assert.NoError(t, err, "the first add must open the tenure")
				_, err = run.AddShard(t.Context(), &replica.AddShardRequest{Request: add, Token: token})
				assert.NoError(t, err, "an add of the tenure that runs must succeed")
				expect.Equal(t, o.calls(), 1, "an add of the tenure that runs must not open it again")
				synctest.Wait()
				select {
				case <-o.last(t).ended:
					t.Error("the work of the tenure must keep running")
				default:
				}
				_, err = run.DropShard(t.Context(), &replica.DropShardRequest{Request: add})
				assert.NoError(t, err, "the drop must end the work")
			})
		})

		t.Run("ends the work of an earlier tenure before it opens a newer tenure", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				o := newOpener(t, opening{})
				run := replica.NewRunner(o)
				first := request(protocol.Add, shardA, 1, 1, 1)
				_, err := run.AddShard(t.Context(), &replica.AddShardRequest{
					Request: first,
					Token:   fence.Token{Shard: shardA, Epoch: 1},
				})
				assert.NoError(t, err, "the first add must open the tenure")
				earlier := o.last(t)
				second := request(protocol.Add, shardA, 2, 2, 1)
				_, err = run.AddShard(t.Context(), &replica.AddShardRequest{
					Request: second,
					Token:   fence.Token{Shard: shardA, Epoch: 2},
				})
				assert.NoError(t, err, "the add of the newer tenure must open it")
				select {
				case <-earlier.ended:
				default:
					t.Error("the work of the earlier tenure must have ended before the newer tenure opened")
				}
				expect.Equal(t, o.calls(), 2, "the runner must open the newer tenure")
				_, err = run.DropShard(t.Context(), &replica.DropShardRequest{Request: second})
				assert.NoError(t, err, "the drop must end the work")
			})
		})

		t.Run("clears the request of Open once Open returns", func(t *testing.T) {
			t.Parallel()
			o := newOpener(t, opening{idle: true})
			run := replica.NewRunner(o)
			_, err := run.AddShard(t.Context(), &replica.AddShardRequest{
				Request: request(protocol.Add, shardA, 1, 1, 1),
				Token:   fence.Token{Shard: shardA, Epoch: 1},
			})
			assert.NoError(t, err, "the add must open the tenure")
			requests := o.OnOpen.AssertCalledN(t, 1, "the runner must open the tenure once")
			expect.Nil(t, requests[0].Req.Request, "the runner must clear the add of the request")
			expect.Equal(t, requests[0].Req.Token, fence.Token{}, "the runner must clear the token of the request")
		})

		t.Run("opens a tenure again after its drop", func(t *testing.T) {
			t.Parallel()
			o := newOpener(t, opening{idle: true})
			run := replica.NewRunner(o)
			add := &replica.AddShardRequest{
				Request: request(protocol.Add, shardA, 1, 1, 1),
				Token:   fence.Token{Shard: shardA, Epoch: 1},
			}
			_, err := run.AddShard(t.Context(), add)
			assert.NoError(t, err, "the add must open the tenure")
			_, err = run.DropShard(t.Context(), &replica.DropShardRequest{Request: add.Request})
			assert.NoError(t, err, "the drop must end the tenure")
			_, err = run.AddShard(t.Context(), add)
			assert.NoError(t, err, "the add after the drop must open the tenure")
			assert.Equal(t, o.calls(), 2, "the runner must open a tenure again after its drop")
		})

		t.Run("returns the error of Open", func(t *testing.T) {
			t.Parallel()
			run := replica.NewRunner(newOpener(t, opening{fail: errBroken}))
			_, err := run.AddShard(t.Context(), &replica.AddShardRequest{
				Request: request(protocol.Add, shardA, 1, 1, 1),
				Token:   fence.Token{Shard: shardA, Epoch: 1},
			})
			assert.ErrorIs(t, err, errBroken, "the add must fail with the error of Open")
		})

		t.Run("opens a tenure without work once", func(t *testing.T) {
			t.Parallel()
			o := newOpener(t, opening{idle: true})
			run := replica.NewRunner(o)
			add := &replica.AddShardRequest{
				Request: request(protocol.Add, shardA, 1, 1, 1),
				Token:   fence.Token{Shard: shardA, Epoch: 1},
			}
			_, err := run.AddShard(t.Context(), add)
			assert.NoError(t, err, "the add must open the tenure")
			_, err = run.AddShard(t.Context(), add)
			assert.NoError(t, err, "an add of the tenure must succeed")
			assert.Equal(t, o.calls(), 1, "an add of a tenure without work must not open it again")
		})

		t.Run("reports a permanent fault of the replica whose work fails", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				o := newOpener(t, opening{err: errBroken, early: true})
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, replica.NewRunner(o))
				t.Cleanup(func() {
					stop()
					<-a.Done()
				})
				f.serve(t, a, shardA, 1)
				synctest.Wait()
				got := f.observed(t, shardA)
				expect.Equal(t, got.Fault, shard.FaultPermanent, "the runner must report a permanent fault")
				expect.Equal(t, got.Cause, errBroken.Error(), "the fault must have the error of the work as its cause")
			})
		})

		t.Run("does not report a fault of a replica whose work returns nil", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				o := newOpener(t, opening{early: true})
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, replica.NewRunner(o))
				t.Cleanup(func() {
					stop()
					<-a.Done()
				})
				f.serve(t, a, shardA, 1)
				synctest.Wait()
				expect.Equal(
					t,
					f.observed(t, shardA).Fault,
					shard.FaultNone,
					"a work that ends must not fault the replica",
				)
			})
		})

		t.Run("does not report a fault of a replica whose work fails at its drop", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				o := newOpener(t, opening{err: errBroken})
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, replica.NewRunner(o))
				t.Cleanup(func() {
					stop()
					<-a.Done()
				})
				f.serve(t, a, shardA, 1)
				r := request(protocol.Drop, shardA, 2, 1, f.incarnation(t))
				assert.NoError(t, a.Do(t.Context(), controllerMetadata, r), "the drop must succeed")
				synctest.Wait()
				f.ctrl.mu.Lock()
				defer f.ctrl.mu.Unlock()
				for _, report := range f.ctrl.reports {
					for _, o := range report.Replicas {
						expect.Equal(
							t,
							o.Fault,
							shard.FaultNone,
							"the work that ends at its drop must not fault the replica",
						)
					}
				}
			})
		})

		t.Run("does not report a fault of a replica whose work a later tenure fenced", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				o := newOpener(t, opening{err: fmt.Errorf("test: a write: %w", epoch.ErrFenced), early: true})
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, replica.NewRunner(o))
				t.Cleanup(func() {
					stop()
					<-a.Done()
				})
				f.serve(t, a, shardA, 1)
				synctest.Wait()
				expect.Equal(
					t,
					f.observed(t, shardA).Fault,
					shard.FaultNone,
					"the runner must not report a fenced tenure",
				)
			})
		})
	})

	t.Run("DropShard", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil for a shard without a tenure", func(t *testing.T) {
			t.Parallel()
			run := replica.NewRunner(newOpener(t, opening{}))
			_, err := run.DropShard(
				t.Context(),
				&replica.DropShardRequest{Request: request(protocol.Drop, shardA, 1, 1, 1)},
			)
			assert.NoError(t, err, "a drop of a shard without a tenure must succeed")
		})
	})

	t.Run("RevokeShard", func(t *testing.T) {
		t.Parallel()

		t.Run("ends the work of the tenure of the token", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				o := newOpener(t, opening{})
				run := replica.NewRunner(o)
				token := fence.Token{Shard: shardA, Epoch: 1}
				_, err := run.AddShard(t.Context(), &replica.AddShardRequest{
					Request: request(protocol.Add, shardA, 1, 1, 1),
					Token:   token,
				})
				assert.NoError(t, err, "the add must open the tenure")
				_, err = run.RevokeShard(t.Context(), &replica.RevokeShardRequest{Token: token})
				assert.NoError(t, err, "the revocation must succeed")
				select {
				case <-o.last(t).ended:
				default:
					t.Fatal("the revocation must return once the work has returned")
				}
			})
		})

		t.Run("keeps the work of a newer tenure of the shard", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				o := newOpener(t, opening{})
				run := replica.NewRunner(o)
				newer := request(protocol.Add, shardA, 2, 2, 1)
				_, err := run.AddShard(t.Context(), &replica.AddShardRequest{
					Request: newer,
					Token:   fence.Token{Shard: shardA, Epoch: 2},
				})
				assert.NoError(t, err, "the add must open the newer tenure")
				_, err = run.RevokeShard(t.Context(), &replica.RevokeShardRequest{
					Token: fence.Token{Shard: shardA, Epoch: 1},
				})
				assert.NoError(t, err, "the revocation of the older tenure must succeed")
				synctest.Wait()
				select {
				case <-o.last(t).ended:
					t.Error("the revocation of an older tenure must not end the work of the newer tenure")
				default:
				}
				_, err = run.DropShard(t.Context(), &replica.DropShardRequest{Request: newer})
				assert.NoError(t, err, "the drop must end the work")
			})
		})

		t.Run("returns nil for a shard without a tenure", func(t *testing.T) {
			t.Parallel()
			run := replica.NewRunner(newOpener(t, opening{}))
			_, err := run.RevokeShard(t.Context(), &replica.RevokeShardRequest{
				Token: fence.Token{Shard: shardA, Epoch: 1},
			})
			assert.NoError(t, err, "a revocation of a shard without a tenure must succeed")
		})

		t.Run("opens a tenure again after its revocation", func(t *testing.T) {
			t.Parallel()
			o := newOpener(t, opening{idle: true})
			run := replica.NewRunner(o)
			add := &replica.AddShardRequest{
				Request: request(protocol.Add, shardA, 1, 1, 1),
				Token:   fence.Token{Shard: shardA, Epoch: 1},
			}
			_, err := run.AddShard(t.Context(), add)
			assert.NoError(t, err, "the add must open the tenure")
			_, err = run.RevokeShard(t.Context(), &replica.RevokeShardRequest{Token: add.Token})
			assert.NoError(t, err, "the revocation must end the tenure")
			_, err = run.AddShard(t.Context(), add)
			assert.NoError(t, err, "the add after the revocation must open the tenure")
			assert.Equal(t, o.calls(), 2, "the runner must forget a tenure that it revoked")
		})

		t.Run("ends the work of a tenure when the session of the node ends under stop", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.stop(t)
				o := newOpener(t, opening{})
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, replica.NewRunner(o))
				t.Cleanup(func() {
					stop()
					<-a.Done()
				})
				f.serve(t, a, shardA, 1)
				w := o.last(t)
				<-w.started
				f.lose(t)
				_, err := a.Token(t.Context(), &replica.TokenRequest{Shard: shardA})
				assert.ErrorIs(t, err, epoch.ErrFenced, "the end of the lease must revoke the token of the tenure")
				select {
				case <-w.ended:
				default:
					t.Fatal("the end of the lease must end the work of the tenure under stop")
				}
			})
		})

		t.Run("keeps the work of a tenure when the session of the node ends under continue-fenced",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					o := newOpener(t, opening{})
					ctx, stop := context.WithCancel(t.Context())
					a := f.run(t, ctx, replica.NewRunner(o))
					t.Cleanup(func() {
						stop()
						<-a.Done()
					})
					f.serve(t, a, shardA, 1)
					w := o.last(t)
					<-w.started
					f.lose(t)
					select {
					case <-w.ended:
						t.Error("the end of the lease must keep the work of the tenure under continue-fenced")
					default:
					}
				})
			})

		t.Run("ends the work of a tenure when the replies to the renewals of its lease are lost under stop",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					f.stop(t)
					o := newOpener(t, opening{})
					ctx, stop := context.WithCancel(t.Context())
					a := f.run(t, ctx, replica.NewRunner(o))
					t.Cleanup(func() {
						stop()
						<-a.Done()
					})
					f.serve(t, a, shardA, 1)
					w := o.last(t)
					<-w.started
					for range lostReplies {
						f.st.FailAt(memory.PointRenewAck)
					}
					f.clock.Advance(leaseTTL + time.Second)
					synctest.Wait()
					select {
					case <-w.ended:
					default:
						t.Fatal("the end of a lease whose renewals lost their replies must end the work under stop")
					}
				})
			})

		t.Run("ends the work of a tenure whose open returns after the end of the session under stop",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					f.stop(t)
					o := newOpener(t, opening{gate: make(chan struct{})})
					ctx, stop := context.WithCancel(t.Context())
					a := f.run(t, ctx, replica.NewRunner(o))
					t.Cleanup(func() {
						stop()
						<-a.Done()
					})
					f.place(t, shardA, 1)
					r := request(protocol.Add, shardA, 1, 1, f.incarnation(t))
					added := make(chan error, 1)
					go func() { added <- a.Do(t.Context(), controllerMetadata, r) }()
					synctest.Wait()
					f.lose(t)
					close(o.gate)
					synctest.Wait()
					assert.NoError(t, <-added, "the add must succeed once its open returns")
					select {
					case <-o.last(t).ended:
					default:
						t.Fatal("the work of a tenure whose open returns after the end of the session must end")
					}
				})
			})

		t.Run("ends the work of a tenure when another session of the node replaces its session under stop",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					f.stop(t)
					o := newOpener(t, opening{})
					ctx, stop := context.WithCancel(t.Context())
					a := f.run(t, ctx, replica.NewRunner(o))
					t.Cleanup(func() {
						stop()
						<-a.Done()
					})
					f.serve(t, a, shardA, 1)
					w := o.last(t)
					<-w.started
					f.rejoin(t)
					select {
					case <-w.ended:
					default:
						t.Fatal("the end of a replaced session must end the work of the tenure under stop")
					}
				})
			})

		t.Run("keeps the work of a newer tenure after the node joins again under stop", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.stop(t)
				o := newOpener(t, opening{})
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, replica.NewRunner(o))
				t.Cleanup(func() {
					stop()
					<-a.Done()
				})
				f.serve(t, a, shardA, 1)
				f.rejoin(t)
				f.serve(t, a, shardA, 2)
				synctest.Wait()
				assert.Equal(t, o.calls(), 2, "the runner must open the newer tenure")
				select {
				case <-o.last(t).ended:
					t.Error("the revocation of the older tenure must not end the work of the newer tenure")
				default:
				}
			})
		})
	})

	t.Run("ends the work of every tenure before the agent closes Done", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(t)
			o := newOpener(t, opening{})
			ctx, stop := context.WithCancel(t.Context())
			a := f.run(t, ctx, replica.NewRunner(o))
			f.serve(t, a, shardA, 1)
			w := o.last(t)
			<-w.started
			stop()
			<-a.Done()
			select {
			case <-w.ended:
			default:
				t.Fatal("the work must have ended before the agent closed Done")
			}
		})
	})

	t.Run("forgets every tenure once the agent has ended", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(t)
			o := newOpener(t, opening{idle: true})
			ctx, stop := context.WithCancel(t.Context())
			run := replica.NewRunner(o)
			a := f.run(t, ctx, run)
			f.serve(t, a, shardA, 1)
			stop()
			<-a.Done()
			assert.Length(t, o.opens, 1, "the runner must have opened the tenure once")
			_, err := run.AddShard(t.Context(), &replica.AddShardRequest{
				Request: o.opens[0].request,
				Token:   o.opens[0].token,
			})
			assert.NoError(t, err, "the add after the end of the agent must open the tenure")
			assert.Equal(t, o.calls(), 2, "the runner must open the tenure again after the end of the agent")
		})
	})
}

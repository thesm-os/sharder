// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver_test

import (
	"context"
	"runtime"
	"testing"
	"testing/synctest"
	"weak"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/protocoltest"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// foreign is an effect of a type that the driver does not perform. It embeds a Timer, so it
// implements machine.Effect.
type foreign struct{ machine.Timer }

// TestRun checks the loop of Run: the order of its events, the effects that it performs,
// and how it stops.
func TestRun(t *testing.T) {
	t.Parallel()

	t.Run("Run", func(t *testing.T) {
		t.Parallel()

		t.Run("passes Started before the event of a call that waits for the run", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, serve)
				d := f.driver(t)
				calls := call[struct{}](t.Context(), d, func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				synctest.Wait()
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				_, ok := received(t, f.events, "the run passes its first event").(machine.Started)
				assert.True(t, ok, "the first event must be Started")
				_, ok = received(t, f.events, "the run passes the event of the call").(machine.Recovered)
				assert.True(t, ok, "the second event must be the event of the call")
				stopped(t, cancel, errc)
				received(t, calls, "the call returns")
			})
		})

		t.Run("passes Stopped as its last event once its context ends", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, serve)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.events, "the run passes Started")
				stopped(t, cancel, errc)
				assert.Equal(t, received(t, f.events, "the run passes Stopped"), machine.Event(machine.Stopped{}),
					"the run must pass Stopped once its context ends")
				streamtest.Blocks(t, f.events, "the run must not pass an event after Stopped")
			})
		})

		t.Run("returns the cause of its context once the context ends", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, serve)
				ctx, cancel := context.WithCancelCause(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.events, "the run passes Started")
				cancel(errCause)
				assert.ErrorIs(t, received(t, errc, "Run returns"), errCause,
					"Run must return the cause of its context")
			})
		})

		t.Run("returns the error of the reply to Started", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					if s, ok := ev.(machine.Started); ok {
						return []machine.Effect{machine.Reply[struct{}]{To: s.Reply, Err: errBroken}}
					}
					return nil
				})
				errc := start(t.Context(), f.driver(t))
				received(t, f.events, "the run passes Started")
				assert.ErrorIs(t, received(t, errc, "Run returns"), errBroken,
					"Run must return the error of the reply to Started")
			})
		})

		t.Run("passes Stopped after a reply to Started with an error", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					if s, ok := ev.(machine.Started); ok {
						return []machine.Effect{machine.Reply[struct{}]{To: s.Reply, Err: errBroken}}
					}
					return nil
				})
				errc := start(t.Context(), f.driver(t))
				received(t, f.events, "the run passes Started")
				assert.Equal(t, received(t, f.events, "the run passes Stopped"), machine.Event(machine.Stopped{}),
					"the run must pass Stopped after a reply to Started with an error")
				assert.ErrorIs(t, received(t, errc, "Run returns"), errBroken,
					"Run must return the error of the reply to Started")
			})
		})

		t.Run("returns the error of a machine that has stopped itself", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var failure error
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}}
					case machine.Recovered:
						failure = errBroken
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply, Err: errBroken}}
					}
					return nil
				})
				f.machine.OnErr.Func(func() error { return failure })
				d := f.driver(t)
				errc := start(t.Context(), d)
				_, err := d.Call[struct{}](t.Context(), func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				assert.ErrorIs(t, err, errBroken, "the call must return the reply of the step that stopped the machine")
				assert.ErrorIs(t, received(t, errc, "Run returns"), errBroken,
					"Run must return the error of a machine that has stopped itself")
			})
		})

		t.Run("returns ErrStopped to a call that waits when the machine stops itself", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var failure error
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}}
					case machine.Report:
						failure = errBroken
					}
					return nil
				})
				f.machine.OnErr.Func(func() error { return failure })
				d := f.driver(t)
				errc := start(t.Context(), d)
				received(t, f.events, "the run passes Started")
				calls := call[struct{}](t.Context(), d, func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				received(t, f.events, "the run passes the event of the call")
				_, err := d.Call[protocol.Ack](t.Context(), func(tok machine.ReplyToken) machine.Event {
					return machine.Report{Report: &protocol.Report{}, Reply: tok}
				})
				assert.ErrorIs(
					t,
					err,
					driver.ErrStopped,
					"the call of the step that stops the machine must return ErrStopped",
				)
				assert.ErrorIs(t, received(t, calls, "the call that waits returns").err, driver.ErrStopped,
					"a call that waits when the machine stops itself must return ErrStopped")
				assert.ErrorIs(t, received(t, errc, "Run returns"), errBroken,
					"Run must return the error of a machine that has stopped itself")
			})
		})

		t.Run("passes Stopped after the machine has stopped itself", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var failure error
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}}
					case machine.Recovered:
						failure = errBroken
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply, Err: errBroken}}
					}
					return nil
				})
				f.machine.OnErr.Func(func() error { return failure })
				d := f.driver(t)
				errc := start(t.Context(), d)
				received(t, f.events, "the run passes Started")
				calls := call[struct{}](t.Context(), d, func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				received(t, f.events, "the run passes the event of the call")
				assert.Equal(t, received(t, f.events, "the run passes Stopped"), machine.Event(machine.Stopped{}),
					"the run must pass Stopped after the machine has stopped itself")
				received(t, calls, "the call returns")
				assert.ErrorIs(t, received(t, errc, "Run returns"), errBroken,
					"Run must return the error of a machine that has stopped itself")
			})
		})

		t.Run("returns ErrReply when the machine replies to Started with another type", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					if s, ok := ev.(machine.Started); ok {
						return []machine.Effect{machine.Reply[protocol.Ack]{To: s.Reply}}
					}
					return nil
				})
				errc := start(t.Context(), f.driver(t))
				received(t, f.events, "the run passes Started")
				assert.ErrorIs(t, received(t, errc, "Run returns"), driver.ErrReply,
					"Run must return ErrReply for a reply to Started of another type")
			})
		})

		t.Run("returns ErrEffect for an effect of a type that it does not perform", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(foreign{}))
				errc := start(t.Context(), f.driver(t))
				received(t, f.events, "the run passes Started")
				assert.Equal(t, received(t, f.events, "the run passes Stopped"), machine.Event(machine.Stopped{}),
					"the run must pass Stopped after an effect that it does not perform")
				assert.ErrorIs(t, received(t, errc, "Run returns"), driver.ErrEffect,
					"Run must return ErrEffect for an effect that it does not perform")
			})
		})

		t.Run("performs only the replies of the effects of Stopped", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var kept machine.ReplyToken
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}}
					case machine.Recovered:
						kept = ev.Reply
					case machine.Stopped:
						return []machine.Effect{
							machine.Commit{
								Mutation:    mutation(1),
								Scope:       scope,
								Token:       1,
								Term:        term,
								Incarnation: inc,
							},
							machine.Reply[struct{}]{To: kept, Err: errBroken},
						}
					}
					return nil
				})
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				received(t, f.events, "the run passes Started")
				calls := call[struct{}](t.Context(), d, func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				received(t, f.events, "the run passes the event of the call")
				stopped(t, cancel, errc)
				assert.ErrorIs(t, received(t, calls, "the call returns").err, errBroken,
					"the call must return the reply that the machine returns for Stopped")
				head, err := f.store.Head(t.Context(), scope)
				assert.NoError(t, err, "Head reads the scope")
				assert.Equal(
					t,
					head,
					store.Head{},
					"the run must not perform the commit that the machine returns for Stopped",
				)
			})
		})

		t.Run("cancels the effects in flight before it passes Stopped", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Callback{Node: &node, Request: &protocol.Request{}, Token: 1}))
				ctxs := make(chan context.Context, 1)
				f.cfg.Agents = protocoltest.NewAgentClientStub(t,
					protocoltest.WithAgentClientDo(func(ctx context.Context, _ *shard.Node, _ *protocol.Request) error {
						ctxs <- ctx
						return blocked(ctx)
					}))
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.events, "the run passes Started")
				callback := received(t, ctxs, "the run performs the callback")
				cancel()
				received(t, f.events, "the run passes Stopped")
				assert.ErrorIs(
					t,
					callback.Err(),
					context.Canceled,
					"the run must cancel the callback before it passes Stopped",
				)
				assert.ErrorIs(t, received(t, errc, "Run returns"), context.Canceled,
					"Run must return the cause of its context")
			})
		})

		t.Run(
			"cancels the effects in flight before it passes Stopped after a reply to Started with an error",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					ctxs := make(chan context.Context, 1)
					ended := make(chan error, 1)
					f := newFixture(t, func(ev machine.Event) []machine.Effect {
						switch ev := ev.(type) {
						case machine.Started:
							return []machine.Effect{
								machine.Callback{Node: &node, Request: &protocol.Request{}, Token: 1},
								machine.Reply[struct{}]{To: ev.Reply, Err: errBroken},
							}
						case machine.Stopped:
							callback := <-ctxs
							ended <- callback.Err()
						}
						return nil
					})
					do := func(ctx context.Context, _ *shard.Node, _ *protocol.Request) error {
						ctxs <- ctx
						return blocked(ctx)
					}
					f.cfg.Agents = protocoltest.NewAgentClientStub(t, protocoltest.WithAgentClientDo(do))
					errc := start(t.Context(), f.driver(t))
					received(t, f.events, "the run passes Started")
					assert.ErrorIs(
						t,
						received(t, ended, "the machine reads the context of the callback at Stopped"),
						context.Canceled,
						"the run must cancel the callback before it passes Stopped",
					)
					assert.ErrorIs(t, received(t, errc, "Run returns"), errBroken,
						"Run must return the error of the reply to Started")
				})
			},
		)

		t.Run("performs no effect of a step once its context has ended", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(t.Context())
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					if s, ok := ev.(machine.Started); ok {
						cancel(errCause)
						return []machine.Effect{
							machine.Reply[struct{}]{To: s.Reply},
							machine.Callback{Node: &node, Request: &protocol.Request{}, Token: 1},
						}
					}
					return nil
				})
				agents := protocoltest.NewAgentClientStub(t)
				f.cfg.Agents = agents
				errc := start(ctx, f.driver(t))
				assert.ErrorIs(t, received(t, errc, "Run returns"), errCause,
					"Run must return the cause of its context")
				agents.OnDo.AssertNotCalled(t, "the run must not perform the callback after its context ended")
			})
		})

		t.Run("ignores the Unwatch of a token without an effect", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Unwatch{Token: 1}))
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.events, "the run passes Started")
				stopped(t, cancel, errc)
			})
		})

		t.Run("cancels the context of a call with a Timeout once the call returns", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(
					t,
					performing(machine.Callback{Node: &node, Request: &protocol.Request{}, Token: 1, Timeout: timeout}),
				)
				agents := protocoltest.NewAgentClientStub(t)
				f.cfg.Agents = agents
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.events, "the run passes Started")
				received(t, f.events, "the run passes the result of the callback")
				synctest.Wait()
				assert.ErrorIs(
					t,
					agents.OnDo.AssertCalledOnce(t, "the run performs the callback").Ctx.Err(),
					context.Canceled,
					"the run must cancel the context of the callback once the callback returns",
				)
				stopped(t, cancel, errc)
			})
		})

		t.Run("drops the result of an effect whose run stops while the machine steps", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				gate := make(chan struct{})
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{
							machine.Reply[struct{}]{To: ev.Reply},
							machine.Callback{Node: &node, Request: &protocol.Request{}, Token: 1},
						}
					case machine.Recovered:
						<-gate
					}
					return nil
				})
				calling := make(chan struct{}, 1)
				release := make(chan struct{})
				f.cfg.Agents = protocoltest.NewAgentClientStub(t,
					protocoltest.WithAgentClientDo(func(context.Context, *shard.Node, *protocol.Request) error {
						calling <- struct{}{}
						<-release
						return nil
					}))
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				received(t, f.events, "the run passes Started")
				received(t, calling, "the run performs the callback")
				calls := call[struct{}](t.Context(), d, func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				received(t, f.events, "the run passes the event of the call")
				close(release)
				synctest.Wait()
				cancel()
				synctest.Wait()
				close(gate)
				assert.Equal(
					t,
					received(t, f.events, "the run passes its next event"),
					machine.Event(machine.Stopped{}),
					"the run must drop the result of an effect that waits for the run when the run stops",
				)
				assert.ErrorIs(t, received(t, errc, "Run returns"), context.Canceled,
					"Run must return the cause of its context")
				received(t, calls, "the call returns")
			})
		})

		t.Run("keeps no effect of a step once it has performed the effect", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				requests := make(chan *protocol.Request, 1)
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					if s, ok := ev.(machine.Started); ok {
						return []machine.Effect{
							machine.Reply[struct{}]{To: s.Reply},
							machine.Callback{Node: &node, Request: <-requests, Token: 1},
						}
					}
					return nil
				})
				f.cfg.Agents = protocoltest.NewAgentClientStub(t, protocoltest.AgentClientStubBenchMode())
				gone := func() weak.Pointer[protocol.Request] {
					r := &protocol.Request{Call: protocol.Add}
					requests <- r
					return weak.Make(r)
				}()
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.events, "the run passes Started")
				received(t, f.events, "the run passes the result of the callback")
				synctest.Wait()
				runtime.GC()
				runtime.GC()
				assert.Nil(t, gone.Value(), "the run must not keep the request of a callback that it has performed")
				stopped(t, cancel, errc)
			})
		})
	})
}

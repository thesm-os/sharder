// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/history"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/machinetest"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// mode is how the machine of the property of Call replies to one call.
type mode uint8

// The modes of the calls of the property of Call.
const (
	// echoed is a call that the machine replies to at once, with the Sequence of its
	// report.
	echoed mode = 1
	// delayed is a call that the machine keeps until its context ends.
	delayed mode = 2
)

// delayedNode is the Node of the report of a delayed call.
const delayedNode shard.NodeID = "delayed"

// delayFor is the time after which the context of a delayed call ends.
const delayFor = time.Millisecond

// maxCalls is the most calls of one case of the property of Call.
const maxCalls = 8

// labelModes labels the draw of the modes of the calls of one case.
const labelModes = "modes of the calls"

// callAllocs is the most allocations of a call that the machine replies to at once: the
// event of the call and the reply of the machine. The channel of the reply comes from a
// pool.
const callAllocs = 2

// TestInbound checks Call.
func TestInbound(t *testing.T) {
	t.Parallel()

	t.Run("Call", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the value and the error of the reply", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				want := protocol.Ack{Sequence: 7}
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}}
					case machine.Report:
						return []machine.Effect{machine.Reply[protocol.Ack]{To: ev.Reply, Value: want, Err: errBusy}}
					}
					return nil
				})
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				got, err := d.Call[protocol.Ack](t.Context(), func(tok machine.ReplyToken) machine.Event {
					return machine.Report{Report: &protocol.Report{}, Reply: tok}
				})
				assert.ErrorIs(t, err, errBusy, "Call must return the error of the reply")
				assert.Equal(t, got, want, "Call must return the value of the reply")
				stopped(t, cancel, errc)
			})
		})

		t.Run("returns the receipt of a reply to a Submit", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				want := protocol.Receipt{Applied: true, Generation: 2}
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}}
					case machine.Submit:
						return []machine.Effect{machine.Reply[protocol.Receipt]{To: ev.Reply, Value: want}}
					}
					return nil
				})
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				got, err := d.Call[protocol.Receipt](t.Context(), func(tok machine.ReplyToken) machine.Event {
					return machine.Submit{Proposal: &proposal.Proposal{}, Reply: tok}
				})
				assert.NoError(t, err, "Call must return the error of the reply")
				assert.Equal(t, got, want, "Call must return the receipt of the reply")
				stopped(t, cancel, errc)
			})
		})

		t.Run("returns the decision of a reply to a Disrupt", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				want := budget.Decision{Allowed: true}
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}}
					case machine.Disrupt:
						return []machine.Effect{machine.Reply[budget.Decision]{To: ev.Reply, Value: want}}
					}
					return nil
				})
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				got, err := d.Call[budget.Decision](t.Context(), func(tok machine.ReplyToken) machine.Event {
					return machine.Disrupt{Disruption: &budget.Disruption{}, Reply: tok}
				})
				assert.NoError(t, err, "Call must return the error of the reply")
				assert.Equal(t, got, want, "Call must return the decision of the reply")
				stopped(t, cancel, errc)
			})
		})

		t.Run("passes the event that event returns for the token of the call", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, serve)
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				received(t, f.events, "the run passes Started")
				tokens := make(chan machine.ReplyToken, 1)
				calls := call[struct{}](t.Context(), d, func(tok machine.ReplyToken) machine.Event {
					tokens <- tok
					return machine.Recovered{Reply: tok}
				})
				tok := received(t, tokens, "Call passes a token to event")
				assert.Equal(t, received(t, f.events, "the run passes the event of the call"),
					machine.Event(machine.Recovered{Reply: tok}), "the run must pass the event that event returns")
				stopped(t, cancel, errc)
				received(t, calls, "the call returns")
			})
		})

		t.Run("gives each call a token of its own", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, serve)
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				started, ok := received(t, f.events, "the run passes Started").(machine.Started)
				assert.True(t, ok, "the first event is Started")
				tokens := make(chan machine.ReplyToken, 2)
				event := func(tok machine.ReplyToken) machine.Event {
					tokens <- tok
					return machine.Recovered{Reply: tok}
				}
				first := call[struct{}](t.Context(), d, event)
				second := call[struct{}](t.Context(), d, event)
				all := []machine.ReplyToken{
					started.Reply,
					received(t, tokens, "the first call takes a token"),
					received(t, tokens, "the second call takes a token"),
				}
				assert.NoDuplicates(t, func() ([]machine.ReplyToken, error) { return all, nil },
					"each call must have a token of its own")
				stopped(t, cancel, errc)
				received(t, first, "the first call returns")
				received(t, second, "the second call returns")
			})
		})

		t.Run("returns the cause of its context when the context ends before the reply", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, serve)
				d := f.driver(t)
				run, stop := context.WithCancel(t.Context())
				errc := start(run, d)
				received(t, f.events, "the run passes Started")
				ctx, cancel := context.WithCancelCause(t.Context())
				calls := call[struct{}](ctx, d, func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				received(t, f.events, "the run passes the event of the call")
				cancel(errCause)
				assert.ErrorIs(t, received(t, calls, "the call returns").err, errCause,
					"Call must return the cause of its context")
				stopped(t, stop, errc)
			})
		})

		t.Run("passes Cancelled once the context of the call ends before the reply", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, serve)
				d := f.driver(t)
				run, stop := context.WithCancel(t.Context())
				errc := start(run, d)
				received(t, f.events, "the run passes Started")
				ctx, cancel := context.WithCancel(t.Context())
				calls := call[struct{}](ctx, d, func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				ev, ok := received(t, f.events, "the run passes the event of the call").(machine.Recovered)
				assert.True(t, ok, "the event of the call is a Recovered")
				cancel()
				received(t, calls, "the call returns")
				assert.Equal(t, received(t, f.events, "the run passes Cancelled"),
					machine.Event(machine.Cancelled{Reply: ev.Reply}), "the run must pass Cancelled for the call")
				stopped(t, stop, errc)
			})
		})

		t.Run("returns its own reply to a call after the reply to a cancelled call", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}}
					case machine.Cancelled:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply, Err: errBroken}}
					case machine.Report:
						return []machine.Effect{
							machine.Reply[protocol.Ack]{To: ev.Reply, Value: protocol.Ack{Sequence: 2}},
						}
					}
					return nil
				})
				d := f.driver(t)
				run, stop := context.WithCancel(t.Context())
				errc := start(run, d)
				received(t, f.events, "the run passes Started")
				ctx, cancel := context.WithCancel(t.Context())
				first := call[struct{}](ctx, d, func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				received(t, f.events, "the run passes the event of the first call")
				cancel()
				assert.ErrorIs(t, received(t, first, "the first call returns").err, context.Canceled,
					"the cancelled call must return the error of its context")
				received(t, f.events, "the run passes Cancelled")
				got, err := d.Call[protocol.Ack](t.Context(), func(tok machine.ReplyToken) machine.Event {
					return machine.Report{Report: &protocol.Report{}, Reply: tok}
				})
				assert.NoError(t, err, "the second call must return the error of its own reply")
				assert.Equal(
					t,
					got,
					protocol.Ack{Sequence: 2},
					"the second call must return the value of its own reply",
				)
				stopped(t, stop, errc)
			})
		})

		t.Run("does not pass Cancelled for a call that the machine has replied to", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				gate := make(chan struct{})
				var firstCall machine.ReplyToken
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}}
					case machine.Recovered:
						firstCall = ev.Reply
					case machine.Report:
						<-gate
						return []machine.Effect{
							machine.Reply[struct{}]{To: firstCall},
							machine.Reply[protocol.Ack]{To: ev.Reply},
						}
					}
					return nil
				})
				d := f.driver(t)
				run, stop := context.WithCancel(t.Context())
				errc := start(run, d)
				received(t, f.events, "the run passes Started")
				ctx, cancel := context.WithCancel(t.Context())
				first := call[struct{}](ctx, d, func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				received(t, f.events, "the run passes the event of the first call")
				second := call[protocol.Ack](t.Context(), d, func(tok machine.ReplyToken) machine.Event {
					return machine.Report{Report: &protocol.Report{}, Reply: tok}
				})
				received(t, f.events, "the run passes the event of the second call")
				cancel()
				synctest.Wait()
				close(gate)
				assert.ErrorIs(t, received(t, first, "the first call returns").err, context.Canceled,
					"the first call must return the error of its context")
				assert.NoError(
					t,
					received(t, second, "the second call returns").err,
					"the second call must return its reply",
				)
				stop()
				assert.Equal(
					t,
					received(t, f.events, "the run passes its next event"),
					machine.Event(machine.Stopped{}),
					"the run must not pass Cancelled for a call that the machine has replied to",
				)
				assert.ErrorIs(
					t,
					received(t, errc, "Run returns"),
					context.Canceled,
					"Run must return the cause of its context",
				)
			})
		})

		t.Run("returns ErrStopped after the run has stopped", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, serve)
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				received(t, f.events, "the run passes Started")
				stopped(t, cancel, errc)
				_, err := d.Call[struct{}](t.Context(), func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				assert.ErrorIs(t, err, driver.ErrStopped, "Call must return ErrStopped after the run has stopped")
			})
		})

		t.Run("returns ErrStopped when the run returns without a reply to the call", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, serve)
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				received(t, f.events, "the run passes Started")
				calls := call[struct{}](t.Context(), d, func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				received(t, f.events, "the run passes the event of the call")
				stopped(t, cancel, errc)
				assert.ErrorIs(t, received(t, calls, "the call returns").err, driver.ErrStopped,
					"Call must return ErrStopped when the run returns without a reply")
			})
		})

		t.Run("returns ErrReply when the machine replies with another type", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, func(ev machine.Event) []machine.Effect {
					switch ev := ev.(type) {
					case machine.Started:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}}
					case machine.Report:
						return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}}
					}
					return nil
				})
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				_, err := d.Call[protocol.Ack](t.Context(), func(tok machine.ReplyToken) machine.Event {
					return machine.Report{Report: &protocol.Report{}, Reply: tok}
				})
				assert.ErrorIs(t, err, driver.ErrReply, "Call must return ErrReply for a reply of another type")
				stopped(t, cancel, errc)
			})
		})

		t.Run(
			"returns the cause of its context when the context ends before the run takes the call",
			func(t *testing.T) {
				t.Parallel()
				d := newFixture(t, serve).driver(t)
				ctx, cancel := context.WithCancelCause(t.Context())
				cancel(errCause)
				_, err := d.Call[struct{}](ctx, func(tok machine.ReplyToken) machine.Event {
					return machine.Recovered{Reply: tok}
				})
				assert.ErrorIs(t, err, errCause, "Call must return the cause of its context")
			},
		)

		t.Run("returns each call once with its own reply or the cause of its context", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(
				t,
				"concurrent calls each return their own reply, or the cause of a context that ended first",
				func(c *prop.Case) {
					modes := c.Draw(
						prop.List(prop.SampledFrom(echoed, delayed), prop.MinSize(1), prop.MaxSize(maxCalls)),
						labelModes,
					)
					var waiting, cancelled []machine.ReplyToken
					f := newFixture(c, func(ev machine.Event) []machine.Effect {
						switch ev := ev.(type) {
						case machine.Started:
							return []machine.Effect{machine.Reply[struct{}]{To: ev.Reply}}
						case machine.Report:
							if ev.Report.Node == delayedNode {
								waiting = append(waiting, ev.Reply)
								return nil
							}
							return []machine.Effect{
								machine.Reply[protocol.Ack]{
									To:    ev.Reply,
									Value: protocol.Ack{Sequence: ev.Report.Sequence},
								},
							}
						case machine.Cancelled:
							cancelled = append(cancelled, ev.Reply)
						}
						return nil
					})
					d := f.driver(c)
					run, stop := context.WithCancel(c.Context())
					errc := start(run, d)
					outcomes := history.Concurrently(len(modes), waitFor, func(i int) (any, error) {
						ctx, cancel := context.WithCancel(c.Context())
						defer cancel()
						report := &protocol.Report{Sequence: uint64(i)}
						if modes[i] == delayed {
							ctx, cancel = context.WithTimeout(ctx, delayFor)
							defer cancel()
							report.Node = delayedNode
						}
						ack, err := d.Call[protocol.Ack](ctx, func(tok machine.ReplyToken) machine.Event {
							return machine.Report{Report: report, Reply: tok}
						})
						return ack, err
					})
					stopped(c, stop, errc)
					for i, o := range outcomes {
						assert.True(c, o.Finished, "every call must return")
						if modes[i] == delayed {
							assert.ErrorIs(
								c,
								o.Error,
								context.DeadlineExceeded,
								"a delayed call must return the error of its context",
							)
							continue
						}
						assert.NoError(c, o.Error, "an echoed call must return the error of its reply")
						assert.Equal(
							c,
							o.Output,
							any(protocol.Ack{Sequence: uint64(i)}),
							"an echoed call must return its own reply",
						)
					}
					assert.Permutation(
						c,
						cancelled,
						waiting,
						"the run must pass one Cancelled for each call that the machine keeps",
					)
				},
			)
		})
	})
}

// TestInboundAllocs checks the allocations of a call that the machine replies to at once.
func TestInboundAllocs(t *testing.T) {
	f := newFixture(t, serve)
	f.cfg.Machine = machinetest.NewMachineStub(t, machinetest.MachineStubBenchMode(), machinetest.WithMachineStep(echo))
	d := f.driver(t)
	ctx, cancel := context.WithCancel(t.Context())
	errc := start(ctx, d)
	assert.NoError(t, d.Ready(t.Context()), "the machine serves")

	t.Run("Call", func(t *testing.T) {
		t.Run("allocates only the event of the call and the reply of the machine", func(t *testing.T) {
			event := func(tok machine.ReplyToken) machine.Event { return machine.Recovered{Reply: tok} }
			var err error
			assert.MaxAllocs(t, func() { _, err = d.Call[struct{}](t.Context(), event) }, callAllocs,
				"a call must allocate at most its event and the reply of the machine")
			assert.NoError(t, err, "the call returns its reply")
		})
	})

	stopped(t, cancel, errc)
}

// BenchmarkInbound measures a call that the machine replies to at once.
func BenchmarkInbound(b *testing.B) {
	f := newFixture(b, serve)
	f.cfg.Machine = machinetest.NewMachineStub(b, machinetest.MachineStubBenchMode(), machinetest.WithMachineStep(echo))
	d := f.driver(b)
	ctx, cancel := context.WithCancel(b.Context())
	errc := start(ctx, d)
	assert.NoError(b, d.Ready(b.Context()), "the machine serves")

	b.Run("Call", func(b *testing.B) {
		event := func(tok machine.ReplyToken) machine.Event { return machine.Recovered{Reply: tok} }
		var err error
		c := bench.Start(b).MaxAllocs(callAllocs)
		defer c.End()
		for c.Loop() {
			_, err = d.Call[struct{}](b.Context(), event)
		}
		assert.NoError(b, err, "the call returns its reply")
	})

	stopped(b, cancel, errc)
}

// echo is the Step of a machine that replies at once to Started and to every Recovered,
// without an error. It appends the reply to out, so a step allocates nothing.
func echo(ev machine.Event, out []machine.Effect) []machine.Effect {
	switch ev := ev.(type) {
	case machine.Started:
		return append(out, machine.Reply[struct{}]{To: ev.Reply})
	case machine.Recovered:
		return append(out, machine.Reply[struct{}]{To: ev.Reply})
	}
	return out
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package embedded_test

import (
	"context"
	"testing"
	"testing/synctest"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/controller"
	"go.thesmos.sh/sharder/embedded"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/router"
	"go.thesmos.sh/sharder/shard"
)

// absent is a namespace that the processes of the tests do not have.
const absent shard.Namespace = "absent"

// cell is the cell of a callback that a test sends before the process has read its cells.
const cell shard.CellID = "cell"

// operator is the metadata of a call that a test sends to a process.
var operator = protocol.Metadata{Caller: "operator", Version: protocol.Current}

// other is a namespace incarnation that differs from the incarnation of each namespace of the
// tests.
var other = shard.NamespaceIncarnation{ID: id.New128([id.Size128]byte{0: 0xff, 15: 0xff})}

// TestProcess checks the composition of an application process, its start and its end.
func TestProcess(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			want error
			give func(cfg *embedded.Config) *embedded.Config
			name string
		}{
			{
				name: "returns ErrConfig for a nil Config",
				give: func(*embedded.Config) *embedded.Config { return nil },
				want: embedded.ErrConfig,
			},
			{
				name: "returns ErrConfig for a Config without a namespace",
				give: func(cfg *embedded.Config) *embedded.Config {
					cfg.Namespaces = nil
					return cfg
				},
				want: embedded.ErrConfig,
			},
			{
				name: "returns ErrConfig for two namespaces with one name",
				give: func(cfg *embedded.Config) *embedded.Config {
					cfg.Namespaces[1].Spec.Namespace = cfg.Namespaces[0].Spec.Namespace
					return cfg
				},
				want: embedded.ErrConfig,
			},
			{
				name: "returns an error that wraps controller.ErrConfig for a negative cell capacity",
				give: func(cfg *embedded.Config) *embedded.Config {
					cfg.CellCapacity = -1
					return cfg
				},
				want: controller.ErrConfig,
			},
			{
				name: "returns an error that wraps replica.ErrConfig for a namespace without a guard",
				give: func(cfg *embedded.Config) *embedded.Config {
					cfg.Namespaces[1].Guard = nil
					return cfg
				},
				want: replica.ErrConfig,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				cfg, _ := newSystem(t).config(nodeA)
				_, err := embedded.New(tt.give(cfg))
				expect.That(t, err).
					ErrorIs(embedded.ErrConfig, "New must return an error that wraps ErrConfig").
					ErrorIs(tt.want, "New must return the error of the configuration")
			})
		}
	})

	t.Run("AgentServer", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			want error
			name string
			give shard.Scope
		}{
			{
				name: "returns a server that passes a callback of the root scope to the controller",
				give: shard.Scope{Namespace: orders},
				want: controller.ErrNotServing,
			},
			{
				name: "returns a server that passes a callback of a cell to the agent of its namespace",
				give: shard.Scope{Namespace: orders, Cell: cell},
				want: replica.ErrNotServing,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				p := newSystem(t).build(t, nodeA)
				err := p.AgentServer().Do(t.Context(), operator, &protocol.Request{Scope: tt.give})
				assert.ErrorIs(t, err, tt.want, "the component of the scope must refuse the callback before Start")
			})
		}

		t.Run("returns a server whose controller refuses a callback of the root scope from another caller",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					p := newSystem(t).start(t, nodeA)
					err := p.AgentServer().
						Do(t.Context(), operator, &protocol.Request{Scope: shard.Scope{Namespace: orders}})
					assert.Equal(
						t,
						errs.Classify(err),
						errs.Denied,
						"the controller must refuse a caller other than the root",
					)
				})
			})

		t.Run("returns a server whose agent refuses a callback of a cell from another caller", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				p := newSystem(t).start(t, nodeA)
				err := p.AgentServer().
					Do(t.Context(), operator, &protocol.Request{Scope: shard.Scope{Namespace: orders, Cell: cell}})
				assert.Equal(t, errs.Classify(err), errs.Denied,
					"the agent must refuse a caller other than the controller of the cell")
			})
		})

		t.Run("returns a server whose agent checks no caller of an unauthenticated process", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				p := newSystem(t).start(t, nodeA, func(cfg *embedded.Config) { cfg.Unauthenticated = true })
				err := p.AgentServer().
					Do(t.Context(), operator, &protocol.Request{Scope: shard.Scope{Namespace: orders, Cell: cell}})
				assert.ErrorIs(
					t,
					err,
					protocol.ErrUnsupported,
					"the agent must check the call after it skips the caller",
				)
			})
		})
	})

	t.Run("ControllerServer", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the controller of the process", func(t *testing.T) {
			t.Parallel()
			p := newSystem(t).build(t, nodeA)
			_, err := p.ControllerServer().Submit(t.Context(), operator,
				&proposal.Proposal{Scope: shard.Scope{Namespace: orders}})
			assert.ErrorIs(t, err, controller.ErrNotServing, "the controller must refuse a proposal before Start")
		})
	})

	t.Run("Start", func(t *testing.T) {
		t.Parallel()

		t.Run("starts the agent of each namespace", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				p := newSystem(t).start(t, nodeA)
				p.apps[orders].serves(t, slots)
				p.apps[audits].serves(t, slots)
			})
		})

		t.Run("starts the router of a namespace that routes", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				r := route(t, newSystem(t).start(t, nodeA))
				assert.Length(t, r.Replicas, 1, "the route must list the writer of the shard")
				assert.Equal(t, r.Replicas[0].Node, nodeA.ID, "the writer must be the node of the process")
			})
		})

		t.Run("returns ErrStarted for a second call", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				p := newSystem(t).start(t, nodeA)
				assert.ErrorIs(t, p.Start(t.Context()), embedded.ErrStarted, "a second Start must fail")
			})
		})

		t.Run("returns ErrStarted after Close", func(t *testing.T) {
			t.Parallel()
			p := newSystem(t).build(t, nodeA)
			assert.NoError(t, p.Close(t.Context()), "Close before Start stops nothing")
			assert.ErrorIs(t, p.Start(t.Context()), embedded.ErrStarted, "Start after Close must fail")
		})

		t.Run("returns the cause of a context that ended before the call", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				p := newSystem(t).build(t, nodeA)
				ctx, stop := context.WithCancelCause(t.Context())
				stop(errStop)
				assert.ErrorIs(t, p.Start(ctx), errStop, "Start must return the cause of its context")
				assert.True(t, closed(p.Done()), "Start must return once every goroutine has returned")
			})
		})

		t.Run("returns the cause of a context that ends while a router starts", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, stop := context.WithCancelCause(t.Context())
				p := newSystem(t).build(t, nodeA, func(cfg *embedded.Config) {
					cfg.Reporter = &reporter{Reporter: noop.New(), bound: func() { stop(errStop) }}
				})
				assert.ErrorIs(t, p.Start(ctx), errStop, "Start must return the cause of its context")
				_, ok := p.Router(orders)
				assert.False(t, ok, "the process must not return a router that did not start")
			})
		})

		t.Run("returns an error that wraps replica.ErrStarted for an agent that has started", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				p := newSystem(t).build(t, nodeA)
				a, _ := p.Agent(audits)
				ctx, stop := context.WithCancel(t.Context())
				stop()
				assert.HasError(t, a.Start(ctx), "the agent does not serve under an ended context")
				assert.ErrorIs(t, p.Start(t.Context()), replica.ErrStarted, "Start must return the error of the agent")
				assert.True(t, closed(p.Done()), "Start must return once every goroutine has returned")
			})
		})
	})

	t.Run("Agent", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the agent of a namespace of the process", func(t *testing.T) {
			t.Parallel()
			a, ok := newSystem(t).build(t, nodeA).Agent(orders)
			assert.True(t, ok, "the process must have the agent of orders")
			assert.NotNil(t, a, "the agent of orders must exist")
		})

		t.Run("reports false for a namespace that the process does not have", func(t *testing.T) {
			t.Parallel()
			_, ok := newSystem(t).build(t, nodeA).Agent(absent)
			assert.False(t, ok, "the process must not have an agent of another namespace")
		})
	})

	t.Run("Router", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the router of a namespace that routes once Start has returned", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				w, ok := newSystem(t).start(t, nodeA).Router(orders)
				assert.True(t, ok, "the process must run the router of orders")
				assert.NotNil(t, w, "the router of orders must exist")
			})
		})

		t.Run("reports false for a namespace that does not route", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				_, ok := newSystem(t).start(t, nodeA).Router(audits)
				assert.False(t, ok, "the process must not run a router of audits")
			})
		})

		t.Run("reports false before Start", func(t *testing.T) {
			t.Parallel()
			_, ok := newSystem(t).build(t, nodeA).Router(orders)
			assert.False(t, ok, "the process must not run a router before Start")
		})
	})

	t.Run("Close", func(t *testing.T) {
		t.Parallel()

		t.Run("moves the replicas of the node to another process", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := newSystem(t)
				a := s.start(t, nodeA)
				a.apps[orders].serves(t, slots)
				a.apps[audits].serves(t, slots)
				b := s.start(t, nodeB)
				b.apps[orders].serves(t, slots/2)
				b.apps[audits].serves(t, slots/2)
				ctx, cancel := context.WithTimeout(t.Context(), drainFor)
				defer cancel()
				assert.NoError(t, b.Close(ctx), "Close must move the replicas of the node")
				expect.Equal(t, b.apps[orders].count(), 0, "the node must have dropped every shard of orders")
				expect.Equal(t, b.apps[audits].count(), 0, "the node must have dropped every shard of audits")
				expect.True(t, closed(b.Done()), "Close must return once every goroutine has returned")
				a.apps[orders].serves(t, slots)
				a.apps[audits].serves(t, slots)
			})
		})

		t.Run("returns nil before Start", func(t *testing.T) {
			t.Parallel()
			p := newSystem(t).build(t, nodeA)
			assert.NoError(t, p.Close(t.Context()), "Close before Start must stop nothing")
		})

		t.Run("returns nil after the end of the run", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				p := newSystem(t).build(t, nodeA)
				ctx, stop := context.WithCancel(t.Context())
				assert.NoError(t, p.Start(ctx), "Start starts the process")
				stop()
				received(t, p.Done(), "the end of the run")
				assert.NoError(t, p.Close(t.Context()), "Close must return nil after the end of the run")
			})
		})

		t.Run("returns an error that wraps replica.ErrConfig for a context without a deadline", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				p := newSystem(t).start(t, nodeA)
				assert.ErrorIs(t, p.Close(t.Context()), replica.ErrConfig, "Close must return the error of each drain")
				assert.True(t, closed(p.Done()), "Close must stop the process")
			})
		})

		t.Run("returns the cause of a context that ends before the replicas have moved", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				p := newSystem(t).start(t, nodeA)
				p.apps[orders].serves(t, slots)
				ctx, cancel := context.WithTimeoutCause(t.Context(), drainFor, errStop)
				defer cancel()
				assert.ErrorIs(t, p.Close(ctx), errStop, "Close must return the cause of its context")
			})
		})
	})

	t.Run("Done", func(t *testing.T) {
		t.Parallel()

		t.Run("is closed once the context of Start ends", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				p := newSystem(t).build(t, nodeA)
				ctx, stop := context.WithCancel(t.Context())
				assert.NoError(t, p.Start(ctx), "Start starts the process")
				stop()
				received(t, p.Done(), "the end of the run")
			})
		})

		t.Run("is closed by a Close before Start", func(t *testing.T) {
			t.Parallel()
			p := newSystem(t).build(t, nodeA)
			assert.NoError(t, p.Close(t.Context()), "Close before Start stops nothing")
			assert.True(t, closed(p.Done()), "Close before Start must close Done")
		})
	})

	t.Run("Err", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil while the process runs", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				p := newSystem(t).start(t, nodeA)
				assert.Nil(t, p.Err(), "Err must be nil while the process runs")
			})
		})

		t.Run("returns the cause of the context of Start once the run has ended", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				p := newSystem(t).build(t, nodeA)
				ctx, stop := context.WithCancelCause(t.Context())
				assert.NoError(t, p.Start(ctx), "Start starts the process")
				stop(errStop)
				received(t, p.Done(), "the end of the run")
				assert.Equal(t, p.Err(), errStop, "Err must return the cause of the context of Start")
			})
		})

		t.Run("returns nil after a Close that ended the run", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				p := newSystem(t).start(t, nodeA)
				assert.HasError(t, p.Close(t.Context()), "the drains fail without a deadline")
				assert.Nil(t, p.Err(), "Err must be nil after a Close")
			})
		})

		t.Run("returns the error of an agent that stopped itself", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := newSystem(t)
				p := s.start(t, nodeA)
				r := route(t, p)
				s.supersede(t, nodeA, shard.Scope{Namespace: orders, Cell: r.Cell}, r.Incarnation)
				received(t, p.Done(), "the end of the run")
				assert.ErrorIs(t, p.Err(), errRefused, "Err must return the error of the agent")
			})
		})

		t.Run("returns the error of the controller that stopped itself", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := newSystem(t)
				p := s.start(t, nodeA)
				r := route(t, p)
				s.supersede(t, nodeA, shard.Scope{Namespace: orders}, r.Incarnation)
				received(t, p.Done(), "the end of the run")
				assert.ErrorIs(t, p.Err(), errRefused, "Err must return the error of the controller")
			})
		})

		t.Run("returns the error of a router that stopped itself", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := newSystem(t)
				p := s.start(t, nodeA)
				r := route(t, p)
				root := shard.Scope{Namespace: orders}
				s.store.refuse.Store(&root)
				w, _ := p.Router(orders)
				_, err := w.Refresh(
					t.Context(),
					&router.RefreshRequest{Cell: r.Cell, Min: r.Generation, Incarnation: other},
				)
				assert.ErrorIs(t, err, router.ErrClosed, "the refresh must end with the router")
				received(t, p.Done(), "the end of the run")
				assert.ErrorIs(t, p.Err(), errRefused, "Err must return the error of the router")
			})
		})
	})
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// node is the node of the callbacks and the sessions of the tests.
var node = shard.Node{ID: "node-a", Address: "node-a:7000", Incarnation: 1}

// TestDriver checks New, Run and Ready.
func TestDriver(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		t.Run("returns a driver for a config with every required field", func(t *testing.T) {
			t.Parallel()
			d, err := driver.New(newFixture(t, serve).cfg)
			assert.NoError(t, err, "New must accept a config with every required field")
			assert.NotNil(t, d, "New must return a driver")
		})

		tests := []struct {
			give func(cfg *driver.Config) *driver.Config
			name string
		}{
			{
				name: "returns ErrConfig for a nil config",
				give: func(*driver.Config) *driver.Config { return nil },
			},
			{
				name: "returns ErrConfig for a config without a machine",
				give: func(cfg *driver.Config) *driver.Config {
					cfg.Machine = nil
					return cfg
				},
			},
			{
				name: "returns ErrConfig for a config without a reader",
				give: func(cfg *driver.Config) *driver.Config {
					cfg.Reader = nil
					return cfg
				},
			},
			{
				name: "returns ErrConfig for a config without a membership",
				give: func(cfg *driver.Config) *driver.Config {
					cfg.Membership = nil
					return cfg
				},
			},
			{
				name: "returns ErrConfig for a config without a clock",
				give: func(cfg *driver.Config) *driver.Config {
					cfg.Clock = nil
					return cfg
				},
			},
			{
				name: "returns ErrConfig for a config without a logger",
				give: func(cfg *driver.Config) *driver.Config {
					cfg.Logger = nil
					return cfg
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				_, err := driver.New(tt.give(newFixture(t, serve).cfg))
				assert.ErrorIs(t, err, driver.ErrConfig, "New must refuse the config")
			})
		}
	})

	t.Run("Run", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrRun for a second call", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, serve)
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				received(t, f.probe.events, "the run passes Started")
				assert.ErrorIs(t, d.Run(ctx), driver.ErrRun, "a second Run must return ErrRun")
				stopped(t, cancel, errc)
			})
		})

		t.Run("leaves no goroutine once it returns", func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, performing(
				machine.Watch{Scope: scope, Token: 1, Incarnation: inc},
				machine.WatchViews{Scope: scope, Token: 2, Incarnation: inc},
				machine.Campaign{Scope: scope, Token: 3},
				machine.Join{Node: &node, Scope: scope, Token: 4, Lease: lease, Incarnation: inc},
				machine.Timer{Token: 5, Duration: timeout},
				machine.Callback{Node: &node, Request: &protocol.Request{}, Token: 6},
			))
			f.cfg.Agents = agentClient(func(ctx context.Context, _ *shard.Node, _ *protocol.Request) error {
				return blocked(ctx)
			})
			d := f.driver(t)
			check := assert.NoGoroutineLeaks(t, "the run must leave no goroutine once it returns")
			ctx, cancel := context.WithCancel(t.Context())
			errc := start(ctx, d)
			received(t, f.probe.events, "the run passes Started")
			for granted := 0; granted < 2; {
				switch received(t, f.probe.events, "the run passes a result of an effect").(type) {
				case machine.Campaigned, machine.Joined:
					granted++
				}
			}
			stopped(t, cancel, errc)
			check()
		})

		t.Run("returns once every effect has returned", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Callback{Node: &node, Request: &protocol.Request{}, Token: 1}))
				calling := make(chan struct{}, 1)
				var done atomic.Bool
				f.cfg.Agents = agentClient(func(ctx context.Context, _ *shard.Node, _ *protocol.Request) error {
					calling <- struct{}{}
					<-ctx.Done()
					done.Store(true)
					return context.Cause(ctx)
				})
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, calling, "the run performs the callback")
				stopped(t, cancel, errc)
				assert.True(t, done.Load(), "Run must return after the callback has returned")
			})
		})
	})

	t.Run("Ready", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil once the machine replies to Started without an error", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, serve)
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				assert.NoError(t, d.Ready(t.Context()), "Ready must return nil once the machine serves")
				assert.NoError(t, d.Ready(t.Context()), "a second Ready must return nil as well")
				stopped(t, cancel, errc)
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
				d := f.driver(t)
				errc := start(t.Context(), d)
				assert.ErrorIs(
					t,
					d.Ready(t.Context()),
					errBroken,
					"Ready must return the error of the reply to Started",
				)
				assert.ErrorIs(
					t,
					received(t, errc, "Run returns"),
					errBroken,
					"Run must return the error of the reply to Started",
				)
			})
		})

		t.Run("returns the cause of its context when the context ends before the reply", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, func(machine.Event) []machine.Effect { return nil })
				d := f.driver(t)
				run, stop := context.WithCancel(t.Context())
				errc := start(run, d)
				received(t, f.probe.events, "the run passes Started")
				ctx, cancel := context.WithCancelCause(t.Context())
				cancel(errCause)
				assert.ErrorIs(t, d.Ready(ctx), errCause, "Ready must return the cause of its context")
				stopped(t, stop, errc)
			})
		})

		t.Run("returns ErrStopped when the run returns without a reply to Started", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, func(machine.Event) []machine.Effect { return nil })
				d := f.driver(t)
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, d)
				received(t, f.probe.events, "the run passes Started")
				stopped(t, cancel, errc)
				assert.ErrorIs(t, d.Ready(t.Context()), driver.ErrStopped,
					"Ready must return ErrStopped after a run without a reply to Started")
			})
		})
	})
}

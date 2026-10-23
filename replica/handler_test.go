// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica_test

import (
	"context"
	"testing"
	"testing/synctest"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// cpu is the load metric of the tests.
const cpu = "cpu"

// logRevocation is the log message of the agent for a revocation that fails.
const logRevocation = "agent: the revocation of a tenure failed"

// TestHandler checks the calls of the handler of the application by an agent.
func TestHandler(t *testing.T) {
	t.Parallel()

	t.Run("AddShard", func(t *testing.T) {
		t.Parallel()

		t.Run("receives the callback of an add", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				f.place(t, shardA, 1)
				r := request(protocol.Add, shardA, 1, 1, f.incarnation(t))
				assert.NoError(t, a.Do(t.Context(), controllerMetadata, r), "the add must succeed")
				assert.Length(t, f.app.calls, 1, "the handler must receive one callback")
				assert.Equal(t, f.app.calls[0], r, "the handler must receive the callback of the add")
			})
		})

		t.Run("receives a request whose callback the agent clears once the call returns", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				f.serve(t, a, shardA, 1)
				assert.Length(t, f.app.adds, 1, "the handler must receive one add")
				expect.Nil(t, f.app.adds[0].Request, "the agent must clear the request once the call returns")
				expect.Equal(
					t,
					f.app.adds[0].Token,
					fence.Token{},
					"the agent must clear the token once the call returns",
				)
			})
		})

		t.Run("receives the fencing token of the tenure of the add", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				f.serve(t, a, shardA, 1)
				assert.Length(t, f.app.tokens, 1, "the handler must receive one add")
				assert.Length(t, f.guard.acquired, 1, "the agent must acquire one fence")
				assert.Equal(
					t,
					f.app.tokens[0],
					f.guard.acquired[0],
					"the handler must receive the token that the add acquired",
				)
			})
		})

		t.Run("passes the error of the handler to the controller", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.app.fail = func(*protocol.Request) error { return errBroken }
				a := f.start(t)
				f.place(t, shardA, 1)
				err := a.Do(t.Context(), controllerMetadata, request(protocol.Add, shardA, 1, 1, f.incarnation(t)))
				assert.ErrorIs(t, err, errBroken, "the add must fail with the error of the handler")
			})
		})
	})

	t.Run("DropShard", func(t *testing.T) {
		t.Parallel()

		t.Run("receives the callback of a drop", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				f.serve(t, a, shardA, 1)
				r := request(protocol.Drop, shardA, 2, 1, f.incarnation(t))
				assert.NoError(t, a.Do(t.Context(), controllerMetadata, r), "the drop must succeed")
				assert.Length(t, f.app.calls, 2, "the handler must receive the add and the drop")
				assert.Equal(t, f.app.calls[1], r, "the handler must receive the callback of the drop")
			})
		})

		t.Run("receives a request whose callback the agent clears once the call returns", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				f.serve(t, a, shardA, 1)
				r := request(protocol.Drop, shardA, 2, 1, f.incarnation(t))
				assert.NoError(t, a.Do(t.Context(), controllerMetadata, r), "the drop must succeed")
				assert.Length(t, f.app.drops, 1, "the handler must receive one drop")
				assert.Nil(t, f.app.drops[0].Request, "the agent must clear the request once the call returns")
			})
		})

		t.Run("passes the error of the handler to the controller", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				f.serve(t, a, shardA, 1)
				f.app.mu.Lock()
				f.app.fail = func(*protocol.Request) error { return errBroken }
				f.app.mu.Unlock()
				err := a.Do(t.Context(), controllerMetadata, request(protocol.Drop, shardA, 2, 1, f.incarnation(t)))
				assert.ErrorIs(t, err, errBroken, "the drop must fail with the error of the handler")
			})
		})
	})

	t.Run("RevokeShard", func(t *testing.T) {
		t.Parallel()

		t.Run("receives the token of a tenure when the session of the node ends under stop", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.stop(t)
				h := &revoking{application: f.app}
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, h)
				t.Cleanup(func() {
					stop()
					<-a.Done()
				})
				f.serve(t, a, shardA, 1)
				f.lose(t)
				h.mu.Lock()
				defer h.mu.Unlock()
				assert.Length(t, h.revoked, 1, "the handler must receive one revocation")
				assert.Equal(t, h.revoked[0], f.guard.acquired[0], "the handler must receive the token of the tenure")
			})
		})

		t.Run("receives a request whose token the agent clears once the call returns", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.stop(t)
				h := &revoking{application: f.app}
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, h)
				t.Cleanup(func() {
					stop()
					<-a.Done()
				})
				f.serve(t, a, shardA, 1)
				f.lose(t)
				h.mu.Lock()
				defer h.mu.Unlock()
				assert.Length(t, h.revocations, 1, "the handler must receive one revocation")
				assert.Equal(t, h.revocations[0].Token, fence.Token{},
					"the agent must clear the token once the call returns")
			})
		})

		t.Run("passes its error to the log of the agent", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.stop(t)
				h := &revoking{application: f.app, err: errBroken}
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, h)
				t.Cleanup(func() {
					stop()
					<-a.Done()
				})
				f.serve(t, a, shardA, 1)
				f.lose(t)
				failures := f.errorsOf(logRevocation)
				assert.Length(t, failures, 1, "the agent must log the failed revocation once")
				assert.ErrorIs(t, failures[0], errBroken, "the log must have the error of RevokeShard")
			})
		})

		t.Run("passes no error to the log of the agent when it returns nil", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.stop(t)
				h := &revoking{application: f.app}
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, h)
				t.Cleanup(func() {
					stop()
					<-a.Done()
				})
				f.serve(t, a, shardA, 1)
				f.lose(t)
				h.mu.Lock()
				defer h.mu.Unlock()
				assert.Length(t, h.revoked, 1, "the handler must receive one revocation")
				assert.Empty(t, f.errorsOf(logRevocation), "the agent must not log a revocation that succeeds")
			})
		})

		t.Run("returns nil for a handler without RevokeShard", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.stop(t)
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, f.app)
				t.Cleanup(func() {
					stop()
					<-a.Done()
				})
				f.serve(t, a, shardA, 1)
				f.lose(t)
				assert.Empty(
					t,
					f.errorsOf(logRevocation),
					"the revocation of a handler without RevokeShard must succeed",
				)
			})
		})

		t.Run("receives no revocation when the session of the node ends under continue-fenced", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				h := &revoking{application: f.app}
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, h)
				t.Cleanup(func() {
					stop()
					<-a.Done()
				})
				f.serve(t, a, shardA, 1)
				f.lose(t)
				h.mu.Lock()
				defer h.mu.Unlock()
				assert.Empty(t, h.revoked, "the handler must not receive a revocation under continue-fenced")
			})
		})
	})

	t.Run("ShardLoad", func(t *testing.T) {
		t.Parallel()

		t.Run("reports the load of each replica to the controller", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				load := shard.Load{cpu: 0.5}
				ctx, stop := context.WithCancel(t.Context())
				t.Cleanup(stop)
				a := f.run(t, ctx, &loaded{application: f.app, load: load})
				t.Cleanup(func() {
					stop()
					<-a.Done()
				})
				f.serve(t, a, shardA, 1)
				synctest.Wait()
				expect.Equal(t, f.observed(t, shardA).Load, load, "the report must have the load of the replica")
			})
		})
	})
}

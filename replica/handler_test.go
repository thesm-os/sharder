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

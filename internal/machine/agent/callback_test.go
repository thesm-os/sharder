// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// otherIdentity is the identity of a caller other than the controller.
const otherIdentity shard.Identity = "intruder"

// incB is a namespace incarnation other than the incarnation of the tests.
var incB = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: 0x1a, 15: 0x0b})}

// TestCallback checks the callbacks that an agent takes and refuses.
func TestCallback(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give func(*protocol.Request)
			want error
			name string
		}{
			{
				name: "returns ErrRequest for a request that does not encode",
				give: func(r *protocol.Request) { r.Call = 99 },
				want: agent.ErrRequest,
			},
			{
				name: "returns ErrUnseen for a controller epoch above the record of the controller",
				give: func(r *protocol.Request) { r.ControllerEpoch = cellTerm + 1 },
				want: agent.ErrUnseen,
			},
			{
				name: "returns protocol.ErrUnsupported for a request without a callback",
				give: func(r *protocol.Request) { r.Call = 0 },
				want: protocol.ErrUnsupported,
			},
			{
				name: "returns protocol.ErrUnsupported for a role other than the writer role",
				give: func(r *protocol.Request) { r.Role = shard.RoleOffline },
				want: protocol.ErrUnsupported,
			},
			{
				name: "returns ErrRequest for a request on the path of a cleanup",
				give: func(r *protocol.Request) { r.Path = protocol.Cleanup },
				want: agent.ErrRequest,
			},
			{
				name: "returns ErrRequest for a request without a path",
				give: func(r *protocol.Request) { r.Path = 0 },
				want: agent.ErrRequest,
			},
			{
				name: "returns ErrRequest for a request without an epoch",
				give: func(r *protocol.Request) { r.Epoch = epoch.Zero },
				want: agent.ErrRequest,
			},
			{
				name: "returns ErrStale for a request of another cell",
				give: func(r *protocol.Request) { r.Scope.Cell = otherCell },
				want: agent.ErrStale,
			},
			{
				name: "returns ErrStale for a request of another namespace incarnation",
				give: func(r *protocol.Request) { r.Operation.Incarnation = incB },
				want: agent.ErrStale,
			},
			{
				name: "returns ErrStale for a request of another node",
				give: func(r *protocol.Request) { r.Node = otherNode },
				want: agent.ErrStale,
			},
			{
				name: "returns ErrNotServing for a later incarnation of the node",
				give: func(r *protocol.Request) { r.NodeIncarnation++ },
				want: agent.ErrNotServing,
			},
			{
				name: "returns ErrStale for an earlier incarnation of the node",
				give: func(r *protocol.Request) { r.NodeIncarnation-- },
				want: agent.ErrStale,
			},
			{
				name: "returns ErrStale for a controller epoch below the epoch of the record",
				give: func(r *protocol.Request) { r.ControllerEpoch = cellTerm - 1 },
				want: agent.ErrStale,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t)
				h.start(t)
				h.place(t, shardA, firstEpoch)
				r := h.request(protocol.Add, shardA, 1, firstEpoch)
				tt.give(r)
				got := replyTo[struct{}](t, h, h.do(t, r))
				expect.ErrorIs(t, got.Err, tt.want, "the agent must refuse the request")
				expect.Empty(t, h.acquires, "a refused request must not reach the fence")
			})
		}

		t.Run("returns protocol.ErrUnsupported for metadata of another major version", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.place(t, shardA, firstEpoch)
			to := h.call()
			md := protocol.Metadata{Caller: controllerIdentity, Version: protocol.Current + 1}
			h.step(t, machine.Do{Request: h.request(protocol.Add, shardA, 1, firstEpoch), Metadata: md, Reply: to})
			assert.ErrorIs(t, replyTo[struct{}](t, h, to).Err, protocol.ErrUnsupported,
				"the agent must refuse metadata that this release does not implement")
		})

		t.Run("returns ErrUnseen before the controller of the cell has a record", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.acquire = false })
			h.start(t)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			assert.ErrorIs(t, got.Err, agent.ErrUnseen, "the agent must wait for the record of the controller")
		})

		t.Run("returns ErrDenied for a caller other than the controller", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.place(t, shardA, firstEpoch)
			to := h.call()
			md := protocol.Metadata{Caller: otherIdentity, Version: protocol.Current}
			h.step(t, machine.Do{Request: h.request(protocol.Add, shardA, 1, firstEpoch), Metadata: md, Reply: to})
			assert.ErrorIs(t, replyTo[struct{}](t, h, to).Err, agent.ErrDenied,
				"the agent must take callbacks only from the controller of the record")
		})

		t.Run("takes a callback without a caller when it skips the identity of callers", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.cfg.Unauthenticated = true })
			h.start(t)
			h.place(t, shardA, firstEpoch)
			to := h.call()
			md := protocol.Metadata{Version: protocol.Current}
			h.step(t, machine.Do{Request: h.request(protocol.Add, shardA, 1, firstEpoch), Metadata: md, Reply: to})
			h.run(t)
			assert.NoError(t, replyTo[struct{}](t, h, to).Err, "an unauthenticated agent must take a callback")
		})

		t.Run("returns ErrStale for the incarnation of a session that has ended", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.place(t, shardA, firstEpoch)
			h.step(t, machine.SessionEnded{Token: h.join})
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			assert.ErrorIs(t, got.Err, agent.ErrStale, "the agent must refuse a callback of an ended session")
		})

		t.Run("returns ErrStale for a drop of the incarnation of a session that has ended", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.step(t, machine.SessionEnded{Token: h.join})
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch)))
			expect.ErrorIs(t, got.Err, agent.ErrStale, "the agent must refuse a drop of an ended session")
			expect.Length(t, h.handles, 1, "a refused drop must not call the handler")
		})

		t.Run("returns ErrStale for an epoch below the highest epoch of the shard", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, secondEpoch)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch)))
			assert.ErrorIs(t, got.Err, agent.ErrStale, "the agent must refuse a lower epoch of the shard")
		})

		t.Run("returns ErrStale for an operation older than the newest operation of the shard", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 2, firstEpoch)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Drop, shardA, 1, firstEpoch)))
			assert.ErrorIs(t, got.Err, agent.ErrStale, "the agent must refuse an older operation of the shard")
		})

		t.Run("returns ErrStale for an operation that has ended", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.place(t, shardA, firstEpoch)
			op := shard.OperationID{Incarnation: incA, Shard: shardA, Sequence: 1}
			h.commitCell(t, &store.Mutation{Transitions: []store.Transition{transition(op, shard.PhaseRunning)}})
			h.commitCell(t, &store.Mutation{Done: []shard.ID{shardA}})
			h.follow(t)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			assert.ErrorIs(t, got.Err, agent.ErrStale, "the agent must refuse a callback of an operation that ended")
		})

		t.Run("returns ErrStale for a forward callback of an operation on its abort path", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			abort := h.request(protocol.Drop, shardA, 1, firstEpoch)
			abort.Path = protocol.Abort
			assert.NoError(t, replyTo[struct{}](t, h, h.do(t, abort)).Err, "the agent must take the abort path")
			forward := h.request(protocol.Drop, shardA, 1, firstEpoch)
			forward.Step = 1
			got := replyTo[struct{}](t, h, h.do(t, forward))
			assert.ErrorIs(t, got.Err, agent.ErrStale, "the agent must refuse the forward path after the abort path")
		})

		t.Run("takes a callback on the abort path of an operation on its abort path", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			first := h.request(protocol.Drop, shardA, 1, firstEpoch)
			first.Path = protocol.Abort
			h.do(t, first)
			second := h.request(protocol.Drop, shardA, 1, firstEpoch)
			second.Path, second.Step = protocol.Abort, 1
			assert.NoError(t, replyTo[struct{}](t, h, h.do(t, second)).Err,
				"the agent must take the later steps of the abort path")
		})

		t.Run("takes a callback of a newer operation after an operation ended", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.place(t, shardA, firstEpoch)
			op := shard.OperationID{Incarnation: incA, Shard: shardA, Sequence: 1}
			h.commitCell(t, &store.Mutation{Transitions: []store.Transition{transition(op, shard.PhaseAborting)}})
			h.commitCell(t, &store.Mutation{Done: []shard.ID{shardA}})
			h.follow(t)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 2, firstEpoch)))
			assert.NoError(t, got.Err, "the end and the abort path of an operation must not refuse a newer one")
		})

		t.Run("returns the kept result of a key without the handler", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			expect.NoError(t, got.Err, "a repeated key must get the kept result")
			expect.Length(t, h.handles, 1, "a repeated key must not call the handler again")
		})

		t.Run("returns the kept result to each repeat of a key", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			first := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			second := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			expect.NoError(t, first.Err, "the first repeat must get the kept result")
			expect.NoError(t, second.Err, "the second repeat must get the kept result")
			expect.Length(t, h.handles, 1, "a repeated key must not call the handler again")
		})

		t.Run("returns the kept error of a key without the handler", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.handler = func(*protocol.Request) error { return errBroken }
			h.place(t, shardA, firstEpoch)
			h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			expect.ErrorIs(t, got.Err, errBroken, "a repeated key must get the kept error")
			expect.Length(t, h.handles, 1, "a repeated key must not call the handler again")
		})

		t.Run("calls the handler again for a key whose error was transient", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.handler = func(*protocol.Request) error { return errBusy }
			h.place(t, shardA, firstEpoch)
			h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.handler = nil
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			expect.NoError(t, got.Err, "a retry of a transient error must run the callback again")
			expect.Length(t, h.handles, 2, "the agent must not keep a transient error")
		})

		t.Run("returns ErrReused for a kept key with another payload", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			r := h.request(protocol.Add, shardA, 1, firstEpoch)
			r.Range = shard.Range{Start: []byte{0x01}}
			got := replyTo[struct{}](t, h, h.do(t, r))
			assert.ErrorIs(t, got.Err, agent.ErrReused, "the agent must refuse a key with another payload")
		})

		t.Run("returns ErrReused for the key of a callback that runs with another payload", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Handle); return ok }
			h.place(t, shardA, firstEpoch)
			h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			r := h.request(protocol.Add, shardA, 1, firstEpoch)
			r.Range = shard.Range{Start: []byte{0x01}}
			got := replyTo[struct{}](t, h, h.do(t, r))
			assert.ErrorIs(t, got.Err, agent.ErrReused, "the agent must refuse a key with another payload")
		})

		t.Run("replies to a repeated key of a callback that runs with its result", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Handle); return ok }
			h.place(t, shardA, firstEpoch)
			first := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			second := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.release(t)
			expect.NoError(t, replyTo[struct{}](t, h, first).Err, "the first call must get the result")
			expect.NoError(t, replyTo[struct{}](t, h, second).Err, "the repeated call must get the same result")
			expect.Length(t, h.handles, 1, "the repeated key must not call the handler again")
		})

		t.Run("waits for the placement of an add before it acquires the fence", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			expect.Empty(t, h.acquires, "the add must wait for its placement")
			h.place(t, shardA, firstEpoch)
			expect.Length(t, h.acquires, 1, "the placement must start the add")
			expect.NoError(t, replyTo[struct{}](t, h, to).Err, "the add must succeed once its placement arrives")
		})

		t.Run("waits for the placement of an add at the epoch of the add", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.place(t, shardA, firstEpoch)
			h.do(t, h.request(protocol.Add, shardA, 2, secondEpoch))
			assert.Empty(t, h.acquires, "the add must wait for the placement at its epoch")
		})
	})
}

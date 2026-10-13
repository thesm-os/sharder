// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/history"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// The clients of the history of the property of Token, and the bounds of a case.
const (
	// readers is the number of clients that call Token while the agent steps.
	readers = 2
	// reads is the most rounds of calls of Token of a reader, one call for each shard in a
	// round.
	reads = 100
	// stepClient is the first client of the history that records the steps of the agent.
	// The client of the shard i is stepClient + i.
	stepClient = 100
	// scriptSize is the most entries of the script of a case.
	scriptSize = 12
	// within is the time that the clients of a case may take.
	within = time.Minute
)

// The names of the operations of the history of the property of Token.
const (
	// stepOperation is a step of the agent.
	stepOperation = "step"
	// tokenOperation is a call of Token.
	tokenOperation = "token"
)

// The kinds of the entries of the script of the property of Token.
const (
	// scriptServe places the shard on the node at its next epoch, and adds it.
	scriptServe = 0
	// scriptDrop drops the tenure of the shard.
	scriptDrop = 1
	// scriptMove places the shard on another node, so the agent cleans up its tenure.
	scriptMove = 2
	// scriptRejoin ends the session of the node, and joins again.
	scriptRejoin = 3
	// scriptKinds is the number of kinds.
	scriptKinds = 4
)

// tokenState is what Token returns for one shard: the token, and whether Token returned it
// without an error.
type tokenState struct {
	// token is the token, and the zero Token with an error.
	token fence.Token
	// ok is true when Token returned no error.
	ok bool
}

// tokenSpec is the sequential specification of what Token returns for one shard. A step of
// the agent leaves the state that Token returned after it, and a call of Token returns the
// state.
var tokenSpec = history.Spec[tokenState]{
	Initial: func() tokenState { return tokenState{} },
	Next: func(s tokenState, op history.Operation) []tokenState {
		switch op.Name {
		case stepOperation:
			next, _ := op.Output.(tokenState)
			return []tokenState{next}
		case tokenOperation:
			if op.Returned(s) {
				return []tokenState{s}
			}
		}
		return nil
	},
}

// TestToken checks the fencing tokens that an agent publishes.
func TestToken(t *testing.T) {
	t.Parallel()

	t.Run("Token", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrNotWriter for a shard without a tenure", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			_, err := h.machine.Token(shardA)
			assert.ErrorIs(t, err, agent.ErrNotWriter, "a shard without a tenure has no token")
		})

		t.Run("returns ErrNotWriter while the fence of an add is acquired", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Acquire); return ok }
			h.place(t, shardA, firstEpoch)
			h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			_, err := h.machine.Token(shardA)
			assert.ErrorIs(t, err, agent.ErrNotWriter, "a tenure without its fence has no token")
		})

		t.Run("returns the token of a tenure once its fence is acquired", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Handle); return ok }
			h.place(t, shardA, secondEpoch)
			h.do(t, h.request(protocol.Add, shardA, 1, secondEpoch))
			got, err := h.machine.Token(shardA)
			assert.NoError(t, err, "the handler of an add must get the token of its tenure")
			want := fence.Token{
				Namespace: namespace, Cell: cellID, Incarnation: incA, Shard: shardA, Epoch: secondEpoch,
			}
			assert.Equal(t, got, want, "the token must name the tenure")
		})

		t.Run("returns an error that wraps epoch.ErrFenced after the agent has stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.step(t, machine.Stopped{})
			_, err := h.machine.Token(shardA)
			expect.ErrorIs(t, err, epoch.ErrFenced, "a stopped agent must revoke every token")
		})

		t.Run("returns the token that a step published while the agent steps", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Token returns the token that a step of the agent published for the shard", race)
		})
	})
}

// TestTokenAllocs checks that Token does not allocate. The shard A has a tenure, the node has
// dropped the tenure of the shard B, and the drop of the tenure of the shard C failed.
func TestTokenAllocs(t *testing.T) {
	h := newHarness(t)
	h.start(t)
	h.serve(t, shardA, 1, firstEpoch)
	h.serve(t, shardB, 1, firstEpoch)
	h.serve(t, shardC, 1, firstEpoch)
	h.do(t, h.request(protocol.Drop, shardB, 2, firstEpoch))
	h.handler = func(*protocol.Request) error { return errBroken }
	h.do(t, h.request(protocol.Drop, shardC, 2, firstEpoch))
	m := h.machine

	t.Run("Token", func(t *testing.T) {
		t.Run("of a tenure", func(t *testing.T) {
			var err error
			expect.MaxAllocs(t, func() { _, err = m.Token(shardA) }, 0, "Token of a tenure must not allocate")
			expect.NoError(t, err, "the measurement must take the token of a tenure")
		})

		t.Run("of a shard without a tenure", func(t *testing.T) {
			var err error
			expect.MaxAllocs(t, func() { _, err = m.Token(shardB) }, 0, "Token without a tenure must not allocate")
			expect.ErrorIs(t, err, agent.ErrNotWriter, "the measurement must take a shard without a tenure")
		})

		t.Run("of a tenure whose token is revoked", func(t *testing.T) {
			var err error
			expect.MaxAllocs(t, func() { _, err = m.Token(shardC) }, 0, "Token of a revoked tenure must not allocate")
			expect.ErrorIs(t, err, epoch.ErrFenced, "the measurement must take a revoked tenure")
		})
	})
}

// BenchmarkToken measures Token.
func BenchmarkToken(b *testing.B) {
	h := newHarness(b)
	h.start(b)
	h.serve(b, shardA, 1, firstEpoch)
	m := h.machine

	b.Run("Token", func(b *testing.B) {
		c := bench.Start(b).MaxAllocs(0)
		defer c.End()
		var got fence.Token
		var err error
		for c.Loop() {
			got, err = m.Token(shardA)
		}
		assert.NoError(b, err, "the benchmark must measure a tenure")
		assert.Equal(b, got.Shard, shardA, "the benchmark must return the token of the shard")
	})
}

// race plays a script that c draws on one client, while the readers call Token for each
// shard. Each step of the agent is a call of the history for each shard, whose output is
// what Token returns for the shard after the step. The history must be linearizable against
// tokenSpec, one shard at a time.
func race(c *prop.Case) {
	h := newHarness(c)
	h.start(c)
	entry := prop.Integer(0, scriptKinds*len(allShards)-1)
	script := c.Draw(prop.List(entry, prop.MinSize(1), prop.MaxSize(scriptSize)), "script")
	calls := c.History()
	h.wrap = func(step func()) {
		open := make([]history.Call, 0, len(allShards))
		for i, sid := range allShards {
			open = append(open, calls.Invoke(stepClient+i, stepOperation, nil, sid))
		}
		step()
		for i, sid := range allShards {
			tok, err := h.machine.Token(sid)
			open[i].OK(tokenState{token: tok, ok: err == nil})
		}
	}
	done := make(chan struct{})
	outcomes := history.Concurrently(readers+1, within, func(client int) (any, error) {
		if client == 0 {
			defer close(done)
			play(c, h, script)
			return nil, nil
		}
		for range reads {
			select {
			case <-done:
				return nil, nil
			default:
			}
			for _, sid := range allShards {
				call := calls.Invoke(client, tokenOperation, nil, sid)
				tok, err := h.machine.Token(sid)
				call.OK(tokenState{token: tok, ok: err == nil})
			}
		}
		return nil, nil
	})
	for _, o := range outcomes {
		assert.True(c, o.Finished, "every client of the property must finish")
	}
	history.Linearizable(
		c,
		calls,
		tokenSpec,
		"Token must return the token that a step of the agent published for the shard",
	)
}

// play plays script on the agent of h. Each entry is a kind of step for one shard: the kind
// is the entry divided by the number of shards, and the shard is the remainder.
func play(c *prop.Case, h *harness, script []int) {
	epochs := make(map[shard.ID]epoch.Epoch)
	operations := make(map[shard.ID]uint64)
	for _, entry := range script {
		sid := allShards[entry%len(allShards)]
		operations[sid]++
		switch entry / len(allShards) {
		case scriptServe:
			epochs[sid]++
			h.place(c, sid, epochs[sid])
			h.do(c, h.request(protocol.Add, sid, operations[sid], epochs[sid]))
		case scriptDrop:
			h.do(c, h.request(protocol.Drop, sid, operations[sid], epochs[sid]))
		case scriptMove:
			epochs[sid]++
			h.unplace(c, sid, epochs[sid])
		case scriptRejoin:
			h.step(c, machine.SessionEnded{Token: h.join})
			h.elapse(c, firstBackoff)
		}
	}
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// otherNamespace is a namespace that the dispatch of the tests has no agent of.
const otherNamespace shard.Namespace = "payments"

// errHandler is the error of the server of a test that fails its calls.
var errHandler = errors.New("protocol_test: the handler failed")

// recorder is an AgentServer that records the requests that it receives, and returns err.
type recorder struct {
	err      error
	requests []*protocol.Request
	mu       sync.Mutex
}

// Do records r, and returns the error of the recorder.
func (s *recorder) Do(_ context.Context, _ protocol.Metadata, r *protocol.Request) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, r)
	return s.err
}

// TestAgent checks the dispatch of the callbacks of a process to the root and the agents
// of its namespaces.
func TestAgent(t *testing.T) {
	t.Parallel()

	current := protocol.Metadata{Version: protocol.Current}

	t.Run("Dispatch", func(t *testing.T) {
		t.Parallel()

		t.Run("passes a request for a root scope to the root", func(t *testing.T) {
			t.Parallel()
			root, agent := &recorder{}, &recorder{}
			r := protocol.Request{Scope: shard.Scope{Namespace: recordedNamespace}}
			err := protocol.Dispatch(root, map[shard.Namespace]protocol.AgentServer{recordedNamespace: agent}).
				Do(t.Context(), current, &r)
			assert.NoError(t, err, "Do returns the result of the root")
			assert.Length(t, root.requests, 1, "the root receives one request")
			expect.Equal(t, root.requests[0], &r, "the root receives the request", assert.ByIdentity())
			expect.Empty(t, agent.requests, "the agent of the namespace receives nothing")
		})

		t.Run("passes a request for a cell to the agent of its namespace", func(t *testing.T) {
			t.Parallel()
			root, agent := &recorder{}, &recorder{}
			r := protocol.Request{Scope: recordedScope}
			err := protocol.Dispatch(root, map[shard.Namespace]protocol.AgentServer{recordedNamespace: agent}).
				Do(t.Context(), current, &r)
			assert.NoError(t, err, "Do returns the result of the agent")
			assert.Length(t, agent.requests, 1, "the agent of the namespace receives one request")
			expect.Equal(
				t,
				agent.requests[0],
				&r,
				"the agent of the namespace receives the request",
				assert.ByIdentity(),
			)
			expect.Empty(t, root.requests, "the root receives nothing")
		})

		t.Run("returns ErrUnreachable for a namespace without an agent", func(t *testing.T) {
			t.Parallel()
			r := protocol.Request{Scope: shard.Scope{Namespace: otherNamespace, Cell: recordedCell}}
			err := protocol.Dispatch(&recorder{}, map[shard.Namespace]protocol.AgentServer{recordedNamespace: &recorder{}}).
				Do(t.Context(), current, &r)
			assert.ErrorIs(
				t,
				err,
				protocol.ErrUnreachable,
				"Do returns ErrUnreachable for a namespace without an agent",
			)
		})

		t.Run("returns ErrUnsupported for a call of another major version", func(t *testing.T) {
			t.Parallel()
			root, agent := &recorder{}, &recorder{}
			r := protocol.Request{Scope: recordedScope}
			err := protocol.Dispatch(root, map[shard.Namespace]protocol.AgentServer{recordedNamespace: agent}).
				Do(t.Context(), protocol.Metadata{Version: protocol.Current + 1}, &r)
			assert.ErrorIs(t, err, protocol.ErrUnsupported, "Do refuses a call of another major version")
			expect.Empty(t, agent.requests, "the agent receives no call of another major version")
			expect.Empty(t, root.requests, "the root receives no call of another major version")
		})

		t.Run("returns the error of the server", func(t *testing.T) {
			t.Parallel()
			r := protocol.Request{Scope: recordedScope}
			agent := &recorder{err: errHandler}
			err := protocol.Dispatch(&recorder{}, map[shard.Namespace]protocol.AgentServer{recordedNamespace: agent}).
				Do(t.Context(), current, &r)
			assert.ErrorIs(t, err, errHandler, "Do returns the error of the agent")
		})
	})
}

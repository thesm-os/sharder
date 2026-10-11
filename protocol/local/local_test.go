// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package local_test

import (
	"context"
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/local"
	"go.thesmos.sh/sharder/protocol/protocoltest"
	"go.thesmos.sh/sharder/shard"
)

// The names of the servers of the tests.
const (
	namespace shard.Namespace = "orders"
	cell      shard.CellID    = "c1"
	nodeID    shard.NodeID    = "n1"
)

// The scope, the node and the metadata of the tests.
var (
	// scope is the scope of the controller of the tests.
	scope = shard.Scope{Namespace: namespace, Cell: cell}
	// node is the node of the agent of the tests.
	node = shard.Node{ID: nodeID, Cell: cell}
	// current is the metadata of a process of this release.
	current = protocol.Metadata{Version: protocol.Current}
)

// TestLocal checks the laws of the protocol against Local, and the calls of a Local to a
// server that its network does not contain.
func TestLocal(t *testing.T) {
	t.Parallel()

	t.Run("passes the laws of the protocol", func(t *testing.T) {
		t.Parallel()
		protocoltest.AssertTransport(t, func(_ assert.TB, md protocol.Metadata, e *protocoltest.Endpoints) (
			protocol.AgentClient, protocol.ControllerClient,
		) {
			n := local.NewNetwork()
			n.AddAgent(e.Scope.Namespace, e.Node.ID, e.Agent)
			n.AddController(e.Scope, e.Controller)
			l := n.Local(md)
			return l, l
		})
	})

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		t.Run("returns a client of a network of its own", func(t *testing.T) {
			t.Parallel()
			l := local.New(current)
			assert.NotEqual(t, l.Network(), local.New(current).Network(), "each process has a network of its own",
				assert.ByIdentity())
		})
	})

	tests := []struct {
		name string
		call func(ctx context.Context, l *local.Local) error
	}{
		{name: "Do", call: func(ctx context.Context, l *local.Local) error {
			return l.Do(ctx, &node, &protocol.Request{Scope: scope})
		}},
		{name: "Report", call: func(ctx context.Context, l *local.Local) error {
			_, err := l.Report(ctx, &shard.Cell{ID: cell}, &protocol.Report{Scope: scope})
			return err
		}},
		{name: "Submit", call: func(ctx context.Context, l *local.Local) error {
			_, err := l.Submit(ctx, &shard.Cell{ID: cell}, &proposal.Proposal{Scope: scope})
			return err
		}},
		{name: "Disrupt", call: func(ctx context.Context, l *local.Local) error {
			_, err := l.Disrupt(ctx, &shard.Cell{ID: cell}, &budget.Disruption{Scope: scope})
			return err
		}},
		{name: "Done", call: func(ctx context.Context, l *local.Local) error {
			return l.Done(ctx, &shard.Cell{ID: cell}, &protocol.DoneArgs{Scope: scope})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("returns ErrUnreachable for a server that the network does not contain", func(t *testing.T) {
				t.Parallel()
				assert.ErrorIs(t, tt.call(t.Context(), local.New(current)), protocol.ErrUnreachable,
					"a call to a server that the network does not contain returns ErrUnreachable")
			})
		})
	}
}

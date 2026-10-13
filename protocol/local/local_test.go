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

// TestLocal checks the laws of the protocol against Local, the route of each call of a
// controller to the address in the record of its cell, and the calls of a Local to a server
// that its network does not contain.
func TestLocal(t *testing.T) {
	t.Parallel()

	t.Run("passes the laws of the protocol", func(t *testing.T) {
		t.Parallel()
		protocoltest.AssertTransport(t, func(_ assert.TB, md protocol.Metadata, e *protocoltest.Endpoints) (
			protocol.AgentClient, protocol.ControllerClient,
		) {
			n := local.NewNetwork()
			n.AddAgent(e.Scope.Namespace, e.Node.ID, e.Agent)
			e.Cell.Controller = controllerA
			n.AddController(controllerA, e.Controller)
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

	t.Run("Do", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrUnreachable for an agent that the network does not contain", func(t *testing.T) {
			t.Parallel()
			err := local.New(current).Do(t.Context(), &node, &protocol.Request{Scope: scope})
			assert.ErrorIs(t, err, protocol.ErrUnreachable,
				"a callback to an agent that the network does not contain returns ErrUnreachable")
		})
	})

	tests := []struct {
		call func(ctx context.Context, l *local.Local, record *shard.Cell) error
		name string
	}{
		{name: "Report", call: func(ctx context.Context, l *local.Local, record *shard.Cell) error {
			_, err := l.Report(ctx, record, &protocol.Report{Scope: scope})
			return err
		}},
		{name: "Submit", call: func(ctx context.Context, l *local.Local, record *shard.Cell) error {
			_, err := l.Submit(ctx, record, &proposal.Proposal{Scope: scope})
			return err
		}},
		{name: "Disrupt", call: func(ctx context.Context, l *local.Local, record *shard.Cell) error {
			_, err := l.Disrupt(ctx, record, &budget.Disruption{Scope: scope})
			return err
		}},
		{name: "Done", call: func(ctx context.Context, l *local.Local, record *shard.Cell) error {
			return l.Done(ctx, record, &protocol.DoneArgs{Scope: scope})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("passes the call to the controller at the address in the record of the cell", func(t *testing.T) {
				t.Parallel()
				n := local.NewNetwork()
				n.AddController(controllerA, failing{err: errFirst})
				n.AddController(controllerB, failing{err: errSecond})
				err := tt.call(t.Context(), n.Local(current), &shard.Cell{ID: cell, Controller: controllerB})
				assert.ErrorIs(t, err, errSecond, "the call reaches the controller at the address of the record")
			})

			t.Run("returns ErrUnreachable for an address that the network does not contain", func(t *testing.T) {
				t.Parallel()
				err := tt.call(t.Context(), local.New(current), &shard.Cell{ID: cell, Controller: controllerA})
				assert.ErrorIs(t, err, protocol.ErrUnreachable,
					"a call to an address that the network does not contain returns ErrUnreachable")
			})

			t.Run("returns ErrUnreachable for a record without the address of its controller", func(t *testing.T) {
				t.Parallel()
				n := local.NewNetwork()
				n.AddController(controllerA, failing{err: errFirst})
				err := tt.call(t.Context(), n.Local(current), &shard.Cell{ID: cell})
				assert.ErrorIs(t, err, protocol.ErrUnreachable,
					"a call without the address of a controller returns ErrUnreachable")
			})

			t.Run("returns ErrUnreachable without a record", func(t *testing.T) {
				t.Parallel()
				n := local.NewNetwork()
				n.AddController(controllerA, failing{err: errFirst})
				err := tt.call(t.Context(), n.Local(current), nil)
				assert.ErrorIs(t, err, protocol.ErrUnreachable, "a call without a record returns ErrUnreachable")
			})
		})
	}
}

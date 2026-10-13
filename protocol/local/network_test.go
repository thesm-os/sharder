// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package local_test

import (
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/local"
	"go.thesmos.sh/sharder/shard"
)

// TestNetwork checks that a later server of an address replaces the earlier one.
func TestNetwork(t *testing.T) {
	t.Parallel()

	t.Run("AddAgent", func(t *testing.T) {
		t.Parallel()

		t.Run("replaces the agent of the same namespace and node", func(t *testing.T) {
			t.Parallel()
			n := local.NewNetwork()
			n.AddAgent(namespace, nodeID, failing{err: errFirst})
			n.AddAgent(namespace, nodeID, failing{err: errSecond})
			err := n.Local(current).Do(t.Context(), &node, &protocol.Request{Scope: scope})
			assert.ErrorIs(t, err, errSecond, "the network passes a callback to the later agent")
		})

		t.Run("adds an agent for the clients that the network returned before", func(t *testing.T) {
			t.Parallel()
			n := local.NewNetwork()
			l := n.Local(current)
			r := protocol.Request{Scope: scope}
			assert.ErrorIs(t, l.Do(t.Context(), &node, &r), protocol.ErrUnreachable,
				"a callback before AddAgent returns ErrUnreachable")
			n.AddAgent(namespace, nodeID, failing{err: errFirst})
			assert.ErrorIs(t, l.Do(t.Context(), &node, &r), errFirst,
				"a callback after AddAgent returns the error of the agent")
		})
	})

	t.Run("AddController", func(t *testing.T) {
		t.Parallel()

		t.Run("replaces the controller of the same address", func(t *testing.T) {
			t.Parallel()
			n := local.NewNetwork()
			n.AddController(controllerA, failing{err: errFirst})
			n.AddController(controllerA, failing{err: errSecond})
			record := shard.Cell{ID: cell, Controller: controllerA}
			_, err := n.Local(current).Report(t.Context(), &record, &protocol.Report{Scope: scope})
			assert.ErrorIs(t, err, errSecond, "the network passes a report to the later controller")
		})

		t.Run("adds a controller for the clients that the network returned before", func(t *testing.T) {
			t.Parallel()
			n := local.NewNetwork()
			l := n.Local(current)
			record := shard.Cell{ID: cell, Controller: controllerA}
			r := protocol.Report{Scope: scope}
			_, err := l.Report(t.Context(), &record, &r)
			assert.ErrorIs(t, err, protocol.ErrUnreachable, "a report before AddController returns ErrUnreachable")
			n.AddController(controllerA, failing{err: errFirst})
			_, err = l.Report(t.Context(), &record, &r)
			assert.ErrorIs(t, err, errFirst, "a report after AddController returns the error of the controller")
		})
	})
}

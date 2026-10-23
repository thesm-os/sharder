// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package handoff_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/shard"
)

const (
	// nodeA is the node that a transfer of the tests takes the replica from.
	nodeA shard.NodeID = "node-a"
	// nodeB is the node that a transfer of the tests gives the replica to.
	nodeB shard.NodeID = "node-b"
	// nodeF is the node that becomes the writer on the abort path of a transfer of the
	// tests.
	nodeF shard.NodeID = "node-f"
)

// everySide is a transfer with a node on every side.
var everySide = handoff.Transfer{From: nodeA, To: nodeB, Fallback: nodeF}

// TestProtocol checks the sides of a transfer.
func TestProtocol(t *testing.T) {
	t.Parallel()

	t.Run("Transfer", func(t *testing.T) {
		t.Parallel()

		t.Run("Node", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name string
				give handoff.Side
				want shard.NodeID
			}{
				{name: "returns From for the side From", give: handoff.From, want: nodeA},
				{name: "returns To for the side To", give: handoff.To, want: nodeB},
				{name: "returns Fallback for the side Fallback", give: handoff.Fallback, want: nodeF},
				{name: "returns the empty node for the zero Side", give: 0, want: ""},
				{name: "returns the empty node for a value above Fallback", give: handoff.Fallback + 1, want: ""},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					assert.Equal(t, everySide.Node(tt.give), tt.want, "Node must return the node of the side")
				})
			}

			t.Run("returns the empty node for every side of the zero Transfer", func(t *testing.T) {
				t.Parallel()
				prop.Equal(t, handoff.Transfer{}.Node, func(handoff.Side) shard.NodeID { return "" },
					"the zero Transfer must have no node on any side",
					prop.Using(prop.Integer[handoff.Side](0, 255)))
			})
		})
	})
}

// TestProtocolAllocs checks the allocation contract of Transfer. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestProtocolAllocs(t *testing.T) {
	side := handoff.Fallback

	t.Run("Transfer", func(t *testing.T) {
		t.Run("Node", func(t *testing.T) {
			var got shard.NodeID
			expect.MaxAllocs(t, func() { got = everySide.Node(side) }, 0, "Node must not allocate")
			assert.Equal(t, got, nodeF, "the test must measure the node of a side")
		})
	})
}

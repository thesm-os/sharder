// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package membership_test

import (
	"slices"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/shard"
)

// The labels of the values that the properties of View generate.
const (
	drawNodes  = "nodes"
	drawAbsent = "absent"
)

// sortedNodes returns a generator of node lists in the order of their IDs, with
// distinct IDs of up to four letters, as the Nodes of a View are.
func sortedNodes() prop.Generator[[]shard.Node] {
	ids := prop.List(prop.String(prop.Alphabet("abcd"), prop.MinSize(1), prop.MaxSize(4)), prop.Unique(),
		prop.MaxSize(16))
	return ids.Map(func(ids []string) []shard.Node {
		slices.Sort(ids)
		nodes := make([]shard.Node, 0, len(ids))
		for i, id := range ids {
			nodes = append(nodes, shard.Node{ID: shard.NodeID(id), Address: id, Incarnation: epoch.Epoch(i + 1)})
		}
		return nodes
	})
}

// TestMembership checks the lookup of a node in a View.
func TestMembership(t *testing.T) {
	t.Parallel()

	t.Run("View", func(t *testing.T) {
		t.Parallel()

		t.Run("Node", func(t *testing.T) {
			t.Parallel()

			t.Run("returns each node of the view", func(t *testing.T) {
				t.Parallel()
				prop.ForAll(t, "Node returns every node that the view lists", func(c *prop.Case) {
					v := membership.View{Nodes: c.Draw(sortedNodes(), drawNodes)}
					assert.Total(c, func(want shard.Node) error {
						got, ok := v.Node(want.ID)
						assert.True(c, ok, "Node must find a node of the view")
						assert.Equal(c, got, want, "Node must return the node with the ID")
						return nil
					}, v.Nodes, "Node must find every node of the view")
				})
			})

			t.Run("reports false for an ID that the view does not list", func(t *testing.T) {
				t.Parallel()
				prop.ForAll(t, "Node reports false for an ID outside the view", func(c *prop.Case) {
					v := membership.View{Nodes: c.Draw(sortedNodes(), drawNodes)}
					absent := c.Draw(prop.String(prop.Alphabet("abcde"), prop.MaxSize(5)), drawAbsent)
					c.Assume(!slices.ContainsFunc(v.Nodes, func(n shard.Node) bool { return string(n.ID) == absent }))
					_, ok := v.Node(shard.NodeID(absent))
					assert.False(c, ok, "Node must report false for an ID that the view does not list")
				})
			})
		})
	})
}

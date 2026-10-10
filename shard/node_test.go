// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard_test

import (
	"encoding/hex"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/shard"
)

// recordedNodeHex pins the encoding of recordedNode, which the membership of every
// cell persists. Field 1 is the ID n1, field 2 the incarnation 5, field 3 the cell
// c1, and field 4 the address h:1. Field 5 is the label zone=a, as one element with
// the length of its encoding. Field 6 is the capacity cpu=2, as the key with its
// length and the little-endian bits of the float. Field 7 is the identity i1.
const recordedNodeHex = "0a02" + "6e31" +
	"1005" +
	"1a02" + "6331" +
	"2203" + "683a31" +
	"2a0a" + "09" + "0a04" + "7a6f6e65" + "1201" + "61" +
	"320c" + "03" + "637075" + "0000000000000040" +
	"3a02" + "6931"

// recordedNode is the node whose encoding recordedNodeHex pins.
var recordedNode = shard.Node{
	ID:          "n1",
	Incarnation: 5,
	Cell:        "c1",
	Address:     "h:1",
	Labels:      tag.Tags{{Key: "zone", Value: "a"}},
	Capacity:    map[string]float64{"cpu": 2},
	Identity:    "i1",
}

// TestNode checks the recorded encoding of a Node.
func TestNode(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Node", func(t *testing.T) {
			t.Parallel()
			n := recordedNode
			got, err := n.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Node")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedNodeHex,
				"AppendBinary must write fields 1 to 7 as recorded",
			)
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Node of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			var got shard.Node
			assert.NoError(
				t,
				got.UnmarshalBinary(decodeHex(t, recordedNodeHex)),
				"UnmarshalBinary must decode the recorded encoding",
			)
			assert.Equal(t, got, recordedNode, "UnmarshalBinary must return the recorded Node")
		})
	})
}

// TestNodeAllocs checks the allocation contract of Node. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestNodeAllocs(t *testing.T) {
	n := recordedNode

	t.Run("constructs a Node without an allocation", func(t *testing.T) {
		var got shard.Node
		expect.MaxAllocs(t, func() {
			got = shard.Node{
				ID: n.ID, Incarnation: n.Incarnation, Cell: n.Cell, Address: n.Address, Labels: n.Labels,
				Capacity: n.Capacity, Identity: n.Identity,
			}
		}, 0, "constructing a Node must not allocate")
		assert.Equal(t, got, recordedNode, "the test must measure the construction of the Node")
	})
}

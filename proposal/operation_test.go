// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package proposal_test

import (
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/shard"
)

// The recorded JSON of the bodies and the results of the operations. sharderctl writes
// the bodies, and the controller of a cell writes the results.
const (
	// recordedMoveJSON pins the body of a move of the shard 0H289H858P2WC1J6GV3GEHW7S0,
	// the text of the ULID of the bytes 0x11 to 0x20, to the node node-a.
	recordedMoveJSON = `{"node":"node-a","shard":"0H289H858P2WC1J6GV3GEHW7S0"}`
	// recordedNodeJSON pins the body of a drain of the node node-a.
	recordedNodeJSON = `{"node":"node-a"}`
	// recordedShardJSON pins the body of an explain of the shard of recordedMoveJSON.
	recordedShardJSON = `{"shard":"0H289H858P2WC1J6GV3GEHW7S0"}`
	// recordedExplanationJSON pins the result of an explain with one violation of the
	// constraint capacity and the costs of two tiers.
	recordedExplanationJSON = `{"violations":[{"constraint":"capacity","node":"node-a",` +
		`"reason":"node-a is full","role":"primary"}],"costs":[1.5,0]}`

	// recordedNode is the node of the recorded operations.
	recordedNode shard.NodeID = "node-a"
	// recordedConstraint is the constraint of the recorded violation.
	recordedConstraint shard.StrategyName = "capacity"
	// recordedReason is the reason of the recorded violation.
	recordedReason = "node-a is full"
	// recordedCost is the cost of the first tier of the recorded explanation.
	recordedCost = 1.5
)

// recordedShard is the shard of the recorded operations.
var recordedShard = shard.ID{ID128: id.ID128{
	0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
	0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
}}

// TestOperation checks the JSON of the bodies and the results of the operations of a cell.
func TestOperation(t *testing.T) {
	t.Parallel()

	t.Run("MoveArgs", func(t *testing.T) {
		t.Parallel()

		give := proposal.MoveArgs{Shard: recordedShard, Node: recordedNode}

		t.Run("encodes as the recorded JSON", func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(give)
			assert.NoError(t, err, "encoding/json must encode the body of a move")
			assert.Equal(t, string(got), recordedMoveJSON, "the body of a move must keep its recorded JSON")
		})

		t.Run("decodes from its JSON", func(t *testing.T) {
			t.Parallel()
			assert.RoundTrip(t, encode[proposal.MoveArgs], decode[proposal.MoveArgs], give,
				"the body of a move must decode from the JSON that it encodes as")
		})
	})

	t.Run("NodeArgs", func(t *testing.T) {
		t.Parallel()

		give := proposal.NodeArgs{Node: recordedNode}

		t.Run("encodes as the recorded JSON", func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(give)
			assert.NoError(t, err, "encoding/json must encode the body of a drain")
			assert.Equal(t, string(got), recordedNodeJSON, "the body of a drain must keep its recorded JSON")
		})

		t.Run("decodes from its JSON", func(t *testing.T) {
			t.Parallel()
			assert.RoundTrip(t, encode[proposal.NodeArgs], decode[proposal.NodeArgs], give,
				"the body of a drain must decode from the JSON that it encodes as")
		})
	})

	t.Run("ShardArgs", func(t *testing.T) {
		t.Parallel()

		give := proposal.ShardArgs{Shard: recordedShard}

		t.Run("encodes as the recorded JSON", func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(give)
			assert.NoError(t, err, "encoding/json must encode the body of an explain")
			assert.Equal(t, string(got), recordedShardJSON, "the body of an explain must keep its recorded JSON")
		})

		t.Run("decodes from its JSON", func(t *testing.T) {
			t.Parallel()
			assert.RoundTrip(t, encode[proposal.ShardArgs], decode[proposal.ShardArgs], give,
				"the body of an explain must decode from the JSON that it encodes as")
		})
	})

	t.Run("Explanation", func(t *testing.T) {
		t.Parallel()

		give := proposal.Explanation{
			Violations: []proposal.Violation{{
				Constraint: recordedConstraint,
				Node:       recordedNode,
				Reason:     recordedReason,
				Role:       shard.RolePrimary,
			}},
			Costs: []float64{recordedCost, 0},
		}

		t.Run("encodes as the recorded JSON", func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(give)
			assert.NoError(t, err, "encoding/json must encode an explanation")
			assert.Equal(t, string(got), recordedExplanationJSON, "an explanation must keep its recorded JSON")
		})

		t.Run("decodes from its JSON", func(t *testing.T) {
			t.Parallel()
			assert.RoundTrip(t, encode[proposal.Explanation], decode[proposal.Explanation], give,
				"an explanation must decode from the JSON that it encodes as")
		})
	})
}

// encode returns the JSON of v. RoundTrip takes a function of the type of v, which
// json.Marshal is not.
func encode[T any](v T) ([]byte, error) {
	return json.Marshal(v)
}

// decode returns the value of type T that the JSON data encodes.
func decode[T any](data []byte) (T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	return v, err
}

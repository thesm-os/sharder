// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package proposal

import "go.thesmos.sh/sharder/shard"

// MoveArgs is the Body of a [Move] proposal, as JSON. The controller of the cell moves
// the writer of the shard to the node through the handoff protocol.
//
// # Encoding
//
// encoding/json encodes MoveArgs as an object with the keys node and shard. The shard is
// the canonical text of its ULID.
//
// # Concurrency
//
// A MoveArgs is a comparable value.
type MoveArgs struct {
	// Node is the node that receives the writer. It is a live node of the cell.
	Node shard.NodeID `json:"node"`
	// Shard is the shard whose writer moves.
	Shard shard.ID `json:"shard"`
}

// NodeArgs is the Body of a [Drain] or an [Undrain] proposal, as JSON. A drain adds the
// manual drain of the node, and an undrain removes it.
//
// # Encoding
//
// encoding/json encodes NodeArgs as an object with the key node.
//
// # Concurrency
//
// A NodeArgs is a comparable value.
type NodeArgs struct {
	// Node is the node of the drain.
	Node shard.NodeID `json:"node"`
}

// ShardArgs is the Body of an [Explain] proposal, as JSON.
//
// # Encoding
//
// encoding/json encodes ShardArgs as an object with the key shard. The shard is the
// canonical text of its ULID.
//
// # Concurrency
//
// A ShardArgs is a comparable value.
type ShardArgs struct {
	// Shard is the shard to explain.
	Shard shard.ID `json:"shard"`
}

// Explanation is the Result of the receipt of an [Explain] proposal, as JSON. It lists
// the replicas of the shard that a constraint rejects, and the cost of each tier of the
// goals of the placer for the current assignment of the cell.
//
// # Encoding
//
// encoding/json encodes Explanation as an object with the keys violations and costs.
//
// # Concurrency
//
// A copy of an Explanation shares the elements of Violations and Costs with the
// original.
type Explanation struct {
	// Violations lists the replicas of the shard that a constraint rejects, in the order
	// of placement.Validate.
	Violations []Violation `json:"violations"`
	// Costs lists the cost of each tier of the goals, in ascending tier order. Lower is
	// better.
	Costs []float64 `json:"costs"`
}

// Violation is a replica of an explained shard that a constraint rejects.
//
// # Encoding
//
// encoding/json encodes Violation as an object with the keys constraint, node, role and
// reason.
//
// # Concurrency
//
// A Violation is a comparable value.
type Violation struct {
	// Constraint is the name of the constraint.
	Constraint shard.StrategyName `json:"constraint"`
	// Node is the node of the replica.
	Node shard.NodeID `json:"node"`
	// Reason is the text of the error that the constraint returned.
	Reason string `json:"reason"`
	// Role is the role of the replica.
	Role shard.Role `json:"role"`
}

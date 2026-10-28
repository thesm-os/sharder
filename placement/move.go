// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placement

import "go.thesmos.sh/sharder/shard"

// Move adds, removes or relocates one replica of a shard. A move with both a From and
// a To relocates the replica, and the replica keeps its role.
//
// A move whose From equals its To keeps the replica on its node. A constraint checks
// such a move against the state with the replica on the node. Validate and the
// planner check the replicas that a plan keeps this way, and State.Apply refuses such
// a move.
//
// State.Apply refuses the zero Move.
//
// # Concurrency
//
// A Move is a comparable value.
type Move struct {
	// From is the node that the replica leaves. It is empty for a move that adds a
	// replica.
	From shard.NodeID
	// To is the node that receives the replica. It is empty for a move that removes a
	// replica.
	To shard.NodeID
	// Shard is the shard of the replica.
	Shard shard.ID
	// Role is the role of the replica.
	Role shard.Role
}

// Constraint is a hard rule of placement. The planner never applies a move that a
// constraint rejects. Validate rejects a plan that adds or moves a replica that a
// constraint rejects, and reports a replica that the plan keeps and that a constraint
// rejects.
//
// # Concurrency
//
// The planner calls Check from as many goroutines as it evaluates in parallel,
// between the moves that it applies. A Constraint is safe for concurrent use.
//
// # Allocation contract
//
// Check does not allocate.
//
//dokimi:stub
//dokimi:suite
//dokimi:out path=placementtest/
type Constraint interface {
	// Name returns the registered name of the constraint, such as "capacity".
	Name() shard.StrategyName
	// Check returns nil when the constraint admits m in st, and otherwise an error
	// that explains why not. It does not modify st.
	//
	//dokimi:budget allocs=0
	Check(st *State, m Move) error
}

// Goal is a soft objective of placement. The planner minimises the weighted sum of
// the costs of the goals of each tier. A lower tier has priority over a higher one, so
// the planner never raises the cost of a lower tier to lower the cost of a higher one.
//
// # Concurrency
//
// The planner calls Cost and Delta from as many goroutines as it evaluates in
// parallel, between the moves that it applies. A Goal is safe for concurrent use.
//
// # Allocation contract
//
// Delta does not allocate.
//
//dokimi:stub
//dokimi:suite
//dokimi:out path=placementtest/
type Goal interface {
	// Name returns the registered name of the goal, such as "balance".
	Name() shard.StrategyName
	// Cost returns the cost of st. Lower is better. It does not modify st.
	Cost(st *State) float64
	// Delta returns the cost after m minus the cost before m, without applying m. It
	// returns 0 for a move whose From equals its To. It does not modify st.
	//
	//dokimi:budget allocs=0
	Delta(st *State, m Move) float64
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placement

import (
	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/rand"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// Input is everything that a placer reads about one cell in one pass. A placer must
// not modify the Input or anything that it refers to.
//
// The zero Input is not valid, because its model has no name.
//
// # Concurrency
//
// A placer reads an Input from as many goroutines as it evaluates in parallel. The
// caller must not modify an Input while a placer reads it.
type Input struct {
	// Clock is the clock of the controller. A placer reads the time of the pass
	// through it, so a simulator replays a pass under a virtual clock.
	Clock clock.Clock
	// Rand is the only source of randomness that a placer may use. The core seeds it
	// for each pass from Snapshot, so a pass can be replayed. A placer reads it on one
	// goroutine.
	Rand rand.Rand
	// Transitions maps each shard with a transition in flight to the nodes on which the
	// transition can place the replica of the shard. A placer does not plan a move for
	// such a shard.
	Transitions map[shard.ID]InFlight
	// Faults maps a shard to the nodes whose replica of the shard has a permanent
	// fault, as the live incarnations of the nodes reported it. It also lists each node
	// whose live incarnation failed an add of the shard definitively. A placer treats
	// each such replica as missing, and does not place a replica of the shard on those
	// nodes.
	Faults map[shard.ID][]shard.NodeID
	// Draining contains the live nodes whose replicas must move off. A draining node
	// does not accept new replicas, and Nodes does not list it. A node whose value is
	// false does not drain.
	Draining map[shard.NodeID]bool
	// ShardLoad maps a shard to its load over the load window of the namespace. A
	// shard without an entry has the mean load of the shards with one.
	ShardLoad map[shard.ID]shard.Load
	// Model is the replica state model of the namespace. Each shard has Max replicas
	// in each role of the model.
	Model replication.Model
	// Layout lists the shards of the cell.
	Layout partition.Layout
	// Current is the committed assignment of the cell.
	Current shard.Assignment
	// Nodes lists the live nodes of the cell that accept new replicas.
	Nodes []shard.Node
	// Metrics lists the load metrics of the namespace, each with the capacity of a
	// node that it fills.
	Metrics []Metric
	// Snapshot identifies the state that the pass reads.
	Snapshot Snapshot
	// Parallelism is the most evaluations that a placer runs at once, such as the CPU
	// limit of the controller. It is at least 1.
	Parallelism int
	// RepairFrom is the repair cursor. A pass examines the shards that need repair in
	// the order of their IDs, from the first shard whose ID is at or above RepairFrom.
	// After the highest ID it continues from the lowest. RepairFrom is the zero ID in
	// the first pass of a controller.
	RepairFrom shard.ID
	// Emergency reports that shards are under-replicated after failures, or have a
	// replica that Faults lists. A placer then restores the replicas first, and may
	// leave its goals unimproved.
	Emergency bool
}

// Snapshot identifies the state that one pass reads. The controller logs it with
// each plan. The simulator records it with the whole Input of the pass, and replays a
// recorded pass from them.
//
// # Concurrency
//
// A Snapshot is a comparable value.
type Snapshot struct {
	// Term is the epoch of the controller that runs the pass.
	Term epoch.Epoch
	// SpecVersion is the version of the spec that the pass applies.
	SpecVersion epoch.Epoch
	// Generation is the committed generation of the cell that Input.Current is at.
	Generation epoch.Epoch
	// Membership is the revision of the membership view that Input.Nodes lists.
	Membership epoch.Epoch
	// Reports counts the reports of agents that the controller accepted in its term
	// before the pass. It identifies the loads of Input.ShardLoad and the faults of
	// Input.Faults.
	Reports uint64
}

// InFlight is the part of a transition in flight that placement accounts for: the
// nodes on which the transition can place the replica of the shard, and the role of
// that replica. The load of the shard counts on both nodes until the transition ends,
// and so does the shard in the peak counts of the nodes.
//
// # Concurrency
//
// An InFlight is a comparable value.
type InFlight struct {
	// To is the destination of the transition. It is empty for a transition that
	// only removes a replica.
	To shard.NodeID
	// Fallback is the node that the abort path of the transition makes the writer. It
	// is empty when the transition has none.
	Fallback shard.NodeID
	// Role is the role of the replica that the transition places. A transition whose
	// Role is shard.RoleNone counts in no role of the peak counts.
	Role shard.Role
}

// Metric is a load metric of the namespace, with the capacity of a node that it
// fills.
//
// # Concurrency
//
// A Metric is a comparable value.
type Metric struct {
	// Name is the name under which agents report the metric, such as "cpu".
	Name string
	// Capacity is the name of the capacity in shard.Node.Capacity that the metric
	// fills. An empty Capacity is the capacity whose name is Name.
	Capacity string
}

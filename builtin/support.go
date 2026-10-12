// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin

import (
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/handoff/breakbeforemake"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// Support returns the support matrix of the release. A spec selects fixed-slots or
// singleton partitioning, the planner, primary-only replication, exclusive ownership,
// break-before-make handoff, the topology embedded and the routing source watch. The
// constraint, goal and hash axes are open, so a spec selects any constraint, goal or
// hash function that the registry has. Every namespace needs the capability fenced.
// Each call returns a new matrix.
func Support() config.Support {
	return config.Support{
		Strategies: map[config.Axis][]shard.StrategyName{
			config.Partitioning: {fixedslots.Name, singleton.Name},
			config.Placement:    {planner.Name},
			config.Replication:  {replication.PrimaryOnlyName},
			config.Ownership:    {ExclusiveName},
			config.Handoff:      {breakbeforemake.Name},
			config.Topology:     {EmbeddedName},
			config.Routing:      {WatchName},
		},
		Open:     []config.Axis{config.Constraint, config.Goal, config.Hash},
		Requires: []config.Capability{config.Fenced},
	}
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin

import (
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/shard"
)

// WatchName is the registered name of the routing source watch, under which routers
// watch the assignment that the controllers commit.
const WatchName shard.StrategyName = "watch"

// Watch returns the factory of the routing source watch, which has no configuration.
// New builds a struct{}, because the support matrix has one routing source and nothing
// to configure in it. Parse refuses every field, hold included. The source requires the
// capability controller from the topology.
func Watch() config.Factory[struct{}] {
	return &factory[struct{}, empty, *empty]{
		name: WatchName,
		capabilities: func(*empty) config.Capabilities {
			return config.Capabilities{Requires: []config.Requirement{{
				Axis:       config.Topology,
				Capability: config.Controller,
				Reason:     "routers watch the assignment that a controller commits",
			}}}
		},
		build: func(*empty) (struct{}, error) {
			return struct{}{}, nil
		},
	}
}

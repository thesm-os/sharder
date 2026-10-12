// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin

import (
	"time"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/handoff/breakbeforemake"
	"go.thesmos.sh/sharder/shard"
)

// The default timeouts of break-before-make.
const (
	// defaultAddTimeout is the default of Config.AddTimeout.
	defaultAddTimeout = shard.Duration(30 * time.Second)
	// defaultDropTimeout is the default of Config.DropTimeout.
	defaultDropTimeout = shard.Duration(10 * time.Second)
)

// BreakBeforeMake returns the factory of the handoff protocol break-before-make, which
// parses a breakbeforemake.Config. The default timeouts are 30s for an add and 10s for
// a drop. The protocol provides the capability exclusive-safe.
//
// Error modes of New:
//   - an error that wraps ErrConfig and the error of breakbeforemake.New.
func BreakBeforeMake() config.Factory[handoff.Protocol] {
	return &factory[handoff.Protocol, breakbeforemake.Config, *breakbeforemake.Config]{
		name: breakbeforemake.Name,
		defaults: func(c *breakbeforemake.Config) {
			*c = breakbeforemake.Config{AddTimeout: defaultAddTimeout, DropTimeout: defaultDropTimeout}
		},
		capabilities: func(*breakbeforemake.Config) config.Capabilities {
			return config.Capabilities{Provides: []config.Capability{config.ExclusiveSafe}}
		},
		build: func(c *breakbeforemake.Config) (handoff.Protocol, error) {
			return built[handoff.Protocol](breakbeforemake.New(*c))
		},
	}
}

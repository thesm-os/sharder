// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin

import (
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/shard"
)

// EmbeddedName is the registered name of the topology embedded, under which the
// controllers run in the application processes of the namespace.
const EmbeddedName shard.StrategyName = "embedded"

// Embedded returns the factory of the topology embedded, which has no configuration.
// New builds a struct{}, because the support matrix has one topology and nothing to
// configure in it. The topology provides the capability controller.
func Embedded() config.Factory[struct{}] {
	return &factory[struct{}, empty, *empty]{
		name: EmbeddedName,
		capabilities: func(*empty) config.Capabilities {
			return config.Capabilities{Provides: []config.Capability{config.Controller}}
		},
		build: func(*empty) (struct{}, error) {
			return struct{}{}, nil
		},
	}
}

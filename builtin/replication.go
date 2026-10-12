// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin

import (
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/replication"
)

// PrimaryOnly returns the factory of the replica state model primary-only, which has no
// configuration. New compiles replication.PrimaryOnly. The model provides the
// capability single-writer.
func PrimaryOnly() config.Factory[*replication.Compiled] {
	return &factory[*replication.Compiled, empty, *empty]{
		name: replication.PrimaryOnlyName,
		capabilities: func(*empty) config.Capabilities {
			return config.Capabilities{Provides: []config.Capability{config.SingleWriter}}
		},
		build: func(*empty) (*replication.Compiled, error) {
			return built(replication.PrimaryOnly.Compile())
		},
	}
}

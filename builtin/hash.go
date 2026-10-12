// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin

import (
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/xxhash64"
)

// XXHash64 returns the factory of the hash xxhash64, which has no configuration. The
// hash does not provide or require a capability.
func XXHash64() config.Factory[partition.Hash] {
	return &factory[partition.Hash, empty, *empty]{
		name: xxhash64.Name,
		build: func(*empty) (partition.Hash, error) {
			return xxhash64.Hash{}, nil
		},
	}
}

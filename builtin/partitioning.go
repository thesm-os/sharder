// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin

import (
	"fmt"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/partition/xxhash64"
)

// defaultSlots is the default number of slots of fixed-slots.
const defaultSlots = 1024

// FixedSlots returns the factory of the partitioner fixed-slots, which parses a
// fixedslots.Config. The defaults are 1,024 slots, the hash xxhash64 and no hash tags.
// Parse checks that r has a factory of the hash with config.ParseNamed, and New builds
// the hash with config.BuildNamed on r. The partitioner does not provide or require a
// capability.
//
// Error modes of Parse and New:
//   - an error that wraps ErrConfig and the error of the registry when r cannot parse or
//     build the hash.
//   - an error that wraps ErrConfig and the error of fixedslots.New.
func FixedSlots(r *config.Registry) config.Factory[partition.Partitioner] {
	return &factory[partition.Partitioner, fixedslots.Config, *fixedslots.Config]{
		name: fixedslots.Name,
		defaults: func(c *fixedslots.Config) {
			*c = fixedslots.Config{Hash: xxhash64.Name, Slots: defaultSlots}
		},
		selected: func(c *fixedslots.Config) error {
			if _, err := config.ParseNamed(r, config.Hash, c.Hash, nil); err != nil {
				return fmt.Errorf("%w: hash: %w", ErrConfig, err)
			}
			return nil
		},
		build: func(c *fixedslots.Config) (partition.Partitioner, error) {
			h, err := config.BuildNamed[partition.Hash](r, config.Hash, c.Hash, nil)
			if err != nil {
				return nil, fmt.Errorf("%w: hash: %w", ErrConfig, err)
			}
			return built[partition.Partitioner](fixedslots.New(*c, h))
		},
	}
}

// Singleton returns the factory of the partitioner singleton, which has no
// configuration. The partitioner does not provide or require a capability.
func Singleton() config.Factory[partition.Partitioner] {
	return &factory[partition.Partitioner, empty, *empty]{
		name: singleton.Name,
		build: func(*empty) (partition.Partitioner, error) {
			return singleton.Partitioner{}, nil
		},
	}
}

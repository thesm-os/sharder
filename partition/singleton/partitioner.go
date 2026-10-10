// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package singleton

import (
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/shard"
)

// Name is the registered name of the partitioner, which the mapping of a namespace
// records.
const Name shard.StrategyName = "singleton"

// config is the configuration of the partitioner as JSON. The partitioner has no field
// to configure, so the configuration is the empty object.
const config = "{}"

// Partitioner is the partitioner singleton. The zero Partitioner is ready for use.
//
// # Concurrency
//
// A Partitioner has no state and is safe for concurrent use.
//
// # Allocation contract
//
// [Partitioner.Position] does not allocate.
type Partitioner struct{}

var _ partition.Partitioner = Partitioner{}

// Position returns dst unchanged, because the position of every key is the empty
// position.
func (Partitioner) Position(dst, _ []byte) []byte {
	return dst
}

// Mapping returns the name of the partitioner and its empty configuration. The
// mapping does not contain a hash.
func (Partitioner) Mapping() shard.Mapping {
	return shard.Mapping{Partitioner: Name, Config: []byte(config)}
}

// Initial returns one span whose range is r, and no error. The span shares the bytes
// of the bounds of r.
func (Partitioner) Initial(r shard.Range) ([]partition.Span, error) {
	return []partition.Span{{Range: r}}, nil
}

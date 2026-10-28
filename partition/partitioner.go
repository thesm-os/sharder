// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package partition

import (
	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/shard"
)

// Partitioner maps the keys of a namespace to positions, and divides the range of a
// new cell into spans. Its results depend only on its arguments. A key has the same
// position, and a range has the same spans, in every process and on every platform.
//
// # Concurrency
//
// A Partitioner does not perform I/O, and it is safe for concurrent use.
//
//dokimi:stub
//dokimi:suite
//dokimi:out path=partitiontest/
type Partitioner interface {
	// Position appends the position of key to dst and returns the extended slice. It
	// does not allocate when dst has room for the position.
	Position(dst, key []byte) []byte
	// Mapping returns the description of the function that Position computes. The
	// description contains the registered name of the partitioner, its
	// configuration, and the name and the version of its hash function. Mapping
	// returns an equal value on every call, and the caller must not modify its
	// Config.
	Mapping() shard.Mapping
	// Initial returns the shards of a new cell whose range is r, as spans in position
	// order that cover r without gaps or overlaps. It divides the position space, and
	// every range whose start and end are bounds of the spans of the position space,
	// because a cell starts and ends at the bounds of shards. It returns an error that
	// wraps ErrRange for a range that it cannot divide.
	Initial(r shard.Range) ([]Span, error)
}

// Span is a shard that a partitioner proposes, with its range of positions and its
// labels. The core gives it an ID when it commits its first placement.
//
// The zero Span covers the whole position space and has no labels.
//
// # Concurrency
//
// A copy of a Span shares the bytes of Range and the elements of Labels with the
// original.
type Span struct {
	// Range is the range of positions of the shard.
	Range shard.Range
	// Labels are the attributes of the shard that placement constraints and goals
	// read, such as a tenant. The partitioners fixed-slots and singleton do not set
	// labels.
	Labels tag.Tags
}

// Hash is a 64-bit hash of keys, which a hashing partitioner applies to a key. The
// configuration of a hashing partitioner contains the registered name of its hash.
//
// # Concurrency
//
// A Hash is safe for concurrent use.
//
//dokimi:stub
//dokimi:suite
//dokimi:out path=partitiontest/
type Hash interface {
	// Sum64 returns the hash of key. It returns the same value for the same key in
	// every process and on every platform, and it does not allocate.
	//
	//dokimi:budget allocs=0
	Sum64(key []byte) uint64
	// Version returns the version of the function that Sum64 computes. The version is
	// at least one. It increases whenever Sum64 returns another value for any key, so
	// a registered name and a version identify one function.
	Version() uint32
}

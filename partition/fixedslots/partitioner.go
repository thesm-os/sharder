// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fixedslots

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/shard"
)

// Name is the registered name of the partitioner, which the mapping of a namespace
// records.
const Name shard.StrategyName = "fixed-slots"

// positionSize is the number of bytes of a position. A position is the number of a
// slot in big-endian order.
const positionSize = 4

// tagOpen and tagClose delimit the hash tag of a key.
const (
	// tagOpen starts a hash tag.
	tagOpen = '{'
	// tagClose ends a hash tag.
	tagClose = '}'
)

// Partitioner is the partitioner fixed-slots. [New] returns a Partitioner, and the
// zero Partitioner is not ready for use.
//
// # Concurrency
//
// A Partitioner does not change after New returns it. It is safe for concurrent use,
// because its hash is.
//
// # Allocation contract
//
// [Partitioner.Position] does not allocate when dst has room for 4 bytes, because
// Sum64 of a hash does not allocate.
//
//dokimi:subject partition.Partitioner new=newTagged
//dokimi:budget Partitioner.Position allocs=1
type Partitioner struct {
	// hash is the hash function of keys.
	hash partition.Hash
	// mapping is the value that Mapping returns.
	mapping shard.Mapping
	// slots is the number of slots, from 1 to MaxSlots.
	slots uint64
	// hashTags reports whether Position hashes the hash tag of a key.
	hashTags bool
}

var _ partition.Partitioner = (*Partitioner)(nil)

// New returns the partitioner fixed-slots with the configuration cfg and the hash
// function hash. hash must compute the function whose registered name is cfg.Hash,
// because the mapping of the partitioner contains cfg.Hash with the version of hash.
//
// Error modes:
//   - an error that wraps ErrConfig for each problem that cfg.Validate reports.
//   - an error that wraps ErrConfig when hash is nil.
func New(cfg Config, hash partition.Hash) (*Partitioner, error) {
	err := cfg.Validate()
	if hash == nil {
		err = errors.Join(err, fmt.Errorf("%w: hash function is nil", ErrConfig))
	}
	if err != nil {
		return nil, err
	}

	// json.Marshal of a string, an int and a bool returns no error.
	config, _ := json.Marshal(&cfg)

	return &Partitioner{
		hash: hash,
		mapping: shard.Mapping{
			Partitioner: Name,
			Hash:        cfg.Hash,
			Config:      config,
			HashVersion: hash.Version(),
		},
		slots:    uint64(cfg.Slots),
		hashTags: cfg.HashTags,
	}, nil
}

// Position appends the position of key to dst and returns the extended slice. The
// position is the number of the slot of key as 4 bytes in big-endian order, and the
// slot is the hash of key modulo the number of slots. With hash tags, Position hashes
// the hash tag of key when key has one.
//
// # Allocation contract
//
// Position does not allocate when dst has room for 4 bytes.
func (p *Partitioner) Position(dst, key []byte) []byte {
	if p.hashTags {
		key = hashTag(key)
	}
	return binary.BigEndian.AppendUint32(dst, uint32(p.hash.Sum64(key)%p.slots))
}

// Mapping returns the name fixed-slots, the configuration as JSON, and the name and
// the version of the hash. It returns an equal value on every call, and the caller
// must not modify its Config.
func (p *Partitioner) Mapping() shard.Mapping {
	return p.mapping
}

// Initial returns one span per slot of r, in slot order. The span of slot s covers the
// positions from the position of s to the position of s+1. The first span starts at
// r.Start instead, and the last span ends at r.End instead. The spans have no labels.
// The caller must not modify the bounds of the spans, which can share bytes with r and
// with each other.
//
// Error modes:
//   - an error that wraps partition.ErrRange for each bound of r that is neither empty
//     nor the position of a slot from 1 to the last slot.
//   - an error that wraps partition.ErrRange when the start of r is not below its end.
func (p *Partitioner) Initial(r shard.Range) ([]partition.Span, error) {
	first, startErr := p.boundSlot(r.Start, 0)
	last, endErr := p.boundSlot(r.End, p.slots)
	if err := errors.Join(startErr, endErr); err != nil {
		return nil, err
	}
	if first >= last {
		return nil, fmt.Errorf("%w: start %x is not below end %x", partition.ErrRange, r.Start, r.End)
	}

	spans := make([]partition.Span, 0, last-first)
	bounds := make([]byte, 0, positionSize*(last-first-1))
	start := r.Start
	for s := first + 1; s < last; s++ {
		bounds = binary.BigEndian.AppendUint32(bounds, uint32(s))
		end := bounds[len(bounds)-positionSize : len(bounds) : len(bounds)]
		spans = append(spans, partition.Span{Range: shard.Range{Start: start, End: end}})
		start = end
	}
	return append(spans, partition.Span{Range: shard.Range{Start: start, End: r.End}}), nil
}

// boundSlot returns the slot whose position is bound, or unbounded when bound is
// empty.
//
// Error modes:
//   - an error that wraps partition.ErrRange when bound is neither empty nor the
//     position of a slot from 1 to the last slot.
func (p *Partitioner) boundSlot(bound []byte, unbounded uint64) (uint64, error) {
	if len(bound) == 0 {
		return unbounded, nil
	}
	if len(bound) == positionSize {
		if s := uint64(binary.BigEndian.Uint32(bound)); s > 0 && s < p.slots {
			return s, nil
		}
	}
	return 0, fmt.Errorf("%w: %x is not the position of a slot from 1 to the last slot", partition.ErrRange, bound)
}

// hashTag returns the bytes between the first { of key and the first } after it. It
// returns key when key has no {, when no } follows the first {, or when the first }
// after the first { follows it directly.
func hashTag(key []byte) []byte {
	// rest is empty when key has no {, so no } follows.
	_, rest, _ := bytes.Cut(key, []byte{tagOpen})
	tag, _, found := bytes.Cut(rest, []byte{tagClose})
	if !found || len(tag) == 0 {
		return key
	}
	return tag
}

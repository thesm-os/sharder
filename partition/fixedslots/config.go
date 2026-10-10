// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fixedslots

import (
	"errors"
	"fmt"

	"go.thesmos.sh/sharder/shard"
)

// MaxSlots is the largest number of slots. A position has 4 bytes, so the number of a
// slot is below 2^32.
const MaxSlots = 4294967296

// Config is the configuration of the partitioner fixed-slots. The slot of a key
// depends on every field, so the mapping of the partitioner contains the whole Config.
//
// The zero Config is not valid, because it has no hash and no slots.
//
// # Encoding
//
// encoding/json encodes a Config as an object with the keys hash, slots and hashTags,
// in that order. The mapping of the partitioner contains that encoding, so the order
// of the fields is part of the mapping of every namespace.
type Config struct {
	// Hash is the registered name of the hash function, such as "xxhash64".
	Hash shard.StrategyName `json:"hash"`
	// Slots is the number of slots, from 1 to MaxSlots.
	Slots int `json:"slots"`
	// HashTags reports whether Position hashes only the hash tag of a key. The hash
	// tag is the bytes between the first { of the key and the first } after it.
	HashTags bool `json:"hashTags"`
}

// Validate returns an error that joins every problem of c, or nil when c is valid.
// Each problem wraps ErrConfig.
//
// Error modes:
//   - an error that wraps ErrConfig when Hash is empty.
//   - an error that wraps ErrConfig when Slots is below 1 or above MaxSlots.
func (c Config) Validate() error {
	var problems []error
	if c.Hash == "" {
		problems = append(problems, fmt.Errorf("%w: hash is empty", ErrConfig))
	}
	if c.Slots < 1 || int64(c.Slots) > MaxSlots {
		problems = append(problems, fmt.Errorf("%w: slots %d is outside 1 to %d", ErrConfig, c.Slots, int64(MaxSlots)))
	}
	return errors.Join(problems...)
}

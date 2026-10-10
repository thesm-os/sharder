// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=Mapping

// Mapping is the description of the function that maps the keys of a namespace to
// positions. It consists of the partitioner, the configuration of the partitioner,
// and the hash function with its version. The root records it when it creates the
// namespace, and it never changes during the namespace incarnation. Every controller and every
// router compares the Mapping of its own partitioner with the recorded one when it
// starts, and refuses the namespace when the two differ.
//
// The zero Mapping is no mapping.
//
// # Encoding
//
// kanon generates the codec of Mapping. Partitioner is field 1, Config is field 2,
// Hash is field 3, and HashVersion is field 4.
//
// # Concurrency
//
// A copy of a Mapping shares the bytes of Config with the original.
//
// # Allocation contract
//
// Construction does not allocate.
type Mapping struct {
	// Partitioner is the registered name of the partitioner, such as "fixed-slots".
	Partitioner string
	// Hash is the registered name of the hash function of the partitioner, such as
	// "xxhash64", or empty for a partitioner without a hash function.
	Hash string
	// Config is the configuration of the partitioner after parsing, with every
	// default applied, encoded with encoding/json. It includes every field that
	// decides the position of a key, such as the number of slots.
	Config []byte
	// HashVersion is the version of the hash function, or zero for a partitioner
	// without a hash function. A hash function whose value changes for any key has a
	// new version.
	HashVersion uint32
}

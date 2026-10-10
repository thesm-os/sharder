// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=Mapping

// StrategyName is the registered name of a strategy, such as the partitioner
// fixed-slots or the hash xxhash64. Each strategy package declares its name, and a
// Mapping records the names of a partitioner and its hash. The empty StrategyName is
// no strategy.
type StrategyName string

// Mapping is the description of the function that maps the keys of a namespace to
// positions. It consists of the partitioner, the configuration of the partitioner,
// and the hash function with its version. The root records it when it creates the
// namespace, and it never changes during the namespace incarnation. Every controller
// and every router compares the Mapping of its own partitioner with the recorded one
// when it starts, and refuses the namespace when the two differ.
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
	Partitioner StrategyName
	// Hash is the registered name of the hash function of the partitioner, such as
	// "xxhash64", or empty for a partitioner without a hash function.
	Hash StrategyName
	// Config is the configuration of the partitioner after parsing, with every
	// default applied, encoded with encoding/json. It includes every field on which
	// the position of a key depends, such as the number of slots.
	Config []byte
	// HashVersion is the version of the hash function, or zero for a partitioner
	// without a hash function. A hash function whose value changes for any key has a
	// new version.
	HashVersion uint32
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package store

//go:generate go tool kanon -type=Drain,DrainKey

import (
	"time"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/shard"
)

// Drain is one reason to keep new replicas off a node. A node is draining while it
// has at least one drain. The node and the disruption ID identify a drain.
//
// The zero disruption ID marks the manual drain of the node, which sharderctl drain
// adds and sharderctl undrain removes. Operators share it, so a second manual drain of
// a node does not change the drains. Every other drain belongs to the reservation of its
// disruption, and ends with the reservation.
//
// # Encoding
//
// kanon generates the codec of Drain. Node is field 1, Disruption is field 2, Caller is
// field 3, and Since is field 4.
//
// # Concurrency
//
// A Drain is a value, and a copy is independent of the original.
type Drain struct {
	// Since is the instant at which the drain started.
	Since time.Time
	// Node is the drained node.
	Node shard.NodeID
	// Caller is the identity that requested the drain.
	Caller shard.Identity
	// Disruption is the ID of the reservation that drains the node, pending or
	// admitted, or zero for the manual drain.
	Disruption budget.ID
}

// DrainKey is the identity of one drain: the node and the disruption ID, which is
// zero for the manual drain.
//
// # Encoding
//
// kanon generates the codec of DrainKey. Node is field 1, and Disruption is field 2.
//
// # Concurrency
//
// A DrainKey is a comparable value, and a copy is independent of the original.
type DrainKey struct {
	// Node is the drained node.
	Node shard.NodeID
	// Disruption is the ID of the reservation of the drain, or zero for the manual
	// drain.
	Disruption budget.ID
}

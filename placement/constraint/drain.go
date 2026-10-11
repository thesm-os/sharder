// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package constraint

import (
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// DrainName is the registered name of the constraint drain.
const DrainName shard.StrategyName = "drain"

// Drain is the constraint drain. It rejects a replica on a draining node. It has no
// configuration, and the zero Drain is ready for use.
//
// A move whose From equals its To on a draining node fails, so Validate reports each
// replica that a plan keeps on a draining node.
//
// # Concurrency
//
// A Drain is safe for concurrent use.
//
// # Allocation contract
//
// Check does not allocate.
type Drain struct{}

var _ placement.Constraint = Drain{}

// Name returns DrainName.
func (Drain) Name() shard.StrategyName {
	return DrainName
}

// Check returns ErrDrain when the destination of m drains, and nil otherwise.
//
// # Allocation contract
//
// Zero alloc.
func (Drain) Check(st *placement.State, m placement.Move) error {
	// The empty node of a removal does not drain.
	if st.Draining(m.To) {
		return ErrDrain
	}
	return nil
}

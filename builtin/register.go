// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin

import (
	"errors"

	"go.thesmos.sh/sharder/config"
)

// Register registers in r the factory of every built-in strategy of the support matrix
// under its name on its axis, and the presets that Presets returns. The factories of
// fixed-slots and the planner build their hash functions, constraints and goals with
// r. config.NewRegistry(Support()) returns a registry with the support matrix of the
// release.
//
// Error modes:
//   - an error that joins the error of each registration that fails, such as an error
//     that wraps config.ErrDuplicate for a name that r has, or one that wraps
//     config.ErrUnsupported for a preset outside the support matrix of r.
func Register(r *config.Registry) error {
	err := errors.Join(
		config.Register(r, config.Partitioning, FixedSlots(r)),
		config.Register(r, config.Partitioning, Singleton()),
		config.Register(r, config.Hash, XXHash64()),
		config.Register(r, config.Placement, Planner(r)),
		config.Register(r, config.Constraint, Capacity()),
		config.Register(r, config.Constraint, Drain()),
		config.Register(r, config.Constraint, Isolation()),
		config.Register(r, config.Constraint, Labels()),
		config.Register(r, config.Constraint, MatchLabel()),
		config.Register(r, config.Constraint, MaxPerNode()),
		config.Register(r, config.Goal, Balance()),
		config.Register(r, config.Goal, Colocate()),
		config.Register(r, config.Goal, Count()),
		config.Register(r, config.Goal, Prefer()),
		config.Register(r, config.Goal, Spread()),
		config.Register(r, config.Goal, Stickiness()),
		config.Register(r, config.Goal, Utilisation()),
		config.Register(r, config.Replication, PrimaryOnly()),
		config.Register(r, config.Ownership, Exclusive()),
		config.Register(r, config.Handoff, BreakBeforeMake()),
		config.Register(r, config.Topology, Embedded()),
		config.Register(r, config.Routing, Watch()),
	)
	presets := Presets()
	for i := range presets {
		err = errors.Join(err, config.RegisterPreset(r, &presets[i]))
	}
	return err
}

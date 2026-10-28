// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin_test

import (
	"errors"
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/handoff/breakbeforemake"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/partition/xxhash64"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// testSpace is the namespace of the specs of the tests.
const testSpace shard.Namespace = "orders"

// provided lists the capabilities of the adapters of the tests: a durable store and a
// membership with leases.
var provided = map[config.Axis][]config.Capability{
	config.Store:      {config.Durable},
	config.Membership: {config.Leases},
}

// named is a strategy name on its axis.
type named struct {
	axis config.Axis
	name shard.StrategyName
}

// builtins lists the name and the axis of every built-in strategy of the support matrix.
var builtins = []named{
	{axis: config.Partitioning, name: fixedslots.Name},
	{axis: config.Partitioning, name: singleton.Name},
	{axis: config.Hash, name: xxhash64.Name},
	{axis: config.Placement, name: planner.Name},
	{axis: config.Constraint, name: constraint.CapacityName},
	{axis: config.Constraint, name: constraint.DrainName},
	{axis: config.Constraint, name: constraint.IsolationName},
	{axis: config.Constraint, name: constraint.LabelsName},
	{axis: config.Constraint, name: constraint.MatchLabelName},
	{axis: config.Constraint, name: constraint.MaxPerNodeName},
	{axis: config.Goal, name: goal.BalanceName},
	{axis: config.Goal, name: goal.ColocateName},
	{axis: config.Goal, name: goal.CountName},
	{axis: config.Goal, name: goal.PreferName},
	{axis: config.Goal, name: goal.SpreadName},
	{axis: config.Goal, name: goal.StickinessName},
	{axis: config.Goal, name: goal.UtilisationName},
	{axis: config.Replication, name: replication.PrimaryOnlyName},
	{axis: config.Ownership, name: builtin.ExclusiveName},
	{axis: config.Handoff, name: breakbeforemake.Name},
	{axis: config.Topology, name: builtin.EmbeddedName},
	{axis: config.Routing, name: builtin.WatchName},
}

// TestRegister checks the strategies and the presets that Register adds to a registry.
func TestRegister(t *testing.T) {
	t.Parallel()

	t.Run("registers every built-in strategy under its name on its axis", func(t *testing.T) {
		t.Parallel()
		r := newRegistry(t)
		assert.Total(t, func(n named) error {
			_, err := config.ParseNamed(r, n.axis, n.name, nil)
			if errors.Is(err, config.ErrUnknown) {
				return err
			}
			return nil
		}, builtins, "Register must add the factory of every built-in strategy")
	})

	t.Run("registers every preset that Presets returns", func(t *testing.T) {
		t.Parallel()
		r := newRegistry(t)
		assert.Total(t, func(p config.Preset) error {
			_, err := config.Resolve(r, spec(p.Name), provided)
			return err
		}, builtin.Presets(), "Register must add every preset, and each must resolve")
	})

	t.Run("returns ErrDuplicate for a name that the registry has", func(t *testing.T) {
		t.Parallel()
		r := config.NewRegistry(builtin.Support())
		assert.NoError(t, config.Register(r, config.Partitioning, builtin.Singleton()),
			"the registry of the test must take the singleton")
		assert.ErrorIs(t, builtin.Register(r), config.ErrDuplicate,
			"Register must return the error of the registration that fails")
	})

	t.Run("registers the other built-ins when a registration fails", func(t *testing.T) {
		t.Parallel()
		r := config.NewRegistry(builtin.Support())
		assert.NoError(t, config.Register(r, config.Partitioning, builtin.Singleton()),
			"the registry of the test must take the singleton")
		assert.HasError(t, builtin.Register(r), "Register must report the duplicate singleton")
		_, err := config.Resolve(r, spec(builtin.ExclusiveWorker), provided)
		assert.NoError(t, err, "Register must add the other strategies and the presets")
	})

	t.Run("returns ErrUnsupported for a preset outside the support matrix of the registry", func(t *testing.T) {
		t.Parallel()
		err := builtin.Register(config.NewRegistry(config.Support{}))
		assert.ErrorIs(t, err, config.ErrUnsupported, "Register must refuse a preset that the matrix does not support")
	})
}

// newRegistry returns a registry of the support matrix with every built-in, and stops
// the test when Register fails.
func newRegistry(tb testing.TB) *config.Registry {
	tb.Helper()
	r := config.NewRegistry(builtin.Support())
	assert.NoError(tb, builtin.Register(r), "Register must register every built-in in a new registry")
	return r
}

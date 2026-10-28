// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin_test

import (
	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/config/configtest"
)

// spec returns a spec of the namespace of the tests that selects preset.
func spec(preset config.PresetName) *config.Spec {
	return new(configtest.NewSpecBuilder().
		WithNamespace(testSpace).
		WithPreset(preset).
		Build())
}

// selecting returns a spec of the preset exclusive-worker that selects sel on axis.
func selecting(axis config.Axis, sel config.Selection) *config.Spec {
	b := configtest.NewSpecBuilderFrom(*spec(builtin.ExclusiveWorker))
	switch axis {
	case config.Partitioning:
		b.WithPartitioning(sel)
	case config.Placement:
		b.WithPlacement(sel)
	case config.Replication:
		b.WithReplication(sel)
	case config.Ownership:
		b.WithOwnership(sel)
	case config.Handoff:
		b.WithHandoff(sel)
	case config.Topology:
		b.WithTopology(sel)
	default:
		b.WithRouting(sel)
	}
	return new(b.Build())
}

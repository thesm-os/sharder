// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control_test

import (
	"encoding/json"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/config/configtest"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/shard/shardtest"
)

// testSpec returns the spec of the tests at the version v.
func testSpec(v epoch.Epoch) config.Spec {
	return configtest.NewSpecBuilder().
		WithNamespace(namespace).
		WithPreset(builtin.ExclusiveWorker).
		WithPartitioning(config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)}).
		WithVersion(v).
		Build()
}

// planned returns a selection of the planner with the configuration raw.
func planned(raw string) config.Selection {
	return configtest.NewSelectionBuilder().
		WithName(planner.Name).
		WithConfig(json.RawMessage(raw)).
		Build()
}

// restarted returns n at its next incarnation.
func restarted(n shard.Node) shard.Node {
	return shardtest.NewNodeBuilderFrom(n).
		WithIncarnation(n.Incarnation + 1).
		Build()
}

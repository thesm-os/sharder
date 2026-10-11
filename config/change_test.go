// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config_test

import (
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/partition/xxhash64"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// otherSpace is a namespace other than the namespace of the tests.
const otherSpace shard.Namespace = "payments"

// The values that the tests of Change change.
const (
	otherConfig  = `{"other":true}`
	otherVersion = 2

	// placeNamespace is the place of the namespace in the errors of Change.
	placeNamespace = "namespace:"
)

// resolvedSpec returns a resolved spec of the namespace of the tests. It has a
// selection on every axis and a mapping.
func resolvedSpec() *config.Resolved {
	empty := json.RawMessage(emptyConfig)
	return &config.Resolved{
		Spec: config.Spec{
			Namespace:    testSpace,
			Preset:       workerPreset,
			Partitioning: config.Selection{Name: singleton.Name, Config: empty},
			Placement:    config.Selection{Name: plannerName, Config: json.RawMessage(plannerConfig)},
			Replication:  config.Selection{Name: replication.PrimaryOnlyName, Config: empty},
			Ownership:    config.Selection{Name: exclusiveName, Config: empty},
			Handoff:      config.Selection{Name: breakBeforeMakeName, Config: empty},
			Topology:     config.Selection{Name: embeddedName, Config: empty},
			Routing:      config.Selection{Name: watchName, Config: empty},
			Budget:       config.Budget{MaxTransitions: new(64), Interruption: config.PlannedInterruption},
			Load:         config.LoadModel{Metrics: []config.Metric{{Name: cpuMetric}}},
			Cells:        config.CellPolicy{MaxNodes: 5000},
			Version:      1,
		},
		Mapping: shard.Mapping{
			Partitioner: singleton.Name,
			Hash:        xxhash64.Name,
			Config:      []byte(emptyConfig),
			HashVersion: 1,
		},
	}
}

// TestChange checks the edits that Change allows, and the changes that it refuses
// with the operation that each needs.
func TestChange(t *testing.T) {
	t.Parallel()

	t.Run("Change", func(t *testing.T) {
		t.Parallel()

		allowed := []struct {
			change func(r *config.Resolved)
			name   string
		}{
			{name: "returns nil for the same spec", change: func(*config.Resolved) {}},
			{
				name:   "returns nil for another version",
				change: func(r *config.Resolved) { r.Spec.Version = otherVersion },
			},
			{
				name: "returns nil for another placement",
				change: func(r *config.Resolved) {
					r.Spec.Placement = config.Selection{Name: balanceName, Config: json.RawMessage(otherConfig)}
				},
			},
			{
				name:   "returns nil for another budget",
				change: func(r *config.Resolved) { r.Spec.Budget.MaxTransitions = new(8) },
			},
			{
				name:   "returns nil for another load model",
				change: func(r *config.Resolved) { r.Spec.Load.Metrics = []config.Metric{{Name: bytesMetric}} },
			},
			{
				name:   "returns nil for other cell bounds",
				change: func(r *config.Resolved) { r.Spec.Cells.MaxNodes = 10 },
			},
		}
		for _, tt := range allowed {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				to := resolvedSpec()
				tt.change(to)
				assert.NoError(t, config.Change(resolvedSpec(), to), "Change must allow an edit of the case")
			})
		}

		refused := []struct {
			change func(r *config.Resolved)
			name   string
		}{
			{
				name:   "returns ErrChange for another namespace",
				change: func(r *config.Resolved) { r.Spec.Namespace = otherSpace },
			},
			{
				name:   "returns ErrChange for another preset",
				change: func(r *config.Resolved) { r.Spec.Preset = tunedPreset },
			},
			{
				name:   "returns ErrChange for another partitioner",
				change: func(r *config.Resolved) { r.Spec.Partitioning.Name = zoneName },
			},
			{
				name:   "returns ErrChange for another configuration of the partitioner",
				change: func(r *config.Resolved) { r.Spec.Partitioning.Config = json.RawMessage(otherConfig) },
			},
			{
				name:   "returns ErrChange for another partitioner of the mapping",
				change: func(r *config.Resolved) { r.Mapping.Partitioner = zoneName },
			},
			{
				name:   "returns ErrChange for another hash of the mapping",
				change: func(r *config.Resolved) { r.Mapping.Hash = zoneName },
			},
			{
				name:   "returns ErrChange for another version of the hash of the mapping",
				change: func(r *config.Resolved) { r.Mapping.HashVersion = otherVersion },
			},
			{
				name:   "returns ErrChange for another configuration of the mapping",
				change: func(r *config.Resolved) { r.Mapping.Config = []byte(otherConfig) },
			},
			{
				name:   "returns ErrChange for another model",
				change: func(r *config.Resolved) { r.Spec.Replication.Name = zoneName },
			},
			{
				name:   "returns ErrChange for another configuration of the model",
				change: func(r *config.Resolved) { r.Spec.Replication.Config = json.RawMessage(otherConfig) },
			},
			{
				name:   "returns ErrChange for another ownership",
				change: func(r *config.Resolved) { r.Spec.Ownership.Name = unfencedName },
			},
			{
				name:   "returns ErrChange for another handoff",
				change: func(r *config.Resolved) { r.Spec.Handoff.Name = zoneName },
			},
			{
				name:   "returns ErrChange for another topology",
				change: func(r *config.Resolved) { r.Spec.Topology.Name = serviceName },
			},
			{
				name:   "returns ErrChange for another routing",
				change: func(r *config.Resolved) { r.Spec.Routing.Name = zoneName },
			},
		}
		for _, tt := range refused {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				to := resolvedSpec()
				tt.change(to)
				err := config.Change(resolvedSpec(), to)
				expect.ErrorIs(t, err, config.ErrChange, "Change must refuse the change of the case")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Change must classify as Invalid")
			})
		}

		t.Run("returns one problem for each change that an edit cannot make", func(t *testing.T) {
			t.Parallel()
			to := resolvedSpec()
			to.Spec.Namespace = otherSpace
			to.Spec.Topology.Name = serviceName
			err := config.Change(resolvedSpec(), to)
			assert.ErrorIs(t, err, config.ErrChange, "Change must refuse the changes")
			expect.Contains(t, err.Error(), placeNamespace, "the error must contain the change of the namespace")
			expect.Contains(t, err.Error(), config.Topology.String(),
				"the error must contain the change of the topology")
		})
	})
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/config"
)

// TestAxis checks Valid and String of every Axis.
func TestAxis(t *testing.T) {
	t.Parallel()

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		t.Run("reports true for each of the twelve axes", func(t *testing.T) {
			t.Parallel()
			prop.True(t, config.Axis.Valid, "every constant of Axis must be Valid",
				prop.Using(prop.SampledFrom(config.Partitioning, config.Placement, config.Replication, config.Ownership,
					config.Handoff, config.Topology, config.Routing, config.Constraint, config.Goal, config.Hash,
					config.Store, config.Membership)))
		})

		t.Run("reports false for the zero Axis", func(t *testing.T) {
			t.Parallel()
			assert.False(t, config.Axis(0).Valid(), "the zero Axis must not be Valid")
		})

		t.Run("reports false for a value above Membership", func(t *testing.T) {
			t.Parallel()
			prop.False(t, config.Axis.Valid, "a value above the last constant must not be Valid",
				prop.Using(prop.Integer[config.Axis](config.Membership+1, 255)))
		})
	})

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			want string
			give config.Axis
		}{
			{name: "returns partitioning for Partitioning", give: config.Partitioning, want: "partitioning"},
			{name: "returns placement for Placement", give: config.Placement, want: "placement"},
			{name: "returns replication for Replication", give: config.Replication, want: "replication"},
			{name: "returns ownership for Ownership", give: config.Ownership, want: "ownership"},
			{name: "returns handoff for Handoff", give: config.Handoff, want: "handoff"},
			{name: "returns topology for Topology", give: config.Topology, want: "topology"},
			{name: "returns routing for Routing", give: config.Routing, want: "routing"},
			{name: "returns constraint for Constraint", give: config.Constraint, want: "constraint"},
			{name: "returns goal for Goal", give: config.Goal, want: "goal"},
			{name: "returns hash for Hash", give: config.Hash, want: "hash"},
			{name: "returns store for Store", give: config.Store, want: "store"},
			{name: "returns membership for Membership", give: config.Membership, want: "membership"},
			{name: "returns the number of a value that is not Valid", give: config.Membership + 1, want: "Axis(13)"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.give.String(), tt.want, "String must return the recorded spelling of the axis")
			})
		}
	})
}

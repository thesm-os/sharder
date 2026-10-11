// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/config"
)

// TestCapability checks Valid and String of every Capability.
func TestCapability(t *testing.T) {
	t.Parallel()

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		t.Run("reports true for each of the six capabilities", func(t *testing.T) {
			t.Parallel()
			prop.True(t, config.Capability.Valid, "every constant of Capability must be Valid",
				prop.Using(prop.SampledFrom(config.Controller, config.SingleWriter, config.ExclusiveSafe, config.Fenced,
					config.Leases, config.Durable)))
		})

		t.Run("reports false for the zero Capability", func(t *testing.T) {
			t.Parallel()
			assert.False(t, config.Capability(0).Valid(), "the zero Capability must not be Valid")
		})

		t.Run("reports false for a value above Durable", func(t *testing.T) {
			t.Parallel()
			prop.False(t, config.Capability.Valid, "a value above the last constant must not be Valid",
				prop.Using(prop.Integer[config.Capability](config.Durable+1, 255)))
		})
	})

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			want string
			give config.Capability
		}{
			{name: "returns controller for Controller", give: config.Controller, want: "controller"},
			{name: "returns single-writer for SingleWriter", give: config.SingleWriter, want: "single-writer"},
			{name: "returns exclusive-safe for ExclusiveSafe", give: config.ExclusiveSafe, want: "exclusive-safe"},
			{name: "returns fenced for Fenced", give: config.Fenced, want: "fenced"},
			{name: "returns leases for Leases", give: config.Leases, want: "leases"},
			{name: "returns durable for Durable", give: config.Durable, want: "durable"},
			{name: "returns the number of a value that is not Valid", give: config.Durable + 1, want: "Capability(7)"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.give.String(), tt.want, "String must return the recorded spelling of the capability")
			})
		}
	})
}

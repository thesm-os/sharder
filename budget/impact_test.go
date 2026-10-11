// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package budget_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/budget"
)

// The spellings of the impacts, which configurations and telemetry record.
const (
	recordedNetworkLoss    = "network-loss"
	recordedProcessRestart = "process-restart"
	recordedFullLoss       = "full-loss"
	// recordedOutsideImpact is the spelling of the value 4, which no constant has.
	recordedOutsideImpact = "Impact(4)"
)

// TestImpact checks Valid and String of every Impact.
func TestImpact(t *testing.T) {
	t.Parallel()

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		t.Run("reports true for each of the three impacts", func(t *testing.T) {
			t.Parallel()
			prop.True(t, budget.Impact.Valid, "every constant of Impact must be Valid",
				prop.Using(prop.SampledFrom(budget.NetworkLoss, budget.ProcessRestart, budget.FullLoss)))
		})

		t.Run("reports false for the zero Impact", func(t *testing.T) {
			t.Parallel()
			assert.False(t, budget.Impact(0).Valid(), "the zero Impact must not be Valid")
		})

		t.Run("reports false for a value above FullLoss", func(t *testing.T) {
			t.Parallel()
			prop.False(t, budget.Impact.Valid, "a value above the last constant must not be Valid",
				prop.Using(prop.Integer[budget.Impact](budget.FullLoss+1, 255)))
		})
	})

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			want string
			give budget.Impact
		}{
			{
				name: "returns network-loss for NetworkLoss",
				give: budget.NetworkLoss,
				want: recordedNetworkLoss,
			},
			{
				name: "returns process-restart for ProcessRestart",
				give: budget.ProcessRestart,
				want: recordedProcessRestart,
			},
			{
				name: "returns full-loss for FullLoss",
				give: budget.FullLoss,
				want: recordedFullLoss,
			},
			{
				name: "returns the number of a value that is not Valid",
				give: budget.FullLoss + 1,
				want: recordedOutsideImpact,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.give.String(), tt.want, "String must return the recorded spelling of the impact")
			})
		}
	})
}

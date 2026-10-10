// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package proposal_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/proposal"
)

// The spellings of the kinds, which configurations and telemetry record.
const (
	recordedSpec    = "spec"
	recordedMove    = "move"
	recordedDrain   = "drain"
	recordedUndrain = "undrain"
	recordedExplain = "explain"
)

// TestOperationKind checks Valid and String of every OperationKind.
func TestOperationKind(t *testing.T) {
	t.Parallel()

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		t.Run("reports true for each of the five kinds", func(t *testing.T) {
			t.Parallel()
			prop.True(t, proposal.OperationKind.Valid, "every constant of OperationKind must be Valid",
				prop.Using(prop.SampledFrom(proposal.SpecChange, proposal.Move, proposal.Drain, proposal.Undrain,
					proposal.Explain)))
		})

		t.Run("reports false for the zero OperationKind", func(t *testing.T) {
			t.Parallel()
			assert.False(t, proposal.OperationKind(0).Valid(), "the zero OperationKind must not be Valid")
		})

		t.Run("reports false for a value above Explain", func(t *testing.T) {
			t.Parallel()
			prop.False(t, proposal.OperationKind.Valid, "a value above the last constant must not be Valid",
				prop.Using(prop.Integer[proposal.OperationKind](proposal.Explain+1, 255)))
		})
	})

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give proposal.OperationKind
			want string
		}{
			{name: "returns spec for SpecChange", give: proposal.SpecChange, want: recordedSpec},
			{name: "returns move for Move", give: proposal.Move, want: recordedMove},
			{name: "returns drain for Drain", give: proposal.Drain, want: recordedDrain},
			{name: "returns undrain for Undrain", give: proposal.Undrain, want: recordedUndrain},
			{name: "returns explain for Explain", give: proposal.Explain, want: recordedExplain},
			{name: "returns the number of a value that is not Valid", give: 6, want: "OperationKind(6)"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.give.String(), tt.want, "String must return the recorded spelling of the kind")
			})
		}
	})
}

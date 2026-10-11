// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/placementtest"
)

// TestInput checks the inputs that Inputs generates and the model Replicated.
func TestInput(t *testing.T) {
	t.Parallel()

	t.Run("Inputs", func(t *testing.T) {
		t.Parallel()

		t.Run("generates inputs that NewState accepts", func(t *testing.T) {
			t.Parallel()
			prop.NoError(t, func(in *placement.Input) error {
				_, err := placement.NewState(in)
				return err
			}, "NewState must accept every generated input", prop.Using(placementtest.Inputs()))
		})
	})

	t.Run("Replicated", func(t *testing.T) {
		t.Parallel()

		t.Run("compiles", func(t *testing.T) {
			t.Parallel()
			_, err := placementtest.Replicated.Compile()
			assert.NoError(t, err, "the model Replicated must compile")
		})
	})
}

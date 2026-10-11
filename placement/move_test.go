// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placement_test

import (
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/placement"
)

// TestMove checks the zero value of a Move.
func TestMove(t *testing.T) {
	t.Parallel()

	t.Run("Move", func(t *testing.T) {
		t.Parallel()

		t.Run("is no move when it is zero", func(t *testing.T) {
			t.Parallel()
			assert.ErrorIs(t, newState(t, cell()).Apply(placement.Move{}), placement.ErrMove,
				"Apply must refuse the zero Move")
		})
	})
}

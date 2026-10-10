// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package partition_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/partition"
)

// TestPartitioner checks the zero value of a Span.
func TestPartitioner(t *testing.T) {
	t.Parallel()

	t.Run("Span", func(t *testing.T) {
		t.Parallel()

		t.Run("covers every position when it is zero", func(t *testing.T) {
			t.Parallel()
			prop.True(t, func(pos []byte) bool { return partition.Span{}.Range.Contains(pos) },
				"the zero Span must cover every position")
		})

		t.Run("has no labels when it is zero", func(t *testing.T) {
			t.Parallel()
			assert.Empty(t, partition.Span{}.Labels, "the zero Span must have no labels")
		})
	})
}

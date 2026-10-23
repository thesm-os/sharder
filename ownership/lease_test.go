// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package ownership_test

import (
	"testing"
	"time"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/shard"
)

// TestLease checks the validity of a lease.
func TestLease(t *testing.T) {
	t.Parallel()

	t.Run("Validity", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give ownership.Lease
			want time.Duration
		}{
			{
				name: "returns the TTL less the drift",
				give: ownership.Lease{TTL: shard.Duration(10 * time.Second), MaxDriftRate: 0.01},
				want: 9900 * time.Millisecond,
			},
			{
				name: "returns the TTL for a drift rate of zero",
				give: ownership.Lease{TTL: shard.Duration(10 * time.Second)},
				want: 10 * time.Second,
			},
			{
				name: "rounds down to a nanosecond",
				give: ownership.Lease{TTL: 3, MaxDriftRate: 0.5},
				want: 1,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.give.Validity(), tt.want, "Validity must return the TTL less the drift")
			})
		}
	})
}

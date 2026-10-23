// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package membershiptest_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/clock"

	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/membership/membershiptest"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// points maps each loss of the suite to the point of the memory store that loses a renewal
// there.
var points = map[membershiptest.Loss]memory.Point{
	membershiptest.LoseRequest: memory.PointRenewApply,
	membershiptest.LoseReply:   memory.PointRenewAck,
}

// TestMembership runs the suite against the membership of the memory store with every
// option.
func TestMembership(t *testing.T) {
	t.Parallel()

	t.Run("AssertMembership", func(t *testing.T) {
		t.Parallel()

		t.Run("passes the membership of the memory store with every option", func(t *testing.T) {
			t.Parallel()
			membershiptest.AssertMembership(t,
				func(_ assert.TB, clk clock.Clock) (membership.Membership, store.Store) {
					s := memory.New(clk)
					m := s.Membership()
					return m, s
				},
				membershiptest.WithLostRenewals(
					func(tb assert.TB, _ membership.Membership, s store.Store, l membershiptest.Loss) {
						m, ok := s.(*memory.Store)
						assert.True(tb, ok, "the suite passes the store that newMembership returned")
						m.FailAt(points[l])
					}),
			)
		})
	})
}

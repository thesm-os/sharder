// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package membershiptest_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
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
// option, and checks Valid and String of every Loss.
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

	t.Run("Loss", func(t *testing.T) {
		t.Parallel()

		t.Run("Valid", func(t *testing.T) {
			t.Parallel()

			t.Run("reports true for LoseRequest and LoseReply", func(t *testing.T) {
				t.Parallel()
				prop.True(t, membershiptest.Loss.Valid, "every constant of Loss must be Valid",
					prop.Using(prop.SampledFrom(membershiptest.LoseRequest, membershiptest.LoseReply)))
			})

			t.Run("reports false for the zero Loss", func(t *testing.T) {
				t.Parallel()
				assert.False(t, membershiptest.Loss(0).Valid(), "the zero Loss must not be Valid")
			})

			t.Run("reports false for a value above LoseReply", func(t *testing.T) {
				t.Parallel()
				prop.False(t, membershiptest.Loss.Valid, "a value above the last constant must not be Valid",
					prop.Using(prop.Integer[membershiptest.Loss](membershiptest.LoseReply+1, 255)))
			})
		})

		t.Run("String", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name string
				give membershiptest.Loss
				want string
			}{
				{name: "returns request for LoseRequest", give: membershiptest.LoseRequest, want: "request"},
				{name: "returns reply for LoseReply", give: membershiptest.LoseReply, want: "reply"},
				{name: "returns the number of the zero Loss", give: 0, want: "Loss(0)"},
				{
					name: "returns the number of a value above LoseReply",
					give: membershiptest.LoseReply + 1,
					want: "Loss(3)",
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					assert.Equal(t, tt.give.String(), tt.want, "String must return the spelling of the loss")
				})
			}
		})
	})
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package membershiptest_test

import (
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/clock/fake"

	"go.thesmos.sh/sharder/membership/membershiptest"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// origin is the start of the clock of each membership of the test.
var origin = time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC)

// points maps each loss of the suite to the point of the memory store that loses a renewal
// there.
var points = map[membershiptest.Loss]memory.Point{
	membershiptest.LoseRequest: memory.PointRenewApply,
	membershiptest.LoseReply:   memory.PointRenewAck,
}

// backedMembership is the membership of a memory store, with the store and the fake clock
// of its leases. It loses the renewals of the losses of the suite.
type backedMembership struct {
	*memory.Membership
	// s is the store of the membership.
	s *memory.Store
	// clk is the clock of the leases of s.
	clk *fake.Clock
}

// Store returns the memory store of the membership.
func (m backedMembership) Store() store.Store {
	return m.s
}

// Advance moves the clock of the leases forward by d.
func (m backedMembership) Advance(d time.Duration) {
	m.clk.Advance(d)
}

// LoseRenewals arms the fault of the memory store at the point of l.
func (m backedMembership) LoseRenewals(l membershiptest.Loss) {
	m.s.FailAt(points[l])
}

// TestMembership checks that the membership of the memory store, the verified fake of the
// planted defects of the suite, passes the checks of the suite of membership.Membership.
func TestMembership(t *testing.T) {
	t.Parallel()

	membershiptest.RunMembershipSuiteFromHandle(t, func(h *membershiptest.MembershipHandle) backedMembership {
		clk := fake.New(origin)
		s := memory.New(clk)
		h.Cleanup(func() { assert.NoError(h, s.Close(), "Close returns nil") })
		return backedMembership{Membership: s.Membership(), s: s, clk: clk}
	})
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine_test

import (
	"testing"

	"go.thesmos.sh/sharder/internal/machine"
)

// TestMembership checks that the calls of the membership are effects and that their
// results are events.
func TestMembership(t *testing.T) {
	t.Parallel()

	effects(t, []member{
		{name: "Join", give: machine.Join{}},
		{name: "Leave", give: machine.Leave{}},
		{name: "WatchViews", give: machine.WatchViews{}},
	})
	events(t, []member{
		{name: "Joined", give: machine.Joined{}},
		{name: "Left", give: machine.Left{}},
		{name: "SessionEnded", give: machine.SessionEnded{}},
		{name: "Viewed", give: machine.Viewed{}},
	})
}

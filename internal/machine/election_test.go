// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine_test

import (
	"testing"

	"go.thesmos.sh/sharder/internal/machine"
)

// TestElection checks that the calls of an election are effects and that their results
// are events.
func TestElection(t *testing.T) {
	t.Parallel()

	effects(t, []member{
		{name: "Campaign", give: machine.Campaign{}},
		{name: "Resign", give: machine.Resign{}},
	})
	events(t, []member{
		{name: "Campaigned", give: machine.Campaigned{}},
		{name: "TermEnded", give: machine.TermEnded{}},
	})
}

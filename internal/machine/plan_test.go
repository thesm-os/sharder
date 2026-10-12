// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine_test

import (
	"testing"

	"go.thesmos.sh/sharder/internal/machine"
)

// TestPlan checks that a planning pass is an effect and that its result is an event.
func TestPlan(t *testing.T) {
	t.Parallel()

	effects(t, []member{
		{name: "Plan", give: machine.Plan{}},
	})
	events(t, []member{
		{name: "Planned", give: machine.Planned{}},
	})
}

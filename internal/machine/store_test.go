// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine_test

import (
	"testing"

	"go.thesmos.sh/sharder/internal/machine"
)

// TestStore checks that the calls of the store are effects and that their results are
// events.
func TestStore(t *testing.T) {
	t.Parallel()

	effects(t, []member{
		{name: "Head", give: machine.Head{}},
		{name: "Read", give: machine.Read{}},
		{name: "Watch", give: machine.Watch{}},
		{name: "Unwatch", give: machine.Unwatch{}},
		{name: "Commit", give: machine.Commit{}},
		{name: "Outcome", give: machine.Outcome{}},
	})
	events(t, []member{
		{name: "HeadRead", give: machine.HeadRead{}},
		{name: "PageRead", give: machine.PageRead{}},
		{name: "Watched", give: machine.Watched{}},
		{name: "Committed", give: machine.Committed{}},
		{name: "OutcomeRead", give: machine.OutcomeRead{}},
	})
}

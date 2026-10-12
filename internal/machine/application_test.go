// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine_test

import (
	"testing"

	"go.thesmos.sh/sharder/internal/machine"
)

// TestApplication checks that the calls of the handler and of the fence of the application
// are effects and that their results are events.
func TestApplication(t *testing.T) {
	t.Parallel()

	effects(t, []member{
		{name: "Handle", give: machine.Handle{}},
		{name: "Acquire", give: machine.Acquire{}},
	})
	events(t, []member{
		{name: "Handled", give: machine.Handled{}},
		{name: "Acquired", give: machine.Acquired{}},
	})
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine_test

import (
	"testing"

	"go.thesmos.sh/sharder/internal/machine"
)

// TestTimer checks that a timer is an effect and that the end of its wait is an event.
func TestTimer(t *testing.T) {
	t.Parallel()

	effects(t, []member{
		{name: "Timer", give: machine.Timer{}},
	})
	events(t, []member{
		{name: "Elapsed", give: machine.Elapsed{}},
	})
}

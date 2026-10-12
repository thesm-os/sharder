// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine_test

import (
	"testing"

	"go.thesmos.sh/sharder/internal/machine"
)

// TestProtocol checks that the calls of the clients of the protocol are effects and that
// their results are events.
func TestProtocol(t *testing.T) {
	t.Parallel()

	effects(t, []member{
		{name: "Callback", give: machine.Callback{}},
		{name: "SendReport", give: machine.SendReport{}},
		{name: "SendDisrupt", give: machine.SendDisrupt{}},
		{name: "SendDone", give: machine.SendDone{}},
	})
	events(t, []member{
		{name: "Called", give: machine.Called{}},
		{name: "Reported", give: machine.Reported{}},
		{name: "Decided", give: machine.Decided{}},
		{name: "Ended", give: machine.Ended{}},
	})
}

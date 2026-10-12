// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine_test

import (
	"testing"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/protocol"
)

// TestInbound checks that the inbound calls and their end are events and that every reply
// is an effect.
func TestInbound(t *testing.T) {
	t.Parallel()

	events(t, []member{
		{name: "Do", give: machine.Do{}},
		{name: "Report", give: machine.Report{}},
		{name: "Submit", give: machine.Submit{}},
		{name: "Disrupt", give: machine.Disrupt{}},
		{name: "Done", give: machine.Done{}},
		{name: "ReportFault", give: machine.ReportFault{}},
		{name: "Recovered", give: machine.Recovered{}},
		{name: "Close", give: machine.Close{}},
		{name: "Refresh", give: machine.Refresh{}},
		{name: "Cancelled", give: machine.Cancelled{}},
	})
	effects(t, []member{
		{name: "Reply[struct{}]", give: machine.Reply[struct{}]{}},
		{name: "Reply[protocol.Ack]", give: machine.Reply[protocol.Ack]{}},
		{name: "Reply[protocol.Receipt]", give: machine.Reply[protocol.Receipt]{}},
		{name: "Reply[budget.Decision]", give: machine.Reply[budget.Decision]{}},
	})
}

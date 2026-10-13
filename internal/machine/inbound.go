// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine

import (
	"time"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// Do is a callback that a server receives through protocol.AgentServer.Do. An agent
// receives the callbacks of the controller of its cell, and a controller receives the
// callbacks of the root of a namespace. The machine replies with a Reply[struct{}].
type Do struct {
	// Request is a copy of the request of the callback.
	Request *protocol.Request
	// Metadata is the metadata of the call.
	Metadata protocol.Metadata
	// Reply is the token of the call.
	Reply ReplyToken
}

var _ Event = Do{}

// event marks Do as an Event.
func (Do) event() {}

// Report is a report of an agent that a controller receives through
// protocol.ControllerServer.Report. The machine replies with a Reply[protocol.Ack].
type Report struct {
	// Report is a copy of the report.
	Report *protocol.Report
	// Metadata is the metadata of the call.
	Metadata protocol.Metadata
	// Reply is the token of the call.
	Reply ReplyToken
}

var _ Event = Report{}

// event marks Report as an Event.
func (Report) event() {}

// Submit is a proposal of an operator that a controller receives through
// protocol.ControllerServer.Submit. The machine replies with a Reply[protocol.Receipt].
type Submit struct {
	// Proposal is a copy of the proposal.
	Proposal *proposal.Proposal
	// Metadata is the metadata of the call.
	Metadata protocol.Metadata
	// Reply is the token of the call.
	Reply ReplyToken
}

var _ Event = Submit{}

// event marks Submit as an Event.
func (Submit) event() {}

// Disrupt is a planned disruption that a controller receives through
// protocol.ControllerServer.Disrupt. The machine replies with a Reply[budget.Decision].
type Disrupt struct {
	// Deadline is the deadline of the context of the call, and the zero time for a context
	// without a deadline. A deferral waits for the admission until WaitMargin before
	// Deadline.
	Deadline time.Time
	// Disruption is a copy of the disruption.
	Disruption *budget.Disruption
	// Metadata is the metadata of the call.
	Metadata protocol.Metadata
	// Reply is the token of the call.
	Reply ReplyToken
}

var _ Event = Disrupt{}

// event marks Disrupt as an Event.
func (Disrupt) event() {}

// WaitMargin is the time before the deadline of a waiting Disrupt at which the controller
// replies with the deferral, so the caller receives the reply before its deadline.
const WaitMargin = time.Second

// Done is the end of a reservation that a controller receives through
// protocol.ControllerServer.Done. The machine replies with a Reply[struct{}].
type Done struct {
	// Args is a copy of the argument of the call, with the ID of the reservation.
	Args *protocol.DoneArgs
	// Metadata is the metadata of the call.
	Metadata protocol.Metadata
	// Reply is the token of the call.
	Reply ReplyToken
}

var _ Event = Done{}

// event marks Done as an Event.
func (Done) event() {}

// ReportFault is the report of a fault in a replica on the node of an agent. The
// application makes the report, and the machine replies with a Reply[struct{}].
type ReportFault struct {
	// Cause is the error that the application reported with the fault.
	Cause error
	// Reply is the token of the call.
	Reply ReplyToken
	// Shard is the shard of the replica.
	Shard shard.ID
	// Fault is the class of the fault.
	Fault shard.Fault
}

var _ Event = ReportFault{}

// event marks ReportFault as an Event.
func (ReportFault) event() {}

// Recovered is the report that a replica on the node of an agent has recovered from a
// transient fault. The application makes the report, and the machine replies with a
// Reply[struct{}].
type Recovered struct {
	// Reply is the token of the call.
	Reply ReplyToken
	// Shard is the shard of the replica.
	Shard shard.ID
}

var _ Event = Recovered{}

// event marks Recovered as an Event.
func (Recovered) event() {}

// Close is the call with which the application closes an agent before its process stops,
// and with which a controller process closes the agent of its root. The machine replies
// with a Reply[struct{}].
type Close struct {
	// Deadline is the deadline of the context of the call, and the zero time for a context
	// without a deadline. The agent of a cell sends a disruption of its node that ends at
	// Deadline. The agent of the root ends its session without a disruption and ignores the
	// field.
	Deadline time.Time
	// Reply is the token of the call.
	Reply ReplyToken
}

var _ Event = Close{}

// event marks Close as an Event.
func (Close) event() {}

// Refresh is a wait of a client of a router for a generation of a cell. The machine
// replies with a Reply[struct{}].
type Refresh struct {
	// Cell is the cell of the generation.
	Cell shard.CellID
	// Reply is the token of the call.
	Reply ReplyToken
	// Min is the generation that the client waits for. A later generation of the cell
	// ends the wait as well.
	Min epoch.Epoch
	// Incarnation is the namespace incarnation of the generation.
	Incarnation shard.NamespaceIncarnation
}

var _ Event = Refresh{}

// event marks Refresh as an Event.
func (Refresh) event() {}

// Cancelled is the end of the wait of the caller of an inbound call. The runtime passes it
// once the context of the call ends before the machine replies. The runtime then replies
// to the caller itself and ignores a later Reply to the call, so the machine drops the
// call.
type Cancelled struct {
	// Reply is the token of the call.
	Reply ReplyToken
}

var _ Event = Cancelled{}

// event marks Cancelled as an Event.
func (Cancelled) event() {}

// Result is the constraint of the value of a [Reply]. The runtime returns the value to the
// caller of the inbound call. A [Report] returns a protocol.Ack, a [Submit] returns a
// protocol.Receipt, and a [Disrupt] returns a budget.Decision. The other calls return only
// an error, through a Reply[struct{}].
type Result interface {
	struct{} | protocol.Ack | protocol.Receipt | budget.Decision
}

// Reply is the reply of a machine to an inbound call. The runtime returns Value and Err to
// the caller, and does not pass a result for a Reply.
//
// The zero Reply is the reply to no call.
type Reply[T Result] struct {
	// Value is the value that the call returns.
	Value T
	// Err is the error that the call returns, or nil.
	Err error
	// To is the token of the call.
	To ReplyToken
}

var (
	_ Effect = Reply[struct{}]{}
	_ Effect = Reply[protocol.Ack]{}
	_ Effect = Reply[protocol.Receipt]{}
	_ Effect = Reply[budget.Decision]{}
)

// effect marks a Reply as an Effect.
func (Reply[T]) effect() {}

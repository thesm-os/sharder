// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine

import (
	"time"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// Callback is a callback to the agent of a node through protocol.AgentClient.Do. The
// runtime passes the result as a [Called].
type Callback struct {
	// Node is the node of the agent.
	Node *shard.Node
	// Request is the request of the callback. A retry of the callback has the same
	// request.
	Request *protocol.Request
	// Token is the token of the effect.
	Token Token
	// Timeout is the longest time that the call can take. It is the timeout of the step of
	// the callback.
	Timeout time.Duration
}

var _ Effect = Callback{}

// effect marks Callback as an Effect.
func (Callback) effect() {}

// Called is the result of a [Callback].
type Called struct {
	// Err is the error of protocol.AgentClient.Do, or nil. A call that runs past its
	// Timeout returns the error of its context.
	Err error
	// Token is the token of the Callback.
	Token Token
}

var _ Event = Called{}

// event marks Called as an Event.
func (Called) event() {}

// SendReport is a report of an agent to the controller of its cell through
// protocol.ControllerClient.Report. The runtime passes the result as a [Reported].
type SendReport struct {
	// Cell is the record of the cell, with the address and the identity of its controller.
	Cell *shard.Cell
	// Report is the report. The agent leaves the Load of each replica nil. The runtime
	// sends a copy of the report in which the Load of each replica is the load that the
	// application reports for the shard, when the application reports loads.
	Report *protocol.Report
	// Token is the token of the effect.
	Token Token
	// Timeout is the longest time that the call can take.
	Timeout time.Duration
}

var _ Effect = SendReport{}

// effect marks SendReport as an Effect.
func (SendReport) effect() {}

// Reported is the result of a [SendReport].
type Reported struct {
	// Err is the error of protocol.ControllerClient.Report, or nil.
	Err error
	// Token is the token of the SendReport.
	Token Token
	// Ack is the ack of the controller.
	Ack protocol.Ack
}

var _ Event = Reported{}

// event marks Reported as an Event.
func (Reported) event() {}

// SendDisrupt is a request for a planned disruption through
// protocol.ControllerClient.Disrupt. An agent requests a disruption of its own node when
// the application closes the agent. The runtime passes the result as a [Decided].
type SendDisrupt struct {
	// Cell is the record of the cell, with the address and the identity of its controller.
	Cell *shard.Cell
	// Disruption is the disruption.
	Disruption *budget.Disruption
	// Token is the token of the effect.
	Token Token
	// Timeout is the longest time that the call can take.
	Timeout time.Duration
}

var _ Effect = SendDisrupt{}

// effect marks SendDisrupt as an Effect.
func (SendDisrupt) effect() {}

// Decided is the result of a [SendDisrupt].
type Decided struct {
	// Err is the error of protocol.ControllerClient.Disrupt, or nil.
	Err error
	// Decision is the decision of the budget of the cell.
	Decision budget.Decision
	// Token is the token of the SendDisrupt.
	Token Token
}

var _ Event = Decided{}

// event marks Decided as an Event.
func (Decided) event() {}

// SendDone is the end of a reservation at the controller of a cell through
// protocol.ControllerClient.Done. The runtime passes the result as an [Ended].
type SendDone struct {
	// Cell is the record of the cell, with the address and the identity of its controller.
	Cell *shard.Cell
	// Args is the argument of the call, with the ID of the reservation.
	Args *protocol.DoneArgs
	// Token is the token of the effect.
	Token Token
	// Timeout is the longest time that the call can take.
	Timeout time.Duration
}

var _ Effect = SendDone{}

// effect marks SendDone as an Effect.
func (SendDone) effect() {}

// Ended is the result of a [SendDone].
type Ended struct {
	// Err is the error of protocol.ControllerClient.Done, or nil.
	Err error
	// Token is the token of the SendDone.
	Token Token
}

var _ Event = Ended{}

// event marks Ended as an Event.
func (Ended) event() {}

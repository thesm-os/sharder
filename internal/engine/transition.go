// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package engine

import (
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/fsm"

	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// noSide is the zero Side. A commit on it commits the placement without an assigned writer.
const noSide handoff.Side = 0

// wait is what a transition waits for.
//
//dokimi:enum text=comment
type wait uint8

// The waits of a transition.
const (
	// waitNone is the wait of a new transition before it waits for anything.
	waitNone wait = 1 // none
	// waitBatch is the wait for the batch that commits the change of the transition.
	waitBatch wait = 2 // batch
	// waitPermit is the wait in the queue of a node for a permit of a callback.
	waitPermit wait = 3 // permit
	// waitCall is the wait for the result of a callback.
	waitCall wait = 4 // call
	// waitRetry is the wait for the Timer of the backoff before the next attempt of a
	// callback.
	waitRetry wait = 5 // retry
	// waitRound is the wait for the Timer before the next round of attempts of a step of
	// an abort path.
	waitRound wait = 6 // round
	// waitLapse is the wait for the Timer of the lock delay before a drop on a node that
	// has left the view completes.
	waitLapse wait = 7 // lapse
)

// event is an event of the wait of a transition. Each event leads to one wait.
//
//dokimi:enum text=comment
type event uint8

// The events of the wait of a transition.
const (
	// eventMark makes the change of the transition wait for the next batch.
	eventMark event = 1 // mark
	// eventQueue makes the callback of the current step wait for a permit of its node.
	eventQueue event = 2 // queue
	// eventSend makes the transition wait for the result of the callback of its current
	// step.
	eventSend event = 3 // send
	// eventBackoff makes the transition wait for the Timer of the backoff before the next
	// attempt of its callback.
	eventBackoff event = 4 // backoff
	// eventRound makes the transition wait for the Timer before the next round of attempts
	// of a step of an abort path.
	eventRound event = 5 // round
	// eventDelay makes the transition wait for the Timer of the lock delay before a drop on
	// a node that has left the view completes.
	eventDelay event = 6 // delay
)

// waitLifecycle is the table of the waits of a transition. Each edge leads from a wait in
// which the Engine can send its event. When the table has no edge for an event, the call
// of the Engine that sent it returns the error of fsm.Machine.Fire.
var waitLifecycle = fsm.Must(fsm.NewBuilder[wait, event, struct{}](waitNone).
	Edges([]wait{waitNone, waitBatch, waitPermit, waitCall, waitRetry, waitRound, waitLapse}, eventMark, waitBatch).
	Edges([]wait{waitNone, waitBatch, waitRetry, waitRound}, eventQueue, waitPermit).
	Edges([]wait{waitNone, waitBatch, waitPermit, waitRetry, waitRound}, eventSend, waitCall).
	Edge(waitCall, eventBackoff, waitRetry).
	Edge(waitCall, eventRound, waitRound).
	Edges([]wait{waitNone, waitBatch, waitPermit, waitCall, waitRetry, waitRound}, eventDelay, waitLapse).
	Build())

// transition is one transition in flight: its record and its placement as the Engine has
// committed them or commits them next, and the state of its current step.
type transition struct {
	// request is the request of the callback of the current step. Every attempt of the
	// callback sends it.
	request *protocol.Request
	// phase is the phase of the transition in handoff.Lifecycle.
	phase fsm.Machine[shard.Phase, handoff.Event, struct{}]
	// waiting is what the transition waits for, in waitLifecycle.
	waiting fsm.Machine[wait, event, struct{}]
	// script is the script of the move.
	script handoff.Script
	// placement is the placement of the shard.
	placement shard.Placement
	// record is the record of the transition. Its phase is the phase of the transition
	// until the transition ends.
	record store.Transition
	// token is the token of the Callback or the Timer that the transition waits for.
	token machine.Token
	// attempt is the attempt of the callback of the current step, from zero.
	attempt int
	// put reports that the change of the transition puts its placement.
	put bool
	// ended reports that the change of the transition ends the transition.
	ended bool
}

// path returns the steps of the current path of t: the script while t runs, and the abort
// path of the failed step while t aborts. It returns nil for another phase, and for a
// failed step that the script does not have.
func (t *transition) path() []handoff.Step {
	phase := t.phase.State()
	if phase == shard.PhaseRunning {
		return t.script.Steps
	}
	if phase == shard.PhaseAborting && t.record.Failed >= 0 && t.record.Failed < len(t.script.Abort) {
		return t.script.Abort[t.record.Failed]
	}
	return nil
}

// step returns the current step of t. The current path of t has the step.
func (t *transition) step() handoff.Step {
	return t.path()[t.record.Step]
}

// endpoint returns the endpoint of the side s of t, and the zero Endpoint for the zero
// Side.
func (t *transition) endpoint(s handoff.Side) store.Endpoint {
	switch s {
	case handoff.From:
		return t.record.From
	case handoff.To:
		return t.record.To
	case handoff.Fallback:
		return t.record.Fallback
	}
	return store.Endpoint{}
}

// epoch returns the epoch of the side s of t, and the zero epoch for the zero Side.
func (t *transition) epoch(s handoff.Side) epoch.Epoch {
	switch s {
	case handoff.From:
		return t.record.OldEpoch
	case handoff.To:
		return t.record.NewEpoch
	case handoff.Fallback:
		return t.record.AbortEpoch
	}
	return epoch.Zero
}

// involves reports whether n is the node of a side of t.
func (t *transition) involves(n shard.NodeID) bool {
	return n == t.record.From.Node || n == t.record.To.Node || n == t.record.Fallback.Node
}

// commit changes the placement of t to the placement that a commit step on the side s
// commits. The replicas on the nodes of the sides of t leave the placement, and the
// replica of s joins it as the writer at the epoch of s. The placement records the
// incarnation of the endpoint of s. For the zero Side, the placement has no assigned
// writer, the zero epoch and the zero incarnation.
func (t *transition) commit(s handoff.Side) {
	ep := t.endpoint(s)
	replicas := make([]shard.Replica, 0, len(t.placement.Replicas)+1)
	for _, r := range t.placement.Replicas {
		if !t.involves(r.Node) {
			replicas = append(replicas, r)
		}
	}
	if ep.Node != "" {
		replicas = append(replicas, shard.Replica{Node: ep.Node, Role: ep.Role})
	}
	t.placement.Replicas, t.placement.Epoch, t.placement.Incarnation = replicas, t.epoch(s), ep.Incarnation
	t.put = true
}

// newRequest returns the request of the callback of the current step of t, for the scope
// of the transitions and the controller epoch term.
func (t *transition) newRequest(scope shard.Scope, term epoch.Epoch) *protocol.Request {
	step := t.step()
	ep := t.endpoint(step.Side)
	call, path := protocol.Add, protocol.Forward
	if step.Action == handoff.Drop {
		call = protocol.Drop
	}
	if t.phase.State() == shard.PhaseAborting {
		path = protocol.Abort
	}
	return &protocol.Request{
		Scope:           scope,
		Node:            ep.Node,
		Range:           t.placement.Range,
		Operation:       t.record.Operation,
		Step:            t.record.Step,
		NodeIncarnation: ep.Incarnation,
		Epoch:           t.epoch(step.Side),
		ControllerEpoch: term,
		Shard:           t.record.Operation.Shard,
		Call:            call,
		Path:            path,
		Role:            ep.Role,
	}
}

// write adds the change of t to m: the end of t, or its record. A change that puts the
// placement of t also puts it.
func (t *transition) write(m *store.Mutation) {
	if t.ended {
		m.Done = append(m.Done, t.record.Operation.Shard)
	} else {
		m.Transitions = append(m.Transitions, t.record)
	}
	if t.put {
		m.Put = append(m.Put, t.placement)
	}
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/resilience"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// closing is the state of a Close: the disruption of the node that the agent sends to the
// controller of the cell, and the call of the application that waits.
type closing struct {
	// disruption is the disruption of the node.
	disruption budget.Disruption
	// reply is the token of the Close that waits for its reply, and zero without one.
	reply machine.ReplyToken
	// flight is the token of the last SendDisrupt or SendDone, and zero before the first.
	flight machine.Token
	// retry is the token of the last Timer before a SendDisrupt or a SendDone, and zero
	// before the first.
	retry machine.Token
	// attempt counts the calls of the Close that failed in a row.
	attempt int
	// leaving is true once the controller has admitted the disruption and the agent has
	// ended the session of the node.
	leaving bool
}

// close takes the Close of the application. The agent sends a negotiable disruption of the
// node at its incarnation to the controller of the cell. The disruption lasts from now until
// the deadline of the call. The controller admits it once the replicas of the node have moved
// away. The agent then ends the session of the node, ends the reservation of the disruption,
// and stops. A node without a session has no replicas to move, so its Close ends at once.
//
// The agent of the root ends the session of the node without a disruption, because the root
// places the cells of the process on other processes once the view no longer lists the
// process. Its Close ends with the result of the Leave.
//
// A Close before the agent serves gets ErrNotServing. A Close of the agent of a cell without
// a deadline after now gets an error that wraps ErrConfig, because the budget needs the end
// of the disruption. Neither changes the state of the agent.
//
// Error modes:
//   - the error of fsm.Machine.Fire.
func (m *Machine) close(ev machine.Close, out []machine.Effect) ([]machine.Effect, error) {
	if st := m.lc.State(); st != stateServing && st != stateRejoining {
		return append(out, machine.Reply[struct{}]{Err: ErrNotServing, To: ev.Reply}), nil
	}
	now := m.clock.Time()
	if !m.inRoot && !ev.Deadline.After(now) {
		err := fmt.Errorf("%w: Close needs a deadline after %v", ErrConfig, now)
		return append(out, machine.Reply[struct{}]{Err: err, To: ev.Reply}), nil
	}
	err := m.fire(eventClose)
	m.closing.reply = ev.Reply
	if !m.session.live {
		var cerr error
		out, cerr = m.closed(nil, out)
		return out, errors.Join(err, cerr)
	}
	if m.inRoot {
		m.closing.leaving = true
		return append(out, machine.Leave{Token: m.session.join}), err
	}
	m.closing.disruption = budget.Disruption{
		Start:           now,
		End:             ev.Deadline,
		Scope:           m.cellScope,
		Nodes:           []shard.NodeID{m.node.ID},
		NodeIncarnation: m.session.incarnation,
		ID:              budget.ID{ID: m.ids.Generate()},
		Incarnation:     m.incarnation,
		Impact:          budget.ProcessRestart,
		Negotiable:      true,
	}
	return m.disrupt(out), err
}

// disrupt asks the controller of the cell to admit the disruption of the Close.
func (m *Machine) disrupt(out []machine.Effect) []machine.Effect {
	m.closing.flight = m.next()
	cell := m.watched.record
	return append(out, machine.SendDisrupt{
		Cell:       &cell,
		Disruption: &m.closing.disruption,
		Token:      m.closing.flight,
		Timeout:    CallTimeout,
	})
}

// decided takes the decision of the controller on the disruption of the Close. After an
// admission, the agent ends the session of the node. After a deferral, it sends the
// disruption again once the RetryAfter of the decision has passed, and at least RetryBase
// later. After a call that failed, it logs the error and sends the disruption again after a
// backoff.
func (m *Machine) decided(ev *machine.Decided, out []machine.Effect) []machine.Effect {
	if ev.Token != m.closing.flight {
		return out
	}
	if ev.Err == nil && ev.Decision.Allowed {
		m.closing.leaving = true
		return append(out, machine.Leave{Token: m.session.join})
	}
	delay := max(ev.Decision.RetryAfter, RetryBase)
	if ev.Err != nil {
		m.logger.LogAttrs(context.Background(), slog.LevelWarn, "agent: a disruption of the node failed",
			slog.Any(attrError, ev.Err))
		m.closing.attempt++
		delay = resilience.Backoff(m.rand, m.closing.attempt, RetryBase, RetryMax)
	}
	m.closing.retry = m.next()
	return append(out, machine.Timer{Token: m.closing.retry, Duration: delay})
}

// again makes the next call of the Close after its wait: the end of the reservation once
// the agent has left the cell, and the disruption before.
func (m *Machine) again(out []machine.Effect) []machine.Effect {
	if m.closing.leaving {
		return m.done(out)
	}
	return m.disrupt(out)
}

// left takes the end of the session of the node during a Close. After the controller has
// admitted the disruption, the agent ends the reservation of the disruption. A session that
// ends before the admission ends the Close with an error that wraps ErrNotServing. The end
// of the session of the agent of the root ends its Close.
//
// Error modes:
//   - the error of closed.
func (m *Machine) left(out []machine.Effect) ([]machine.Effect, error) {
	if !m.closing.leaving {
		err := fmt.Errorf("%w: the session of node %s ended before its disruption", ErrNotServing, m.node.ID)
		return m.closed(err, out)
	}
	if m.inRoot {
		return m.closed(nil, out)
	}
	return m.done(out), nil
}

// departed takes the result of the Leave of a Close of the agent of the root. The Close
// then ends with an error that wraps the error of the Leave, or with success. The agent of a
// cell ends its reservation after the end of the session instead, so it ignores the result.
//
// Error modes:
//   - the error of closed.
func (m *Machine) departed(ev machine.Left, out []machine.Effect) ([]machine.Effect, error) {
	if !m.inRoot || !m.closing.leaving || ev.Token != m.session.join {
		return out, nil
	}
	var err error
	if ev.Err != nil {
		err = fmt.Errorf("agent: the leave of node %s from %v: %w", m.node.ID, m.cellScope, ev.Err)
	}
	return m.closed(err, out)
}

// done asks the controller of the cell to end the reservation of the disruption of the
// Close.
func (m *Machine) done(out []machine.Effect) []machine.Effect {
	m.closing.flight = m.next()
	cell := m.watched.record
	return append(out, machine.SendDone{
		Cell:    &cell,
		Args:    &protocol.DoneArgs{Scope: m.cellScope, Incarnation: m.incarnation, ID: m.closing.disruption.ID},
		Token:   m.closing.flight,
		Timeout: CallTimeout,
	})
}

// released takes the result of the end of the reservation. The Close then ends with the
// error of the call, or asks again after a backoff for a transient error.
//
// Error modes:
//   - the error of closed.
func (m *Machine) released(ev machine.Ended, out []machine.Effect) ([]machine.Effect, error) {
	if ev.Token != m.closing.flight {
		return out, nil
	}
	if errs.Retryable(ev.Err) {
		m.closing.attempt++
		m.closing.retry = m.next()
		delay := resilience.Backoff(m.rand, m.closing.attempt, RetryBase, RetryMax)
		return append(out, machine.Timer{Token: m.closing.retry, Duration: delay}), nil
	}
	return m.closed(ev.Err, out)
}

// abandon takes the end of the wait of the caller of a Close: the runtime has replied with
// the error of the context of the call. The agent ends the session of the node without the
// end of the reservation, and stops. The controller ends the reservation once the
// incarnation of the node has ended and the placements do not list the node. A Close waits
// only while the node has a session, so the agent has a session to end until the controller
// admits the disruption.
//
// Error modes:
//   - the error of fsm.Machine.Fire.
func (m *Machine) abandon(out []machine.Effect) ([]machine.Effect, error) {
	m.closing.reply = 0
	if !m.closing.leaving {
		out = append(out, machine.Leave{Token: m.session.join})
	}
	return m.halt(m.unwatch(out)), m.fire(eventStop)
}

// closed ends the Close with err: it replies to the Close, ends the reads of the scopes,
// replies with ErrNotServing to every callback that waits, and stops the agent. Machine.Err
// returns nil after a Close.
//
// Error modes:
//   - the error of fsm.Machine.Fire.
func (m *Machine) closed(err error, out []machine.Effect) ([]machine.Effect, error) {
	out = append(out, machine.Reply[struct{}]{Err: err, To: m.closing.reply})
	m.closing.reply = 0
	return m.halt(m.unwatch(out)), m.fire(eventStop)
}

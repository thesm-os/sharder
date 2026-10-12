// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"fmt"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/resilience"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/ownership"
)

// session is the membership session of the node in its cell. Each session has an
// incarnation of its own, above the incarnation of every earlier session of the node.
type session struct {
	// join is the token of the Join of the session, and zero without one.
	join machine.Token
	// wait is the token of the last Timer before a Join, and zero before the first.
	wait machine.Token
	// incarnation is the incarnation of the last session that the node joined, and zero
	// before the first.
	incarnation epoch.Epoch
	// attempt counts the joins that failed and the sessions that ended in a row.
	attempt int
	// live is true while the session of incarnation lasts.
	live bool
}

// join appends the Join of the next session of the node to out.
func (m *Machine) join(out []machine.Effect) []machine.Effect {
	m.session.join = m.next()
	node := m.node
	return append(out, machine.Join{
		Node:        &node,
		Scope:       m.cellScope,
		Token:       m.session.join,
		Lease:       m.settings.lease,
		Incarnation: m.incarnation,
	})
}

// rejoin appends the Timer of the backoff before the next Join to out.
func (m *Machine) rejoin(out []machine.Effect) []machine.Effect {
	m.session.attempt++
	m.session.wait = m.next()
	delay := resilience.Backoff(m.rand, m.session.attempt, RetryBase, RetryMax)
	return append(out, machine.Timer{Token: m.session.wait, Duration: delay})
}

// joined takes the result of the Join of the session. A session gives the node a new
// incarnation: the agent refuses the callbacks of every earlier incarnation, and numbers the
// reports of the incarnation from one. The first session serves once the first snapshot of
// the scope of the cell is installed. A Join that fails with a transient error runs again
// after a backoff.
//
// Error modes:
//   - an error that wraps the error of the Join for an error that is not transient.
//   - the error of fsm.Machine.Fire.
func (m *Machine) joined(ev *machine.Joined, out []machine.Effect) ([]machine.Effect, error) {
	if ev.Token != m.session.join {
		return out, nil
	}
	if ev.Err != nil {
		m.session.join = 0
		if !errs.Retryable(ev.Err) {
			return out, fmt.Errorf("agent: the join of node %s to %v: %w", m.node.ID, m.cellScope, ev.Err)
		}
		return m.rejoin(out), nil
	}
	m.session.attempt = 0
	m.session.incarnation, m.session.live = ev.Incarnation, true
	m.reports.sequence = 0
	if m.lc.State() == stateRejoining {
		err := m.fire(eventJoined)
		return m.report(out), err
	}
	return m.serve(out)
}

// ended takes the end of the session of the node. Under ownership.Stop, the deadline of the
// lease has passed, so Machine.Token returns an error for every tenure. The agent then joins
// again after a backoff, with a new incarnation. The end of the session during a Close
// either completes the Close or ends it.
//
// Error modes:
//   - the error of fsm.Machine.Fire.
func (m *Machine) ended(ev machine.SessionEnded, out []machine.Effect) ([]machine.Effect, error) {
	if ev.Token != m.session.join {
		return out, nil
	}
	m.session.join, m.session.live = 0, false
	if m.settings.lease.OnLoss == ownership.Stop {
		m.revoke()
	}
	st := m.lc.State()
	if st == stateClosing {
		return m.left(out)
	}
	var err error
	if st == stateServing {
		err = m.fire(eventLost)
	}
	return m.rejoin(out), err
}

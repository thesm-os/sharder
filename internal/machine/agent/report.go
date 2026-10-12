// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"context"
	"log/slog"

	"go.thesmos.sh/core/resilience"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// reports is the state of the reports of the node to the controller of its cell. One report
// is in flight at a time.
type reports struct {
	// sequence is the Sequence of the last report of the incarnation of the node, and zero
	// before the first.
	sequence uint64
	// flight is the token of the SendReport in flight, and zero without one.
	flight machine.Token
	// window is the token of the Timer of the next periodic report.
	window machine.Token
	// retry is the token of the Timer of the backoff after a report that failed, and zero
	// without one.
	retry machine.Token
	// attempt counts the reports that failed in a row.
	attempt int
	// dirty is true when the agent has a report to send.
	dirty bool
}

// report sends a full report of the replicas of the node to the controller of the cell. The
// agent sends it later while a report is in flight or waits for its backoff, while the node
// has no session, while the cell has no controller, and before the agent serves. The
// runtime fills the loads of the replicas from the application.
func (m *Machine) report(out []machine.Effect) []machine.Effect {
	m.reports.dirty = true
	if m.reports.flight != 0 || m.reports.retry != 0 || !m.session.live || m.watched.record.Controller == "" ||
		m.lc.State() == stateJoining {

		return out
	}
	m.reports.dirty = false
	m.reports.sequence++
	r := &protocol.Report{
		Scope:           m.cellScope,
		Node:            m.node.ID,
		Replicas:        m.observed(),
		NodeIncarnation: m.session.incarnation,
		Sequence:        m.reports.sequence,
		Incarnation:     m.incarnation,
	}
	cell := m.watched.record
	m.reports.flight = m.next()
	return append(out, machine.SendReport{Cell: &cell, Report: r, Token: m.reports.flight, Timeout: CallTimeout})
}

// observed returns what the node reports about each of its tenures, in the order of the
// shards.
func (m *Machine) observed() []shard.Observed {
	now := m.clock.Time()
	var replicas []shard.Observed
	for sid, st := range m.shards.All() {
		if t := st.tenure; t != nil {
			replicas = append(replicas, shard.Observed{
				Cause:    t.cause,
				Reported: now,
				Epoch:    t.request.Epoch,
				Shard:    sid,
				Role:     t.request.Role,
				Status:   t.status,
				Fault:    t.fault,
			})
		}
	}
	return replicas
}

// reported takes the result of the report in flight. A report that the controller accepted
// raises the controller epoch of the agent to the epoch of the controller, which makes the
// agent refuse the callbacks of a replaced controller. It sets the lag of the agent behind
// the generation that the controller has committed, and the next report follows after the
// load window of the spec. A report that failed runs again after a backoff, and the agent
// logs the error.
func (m *Machine) reported(ev *machine.Reported, out []machine.Effect) []machine.Effect {
	if ev.Token != m.reports.flight {
		return out
	}
	m.reports.flight = 0
	if ev.Err != nil {
		m.logger.LogAttrs(context.Background(), slog.LevelWarn, "agent: a report failed", slog.Any(attrError, ev.Err))
		m.reports.attempt++
		m.reports.retry, m.reports.dirty = m.next(), true
		delay := resilience.Backoff(m.rand, m.reports.attempt, RetryBase, RetryMax)
		return append(out, machine.Timer{Token: m.reports.retry, Duration: delay})
	}
	m.reports.attempt = 0
	m.epoch = max(m.epoch, ev.Ack.ControllerEpoch)
	lag := max(ev.Ack.Generation, m.watched.generation) - m.watched.generation
	m.metrics.lag.Set(context.Background(), float64(lag))
	m.reports.window = m.next()
	out = append(out, machine.Timer{Token: m.reports.window, Duration: m.settings.window})
	if m.reports.dirty {
		return m.report(out)
	}
	return out
}

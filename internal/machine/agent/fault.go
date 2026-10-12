// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"fmt"
	"strings"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/shard"
)

// maxCause is the most bytes of the message of the cause of a fault in a report.
const maxCause = 1024

// fault takes the report of a fault in the replica of a shard on the node. The agent reports
// the replica as failed at once, with the fault and at most the first 1,024 bytes of the
// message of its cause. It replies with an error that wraps ErrRequest for a fault other
// than a transient or a permanent fault, and with an error that wraps ErrNoReplica for a
// shard without a replica on the node.
func (m *Machine) fault(ev *machine.ReportFault, out []machine.Effect) []machine.Effect {
	var err error
	t := m.tenureOf(ev.Shard)
	if ev.Fault != shard.FaultTransient && ev.Fault != shard.FaultPermanent {
		err = fmt.Errorf("%w: the fault %v of shard %v", ErrRequest, ev.Fault, ev.Shard)
	} else if t == nil {
		err = fmt.Errorf("%w: shard %v", ErrNoReplica, ev.Shard)
	} else {
		t.status, t.fault, t.cause = shard.StatusFailed, ev.Fault, ""
		if ev.Cause != nil {
			msg := ev.Cause.Error()
			t.cause = strings.ToValidUTF8(msg[:min(len(msg), maxCause)], "")
		}
		out = m.report(out)
	}
	return append(out, machine.Reply[struct{}]{Err: err, To: ev.Reply})
}

// recovered takes the recovery of the replica of a shard on the node from a transient fault.
// The replica serves again, and the agent reports it. It replies with an error that wraps
// ErrNoReplica for a shard without a replica on the node, and with an error that wraps
// ErrNoFault for a replica without a transient fault.
func (m *Machine) recovered(ev machine.Recovered, out []machine.Effect) []machine.Effect {
	var err error
	t := m.tenureOf(ev.Shard)
	if t == nil {
		err = fmt.Errorf("%w: shard %v", ErrNoReplica, ev.Shard)
	} else if t.fault != shard.FaultTransient {
		err = fmt.Errorf("%w: shard %v has the fault %v", ErrNoFault, ev.Shard, t.fault)
	} else {
		t.status, t.fault, t.cause = shard.StatusServing, shard.FaultNone, ""
		out = m.report(out)
	}
	return append(out, machine.Reply[struct{}]{Err: err, To: ev.Reply})
}

// tenureOf returns the tenure of the shard sid on the node, and nil without one.
func (m *Machine) tenureOf(sid shard.ID) *tenure {
	if st, ok := m.shards.Get(sid); ok {
		return st.tenure
	}
	return nil
}

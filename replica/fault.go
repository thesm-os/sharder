// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica

import (
	"context"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/shard"
)

// ReportFaultRequest is the request of Agent.ReportFault.
type ReportFaultRequest struct {
	_ incomparable
	// Cause is the error that made the replica fail. The agent reports the first 1,024 bytes
	// of its message.
	Cause error
	// Shard is the shard of the replica.
	Shard shard.ID
	// Fault is the class of the fault. A transient fault keeps the replica on its node. A
	// permanent fault makes the controller replace the replica on another node.
	Fault shard.Fault
}

// ReportFaultResult is the result of Agent.ReportFault.
type ReportFaultResult struct {
	_ incomparable
}

// RecoveredRequest is the request of Agent.Recovered.
type RecoveredRequest struct {
	_ incomparable
	// Shard is the shard of the replica.
	Shard shard.ID
}

// RecoveredResult is the result of Agent.Recovered.
type RecoveredResult struct {
	_ incomparable
}

// ReportFault reports a fault in the replica of the shard req.Shard on the node. The agent
// reports the replica as failed at once, with the fault and the cause of req. From the report
// on, the controller counts the replica as unavailable. It moves the writer role off a
// faulted writer, and replaces a replica with a permanent fault on another node.
//
// Error modes:
//   - an error that wraps ErrNoReplica for a shard without a replica on the node.
//   - an error that wraps ErrNotServing before the agent serves and once its run has ended.
//   - an error that wraps the cause of ctx when ctx ends before the agent takes the report.
func (a *Agent) ReportFault(ctx context.Context, req *ReportFaultRequest) (ReportFaultResult, error) {
	cause, sid, fault := req.Cause, req.Shard, req.Fault
	err := a.call(ctx, callFault, func(tok machine.ReplyToken) machine.Event {
		return machine.ReportFault{Cause: cause, Shard: sid, Fault: fault, Reply: tok}
	})
	return ReportFaultResult{}, err
}

// Recovered reports that the replica of the shard req.Shard on the node serves again after a
// transient fault. The agent reports the replica as serving at once.
//
// Error modes:
//   - an error that wraps ErrNoReplica for a shard without a replica on the node.
//   - an error that wraps ErrNoFault for a replica without a transient fault.
//   - an error that wraps ErrNotServing before the agent serves and once its run has ended.
//   - an error that wraps the cause of ctx when ctx ends before the agent takes the report.
func (a *Agent) Recovered(ctx context.Context, req *RecoveredRequest) (RecoveredResult, error) {
	sid := req.Shard
	err := a.call(ctx, callRecovery, func(tok machine.ReplyToken) machine.Event {
		return machine.Recovered{Shard: sid, Reply: tok}
	})
	return RecoveredResult{}, err
}

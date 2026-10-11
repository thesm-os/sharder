// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol

//go:generate go tool kanon -type=Report,Ack

import (
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/shard"
)

// Report is a full snapshot of the replicas of one node, which the agent of the node sends
// to the controller of its cell.
//
// The zero Report is no report.
//
// # Encoding
//
// kanon generates the codec of Report. Scope is field 1, Incarnation is field 2, Node is
// field 3, NodeIncarnation is field 4, Sequence is field 5, and Replicas is field 6. The
// codec returns the error of the codec of shard.Observed for a replica that fails to
// encode.
//
// # Concurrency
//
// A copy of a Report shares the elements of Replicas with the original.
type Report struct {
	// Scope is the scope of the cell of the node.
	Scope shard.Scope
	// Node is the reporting node.
	Node shard.NodeID
	// Replicas lists every replica on the node. A replica that an accepted report omits is
	// not on the node.
	Replicas []shard.Observed
	// NodeIncarnation is the incarnation of the process of the node.
	NodeIncarnation epoch.Epoch
	// Sequence increases by one with every report of the node incarnation. The controller
	// discards a report whose Sequence is not above the last one that it accepted from the
	// node incarnation.
	Sequence uint64
	// Incarnation is the namespace incarnation of the node.
	Incarnation shard.NamespaceIncarnation
}

// Ack is the reply of the controller to a report.
//
// The zero Ack is the reply of a controller that has not accepted a report and has not
// committed a generation.
//
// # Encoding
//
// kanon generates the codec of Ack. Sequence is field 1, Generation is field 2, and
// ControllerEpoch is field 3.
//
// # Concurrency
//
// An Ack is a comparable value, and a copy is independent of the original.
type Ack struct {
	// Sequence is the Sequence of the report that the controller accepted last.
	Sequence uint64
	// Generation is the latest generation of the cell that the controller has committed.
	Generation epoch.Epoch
	// ControllerEpoch is the fence epoch of the controller. The agent refuses callbacks
	// with a lower one.
	ControllerEpoch epoch.Epoch
}

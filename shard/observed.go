// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=Observed

import (
	"time"

	"go.thesmos.sh/core/epoch"
)

// Load maps the name of a load metric, such as "cpu", to its value, averaged over the
// load window of the namespace. A published Load is never modified.
type Load map[string]float64

// Observed is what a node reports about one of its replicas.
//
// The zero Observed is no report. Its Status is the zero Status, which is not Valid.
//
// # Encoding
//
// kanon generates the codec of Observed. Shard is field 1, Role is field 2, Epoch is
// field 3, Status is field 4, Load is field 5, Fault is field 6, Cause is field 7,
// Range is field 8, Parents is field 9, and Reported is field 10. The codec returns
// an error that wraps [ErrUnknownRole], [ErrUnknownStatus] or [ErrUnknownFault] for a
// Role, a Status or a Fault other than zero that is not Valid.
//
// # Concurrency
//
// A copy of an Observed shares the map of Load, the bytes of Range and the elements
// of Parents with the original.
//
// # Allocation contract
//
// Construction does not allocate.
type Observed struct {
	// Load is the load of the replica. It is nil when the application does not report
	// a load of its own.
	Load Load
	// Cause is at most the first 1,024 bytes of the message of the error that the
	// application reported with the fault, or empty for FaultNone.
	Cause string
	// Reported is the instant at which the node built the report.
	Reported time.Time
	// Range is the range of a shard that a split or a merge by the authority of a
	// delegated namespace created, and empty for any other shard.
	Range Range
	// Parents lists the shards that a split or a merge by the authority of a
	// delegated namespace replaced with the shard, and is empty for any other shard.
	Parents []ID
	// Epoch is the fence epoch of the tenure of the replica, or epoch.Zero for a
	// replica that does not write.
	Epoch epoch.Epoch
	// Shard is the shard of the replica.
	Shard ID
	// Role is the role in which the node runs the replica.
	Role Role
	// Status is the condition of the replica.
	Status Status
	// Fault is the fault that the application reported in the replica.
	Fault Fault
}

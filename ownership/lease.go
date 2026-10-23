// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package ownership

import (
	"fmt"
	"time"

	"go.thesmos.sh/sharder/shard"
)

// OnLeaseLoss is what the writers of a node do when the lease deadline of the node
// passes. It is the choice between availability and consistency while the store is
// unreachable.
//
// The zero OnLeaseLoss is no action, and Config.Validate refuses it.
//
// # Encoding
//
// [OnLeaseLoss.String] returns the spelling of an OnLeaseLoss, such as "stop", and
// [ParseOnLeaseLoss] returns the action of a spelling. The text methods write and read
// the spelling, so encoding/json writes an OnLeaseLoss as a string.
//
//dokimi:enum text=comment
type OnLeaseLoss uint8

const (
	// Stop stops every writer replica of the node when its lease deadline passes. A
	// store outage that lasts longer than the TTL stops every writer.
	Stop OnLeaseLoss = 1 // stop
	// ContinueFenced keeps the writers running after the deadline. It is safe only
	// when every write of the application goes through a fence, because the fence is
	// then the only protection against a new writer.
	ContinueFenced OnLeaseLoss = 2 // continue-fenced
)

// valid returns nil for Stop and ContinueFenced, and an error that wraps ErrConfig and
// states the number of o for any other. Config.Validate calls it.
func (o OnLeaseLoss) valid() error {
	if !o.IsValid() {
		return fmt.Errorf("%w: unknown action on lease loss %d", ErrConfig, uint8(o))
	}
	return nil
}

// Lease bounds how long the writers of a node act without a renewal of the lease of
// the node.
//
// The zero Lease is not valid, because its TTL is zero.
type Lease struct {
	// TTL is the duration of the lease at the store.
	TTL shard.Duration `json:"ttl"`
	// Renew is the interval between two renewals. It is below the validity of the
	// lease, so that a node renews before its deadline passes.
	Renew shard.Duration `json:"renew"`
	// LockDelay is how long the core waits after the store expires a lease before it
	// gives the writer role to another node. It lowers the chance that a resource
	// without a fence sees two writers, and it delays failover by the same time. It
	// is not a safety bound.
	LockDelay shard.Duration `json:"lockDelay"`
	// MaxDriftRate bounds the difference between the rate of the clock of the node
	// and the rate of the clock of the store, as a fraction from 0 up to 1.
	MaxDriftRate float64 `json:"maxDriftRate"`
	// OnLoss selects what the writers of the node do when the deadline passes.
	OnLoss OnLeaseLoss `json:"onLoss"`
}

// Validity returns TTL*(1-MaxDriftRate), rounded down to a nanosecond. A node acts as
// a writer for that long after the send time of a renewal that the store accepts. A
// renewal that fails, or whose reply does not arrive, does not extend the deadline.
func (l Lease) Validity() time.Duration {
	return time.Duration(float64(l.TTL) * (1 - l.MaxDriftRate))
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin

import (
	"cmp"
	"time"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/shard"
)

// ExclusiveName is the registered name of exclusive ownership.
const ExclusiveName shard.StrategyName = "exclusive"

// The defaults of the lease of exclusive ownership.
const (
	// defaultLeaseTTL is the default TTL of a lease.
	defaultLeaseTTL = shard.Duration(10 * time.Second)
	// renewalsPerTTL is the number of renewals in one TTL at the default renewal
	// interval.
	renewalsPerTTL = 3
	// defaultDriftRate is the default bound of the drift of the clock of a node.
	defaultDriftRate = 0.01
)

// Exclusive returns the factory of exclusive ownership, which parses an
// ownership.Config. Its defaults are these:
//
//   - fenced is false.
//   - The TTL of the lease is 10s, and the renewal interval is a third of the TTL.
//   - maxDriftRate is 0.01.
//   - With "fenced": true, lockDelay is 0 and onLoss is continue-fenced. Without it,
//     lockDelay is the TTL and onLoss is stop.
//
// The defaults of the renewal interval, lockDelay and onLoss follow the TTL and the fence
// that the configuration sets. New returns the configuration after its Validate accepts
// it. The mode provides the capability fenced when the configuration sets "fenced": true.
// It requires single-writer from the replica state model, exclusive-safe from the
// handoff protocol, and leases from the membership.
//
// Error modes of New:
//   - an error that wraps ErrConfig and the error of ownership.Config.Validate.
func Exclusive() config.Factory[ownership.Config] {
	return &factory[ownership.Config, ownership.Config, *ownership.Config]{
		name: ExclusiveName,
		defaults: func(c *ownership.Config) {
			ttl := cmp.Or(c.Lease.TTL, defaultLeaseTTL)
			c.Lease = ownership.Lease{
				TTL:          ttl,
				Renew:        ttl / renewalsPerTTL,
				LockDelay:    ttl,
				MaxDriftRate: defaultDriftRate,
				OnLoss:       ownership.Stop,
			}
			if c.Fenced {
				c.Lease.LockDelay, c.Lease.OnLoss = 0, ownership.ContinueFenced
			}
		},
		capabilities: func(c *ownership.Config) config.Capabilities {
			caps := config.Capabilities{Requires: []config.Requirement{
				{
					Axis:       config.Replication,
					Capability: config.SingleWriter,
					Reason:     "the core fences the replicas of one writer role",
				},
				{
					Axis:       config.Handoff,
					Capability: config.ExclusiveSafe,
					Reason:     "a move must not run two writers of a shard",
				},
				{
					Axis:       config.Membership,
					Capability: config.Leases,
					Reason:     "a lease must end the tenure of a node that stops renewing it",
				},
			}}
			if c.Fenced {
				caps.Provides = []config.Capability{config.Fenced}
			}
			return caps
		},
		build: func(c *ownership.Config) (ownership.Config, error) {
			return built(*c, c.Validate())
		},
	}
}

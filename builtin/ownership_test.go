// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin_test

import (
	"testing"
	"time"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/shard"
)

// The durations of the leases of the tests.
const (
	tenSeconds    = shard.Duration(10 * time.Second)
	thirtySeconds = shard.Duration(30 * time.Second)
	fiveSeconds   = shard.Duration(5 * time.Second)
	oneSecond     = shard.Duration(time.Second)
)

// TestOwnership checks the factory of exclusive ownership.
func TestOwnership(t *testing.T) {
	t.Parallel()

	t.Run("Exclusive", func(t *testing.T) {
		t.Parallel()

		t.Run("Parse", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name string
				give string
				want *ownership.Config
			}{
				{
					name: "returns the defaults of a lease without a fence for no configuration",
					give: "",
					want: &ownership.Config{Lease: ownership.Lease{
						TTL:          tenSeconds,
						Renew:        tenSeconds / 3,
						LockDelay:    tenSeconds,
						MaxDriftRate: 0.01,
						OnLoss:       ownership.Stop,
					}},
				},
				{
					name: "returns the defaults of a fenced lease for a fenced configuration",
					give: `{"fenced":true}`,
					want: &ownership.Config{Fenced: true, Lease: ownership.Lease{
						TTL:          tenSeconds,
						Renew:        tenSeconds / 3,
						MaxDriftRate: 0.01,
						OnLoss:       ownership.ContinueFenced,
					}},
				},
				{
					name: "derives the renewal interval and the lock delay from the TTL of the configuration",
					give: `{"lease":{"ttl":"30s"}}`,
					want: &ownership.Config{Lease: ownership.Lease{
						TTL:          thirtySeconds,
						Renew:        tenSeconds,
						LockDelay:    thirtySeconds,
						MaxDriftRate: 0.01,
						OnLoss:       ownership.Stop,
					}},
				},
				{
					name: "keeps the renewal interval that the configuration sets",
					give: `{"lease":{"ttl":"30s","renew":"5s"}}`,
					want: &ownership.Config{Lease: ownership.Lease{
						TTL:          thirtySeconds,
						Renew:        fiveSeconds,
						LockDelay:    thirtySeconds,
						MaxDriftRate: 0.01,
						OnLoss:       ownership.Stop,
					}},
				},
				{
					name: "keeps the lock delay and the action that a fenced configuration sets",
					give: `{"fenced":true,"lease":{"lockDelay":"1s","onLoss":"stop"}}`,
					want: &ownership.Config{Fenced: true, Lease: ownership.Lease{
						TTL:          tenSeconds,
						Renew:        tenSeconds / 3,
						LockDelay:    oneSecond,
						MaxDriftRate: 0.01,
						OnLoss:       ownership.Stop,
					}},
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					assert.Equal(t, parse(t, builtin.Exclusive(), tt.give), config.Parsed(tt.want),
						"Parse must apply the defaults that the configuration selects")
				})
			}

			t.Run("returns the configuration of its own encoding", func(t *testing.T) {
				t.Parallel()
				f := builtin.Exclusive()
				roundTrip(t, f, parse(t, f, `{"fenced":true,"lease":{"ttl":"30s"}}`))
			})
		})

		t.Run("Capabilities", func(t *testing.T) {
			t.Parallel()

			t.Run("provides fenced for a fenced configuration", func(t *testing.T) {
				t.Parallel()
				f := builtin.Exclusive()
				assert.Equal(t, f.Capabilities(parse(t, f, `{"fenced":true}`)).Provides,
					[]config.Capability{config.Fenced}, "a fenced exclusive must provide fenced")
			})

			t.Run("provides nothing for a configuration without a fence", func(t *testing.T) {
				t.Parallel()
				f := builtin.Exclusive()
				assert.Empty(t, f.Capabilities(parse(t, f, "")).Provides,
					"an exclusive without a fence must not provide fenced")
			})

			t.Run("requires single-writer, exclusive-safe and leases", func(t *testing.T) {
				t.Parallel()
				f := builtin.Exclusive()
				requires := f.Capabilities(parse(t, f, "")).Requires
				got := make([]config.Requirement, len(requires))
				for i, req := range requires {
					got[i] = config.Requirement{Axis: req.Axis, Capability: req.Capability}
				}
				assert.Equal(t, got, []config.Requirement{
					{Axis: config.Replication, Capability: config.SingleWriter},
					{Axis: config.Handoff, Capability: config.ExclusiveSafe},
					{Axis: config.Membership, Capability: config.Leases},
				}, "exclusive must require a writer role, a safe handoff and leases")
			})
		})

		t.Run("New", func(t *testing.T) {
			t.Parallel()

			t.Run("returns the configuration", func(t *testing.T) {
				t.Parallel()
				f := builtin.Exclusive()
				cfg := parse(t, f, `{"fenced":true}`)
				got, err := f.New(cfg)
				assert.NoError(t, err, "New must accept a valid configuration")
				assert.Equal(t, &got, cfg.(*ownership.Config), "New must return the configuration")
			})

			t.Run("returns ErrConfig for a configuration that Validate refuses", func(t *testing.T) {
				t.Parallel()
				_, err := builtin.Exclusive().New(&ownership.Config{})
				assert.ErrorIs(t, err, builtin.ErrConfig, "New must refuse a lease without a TTL")
				assert.ErrorIs(t, err, ownership.ErrConfig, "New must wrap the error of Validate")
			})
		})
	})
}

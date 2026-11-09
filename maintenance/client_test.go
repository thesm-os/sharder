// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package maintenance_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/rand/seeded"

	"go.thesmos.sh/sharder/maintenance"
	"go.thesmos.sh/sharder/membership/membershiptest"
	"go.thesmos.sh/sharder/protocol/protocoltest"
	"go.thesmos.sh/sharder/store/storetest"
)

// TestClient checks the construction of a client.
func TestClient(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give func(*maintenance.Config)
			name string
		}{
			{
				name: "returns ErrConfig for a config without a store",
				give: func(c *maintenance.Config) { c.Store = nil },
			},
			{
				name: "returns ErrConfig for a config without a membership",
				give: func(c *maintenance.Config) { c.Membership = nil },
			},
			{
				name: "returns ErrConfig for a config without a transport",
				give: func(c *maintenance.Config) { c.Transport = nil },
			},
			{
				name: "returns ErrConfig for a config without a clock",
				give: func(c *maintenance.Config) { c.Clock = nil },
			},
			{
				name: "returns ErrConfig for a config without a source of randomness",
				give: func(c *maintenance.Config) { c.Rand = nil },
			},
			{
				name: "returns ErrConfig for a lifetime of zero",
				give: func(c *maintenance.Config) { c.Lifetime = 0 },
			},
			{
				name: "returns ErrConfig for a lifetime below zero",
				give: func(c *maintenance.Config) { c.Lifetime = -lifetime },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				cfg := maintenance.Config{
					Store:      storetest.NewReaderStub(t),
					Membership: membershiptest.NewMembershipStub(t),
					Transport:  protocoltest.NewControllerClientStub(t),
					Clock:      fake.New(origin),
					Rand:       seeded.New(1),
					Lifetime:   lifetime,
				}
				tt.give(&cfg)
				_, err := maintenance.New(&cfg)
				assert.ErrorIs(t, err, maintenance.ErrConfig, "New must refuse an incomplete config")
			})
		}

		t.Run("returns ErrConfig for a nil config", func(t *testing.T) {
			t.Parallel()
			_, err := maintenance.New(nil)
			assert.ErrorIs(t, err, maintenance.ErrConfig, "New must refuse a nil config")
		})
	})
}

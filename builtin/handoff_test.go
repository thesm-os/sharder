// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin_test

import (
	"testing"
	"time"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/handoff/breakbeforemake"
	"go.thesmos.sh/sharder/shard"
)

// TestHandoff checks the factory of the handoff protocol break-before-make.
func TestHandoff(t *testing.T) {
	t.Parallel()

	t.Run("BreakBeforeMake", func(t *testing.T) {
		t.Parallel()

		t.Run("Parse", func(t *testing.T) {
			t.Parallel()

			t.Run("returns timeouts of 30s for an add and 10s for a drop for no configuration", func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, parse(t, builtin.BreakBeforeMake(), ""), config.Parsed(&breakbeforemake.Config{
					AddTimeout:  shard.Duration(30 * time.Second),
					DropTimeout: shard.Duration(10 * time.Second),
				}), "Parse must return the default timeouts")
			})

			t.Run("returns the configuration of its own encoding", func(t *testing.T) {
				t.Parallel()
				f := builtin.BreakBeforeMake()
				roundTrip(t, f, parse(t, f, `{"addTimeout":"1m"}`))
			})
		})

		t.Run("Capabilities", func(t *testing.T) {
			t.Parallel()

			t.Run("provides exclusive-safe", func(t *testing.T) {
				t.Parallel()
				f := builtin.BreakBeforeMake()
				assert.Equal(t, f.Capabilities(parse(t, f, "")),
					config.Capabilities{Provides: []config.Capability{config.ExclusiveSafe}},
					"break-before-make must provide exclusive-safe")
			})
		})

		t.Run("New", func(t *testing.T) {
			t.Parallel()

			t.Run("builds break-before-make with the timeouts of the configuration", func(t *testing.T) {
				t.Parallel()
				f := builtin.BreakBeforeMake()
				cfg := parse(t, f, `{"addTimeout":"1m"}`)
				got, err := f.New(cfg)
				assert.NoError(t, err, "New must build break-before-make")
				want, err := breakbeforemake.New(*cfg.(*breakbeforemake.Config))
				assert.NoError(t, err, "breakbeforemake.New must build the protocol of the test")
				assert.Equal(t, got, handoff.Protocol(want), "New must build the protocol of its configuration")
			})

			t.Run("returns ErrConfig for a configuration that breakbeforemake.New refuses", func(t *testing.T) {
				t.Parallel()
				_, err := builtin.BreakBeforeMake().New(&breakbeforemake.Config{})
				assert.ErrorIs(t, err, builtin.ErrConfig, "New must refuse timeouts of zero")
				assert.ErrorIs(t, err, breakbeforemake.ErrConfig, "New must wrap the error of breakbeforemake.New")
			})
		})
	})
}

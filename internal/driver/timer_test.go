// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/streamtest"
)

// TestTimer checks the effect of a wait that Run performs: Timer.
func TestTimer(t *testing.T) {
	t.Parallel()

	t.Run("Run", func(t *testing.T) {
		t.Parallel()

		t.Run("passes Elapsed once the duration of a Timer has passed on the clock", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Timer{Token: 1, Duration: timeout}))
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.probe.events, "the run passes Started")
				synctest.Wait()
				f.clock.Advance(timeout)
				assert.Equal(t, received(t, f.probe.events, "the run passes the end of the wait"),
					machine.Event(machine.Elapsed{Token: 1}), "the run must pass Elapsed once the duration has passed")
				stopped(t, cancel, errc)
			})
		})

		t.Run("passes no Elapsed before the duration of a Timer has passed", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Timer{Token: 1, Duration: timeout}))
				ctx, cancel := context.WithCancel(t.Context())
				errc := start(ctx, f.driver(t))
				received(t, f.probe.events, "the run passes Started")
				synctest.Wait()
				f.clock.Advance(timeout - time.Second)
				synctest.Wait()
				streamtest.Blocks(t, f.probe.events, "the run must not pass Elapsed before the duration has passed")
				stopped(t, cancel, errc)
			})
		})

		t.Run("passes Elapsed at once for a Timer without a positive duration", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, performing(machine.Timer{Token: 1}))
				assert.Equal(t, f.results(t, 1)[0], machine.Event(machine.Elapsed{Token: 1}),
					"the run must pass Elapsed at once for a wait of no duration")
			})
		})
	})
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver

import (
	"context"

	"go.thesmos.sh/core/clock"

	"go.thesmos.sh/sharder/internal/machine"
)

// timer waits on the clock until the Duration of e has passed, and then passes the
// Elapsed of e. A Duration that is not above zero ends the wait at once. timer passes
// nothing once ctx has ended.
func (d *Driver) timer(ctx context.Context, e *machine.Timer) {
	//dokimi:mutate-skip ror-true: Wait fails only once ctx has ended, and post then drops the Elapsed
	if clock.Wait(ctx, d.clock, e.Duration) == nil {
		d.post(ctx, machine.Elapsed{Token: e.Token})
	}
}

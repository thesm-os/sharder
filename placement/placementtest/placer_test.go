// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest_test

import (
	"context"
	"testing"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/placementtest"
)

// keeper is a placer that leaves every shard where it is. Its plans do not change a
// shard, and their repair cursor is the cursor of the input. keeper passes the laws of
// AssertPlacer.
type keeper struct{}

// Place returns a plan that does not change a shard. It returns the cause of ctx when
// ctx has ended.
func (keeper) Place(ctx context.Context, in *placement.Input) (placement.Plan, error) {
	if ctx.Err() != nil {
		return placement.Plan{}, context.Cause(ctx)
	}
	return placement.Plan{RepairNext: in.RepairFrom}, nil
}

// Schedule returns the zero Schedule.
func (keeper) Schedule() placement.Schedule {
	return placement.Schedule{}
}

// TestPlacer checks the laws of a placer against keeper.
func TestPlacer(t *testing.T) {
	t.Parallel()
	placementtest.AssertPlacer(t, keeper{}, nil)
}

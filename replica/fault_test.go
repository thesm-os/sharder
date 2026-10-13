// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica_test

import (
	"context"
	"testing"
	"testing/synctest"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/shard"
)

// TestFault checks the reports of the faults and the recoveries of the replicas of a node.
func TestFault(t *testing.T) {
	t.Parallel()

	t.Run("ReportFault", func(t *testing.T) {
		t.Parallel()

		t.Run("reports a permanent fault of a replica to the controller", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				f.serve(t, a, shardA, 1)
				req := replica.ReportFaultRequest{Shard: shardA, Fault: shard.FaultPermanent, Cause: errDisk}
				_, err := a.ReportFault(t.Context(), &req)
				assert.NoError(t, err, "ReportFault must take the fault of a replica of the node")
				synctest.Wait()
				o := f.observed(t, shardA)
				expect.Equal(t, o.Fault, shard.FaultPermanent, "the report must have the fault")
				expect.Equal(t, o.Cause, errDisk.Error(), "the report must have the cause of the fault")
			})
		})

		t.Run("returns ErrNoReplica for a shard without a replica on the node", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				a := newFixture(t).start(t)
				req := replica.ReportFaultRequest{Shard: shardA, Fault: shard.FaultPermanent, Cause: errDisk}
				_, err := a.ReportFault(t.Context(), &req)
				assert.ErrorIs(t, err, replica.ErrNoReplica, "ReportFault must refuse a shard without a replica")
			})
		})

		t.Run("returns ErrNotServing once the run of the agent has ended", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				ctx, stop := context.WithCancel(t.Context())
				a := f.run(t, ctx, f.app)
				stop()
				received(t, a.Done(), "the end of the run")
				req := replica.ReportFaultRequest{Shard: shardA, Fault: shard.FaultPermanent, Cause: errDisk}
				_, err := a.ReportFault(t.Context(), &req)
				assert.ErrorIs(t, err, replica.ErrNotServing, "a report after the run must not be served")
			})
		})

		t.Run("returns ErrNotServing before Start", func(t *testing.T) {
			t.Parallel()
			a, err := replica.New(bare(t).config(), newApplication())
			assert.NoError(t, err, "New must return the agent")
			req := replica.ReportFaultRequest{Shard: shardA, Fault: shard.FaultPermanent, Cause: errDisk}
			_, err = a.ReportFault(t.Context(), &req)
			assert.ErrorIs(t, err, replica.ErrNotServing, "a report before Start must not be served")
		})
	})

	t.Run("Recovered", func(t *testing.T) {
		t.Parallel()

		t.Run("reports a replica as serving after a transient fault", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				f.serve(t, a, shardA, 1)
				fault := replica.ReportFaultRequest{Shard: shardA, Fault: shard.FaultTransient, Cause: errDisk}
				_, err := a.ReportFault(t.Context(), &fault)
				assert.NoError(t, err, "ReportFault must take the fault of a replica of the node")
				_, err = a.Recovered(t.Context(), &replica.RecoveredRequest{Shard: shardA})
				assert.NoError(t, err, "Recovered must take the recovery of a replica with a transient fault")
				synctest.Wait()
				o := f.observed(t, shardA)
				expect.Equal(t, o.Fault, shard.FaultNone, "the report must not have the fault")
				expect.Equal(t, o.Status, shard.StatusServing, "the report must have the replica serving")
			})
		})

		t.Run("returns ErrNoFault for a replica without a transient fault", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				f.serve(t, a, shardA, 1)
				_, err := a.Recovered(t.Context(), &replica.RecoveredRequest{Shard: shardA})
				assert.ErrorIs(t, err, replica.ErrNoFault, "Recovered must refuse a replica without a transient fault")
			})
		})

		t.Run("returns ErrNotServing before Start", func(t *testing.T) {
			t.Parallel()
			a, err := replica.New(bare(t).config(), newApplication())
			assert.NoError(t, err, "New must return the agent")
			_, err = a.Recovered(t.Context(), &replica.RecoveredRequest{Shard: shardA})
			assert.ErrorIs(t, err, replica.ErrNotServing, "a recovery before Start must not be served")
		})
	})
}

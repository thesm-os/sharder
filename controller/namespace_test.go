// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package controller_test

import (
	"testing"
	"testing/synctest"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/controller"
	"go.thesmos.sh/sharder/internal/machine/root"
	"go.thesmos.sh/sharder/shard"
)

// The capacities of a process of the tests that sets them.
const (
	// cellCapacity is the cell capacity of the process.
	cellCapacity = 3
	// cpuCapacity is the name of a capacity of the node of the process.
	cpuCapacity = "cpu"
	// cpus is the capacity cpuCapacity of the node of the process.
	cpus = 4
)

// TestNamespace checks what a controller process runs for each namespace.
func TestNamespace(t *testing.T) {
	t.Parallel()

	t.Run("Run", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a spec that the root refuses", func(t *testing.T) {
			t.Parallel()
			s := spec(namespace)
			s.Preset = "unknown"
			err := idle(t).Run(t.Context(), s)
			assert.ErrorIs(t, err, controller.ErrConfig, "Run must refuse a spec that the root refuses")
		})

		t.Run("returns ErrConfig for a spec that the capabilities of the membership do not meet",
			func(t *testing.T) {
				t.Parallel()
				f := newFixture(t)
				cfg := f.config(procA, noop.New())
				cfg.Membership = f.members.MembershipStub
				ctrl, err := controller.New(cfg)
				assert.NoError(t, err, "New accepts a membership without capabilities")
				assert.ErrorIs(t, ctrl.Run(t.Context(), spec(namespace)), controller.ErrConfig,
					"Run must refuse a spec whose ownership needs the leases of the membership")
			})

		t.Run("returns the error of the agent of the root once the namespace has a new incarnation",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					f.newRoot(t)
					p := f.start(t, procA, spec(namespace))
					synctest.Wait()
					f.mem.Delete(rootScope)
					assert.ErrorIs(t, p.result(t), shard.ErrIncarnation,
						"Run must return the error of the agent of the root")
				})
			})

		t.Run("runs the root of each namespace", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.start(t, procA, spec(namespace), spec(otherNamespace))
				synctest.Wait()
				for _, ns := range []shard.Namespace{namespace, otherNamespace} {
					got := record(t, f, shard.Scope{Namespace: ns})
					assert.Equal(
						t,
						got.Controller,
						procA.Address,
						"the root of each namespace must lead in the process",
					)
				}
			})
		})

		t.Run("joins the root scope with a capacity of one cell", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.newRoot(t)
				f.start(t, procA, spec(namespace))
				synctest.Wait()
				v := view(t, f, rootScope)
				n, ok := v.Node(procA.ID)
				assert.True(t, ok, "the process must join the root scope")
				assert.Equal(t, n.Capacity[root.Capacity], float64(1),
					"a process without a cell capacity must have a capacity of one cell")
			})
		})

		t.Run("joins the root scope with the capacities of the node", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.newRoot(t)
				cfg := f.config(procA, noop.New())
				cfg.Node.Capacity = map[string]float64{cpuCapacity: cpus}
				f.launch(t, cfg, spec(namespace))
				synctest.Wait()
				v := view(t, f, rootScope)
				n, ok := v.Node(procA.ID)
				assert.True(t, ok, "the process must join the root scope")
				expect.Equal(
					t,
					n.Capacity[cpuCapacity],
					float64(cpus),
					"the process must keep the capacities of its node",
				)
				expect.Equal(t, n.Capacity[root.Capacity], float64(1), "the process must add its cell capacity")
				expect.Length(t, cfg.Node.Capacity, 1, "New must not modify the capacities of the configuration")
			})
		})

		t.Run("joins the root scope with the cell capacity of the process", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.newRoot(t)
				cfg := f.config(procA, noop.New())
				cfg.CellCapacity = cellCapacity
				f.launch(t, cfg, spec(namespace))
				synctest.Wait()
				v := view(t, f, rootScope)
				n, ok := v.Node(procA.ID)
				assert.True(t, ok, "the process must join the root scope")
				assert.Equal(t, n.Capacity[root.Capacity], float64(cellCapacity),
					"the process must have its cell capacity in the root scope")
			})
		})
	})
}

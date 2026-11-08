// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machinetest_test

import (
	"testing"

	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/sim"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/internal/machine/machinetest"
)

// TestTraces runs the simulation of each failure trace over each built-in preset. The subtest
// Traces runs 152 simulations, so it skips in short mode.
func TestTraces(t *testing.T) {
	t.Parallel()

	t.Run("Traces", func(t *testing.T) {
		t.Parallel()
		if testing.Short() {
			t.Skip("skipping the simulations of the traces in short mode")
		}
		for _, tr := range machinetest.Traces() {
			t.Run(tr.Name, func(t *testing.T) {
				t.Parallel()
				for _, p := range builtin.Presets() {
					t.Run(string(p.Name), func(t *testing.T) {
						t.Parallel()
						sim.Test(t, tr.Simulation(p.Name))
					})
				}
			})
		}
	})

	t.Run("Trace", func(t *testing.T) {
		t.Parallel()

		t.Run("Simulation", func(t *testing.T) {
			t.Parallel()

			t.Run("covers every action of MachineSimulation over the traces", func(t *testing.T) {
				t.Parallel()
				var taken []string
				for _, tr := range machinetest.Traces() {
					for _, a := range tr.Simulation(builtin.ExclusiveWorker).Actions {
						taken = append(taken, a.Name)
					}
				}
				for _, a := range machinetest.MachineSimulation().Actions {
					expect.Contains(t, taken, a.Name, "a trace takes each action of MachineSimulation")
				}
			})
		})
	})
}

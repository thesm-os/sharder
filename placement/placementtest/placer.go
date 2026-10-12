// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/history"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/rand/pcg"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// labelPlanSeed labels the seed of the source of randomness of a pass.
const labelPlanSeed = "seed of the pass"

// parallelism is the Parallelism of the input that a law compares with a Parallelism
// of 1.
const parallelism = 4

// inputView is what an input contains, copied, for a law that checks that a call does
// not change the input.
type inputView struct {
	// Current maps each placed shard to its replicas.
	Current map[shard.ID][]shard.Replica
	// Transitions is a copy of Input.Transitions.
	Transitions map[shard.ID]placement.InFlight
	// Faults maps each shard to a copy of its faulted nodes.
	Faults map[shard.ID][]shard.NodeID
	// Draining is a copy of Input.Draining.
	Draining map[shard.NodeID]bool
	// ShardLoad maps each shard to a copy of its load.
	ShardLoad map[shard.ID]shard.Load
	// Nodes is a copy of Input.Nodes.
	Nodes []shard.Node
	// Shards is a copy of the shards of the layout.
	Shards []shard.ID
	// RepairFrom is Input.RepairFrom.
	RepairFrom shard.ID
}

// AssertPlacer checks the laws of placement.Placer against p, and fails t when p
// breaks one. cs are the constraints that the plans of p obey, which Validate checks.
// AssertPlacer runs each law as a parallel subtest of t, under the name of the method
// that the law constrains.
//
// The laws run p on the inputs that Inputs generates. Each pass gets an input with a
// new source of randomness and a fake clock. The laws do not advance the clock, so the
// timeout of a pass does not end it.
func AssertPlacer(t *testing.T, p placement.Placer, cs []placement.Constraint) {
	t.Helper()

	t.Run("Place", func(t *testing.T) {
		t.Parallel()

		t.Run("returns a plan that Validate accepts", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Place returns a plan that the checks of the core accept", func(c *prop.Case) {
				in := drawPass(c)
				plan, err := p.Place(c.Context(), in(1))
				assert.NoError(c, err, "Place must plan a generated input")
				_, err = placement.Validate(in(1), plan, cs)
				assert.NoError(c, err, "Validate must accept the plan of Place")
			})
		})

		t.Run("returns the same plan for the same input", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Place returns the same plan for the same input", func(c *prop.Case) {
				in := drawPass(c)
				first, err := p.Place(c.Context(), in(1))
				assert.NoError(c, err, "Place must plan a generated input")
				second, err := p.Place(c.Context(), in(1))
				assert.NoError(c, err, "Place must plan the input again")
				assert.Equal(c, second, first, "Place must return the same plan for the same input")
			})
		})

		t.Run("returns the same plan at every parallelism", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "the plan of Place does not depend on Input.Parallelism", func(c *prop.Case) {
				in := drawPass(c)
				serial, err := p.Place(c.Context(), in(1))
				assert.NoError(c, err, "Place must plan a generated input on one goroutine")
				parallel, err := p.Place(c.Context(), in(parallelism))
				assert.NoError(c, err, "Place must plan a generated input on many goroutines")
				assert.Equal(c, parallel, serial, "Place must return the plan of one goroutine on many")
			})
		})

		t.Run("returns the same plans from concurrent calls", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "concurrent calls of Place return the plans of sequential calls", func(c *prop.Case) {
				in := drawPass(c)
				want, err := p.Place(c.Context(), in(1))
				assert.NoError(c, err, "Place must plan a generated input")
				outcomes := history.Concurrently(clients, within, func(int) (any, error) {
					return p.Place(c.Context(), in(1))
				})
				for _, o := range outcomes {
					assert.True(c, o.Finished, "every client must finish its pass")
					assert.NoError(c, o.Error, "Place must plan the input on every client")
					assert.Equal(c, o.Output, any(want), "a concurrent pass must return the plan of a sequential pass")
				}
			})
		})

		t.Run("does not change the input", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Place does not change its input", func(c *prop.Case) {
				in := drawPass(c)(1)
				assert.Pure(c, func() inputView { return viewOfInput(in) }, func() { _, _ = p.Place(c.Context(), in) },
					"Place must not change its input")
			})
		})

		t.Run("returns a repair cursor of the layout", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(
				t,
				"the repair cursor of a plan is the cursor of the input or a shard of the layout",
				func(c *prop.Case) {
					in := drawPass(c)(1)
					plan, err := p.Place(c.Context(), in)
					assert.NoError(c, err, "Place must plan a generated input")
					if plan.RepairNext != in.RepairFrom {
						st := newState(c, in)
						_, ok := st.Shard(plan.RepairNext)
						assert.True(c, ok, "RepairNext must be Input.RepairFrom or a shard of the layout")
					}
				},
			)
		})

		t.Run("returns a target only for a change or a deficit", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(
				t,
				"the plan lists only the shards that it changes or leaves with a deficit",
				func(c *prop.Case) {
					in := drawPass(c)(1)
					plan, err := p.Place(c.Context(), in)
					assert.NoError(c, err, "Place must plan a generated input")
					for _, id := range slices.SortedFunc(maps.Keys(plan.Targets), shard.ID.Compare) {
						target := plan.Targets[id]
						current := in.Current.Placements[id].Replicas
						same := len(target.Replicas) == len(current) &&
							!slices.ContainsFunc(
								target.Replicas,
								func(r shard.Replica) bool { return !slices.Contains(current, r) },
							)
						assert.False(
							c,
							same && target.Deficit == 0,
							"a target must change its shard or report a deficit",
						)
						if target.Deficit > 0 {
							assert.ErrorIs(
								c,
								target.Reason,
								placement.ErrNoNode,
								"the reason of a deficit must wrap ErrNoNode",
							)
						} else {
							assert.Nil(c, target.Reason, "a target without a deficit must have no reason")
						}
					}
				},
			)
		})

		t.Run("returns the cause of an ended context", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Place returns the cause of a context that ended before the pass", func(c *prop.Case) {
				in := drawPass(c)
				cause := errors.New("placementtest: the pass ended")
				ctx, cancel := context.WithCancelCause(c.Context())
				cancel(cause)
				_, err := p.Place(ctx, in(1))
				assert.ErrorIs(c, err, cause, "Place must return the cause of the context")
			})
		})
	})
}

// drawPass generates an input and a seed, and returns a function that returns a
// copy of the input with the parallelism that it receives, a new source of randomness
// of the seed, and a new fake clock. Each copy shares the maps and the slices of the
// input, which a placer must not modify.
func drawPass(c *prop.Case) func(parallel int) *placement.Input {
	in := c.Draw(Inputs(), labelInput)
	seed := rand.Seed(c.Draw(prop.Integer[int64](1, 1<<62), labelPlanSeed))
	return func(parallel int) *placement.Input {
		copied := *in
		copied.Rand = pcg.New(seed)
		copied.Clock = fake.New(origin)
		copied.Parallelism = parallel
		return &copied
	}
}

// viewOfInput returns a copy of what in contains.
func viewOfInput(in *placement.Input) inputView {
	v := inputView{
		Current:     map[shard.ID][]shard.Replica{},
		Transitions: maps.Clone(in.Transitions),
		Faults:      map[shard.ID][]shard.NodeID{},
		Draining:    maps.Clone(in.Draining),
		ShardLoad:   map[shard.ID]shard.Load{},
		Nodes:       make([]shard.Node, len(in.Nodes)),
		RepairFrom:  in.RepairFrom,
	}
	for i, n := range in.Nodes {
		n.Labels = slices.Clone(n.Labels)
		n.Capacity = maps.Clone(n.Capacity)
		v.Nodes[i] = n
	}
	for id := range in.Current.Placements {
		v.Current[id] = slices.Clone(in.Current.Placements[id].Replicas)
	}
	for id, nodes := range in.Faults {
		v.Faults[id] = slices.Clone(nodes)
	}
	for id, l := range in.ShardLoad {
		v.ShardLoad[id] = maps.Clone(l)
	}
	for _, sh := range in.Layout.Shards {
		v.Shards = append(v.Shards, sh.ID)
	}
	return v
}

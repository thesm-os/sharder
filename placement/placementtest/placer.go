// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/history"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/rand/pcg"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/shard"
)

// labelPlanSeed labels the seed of the source of randomness of a pass.
const labelPlanSeed = "seed of the pass"

// parallelism is the Parallelism of the input that a check compares with a Parallelism
// of 1.
const parallelism = 4

// unknownShard is a shard that no layout of Inputs lists.
var unknownShard = shard.ID{ID128: id.ID128{15: 0xff}}

// fakeConstraints are the constraints of the verified fake of the planted defects of
// Placer.
var fakeConstraints = []placement.Constraint{constraint.Drain{}}

// fakeConfig is the configuration of the verified fake of the planted defects of
// Placer: the planner with the constraint drain and the goal stickiness. A pass over a
// cell of Inputs does not reach the limits of its search.
var fakeConfig = planner.Config{
	Constraints: []planner.Selection{{Name: constraint.DrainName}},
	Goals:       []planner.GoalSelection{{Name: goal.StickinessName, Weight: 1}},
	Search: planner.Search{
		MaxMoves:        500,
		MaxEvaluations:  200_000,
		MaxSourceShards: 5_000,
		Sample:          16,
		Timeout:         shard.Duration(time.Minute),
	},
	Churn: planner.Churn{MaxShardFraction: 1, PerNode: 2, PerShard: 1},
	Triggers: planner.Triggers{
		Interval:     shard.Duration(30 * time.Second),
		MinImbalance: 1.1,
		ActiveAbove:  0.25,
		OnMembership: true,
	},
}

// Constrained is a placer whose plans obey constraints. PlaceReturnsValidPlans reads
// the constraints of its subject through it, and a subject that does not implement it
// ends that check with a fault that wraps ErrUnconstrained.
type Constrained interface {
	// Constraints returns the constraints that the plans of the placer obey.
	Constraints() []placement.Constraint
}

// constrained is a placer with the constraints that its plans obey. Each planted defect
// of Placer is a constrained, so it implements every interface of the kit.
type constrained struct {
	placement.Placer

	// constraints are the constraints of the placer.
	constraints []placement.Constraint
}

// Constraints returns the constraints of the placer.
func (c constrained) Constraints() []placement.Constraint {
	return c.constraints
}

// inputView is what an input contains, copied, for a check that a call does not change
// the input.
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

// PlaceReturnsValidPlans checks that Place returns a plan that Validate accepts with
// the constraints of the placer.
//
//dokimi:check Placer.Place defect=addsUnknownShards
func PlaceReturnsValidPlans(h *PlacerHandle, c *prop.Case) {
	s := h.Subject()
	cs, ok := s.(Constrained)
	if !ok {
		assert.Fault(h, fmt.Errorf("%w: %T", ErrUnconstrained, s))
		return
	}
	in := drawPass(c)
	plan, err := s.Place(h.Context(), in(1))
	assert.NoError(h, err, "Place must plan a generated input")
	_, err = placement.Validate(in(1), plan, cs.Constraints())
	assert.NoError(h, err, h.Statement())
}

// addsUnknownShards returns a planted defect over the fake planner whose plans also
// target unknownShard, with a deficit of one replica.
func addsUnknownShards(h *PlacerHandle) placement.Placer {
	fake := newFakePlanner(h)
	return constrained{Placer: NewPlacerStub(h, PlacerStubDelegateTo(fake),
		WithPlacerPlace(func(ctx context.Context, in *placement.Input) (placement.Plan, error) {
			plan, err := fake.Place(ctx, in)
			if err != nil {
				return plan, err
			}
			plan.Targets = withTarget(plan.Targets, unknownShard,
				placement.Target{Reason: placement.ErrNoNode, Deficit: 1})
			return plan, nil
		})), constraints: fakeConstraints}
}

// PlaceIsDeterministic checks that Place returns the same plan for the same input.
//
//dokimi:check Placer.Place defect=driftsPlans
func PlaceIsDeterministic(h *PlacerHandle, c *prop.Case) {
	s := h.Subject()
	in := drawPass(c)
	first, err := s.Place(h.Context(), in(1))
	assert.NoError(h, err, "Place must plan a generated input")
	second, err := s.Place(h.Context(), in(1))
	assert.NoError(h, err, "Place must plan the input again")
	assert.Equal(h, second, first, h.Statement())
}

// driftsPlans returns a planted defect over the fake planner whose plans report the
// other Incomplete from the second call of Place on. A check of concurrency calls Place
// from more than one goroutine, so the count of the calls is atomic.
func driftsPlans(h *PlacerHandle) placement.Placer {
	fake := newFakePlanner(h)
	var calls atomic.Uint64
	return constrained{Placer: NewPlacerStub(h, PlacerStubDelegateTo(fake),
		WithPlacerPlace(func(ctx context.Context, in *placement.Input) (placement.Plan, error) {
			plan, err := fake.Place(ctx, in)
			if calls.Add(1) > 1 {
				plan.Incomplete = !plan.Incomplete
			}
			return plan, err
		})), constraints: fakeConstraints}
}

// PlaceIgnoresParallelism checks that the plan of Place does not depend on
// Input.Parallelism.
//
//dokimi:check Placer.Place defect=driftsPlans
func PlaceIgnoresParallelism(h *PlacerHandle, c *prop.Case) {
	s := h.Subject()
	in := drawPass(c)
	serial, err := s.Place(h.Context(), in(1))
	assert.NoError(h, err, "Place must plan a generated input on one goroutine")
	parallel, err := s.Place(h.Context(), in(parallelism))
	assert.NoError(h, err, "Place must plan a generated input on many goroutines")
	assert.Equal(h, parallel, serial, h.Statement())
}

// ConcurrentPlacesMatchSequentialPlaces checks that concurrent calls of Place return
// the plans of sequential calls.
//
//dokimi:check Placer.Place defect=driftsPlans
func ConcurrentPlacesMatchSequentialPlaces(h *PlacerHandle, c *prop.Case) {
	s := h.Subject()
	in := drawPass(c)
	want, err := s.Place(h.Context(), in(1))
	assert.NoError(h, err, "Place must plan a generated input")
	outcomes := history.Concurrently(clients, within, func(int) (any, error) {
		return s.Place(h.Context(), in(1))
	})
	for _, o := range outcomes {
		assert.True(h, o.Finished, "every client must finish its pass")
		assert.NoError(h, o.Error, "Place must plan the input on every client")
		assert.Equal(h, o.Output, any(want), h.Statement())
	}
}

// PlaceIsPure checks that Place does not change its input.
//
//dokimi:check Placer.Place defect=clearsRepairCursors
func PlaceIsPure(h *PlacerHandle, c *prop.Case) {
	s := h.Subject()
	in := drawPass(c)(1)
	assert.Pure(h, func() inputView { return viewOfInput(in) }, func() { _, _ = s.Place(h.Context(), in) },
		h.Statement())
}

// clearsRepairCursors returns a planted defect over the fake planner whose Place sets
// the repair cursor of its input to the zero ID after the pass.
func clearsRepairCursors(h *PlacerHandle) placement.Placer {
	fake := newFakePlanner(h)
	return constrained{Placer: NewPlacerStub(h, PlacerStubDelegateTo(fake),
		WithPlacerPlace(func(ctx context.Context, in *placement.Input) (placement.Plan, error) {
			plan, err := fake.Place(ctx, in)
			in.RepairFrom = shard.ID{}
			return plan, err
		})), constraints: fakeConstraints}
}

// PlaceKeepsTheCursorInTheLayout checks that the repair cursor of a plan is the cursor
// of the input or a shard of the layout.
//
//dokimi:check Placer.Place defect=movesCursorsOut
func PlaceKeepsTheCursorInTheLayout(h *PlacerHandle, c *prop.Case) {
	in := drawPass(c)(1)
	plan, err := h.Subject().Place(h.Context(), in)
	assert.NoError(h, err, "Place must plan a generated input")
	if plan.RepairNext != in.RepairFrom {
		_, ok := newState(h, in).Shard(plan.RepairNext)
		assert.True(h, ok, h.Statement())
	}
}

// movesCursorsOut returns a planted defect over the fake planner whose plans have the
// repair cursor unknownShard.
func movesCursorsOut(h *PlacerHandle) placement.Placer {
	fake := newFakePlanner(h)
	return constrained{Placer: NewPlacerStub(h, PlacerStubDelegateTo(fake),
		WithPlacerPlace(func(ctx context.Context, in *placement.Input) (placement.Plan, error) {
			plan, err := fake.Place(ctx, in)
			plan.RepairNext = unknownShard
			return plan, err
		})), constraints: fakeConstraints}
}

// PlaceTargetsOnlyChanges checks that the plan lists only the shards that it changes or
// leaves with a deficit, and that the reason of a deficit wraps placement.ErrNoNode.
//
//dokimi:check Placer.Place defect=listsUnchangedShards
func PlaceTargetsOnlyChanges(h *PlacerHandle, c *prop.Case) {
	in := drawPass(c)(1)
	plan, err := h.Subject().Place(h.Context(), in)
	assert.NoError(h, err, "Place must plan a generated input")
	for _, id := range slices.SortedFunc(maps.Keys(plan.Targets), shard.ID.Compare) {
		target := plan.Targets[id]
		current := in.Current.Placements[id].Replicas
		same := len(target.Replicas) == len(current) &&
			!slices.ContainsFunc(target.Replicas, func(r shard.Replica) bool { return !slices.Contains(current, r) })
		assert.False(h, same && target.Deficit == 0, h.Statement())
		if target.Deficit > 0 {
			assert.ErrorIs(h, target.Reason, placement.ErrNoNode, h.Statement())
		} else {
			assert.Nil(h, target.Reason, h.Statement())
		}
	}
}

// listsUnchangedShards returns a planted defect over the fake planner whose plans also
// list the first shard of the layout that has all its replicas and that the plan does
// not change, with its current replicas and no deficit.
func listsUnchangedShards(h *PlacerHandle) placement.Placer {
	fake := newFakePlanner(h)
	return constrained{Placer: NewPlacerStub(h, PlacerStubDelegateTo(fake),
		WithPlacerPlace(func(ctx context.Context, in *placement.Input) (placement.Plan, error) {
			plan, err := fake.Place(ctx, in)
			if err != nil {
				return plan, err
			}
			desired := 0
			for _, role := range in.Model.Roles {
				desired += role.Max
			}
			for _, sh := range in.Layout.Shards {
				current := in.Current.Placements[sh.ID].Replicas
				if _, listed := plan.Targets[sh.ID]; !listed && len(current) == desired {
					plan.Targets = withTarget(plan.Targets, sh.ID, placement.Target{Replicas: slices.Clone(current)})
					break
				}
			}
			return plan, nil
		})), constraints: fakeConstraints}
}

// PlaceReturnsTheCauseOfAnEndedContext checks that Place returns the cause of a context
// that ended before the pass.
//
//dokimi:check Placer.Place defect=ignoresContexts
func PlaceReturnsTheCauseOfAnEndedContext(h *PlacerHandle, c *prop.Case) {
	in := drawPass(c)
	cause := errors.New("placementtest: the pass ended")
	ctx, cancel := context.WithCancelCause(h.Context())
	cancel(cause)
	_, err := h.Subject().Place(ctx, in(1))
	assert.ErrorIs(h, err, cause, h.Statement())
}

// ignoresContexts returns a planted defect over the fake planner whose Place runs the
// pass under a context that does not end.
func ignoresContexts(h *PlacerHandle) placement.Placer {
	fake := newFakePlanner(h)
	return constrained{Placer: NewPlacerStub(h, PlacerStubDelegateTo(fake),
		WithPlacerPlace(func(ctx context.Context, in *placement.Input) (placement.Plan, error) {
			return fake.Place(context.WithoutCancel(ctx), in)
		})), constraints: fakeConstraints}
}

// newFakePlanner returns the verified fake that the planted defects of Placer build on:
// the planner of fakeConfig. It stops tb when New returns an error.
func newFakePlanner(tb assert.TB) *planner.Planner {
	tb.Helper()
	p, err := planner.New(&fakeConfig, fakeConstraints, []placement.Goal{goal.Stickiness{}})
	assert.NoError(tb, err, "New must build the verified fake")
	return p
}

// withTarget returns a copy of targets that also maps id to t.
func withTarget(targets map[shard.ID]placement.Target, id shard.ID, t placement.Target) map[shard.ID]placement.Target {
	out := make(map[shard.ID]placement.Target, len(targets)+1)
	maps.Copy(out, targets)
	out[id] = t
	return out
}

// drawPass generates an input and a seed, and returns a function that returns a copy of
// the input with the parallelism that it receives, a new source of randomness of the
// seed, and a new fake clock. Each copy shares the maps and the slices of the input,
// which a placer must not modify.
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

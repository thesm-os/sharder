// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package planner_test

import (
	"context"
	"flag"
	"strconv"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/rand/pcg"
	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// The cells of BenchmarkPlanner.
const (
	// shardsPerNode is the ratio of the bounds of a cell: 1,000,000 shards on 5,000
	// nodes.
	shardsPerNode = 200
	// benchCapacity is the capacity of cpu of each node of a cell of the benchmark.
	benchCapacity = 800
	// overload is the load of a shard of the infeasible cell, above the capacity of
	// every node.
	overload = 900
	// hotLoad is the load of every 200th shard of the cell of skewed load. Every other
	// shard has a load of 1.
	hotLoad = 50
	// racks is the number of rack labels of each node of a cell of the benchmark.
	racks = 20
	// rackKey starts the key of each rack label.
	rackKey = "rack"
	// rackValue is the value of each rack label of a node.
	rackValue = "r1"
	// rackExcluded is the value of a rack label that the label constraints exclude.
	rackExcluded = "r0"
	// goneNode is the gone node of the infeasible cell.
	goneNode shard.NodeID = "gone"
)

// bound makes BenchmarkPlanner run at the bounds of a cell, 5,000 nodes and 1,000,000
// shards, besides the smaller sizes.
var bound = flag.Bool("bound", false, "run BenchmarkPlanner at the bounds of a cell")

// tiered is a goal with its tier and its weight.
type tiered struct {
	// goal is the goal.
	goal placement.Goal
	// tier is the tier of the goal.
	tier int
	// weight is the weight of the goal.
	weight float64
}

// benchCase is one case of BenchmarkPlanner.
type benchCase struct {
	// name is the name of the case.
	name string
	// constraints lists the constraints of the planner.
	constraints []placement.Constraint
	// goals lists the goals of the planner.
	goals []tiered
	// shape lays out the shards of the cell.
	shape shape
}

// shape is how scaled lays out the shards of a cell.
type shape uint8

const (
	// joined places the shards evenly on every node but the last, which joined empty.
	joined shape = 1
	// full places every shard on a gone node with a load above the capacity of every
	// node, so no move is feasible.
	full shape = 2
	// skewed places the shards evenly on every node but the last, and gives every
	// 200th shard a load of hotLoad.
	skewed shape = 3
	// failed places the shards evenly on every node, and the last node is gone.
	failed shape = 4
)

// constrainedPlanner is a planner with the constraints that its plans obey, the subject
// of the suite of placement.Placer.
type constrainedPlanner struct {
	*planner.Planner

	// constraints are the constraints of the planner.
	constraints []placement.Constraint
}

// Constraints returns the constraints of the planner.
func (p constrainedPlanner) Constraints() []placement.Constraint {
	return p.constraints
}

// TestPlanner checks New, Schedule, and the input that Place refuses.
func TestPlanner(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		drain := defaults()
		drain.Constraints = []planner.Selection{{Name: constraint.DrainName}}
		capacity := defaults()
		capacity.Constraints = []planner.Selection{{Name: constraint.CapacityName}}
		count := defaults()
		count.Goals = []planner.GoalSelection{{Name: goal.CountName, Weight: 1}}
		stickiness := defaults()
		stickiness.Goals = []planner.GoalSelection{{Name: goal.StickinessName, Weight: 1}}

		tests := []struct {
			name        string
			cfg         planner.Config
			constraints []placement.Constraint
			goals       []placement.Goal
		}{
			{name: "returns ErrConfig for a configuration that Validate refuses", cfg: planner.Config{}},
			{
				name:        "returns ErrConfig for a constraint without a selection",
				cfg:         defaults(),
				constraints: []placement.Constraint{constraint.Drain{}},
			},
			{
				name:        "returns ErrConfig for a constraint of another name than its selection",
				cfg:         capacity,
				constraints: []placement.Constraint{constraint.Drain{}},
			},
			{
				name:        "returns ErrConfig for a nil constraint",
				cfg:         drain,
				constraints: []placement.Constraint{nil},
			},
			{
				name:  "returns ErrConfig for a goal without a selection",
				cfg:   defaults(),
				goals: []placement.Goal{goal.Stickiness{}},
			},
			{
				name:  "returns ErrConfig for a goal of another name than its selection",
				cfg:   count,
				goals: []placement.Goal{goal.Stickiness{}},
			},
			{name: "returns ErrConfig for a nil goal", cfg: stickiness, goals: []placement.Goal{nil}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				_, err := planner.New(&tt.cfg, tt.constraints, tt.goals)
				assert.ErrorIs(t, err, planner.ErrConfig, "New must refuse the configuration")
			})
		}

		t.Run("returns ErrConfig for a nil configuration", func(t *testing.T) {
			t.Parallel()
			_, err := planner.New(nil, nil, nil)
			assert.ErrorIs(t, err, planner.ErrConfig, "New must refuse a nil configuration")
		})
	})

	t.Run("Schedule", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the interval and the flag of the triggers", func(t *testing.T) {
			t.Parallel()
			cfg := defaults()
			cfg.Triggers.OnMembership = false
			assert.Equal(t, newPlanner(t, cfg, nil).Schedule(), placement.Schedule{Interval: 30 * time.Second},
				"Schedule must return the triggers of the configuration")
		})
	})

	t.Run("Place", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name   string
			change func(in *placement.Input)
		}{
			{
				name:   "returns ErrInput for an input without a clock",
				change: func(in *placement.Input) { in.Clock = nil },
			},
			{
				name:   "returns ErrInput for an input without a source of randomness",
				change: func(in *placement.Input) { in.Rand = nil },
			},
			{
				name:   "returns ErrInput for a Parallelism of 0",
				change: func(in *placement.Input) { in.Parallelism = 0 },
			},
			{
				name:   "returns the errors of NewState",
				change: func(in *placement.Input) { in.Model = replication.Model{} },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				in := cell(n0)
				tt.change(in)
				_, err := newPlanner(t, defaults(), nil).Place(t.Context(), in)
				assert.ErrorIs(t, err, placement.ErrInput, "Place must refuse the input")
			})
		}

		t.Run("returns ErrInput for a nil input", func(t *testing.T) {
			t.Parallel()
			_, err := newPlanner(t, defaults(), nil).Place(t.Context(), nil)
			assert.ErrorIs(t, err, placement.ErrInput, "Place must refuse a nil input")
		})

		t.Run("returns the cause of a context that ended before the pass", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancelCause(t.Context())
			cancel(errStopped)
			_, err := newPlanner(t, defaults(), nil).Place(ctx, cell(n0))
			assert.ErrorIs(t, err, errStopped, "Place must return the cause of the context")
		})
	})
}

// TestPlannerConformance checks that a planner with every built-in constraint but
// match-label and every built-in goal passes the checks of the suite of
// placement.Placer.
func TestPlannerConformance(t *testing.T) {
	t.Parallel()
	placementtest.RunPlacerSuiteFromHandle(t, func(h *placementtest.PlacerHandle) constrainedPlanner {
		cs, gs := everyStrategy(h)
		return constrainedPlanner{Planner: newPlanner(h, defaults(), cs, gs...), constraints: cs}
	})
}

// BenchmarkPlanner reports the cost of a pass at increasing sizes of a cell, in the
// six cases of the design: the goals of each preset, a pass without an improving move,
// a cell where every move is infeasible, a skewed load, many label constraints, and
// one failed node. Each size has 200 shards per node. Besides ns/op and allocs/op it
// reports the time to build the State as state-ns, and the cost of each tier before
// and after the pass as tierN-before and tierN-after.
func BenchmarkPlanner(b *testing.B) {
	sizes := []int{1_000, 10_000, 100_000}
	if *bound {
		sizes = append(sizes, 1_000_000)
	}
	for _, c := range benchCases(b) {
		b.Run(c.name, func(b *testing.B) {
			for _, shards := range sizes {
				b.Run(strconv.Itoa(shards), func(b *testing.B) {
					in := scaled(shards, c.shape)
					p := newPlanner(b, defaults(), c.constraints, c.goals...)
					start := time.Now()
					st, err := placement.NewState(in)
					built := time.Since(start)
					assert.NoError(b, err, "NewState must accept the cell of the benchmark")
					before := map[int]float64{}
					for _, g := range c.goals {
						before[g.tier] += g.weight * g.goal.Cost(st)
					}
					var plan placement.Plan

					contract := bench.Start(b)
					defer contract.End()

					for contract.Loop() {
						contract.Excluding(func() { in.Rand = pcg.New(1) })
						plan, err = p.Place(b.Context(), in)
					}

					assert.NoError(b, err, "the benchmark must measure a pass that returns a plan")
					b.ReportMetric(float64(built.Nanoseconds()), "state-ns")
					for t, cost := range plan.Costs {
						b.ReportMetric(before[t], "tier"+strconv.Itoa(t)+"-before")
						b.ReportMetric(cost, "tier"+strconv.Itoa(t)+"-after")
					}
				})
			}
		})
	}
}

// benchCases returns the cases of BenchmarkPlanner. The presets exclusive-worker,
// consumer-group and actor-entities balance the replica count, and exclusive-worker
// also spreads the replicas over the zones.
func benchCases(tb assert.TB) []benchCase {
	tb.Helper()
	zone := newGoal(tb, func() (placement.Goal, error) {
		return goal.NewSpread(goal.SpreadConfig{Label: placementtest.Zone})
	})
	balance := newGoal(tb, func() (placement.Goal, error) {
		return goal.NewBalance(goal.BalanceConfig{Metric: placementtest.CPU})
	})
	count := newGoal(tb, func() (placement.Goal, error) { return goal.NewCount(goal.CountConfig{}) })
	capacity, err := constraint.NewCapacity(constraint.CapacityConfig{Headroom: 0.1})
	assert.NoError(tb, err, "NewCapacity must accept a headroom of 0.1")
	isolation, err := constraint.NewIsolation(constraint.IsolationConfig{Label: placementtest.Zone, Max: 1})
	assert.NoError(tb, err, "NewIsolation must accept the label zone")
	rules := make([]constraint.Rule, racks)
	for i := range rules {
		rules[i] = constraint.Rule{
			Key:      rackKey + strconv.Itoa(i),
			Operator: constraint.NotIn,
			Values:   []string{rackExcluded},
		}
	}
	labels, err := constraint.NewLabels(constraint.LabelsConfig{Rules: rules})
	assert.NoError(tb, err, "NewLabels must accept the rules of the racks")
	return []benchCase{
		{
			name:  "exclusive-worker",
			goals: []tiered{{goal: zone, weight: 1}, {goal: count, tier: 1, weight: 1}},
			shape: joined,
		},
		{
			name:  "consumer-group",
			goals: []tiered{{goal: count, weight: 1}, {goal: goal.Stickiness{}, tier: 1, weight: 1}},
			shape: joined,
		},
		{name: "actor-entities", goals: []tiered{{goal: count, weight: 1}}, shape: joined},
		{
			name:  "no-improving-move",
			goals: []tiered{{goal: goal.Stickiness{}, weight: 1}, {goal: count, tier: 1, weight: 1}},
			shape: joined,
		},
		{
			name:        "infeasible",
			constraints: []placement.Constraint{capacity},
			goals:       []tiered{{goal: count, weight: 1}},
			shape:       full,
		},
		{
			name:        "skewed-load",
			constraints: []placement.Constraint{capacity, isolation},
			goals:       []tiered{{goal: balance, weight: 1}, {goal: count, tier: 1, weight: 1}},
			shape:       skewed,
		},
		{
			name:        "label-constraints",
			constraints: []placement.Constraint{labels},
			goals:       []tiered{{goal: count, weight: 1}},
			shape:       joined,
		},
		{
			name:        "failed-node",
			constraints: []placement.Constraint{capacity},
			goals:       []tiered{{goal: count, weight: 1}},
			shape:       failed,
		},
	}
}

// scaled returns the input of a cell under primary-only with the given number of
// shards. The cell has one node per 200 shards and at least two nodes, in three zones,
// with the rack labels on each node. Every node has a capacity of cpu, and every shard
// has a load of cpu. s decides the hosts and the loads of the shards.
func scaled(shards int, s shape) *placement.Input {
	nodes := max(2, shards/shardsPerNode)
	in := &placement.Input{
		Clock:       fake.New(origin),
		Rand:        pcg.New(1),
		ShardLoad:   make(map[shard.ID]shard.Load, shards),
		Model:       replication.PrimaryOnly,
		Layout:      partition.Layout{Shards: make([]partition.Shard, shards)},
		Current:     shard.Assignment{Placements: make(map[shard.ID]shard.Placement, shards)},
		Nodes:       make([]shard.Node, nodes),
		Metrics:     []placement.Metric{{Name: placementtest.CPU}},
		Parallelism: 4,
		Emergency:   s == failed,
	}
	for i := range nodes {
		labels := make(tag.Tags, 0, 1+racks)
		labels = append(labels, tag.Tag{Key: placementtest.Zone, Value: strconv.Itoa(i % 3)})
		for r := range racks {
			labels = append(labels, tag.Tag{Key: rackKey + strconv.Itoa(r), Value: rackValue})
		}
		in.Nodes[i] = shard.Node{
			ID:       shard.NodeID("n" + strconv.Itoa(i)),
			Labels:   labels,
			Capacity: map[string]float64{placementtest.CPU: benchCapacity},
		}
	}
	hosts := nodes - 1
	if s == failed {
		hosts = nodes
	}
	for i := range shards {
		seq := i + 1
		sh := shard.ID{ID128: id.ID128{12: byte(seq >> 24), 13: byte(seq >> 16), 14: byte(seq >> 8), 15: byte(seq)}}
		in.Layout.Shards[i] = partition.Shard{ID: sh}
		host, load := in.Nodes[i%hosts].ID, 1.0
		if s == skewed && i%shardsPerNode == 0 {
			load = hotLoad
		}
		if s == full {
			host, load = goneNode, overload
		}
		in.ShardLoad[sh] = shard.Load{placementtest.CPU: load}
		in.Current.Placements[sh] = shard.Placement{Shard: sh, Replicas: []shard.Replica{
			{Node: host, Role: shard.RolePrimary},
		}}
	}
	if s == failed {
		in.Nodes = in.Nodes[:nodes-1]
	}
	return in
}

// everyStrategy returns every built-in constraint but match-label, which would reject
// every shard of the generated cells, and every built-in goal, in three tiers.
func everyStrategy(tb assert.TB) ([]placement.Constraint, []tiered) {
	tb.Helper()
	capacity, err := constraint.NewCapacity(constraint.CapacityConfig{Headroom: 0.1})
	assert.NoError(tb, err, "NewCapacity must accept a headroom of 0.1")
	isolation, err := constraint.NewIsolation(constraint.IsolationConfig{Label: placementtest.Zone, Max: 2})
	assert.NoError(tb, err, "NewIsolation must accept the label zone")
	perNode, err := constraint.NewMaxPerNode(constraint.MaxPerNodeConfig{Max: 6})
	assert.NoError(tb, err, "NewMaxPerNode must accept a Max of 6")
	labels, err := constraint.NewLabels(constraint.LabelsConfig{Rules: []constraint.Rule{{
		Key: placementtest.Zone, Operator: constraint.NotIn, Values: []string{zoneC}, Role: shard.RoleSecondary,
	}}})
	assert.NoError(tb, err, "NewLabels must accept a rule of the label zone")
	return []placement.Constraint{capacity, constraint.Drain{}, isolation, perNode, labels}, []tiered{
		{goal: newGoal(tb, func() (placement.Goal, error) {
			return goal.NewSpread(goal.SpreadConfig{Label: placementtest.Zone})
		}), weight: 1},
		{goal: newGoal(tb, func() (placement.Goal, error) {
			return goal.NewPrefer(goal.PreferConfig{Label: placementtest.Zone, From: placementtest.Home})
		}), weight: 2},
		{goal: newGoal(tb, func() (placement.Goal, error) {
			return goal.NewUtilisation(goal.UtilisationConfig{Metric: placementtest.CPU, Above: 0.9})
		}), tier: 1, weight: 4},
		{goal: newGoal(tb, func() (placement.Goal, error) {
			return goal.NewBalance(goal.BalanceConfig{Metric: placementtest.CPU})
		}), tier: 2, weight: 1},
		{goal: newGoal(tb, func() (placement.Goal, error) {
			return goal.NewBalance(goal.BalanceConfig{Metric: placementtest.Bytes})
		}), tier: 2, weight: 0.5},
		{
			goal:   newGoal(tb, func() (placement.Goal, error) { return goal.NewCount(goal.CountConfig{}) }),
			tier:   2,
			weight: 1,
		},
		{goal: goal.Stickiness{}, tier: 2, weight: 0.25},
		{goal: newGoal(tb, func() (placement.Goal, error) {
			return goal.NewColocate(goal.ColocateConfig{Group: placementtest.Tenant})
		}), tier: 2, weight: 1},
	}
}

// newPlanner returns the planner of cfg with the constraints cs and the goals gs, and
// selections that name them. It stops tb when New refuses them.
func newPlanner(tb assert.TB, cfg planner.Config, cs []placement.Constraint, gs ...tiered) *planner.Planner {
	tb.Helper()
	for _, c := range cs {
		cfg.Constraints = append(cfg.Constraints, planner.Selection{Name: c.Name()})
	}
	goals := make([]placement.Goal, len(gs))
	for i, g := range gs {
		cfg.Goals = append(cfg.Goals, planner.GoalSelection{Name: g.goal.Name(), Tier: g.tier, Weight: g.weight})
		goals[i] = g.goal
	}
	p, err := planner.New(&cfg, cs, goals)
	assert.NoError(tb, err, "New must accept the planner of the test")
	return p
}

// newGoal returns the goal that build returns, and stops tb when build fails.
func newGoal(tb assert.TB, build func() (placement.Goal, error)) placement.Goal {
	tb.Helper()
	g, err := build()
	assert.NoError(tb, err, "the constructor of the goal must accept its configuration")
	return g
}

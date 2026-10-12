// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package planner_test

import (
	"context"
	"errors"
	"math"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/rand"
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

// The nodes of the cells of the tests of a pass.
const (
	// n0 is accepting, in zone a.
	n0 shard.NodeID = "n0"
	// n1 is accepting, in zone a.
	n1 shard.NodeID = "n1"
	// n2 is accepting, in zone b.
	n2 shard.NodeID = "n2"
	// n3 is accepting, in zone b.
	n3 shard.NodeID = "n3"
	// d9 drains.
	d9 shard.NodeID = "d9"
	// g9 is gone.
	g9 shard.NodeID = "g9"
)

// The values of the label zone of the cells of the tests of a pass.
const (
	zoneA = "a"
	zoneB = "b"
	zoneC = "c"
)

// The capacities and loads of cpu of the cells of the tests of a pass.
const (
	// roomy is the capacity of cpu of each node of a cell.
	roomy = 100
	// tight is a capacity of cpu that puts a node with a load of 20 at the threshold of
	// activity and above it.
	tight = 50
	// exact is a capacity of cpu that puts a node with a load of 20 exactly at the
	// default threshold of activity.
	exact = 80
	// heavy is the load of cpu of a large shard.
	heavy = 10
	// light is the load of cpu of a small shard.
	light = 1
)

// The names of the test doubles of a pass.
const (
	tickingName    shard.StrategyName = "ticking"
	cancellingName shard.StrategyName = "cancelling"
	meddlingName   shard.StrategyName = "meddling"
	faultyName     shard.StrategyName = "faulty"
	firstName      shard.StrategyName = "first"
	scriptedName   shard.StrategyName = "scripted"
	nudgeName      shard.StrategyName = "nudge"
	poisonedName   shard.StrategyName = "poisoned"
	avoidName      shard.StrategyName = "avoid"
	rejectingName  shard.StrategyName = "rejecting"
	rolesName      shard.StrategyName = "roles"
	countedName    shard.StrategyName = "counted"
)

// The metrics of the cells of the tests besides cpu.
const (
	bytesMetric = "bytes"
	memMetric   = "mem"
)

// errStopped is the cause with which a test ends the context of a pass.
var errStopped = errors.New("the test stopped the pass")

// The errors of the constraints of the tests of a pass.
var (
	errAvoided   = errors.New("the test avoids the node")
	errPrimary   = errors.New("the test rejects primaries")
	errSecondary = errors.New("the test rejects secondaries")
)

// pair is a replica state model with one primary and one secondary for each shard.
var pair = replication.Model{
	Name: "pair",
	Roles: []replication.Role{
		{Name: shard.RolePrimary, Max: 1, Writer: true},
		{Name: shard.RoleSecondary, Max: 1},
	},
	Transitions: []replication.Transition{
		{From: shard.RolePrimary, To: shard.RoleOffline, Priority: 0},
		{From: shard.RoleSecondary, To: shard.RoleOffline, Priority: 0},
		{From: shard.RoleOffline, To: shard.RolePrimary, Priority: 1},
		{From: shard.RoleOffline, To: shard.RoleSecondary, Priority: 1},
	},
}

// origin is the time of the clocks of the tests.
var origin = time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC)

// ids are the shards of the cells of the tests, in the order of their IDs.
var ids = func() []shard.ID {
	list := make([]shard.ID, 8)
	for i := range list {
		list[i] = shard.ID{ID128: id.ID128{15: byte(i + 1)}}
	}
	return list
}()

// The targets that put the primary of a shard on one node.
var (
	onN0 = placement.Target{Replicas: []shard.Replica{{Node: n0, Role: shard.RolePrimary}}}
	onN1 = placement.Target{Replicas: []shard.Replica{{Node: n1, Role: shard.RolePrimary}}}
	onN2 = placement.Target{Replicas: []shard.Replica{{Node: n2, Role: shard.RolePrimary}}}
	onN3 = placement.Target{Replicas: []shard.Replica{{Node: n3, Role: shard.RolePrimary}}}
)

// ticking is a constraint that admits every move and advances a fake clock by an hour
// on each check, so a pass ends at its timeout.
type ticking struct {
	// clock is the clock of the input of the pass.
	clock *fake.Clock
}

// Name returns the name ticking.
func (ticking) Name() shard.StrategyName {
	return tickingName
}

// Check advances the clock by an hour, and returns nil.
func (c ticking) Check(*placement.State, placement.Move) error {
	c.clock.Advance(time.Hour)
	return nil
}

// cancelling is a constraint that admits every move and ends a context with
// errStopped on each check.
type cancelling struct {
	// cancel ends the context of the pass.
	cancel context.CancelCauseFunc
}

// Name returns the name cancelling.
func (cancelling) Name() shard.StrategyName {
	return cancellingName
}

// Check ends the context, and returns nil.
func (c cancelling) Check(*placement.State, placement.Move) error {
	c.cancel(errStopped)
	return nil
}

// meddling is a constraint that changes the state, although the contract of a
// constraint does not allow it. It applies the first move whose From differs from its
// To.
type meddling struct {
	// applied reports that the constraint applied a move.
	applied *bool
}

// Name returns the name meddling.
func (meddling) Name() shard.StrategyName {
	return meddlingName
}

// Check applies the first move whose From differs from its To and returns the error
// of the state. It returns nil for every other move.
func (c meddling) Check(st *placement.State, m placement.Move) error {
	if *c.applied || m.From == m.To {
		return nil
	}
	*c.applied = true
	return st.Apply(m)
}

// faulty is a goal with fixed costs and a fixed delta. Its cost is the field start
// before the first move of a pass, and the field moved after it. Its delta is the field
// delta for every move.
type faulty struct {
	// start is the cost of a state before its first move.
	start float64
	// moved is the cost of a state after its first move.
	moved float64
	// delta is the delta of every move.
	delta float64
}

// Name returns the name faulty.
func (faulty) Name() shard.StrategyName {
	return faultyName
}

// Cost returns start before the first move of st, and moved after it.
func (g faulty) Cost(st *placement.State) float64 {
	if st.Moved() > 0 {
		return g.moved
	}
	return g.start
}

// Delta returns delta.
func (g faulty) Delta(*placement.State, placement.Move) float64 {
	return g.delta
}

// first is a goal with a cost of 1 until the pass applies its first move, and 0 after
// it. Any first move lowers its cost, and a later move does not change it.
type first struct{}

// Name returns the name first.
func (first) Name() shard.StrategyName {
	return firstName
}

// Cost returns 1 before the first move of st, and 0 after it.
func (first) Cost(st *placement.State) float64 {
	if st.Moved() > 0 {
		return 0
	}
	return 1
}

// Delta returns -1 for a move before the first move of st, and 0 for every other move
// and for a move whose From equals its To.
func (first) Delta(st *placement.State, m placement.Move) float64 {
	if m.From == m.To || st.Moved() > 0 {
		return 0
	}
	return -1
}

// scripted is a goal with a cost of 0, whose delta of a move of a shard to a node is
// the delta of the shard in deltas. The delta of every other move is 0.
type scripted struct {
	// deltas maps a shard to the delta of each move of the shard to a node.
	deltas map[shard.ID]float64
}

// Name returns the name scripted.
func (scripted) Name() shard.StrategyName {
	return scriptedName
}

// Cost returns 0.
func (scripted) Cost(*placement.State) float64 {
	return 0
}

// Delta returns the delta of the shard of m when m moves a replica to a node, and 0
// otherwise.
func (g scripted) Delta(_ *placement.State, m placement.Move) float64 {
	if m.To == "" || m.From == m.To {
		return 0
	}
	return g.deltas[m.Shard]
}

// nudge is a goal whose cost is start before the first move of a pass and after once a
// move applied. Before the first move, the delta of a move to node n is after minus
// start plus the delta of n in first. After it, the delta of a move to n is the delta
// of n in later.
type nudge struct {
	// first maps a node to the part of the delta of a move to it before the first move.
	first map[shard.NodeID]float64
	// later maps a node to the delta of a move to it after the first move.
	later map[shard.NodeID]float64
	// start is the cost before the first move.
	start float64
	// after is the cost once a move applied.
	after float64
}

// Name returns the name nudge.
func (nudge) Name() shard.StrategyName {
	return nudgeName
}

// Cost returns start before the first move of st, and after once a move applied.
func (g nudge) Cost(st *placement.State) float64 {
	if st.Moved() > 0 {
		return g.after
	}
	return g.start
}

// Delta returns the delta of m by the rules of nudge.
func (g nudge) Delta(st *placement.State, m placement.Move) float64 {
	if m.From == m.To {
		return 0
	}
	if st.Moved() > 0 {
		return g.later[m.To]
	}
	return g.after - g.start + g.first[m.To]
}

// poisoned is a goal with a cost of 0, whose delta of a move to node is +Inf. The
// delta of every other move is 0.
type poisoned struct {
	// node is the node of the infinite deltas.
	node shard.NodeID
}

// Name returns the name poisoned.
func (poisoned) Name() shard.StrategyName {
	return poisonedName
}

// Cost returns 0.
func (poisoned) Cost(*placement.State) float64 {
	return 0
}

// Delta returns +Inf for a move to node, and 0 for every other move.
func (g poisoned) Delta(_ *placement.State, m placement.Move) float64 {
	if m.To == g.node && m.From != m.To {
		return math.Inf(1)
	}
	return 0
}

// avoid is a constraint that rejects a move of one shard to one node with errAvoided.
type avoid struct {
	// node is the node that the constraint avoids.
	node shard.NodeID
	// shard is the shard that the constraint keeps off node.
	shard shard.ID
}

// Name returns the name avoid.
func (avoid) Name() shard.StrategyName {
	return avoidName
}

// Check returns errAvoided for a move of shard to node whose From differs from its To.
func (c avoid) Check(_ *placement.State, m placement.Move) error {
	if m.Shard == c.shard && m.To == c.node && m.From != m.To {
		return errAvoided
	}
	return nil
}

// rejecting is a constraint that rejects each move to a node with the error of the
// node in errs.
type rejecting struct {
	// errs maps a node to the error of each move to it.
	errs map[shard.NodeID]error
}

// Name returns the name rejecting.
func (rejecting) Name() shard.StrategyName {
	return rejectingName
}

// Check returns the error of the destination of m when the From of m differs from its
// To, and nil otherwise.
func (c rejecting) Check(_ *placement.State, m placement.Move) error {
	if m.From == m.To {
		return nil
	}
	return c.errs[m.To]
}

// roles is a constraint that rejects each move of a replica in a role with the error
// of the role in errs.
type roles struct {
	// errs maps a role to the error of each move of a replica in it.
	errs map[shard.Role]error
}

// Name returns the name roles.
func (roles) Name() shard.StrategyName {
	return rolesName
}

// Check returns the error of the role of m when the From of m differs from its To, and
// nil otherwise.
func (c roles) Check(_ *placement.State, m placement.Move) error {
	if m.From == m.To {
		return nil
	}
	return c.errs[m.Role]
}

// counted is a goal with a cost of 0 and a delta of 0 that counts the calls of Cost.
type counted struct {
	// calls counts the calls of Cost.
	calls *atomic.Int32
}

// Name returns the name counted.
func (counted) Name() shard.StrategyName {
	return countedName
}

// Cost counts the call, and returns 0.
func (g counted) Cost(*placement.State) float64 {
	g.calls.Add(1)
	return 0
}

// Delta returns 0.
func (counted) Delta(*placement.State, placement.Move) float64 {
	return 0
}

// cell returns the input of a cell under primary-only on the accepting nodes n0 to n3
// and the draining node d9. Shard i has its primary on hosts[i], or no placement for
// an empty element. Each node has a capacity of cpu roomy, and no shard reports a load.
func cell(hosts ...shard.NodeID) *placement.Input {
	in := &placement.Input{
		Clock:       fake.New(origin),
		Rand:        pcg.New(1),
		Transitions: map[shard.ID]placement.InFlight{},
		Faults:      map[shard.ID][]shard.NodeID{},
		Draining:    map[shard.NodeID]bool{d9: true},
		ShardLoad:   map[shard.ID]shard.Load{},
		Model:       replication.PrimaryOnly,
		Current:     shard.Assignment{Placements: map[shard.ID]shard.Placement{}},
		Metrics:     []placement.Metric{{Name: placementtest.CPU}},
		Parallelism: 1,
	}
	for _, n := range []struct {
		id   shard.NodeID
		zone string
	}{{n0, zoneA}, {n1, zoneA}, {n2, zoneB}, {n3, zoneB}} {
		in.Nodes = append(in.Nodes, shard.Node{
			ID:       n.id,
			Labels:   tag.Tags{{Key: placementtest.Zone, Value: n.zone}},
			Capacity: map[string]float64{placementtest.CPU: roomy},
		})
	}
	for i, host := range hosts {
		in.Layout.Shards = append(in.Layout.Shards, partition.Shard{ID: ids[i]})
		if host != "" {
			in.Current.Placements[ids[i]] = shard.Placement{
				Shard:    ids[i],
				Replicas: []shard.Replica{{Node: host, Role: shard.RolePrimary}},
			}
		}
	}
	return in
}

// paired returns a function that puts an input under the model pair and places shard i
// on the nodes of replicas[i]. The first replica of a shard is its primary, and the
// second is its secondary. The input must not place these shards already.
func paired(replicas ...[]shard.NodeID) func(in *placement.Input) {
	return func(in *placement.Input) {
		in.Model = pair
		for i, nodes := range replicas {
			rs := make([]shard.Replica, len(nodes))
			for k, n := range nodes {
				rs[k] = shard.Replica{Node: n, Role: shard.RoleSecondary}
			}
			if len(rs) > 0 {
				rs[0].Role = shard.RolePrimary
				in.Current.Placements[ids[i]] = shard.Placement{Shard: ids[i], Replicas: rs}
			}
		}
	}
}

// loaded returns a function that sets the load of cpu of shard i of an input to the
// element i of cpu.
func loaded(cpu ...float64) func(in *placement.Input) {
	return func(in *placement.Input) {
		for i, l := range cpu {
			in.ShardLoad[ids[i]] = shard.Load{placementtest.CPU: l}
		}
	}
}

// TestPass checks the repair, the improvement, the limits and the errors of a pass.
func TestPass(t *testing.T) {
	t.Parallel()

	count := newGoal(t, func() (placement.Goal, error) { return goal.NewCount(goal.CountConfig{}) })
	balance := newGoal(t, func() (placement.Goal, error) {
		return goal.NewBalance(goal.BalanceConfig{Metric: placementtest.CPU})
	})
	inZoneA, err := constraint.NewLabels(constraint.LabelsConfig{Rules: []constraint.Rule{
		{Key: placementtest.Zone, Operator: constraint.In, Values: []string{zoneA}},
	}})
	assert.NoError(t, err, "NewLabels must accept the rule of zone a")
	inZoneC, err := constraint.NewLabels(constraint.LabelsConfig{Rules: []constraint.Rule{
		{Key: placementtest.Zone, Operator: constraint.In, Values: []string{zoneC}},
	}})
	assert.NoError(t, err, "NewLabels must accept the rule of zone c")
	// imbalanced puts three shards on n0, and one on n1 and on n2.
	imbalanced := []shard.NodeID{n0, n0, n0, n1, n2}
	// crowded puts six shards on n0.
	crowded := []shard.NodeID{n0, n0, n0, n0, n0, n0}
	// everyMove is a goal under which every move of every shard lowers the cost by 1.
	everyMove := scripted{deltas: map[shard.ID]float64{}}
	for _, id := range ids {
		everyMove.deltas[id] = -1
	}
	// still is a goal whose delta is 0 for every move.
	still := scripted{deltas: map[shard.ID]float64{}}
	// emergency marks an input as an emergency, so a pass only repairs.
	emergency := func(in *placement.Input) { in.Emergency = true }
	// skewed puts two heavy shards on n0 and two light shards on each other node.
	skewed := func(capacity float64) func(in *placement.Input) {
		return func(in *placement.Input) {
			for i := range in.Nodes {
				in.Nodes[i].Capacity[placementtest.CPU] = capacity
			}
			for i, sh := range ids {
				load := float64(light)
				if i < 2 {
					load = heavy
				}
				in.ShardLoad[sh] = shard.Load{placementtest.CPU: load}
			}
		}
	}

	t.Run("Place", func(t *testing.T) {
		t.Parallel()

		plans := []struct {
			name        string
			hosts       []shard.NodeID
			constraints []placement.Constraint
			goals       []tiered
			input       func(in *placement.Input)
			config      func(cfg *planner.Config)
			want        map[shard.ID]placement.Target
		}{
			{
				name:  "places an unplaced shard on the node that the goals prefer",
				hosts: []shard.NodeID{n0, n0, n1, n2, ""},
				goals: []tiered{{goal: count, weight: 1}},
				want:  map[shard.ID]placement.Target{ids[4]: onN3},
			},
			{
				name:  "moves a replica off a gone node",
				hosts: []shard.NodeID{g9, n0, n1, n2},
				goals: []tiered{{goal: count, weight: 1}},
				want:  map[shard.ID]placement.Target{ids[0]: onN3},
			},
			{
				name:  "moves a replica with a fault to a node without the fault",
				hosts: []shard.NodeID{n3, n0, n1, n2},
				goals: []tiered{{goal: count, weight: 1}},
				input: func(in *placement.Input) { in.Faults[ids[0]] = []shard.NodeID{n3} },
				want:  map[shard.ID]placement.Target{ids[0]: onN0, ids[1]: onN3},
			},
			{
				name:  "moves a replica off a draining node",
				hosts: []shard.NodeID{d9, n0, n1, n2},
				goals: []tiered{{goal: count, weight: 1}},
				want:  map[shard.ID]placement.Target{ids[0]: onN3},
			},
			{
				name:  "keeps a replica on a draining node in an emergency",
				hosts: []shard.NodeID{d9, n0, n1, n2},
				goals: []tiered{{goal: count, weight: 1}},
				input: func(in *placement.Input) { in.Emergency = true },
			},
			{
				name:        "moves a replica that a constraint rejects",
				hosts:       []shard.NodeID{n2, n0},
				constraints: []placement.Constraint{inZoneA},
				goals:       []tiered{{goal: count, weight: 1}},
				want:        map[shard.ID]placement.Target{ids[0]: onN1},
			},
			{
				name:  "spreads the replicas of a gone node over nodes of equal score",
				hosts: []shard.NodeID{g9, g9, g9, g9},
				want:  map[shard.ID]placement.Target{ids[0]: onN0, ids[1]: onN1, ids[2]: onN2, ids[3]: onN3},
			},
			{
				name:  "plans no move for a shard with a transition in flight",
				hosts: []shard.NodeID{g9},
				goals: []tiered{{goal: count, weight: 1}},
				input: func(in *placement.Input) { in.Transitions[ids[0]] = placement.InFlight{To: n0} },
			},
			{
				name:  "moves a shard to the node that lowers the cost of its tier the most",
				hosts: imbalanced,
				goals: []tiered{{goal: count, weight: 1}},
				want:  map[shard.ID]placement.Target{ids[0]: onN3},
			},
			{
				name:  "moves at most PerNode replicas out of a node",
				hosts: crowded,
				goals: []tiered{{goal: count, weight: 1}},
				config: func(cfg *planner.Config) {
					cfg.Churn.PerNode = 1
					cfg.Churn.MaxShardFraction = 1
				},
				want: map[shard.ID]placement.Target{ids[0]: onN1},
			},
			{
				name:   "moves the replicas out of a node in one move per sweep",
				hosts:  crowded,
				goals:  []tiered{{goal: count, weight: 1}},
				config: func(cfg *planner.Config) { cfg.Churn.MaxShardFraction = 1 },
				want:   map[shard.ID]placement.Target{ids[0]: onN1, ids[1]: onN2},
			},
			{
				name:  "moves at most MaxShardFraction of the shards, rounded up",
				hosts: crowded,
				goals: []tiered{{goal: count, weight: 1}},
				config: func(cfg *planner.Config) {
					cfg.Churn.PerNode = 10
					cfg.Churn.MaxShardFraction = 0.1
				},
				want: map[shard.ID]placement.Target{ids[0]: onN1},
			},
			{
				name:   "moves no shard when MaxShardFraction is 0",
				hosts:  crowded,
				goals:  []tiered{{goal: count, weight: 1}},
				config: func(cfg *planner.Config) { cfg.Churn.MaxShardFraction = 0 },
			},
			{
				name:   "moves no shard when PerShard is 0",
				hosts:  crowded,
				goals:  []tiered{{goal: count, weight: 1}},
				config: func(cfg *planner.Config) { cfg.Churn.PerShard = 0 },
			},
			{
				name:   "skips the improvement while the highest load is near the mean",
				hosts:  []shard.NodeID{n0, n0, n0, n1, n1, n2, n2, n3},
				goals:  []tiered{{goal: count, weight: 1}},
				config: func(cfg *planner.Config) { cfg.Triggers.MinImbalance = 2 },
			},
			{
				name:   "improves a cell whose highest load is far above the mean",
				hosts:  []shard.NodeID{n0, n0, n0, n1, n1, n2, n2, n3},
				goals:  []tiered{{goal: count, weight: 1}},
				config: func(cfg *planner.Config) { cfg.Triggers.MinImbalance = 1.4 },
				want:   map[shard.ID]placement.Target{ids[0]: onN3},
			},
			{
				name:   "improves a cell whose least loaded node is far below the mean",
				hosts:  []shard.NodeID{n0, n0, n1, n1, n2, n2},
				goals:  []tiered{{goal: count, weight: 1}},
				config: func(cfg *planner.Config) { cfg.Triggers.MinImbalance = 1.5 },
				want:   map[shard.ID]placement.Target{ids[0]: onN3},
			},
			{
				name:  "skips the improvement while every node is below the threshold of activity",
				hosts: []shard.NodeID{n0, n0, n1, n1, n2, n2, n3, n3},
				goals: []tiered{{goal: balance, weight: 1}},
				input: skewed(roomy),
			},
			{
				name:  "improves a cell with a node at the threshold of activity",
				hosts: []shard.NodeID{n0, n0, n1, n1, n2, n2, n3, n3},
				goals: []tiered{{goal: balance, weight: 1}},
				input: skewed(tight),
				want:  map[shard.ID]placement.Target{ids[0]: onN1},
			},
			{
				name:  "keeps the cost of a lower tier when it improves a higher one",
				hosts: imbalanced,
				goals: []tiered{{goal: goal.Stickiness{}, weight: 1}, {goal: count, tier: 1, weight: 1}},
			},
			{
				name:  "applies a move that lowers a lower tier and raises a higher one",
				hosts: imbalanced,
				goals: []tiered{{goal: count, weight: 1}, {goal: goal.Stickiness{}, tier: 1, weight: 1}},
				want:  map[shard.ID]placement.Target{ids[0]: onN3},
			},
			{
				name:  "does not move a shard whose move lowers a lower tier within its tolerance and raises a higher one",
				hosts: []shard.NodeID{n0, n0, n1},
				goals: []tiered{
					{goal: scripted{deltas: map[shard.ID]float64{ids[0]: -0.5e-9}}, weight: 1},
					{goal: scripted{deltas: map[shard.ID]float64{ids[0]: 1}}, tier: 1, weight: 1},
				},
			},
			{
				name:  "moves a shard whose move raises a lower tier within its tolerance and lowers a higher one",
				hosts: []shard.NodeID{n0, n0, n1},
				goals: []tiered{
					{goal: scripted{deltas: map[shard.ID]float64{ids[0]: 0.5e-9}}, weight: 1},
					{goal: scripted{deltas: map[shard.ID]float64{ids[0]: -1}}, tier: 1, weight: 1},
				},
				want: map[shard.ID]placement.Target{ids[0]: onN1},
			},
			{
				name:  "moves the shard that lowers a lower tier over a shard that lowers a higher tier more",
				hosts: []shard.NodeID{n0, n0, n1},
				goals: []tiered{
					{goal: scripted{deltas: map[shard.ID]float64{ids[1]: -1}}, weight: 1},
					{goal: scripted{deltas: map[shard.ID]float64{ids[0]: -100}}, tier: 1, weight: 1},
				},
				input: loaded(5, light, light),
				want:  map[shard.ID]placement.Target{ids[1]: onN1},
			},
			{
				name:   "moves a shard by a higher tier within MaxSourceShards when a lower tier cannot improve",
				hosts:  crowded,
				goals:  []tiered{{goal: still, weight: 1}, {goal: count, tier: 1, weight: 1}},
				config: func(cfg *planner.Config) { cfg.Search.MaxSourceShards = len(crowded) },
				want:   map[shard.ID]placement.Target{ids[0]: onN1},
			},
			{
				name:  "examines at most Sample shards of a node in a sweep",
				hosts: crowded,
				goals: []tiered{{goal: still, weight: 1}},
				config: func(cfg *planner.Config) {
					cfg.Search.Sample = 2
					cfg.Search.MaxSourceShards = 2
				},
			},
			{
				name:   "examines the Sample largest shards of a node in a sweep",
				hosts:  []shard.NodeID{n0, n0},
				goals:  []tiered{{goal: scripted{deltas: map[shard.ID]float64{ids[0]: -1}}, weight: 1}},
				input:  loaded(light, 5),
				config: func(cfg *planner.Config) { cfg.Search.Sample = 1 },
			},
			{
				name:  "does not improve the assignment in an emergency",
				hosts: imbalanced,
				goals: []tiered{{goal: count, weight: 1}},
				input: func(in *placement.Input) { in.Emergency = true },
			},
			{
				name:  "places an unplaced shard on a node without a fault of the shard",
				hosts: []shard.NodeID{n0, n0, n2, n3, ""},
				goals: []tiered{{goal: count, weight: 1}},
				input: func(in *placement.Input) {
					in.Emergency = true
					in.Faults[ids[4]] = []shard.NodeID{n1}
				},
				want: map[shard.ID]placement.Target{ids[4]: onN2},
			},
			{
				name:  "places a replica on the node that the goals prefer before a node with fewer placements",
				hosts: []shard.NodeID{"", "", n1, n1, n2, n2, n3, n3},
				goals: []tiered{{goal: count, weight: 1}},
				input: emergency,
				want:  map[shard.ID]placement.Target{ids[0]: onN0, ids[1]: onN0},
			},
			{
				name:  "spreads the replicas of a gone node over nodes of equal deltas",
				hosts: []shard.NodeID{g9, g9, g9, g9},
				goals: []tiered{{goal: goal.Stickiness{}, weight: 1}},
				want:  map[shard.ID]placement.Target{ids[0]: onN0, ids[1]: onN1, ids[2]: onN2, ids[3]: onN3},
			},
			{
				name:  "places a replica on the lower node of two whose deltas differ by less than the tolerance",
				hosts: []shard.NodeID{""},
				goals: []tiered{
					{goal: nudge{start: 1000, after: 1000, first: map[shard.NodeID]float64{n1: -1e-9}}, weight: 1},
				},
				input: emergency,
				want:  map[shard.ID]placement.Target{ids[0]: onN0},
			},
			{
				name:  "places a replica by the tolerance of the cost that the first move leaves",
				hosts: []shard.NodeID{"", ""},
				goals: []tiered{{
					goal:   nudge{start: 1000, after: 100, later: map[shard.NodeID]float64{n1: -1, n2: -1 - 5e-7}},
					weight: 1,
				}},
				input: emergency,
				want:  map[shard.ID]placement.Target{ids[0]: onN0, ids[1]: onN2},
			},
			{
				name:  "places a replica on the lower node of two that tie within the tolerance after a move",
				hosts: []shard.NodeID{"", ""},
				goals: []tiered{{
					goal:   nudge{start: 1000, after: 100, later: map[shard.NodeID]float64{n1: -1, n2: -1 - 1e-9}},
					weight: 1,
				}},
				input: emergency,
				want:  map[shard.ID]placement.Target{ids[0]: onN0, ids[1]: onN1},
			},
			{
				name:        "places a replica on a node that a constraint rejected for another shard",
				hosts:       []shard.NodeID{"", "", n0, n1, n2},
				constraints: []placement.Constraint{avoid{node: n3, shard: ids[0]}},
				goals:       []tiered{{goal: count, weight: 1}},
				input:       emergency,
				want:        map[shard.ID]placement.Target{ids[0]: onN0, ids[1]: onN3},
			},
			{
				name:  "moves a shard to a node without a fault of the shard",
				hosts: []shard.NodeID{n0, n0, n0, n2, n3},
				goals: []tiered{{goal: count, weight: 1}},
				input: func(in *placement.Input) { in.Faults[ids[0]] = []shard.NodeID{n1} },
				want:  map[shard.ID]placement.Target{ids[1]: onN1},
			},
			{
				name:        "moves the shard that the constraints admit when a rejected move would tie with it",
				hosts:       []shard.NodeID{n0, n0, n1, n2},
				constraints: []placement.Constraint{avoid{node: n3, shard: ids[0]}},
				goals:       []tiered{{goal: count, weight: 1}},
				input:       loaded(light, 5, light, light),
				want:        map[shard.ID]placement.Target{ids[1]: onN3},
			},
			{
				name:  "moves the lower shard ID of two shards with equal deltas",
				hosts: []shard.NodeID{n0, n0, n1},
				goals: []tiered{{goal: scripted{deltas: map[shard.ID]float64{ids[0]: -1, ids[1]: -1}}, weight: 1}},
				input: loaded(light, 5, light),
				want:  map[shard.ID]placement.Target{ids[0]: onN1},
			},
			{
				name:  "moves the shard with the lowest delta when a lower shard ID has a higher delta",
				hosts: []shard.NodeID{n0, n0, n1},
				goals: []tiered{{goal: scripted{deltas: map[shard.ID]float64{ids[0]: -1, ids[1]: -5}}, weight: 1}},
				input: loaded(light, 5, light),
				want:  map[shard.ID]placement.Target{ids[1]: onN1},
			},
			{
				name:  "moves the lower shard ID of two shards whose deltas differ by less than the tolerance",
				hosts: []shard.NodeID{n0, n0, n1},
				goals: []tiered{
					{goal: scripted{deltas: map[shard.ID]float64{ids[0]: -1, ids[1]: -1 - 1e-12}}, weight: 1},
				},
				input: loaded(5, light, light),
				want:  map[shard.ID]placement.Target{ids[0]: onN1},
			},
			{
				name:  "moves a secondary to a node without a replica of its shard",
				hosts: []shard.NodeID{"", "", ""},
				goals: []tiered{{goal: count, weight: 1}},
				input: paired([]shard.NodeID{n1, n0}, []shard.NodeID{n2, n0}, []shard.NodeID{n3, n0}),
				want: map[shard.ID]placement.Target{ids[0]: {Replicas: []shard.Replica{
					{Node: n1, Role: shard.RolePrimary},
					{Node: n2, Role: shard.RoleSecondary},
				}}},
			},
			{
				name:  "improves a cell whose busiest node is exactly at the threshold of activity",
				hosts: []shard.NodeID{n0, n0, n1, n1, n2, n2, n3, n3},
				goals: []tiered{{goal: balance, weight: 1}},
				input: skewed(exact),
				want:  map[shard.ID]placement.Target{ids[0]: onN1},
			},
			{
				name:   "improves a cell whose highest load is exactly MinImbalance times the mean",
				hosts:  []shard.NodeID{n0, n0, n0, n1, n2, n3},
				goals:  []tiered{{goal: count, weight: 1}},
				config: func(cfg *planner.Config) { cfg.Triggers.MinImbalance = 2 },
				want:   map[shard.ID]placement.Target{ids[0]: onN1},
			},
			{
				name:   "skips the improvement on a metric without load",
				hosts:  []shard.NodeID{n0, n0, n1, n2, n3},
				goals:  []tiered{{goal: everyMove, weight: 1}},
				config: func(cfg *planner.Config) { cfg.Triggers.MinImbalance = 10 },
				input: func(in *placement.Input) {
					for i := range in.Nodes {
						in.Nodes[i].Capacity = nil
					}
				},
			},
			{
				name:  "applies a move that lowers a lower tier whatever the order of the goals",
				hosts: imbalanced,
				goals: []tiered{{goal: goal.Stickiness{}, tier: 1, weight: 1}, {goal: count, weight: 1}},
				want:  map[shard.ID]placement.Target{ids[0]: onN3},
			},
			{
				name:  "places a replica by the lowest tier where the nodes differ",
				hosts: []shard.NodeID{"", n0, n0, n1, n1, n3},
				goals: []tiered{{goal: count, weight: 1}, {goal: goal.Stickiness{}, tier: 1, weight: 1}},
				input: emergency,
				want:  map[shard.ID]placement.Target{ids[0]: onN2},
			},
			{
				name:  "places a replica by a higher tier when the nodes tie in the lowest",
				hosts: []shard.NodeID{"", n0, n0, n1, n1, n3},
				goals: []tiered{{goal: goal.Stickiness{}, weight: 1}, {goal: count, tier: 1, weight: 1}},
				input: emergency,
				want:  map[shard.ID]placement.Target{ids[0]: onN2},
			},
			{
				name:  "does not move a shard for a delta of 0",
				hosts: []shard.NodeID{n0, n0, n1, n1, n2, n2, n3},
				goals: []tiered{{goal: count, weight: 1}},
			},
			{
				name:  "does not move a lower shard ID whose delta is within the tolerance of 0",
				hosts: []shard.NodeID{n0, n0, n1},
				goals: []tiered{
					{goal: scripted{deltas: map[shard.ID]float64{ids[0]: -0.8e-9, ids[1]: -1.5e-9}}, weight: 1},
				},
				input: loaded(light, 5, light),
				want:  map[shard.ID]placement.Target{ids[1]: onN1},
			},
		}
		for _, tt := range plans {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				in := cell(tt.hosts...)
				if tt.input != nil {
					tt.input(in)
				}
				cfg := defaults()
				if tt.config != nil {
					tt.config(&cfg)
				}
				plan, err := newPlanner(t, cfg, tt.constraints, tt.goals...).Place(t.Context(), in)
				assert.NoError(t, err, "Place must plan the cell")
				expect.Equal(
					t,
					plan.Targets,
					tt.want,
					"Place must return the targets of the pass",
					assert.EquateEmpty(),
				)
				expect.False(t, plan.Incomplete, "the pass must end before its limits")
			})
		}

		t.Run("returns a deficit for a replica without a feasible node", func(t *testing.T) {
			t.Parallel()
			plan, err := newPlanner(t, defaults(), []placement.Constraint{inZoneC}).Place(t.Context(), cell(g9))
			assert.NoError(t, err, "Place must plan the cell")
			target := plan.Targets[ids[0]]
			expect.Empty(t, target.Replicas, "the target must keep no replica on the gone node")
			expect.Equal(t, target.Deficit, 1, "the target must report the replica as a deficit")
			expect.That(t, target.Reason).ErrorIs(placement.ErrNoNode, "the reason must wrap ErrNoNode").
				ErrorIs(constraint.ErrLabels, "the reason must wrap the error of the constraint")
		})

		t.Run("returns ErrNoNode as the reason of a cell without accepting nodes", func(t *testing.T) {
			t.Parallel()
			in := cell("")
			in.Nodes = nil
			plan, err := newPlanner(t, defaults(), nil).Place(t.Context(), in)
			assert.NoError(t, err, "Place must plan the cell")
			expect.Equal(t, plan.Targets[ids[0]].Deficit, 1, "the target must report the replica as a deficit")
			expect.ErrorIs(t, plan.Targets[ids[0]].Reason, placement.ErrNoNode, "the reason must wrap ErrNoNode")
		})

		cursors := []struct {
			name     string
			hosts    []shard.NodeID
			from     shard.ID
			want     map[shard.ID]placement.Target
			wantNext shard.ID
		}{
			{
				name:     "examines the shards from the repair cursor",
				hosts:    []shard.NodeID{"", "", ""},
				from:     ids[1],
				want:     map[shard.ID]placement.Target{ids[1]: onN0},
				wantNext: ids[2],
			},
			{
				name:     "examines the lowest shard after the highest",
				hosts:    []shard.NodeID{"", n0, n1},
				from:     ids[2],
				want:     map[shard.ID]placement.Target{ids[0]: onN2},
				wantNext: ids[1],
			},
			{
				name:     "returns the cursor of the input when no shard needs repair",
				hosts:    []shard.NodeID{n0, n1},
				from:     ids[1],
				wantNext: ids[1],
			},
		}
		for _, tt := range cursors {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				in := cell(tt.hosts...)
				in.Emergency = true
				in.RepairFrom = tt.from
				cfg := defaults()
				cfg.Search.MaxSourceShards = 1
				plan, err := newPlanner(t, cfg, nil, tiered{goal: count, weight: 1}).Place(t.Context(), in)
				assert.NoError(t, err, "Place must plan the cell")
				expect.Equal(
					t,
					plan.Targets,
					tt.want,
					"Place must repair the shard at the cursor",
					assert.EquateEmpty(),
				)
				expect.Equal(t, plan.RepairNext, tt.wantNext, "Place must move the cursor past the examined shard")
			})
		}

		limits := []struct {
			name   string
			hosts  []shard.NodeID
			goals  []tiered
			input  func(in *placement.Input)
			config func(cfg *planner.Config)
			want   map[shard.ID]placement.Target
		}{
			{
				name:  "ends on MaxMoves",
				hosts: crowded,
				config: func(cfg *planner.Config) {
					cfg.Search.MaxMoves = 1
					cfg.Churn.PerNode = 10
					cfg.Churn.MaxShardFraction = 1
				},
				want: map[shard.ID]placement.Target{ids[0]: onN1},
			},
			{
				name:   "ends on MaxEvaluations",
				hosts:  imbalanced,
				config: func(cfg *planner.Config) { cfg.Search.MaxEvaluations = 1 },
				want:   map[shard.ID]placement.Target{ids[0]: onN3},
			},
			{
				name:   "ends on MaxSourceShards",
				hosts:  imbalanced,
				config: func(cfg *planner.Config) { cfg.Search.MaxSourceShards = 1 },
				want:   map[shard.ID]placement.Target{ids[0]: onN3},
			},
			{
				name:   "ends on MaxSourceShards after the first shard of the node with the most replicas",
				hosts:  []shard.NodeID{n0, n1, n1, n1, n2, n3},
				goals:  []tiered{{goal: everyMove, weight: 1}},
				config: func(cfg *planner.Config) { cfg.Search.MaxSourceShards = 1 },
				want:   map[shard.ID]placement.Target{ids[1]: onN0},
			},
			{
				name:  "ends on MaxSourceShards after the first shard of the node with the highest load",
				hosts: []shard.NodeID{n0, n0, n1, n2, n3},
				goals: []tiered{{goal: everyMove, weight: 1}},
				input: loaded(0.05, 0.05, 0.5, 0.05, 0.05),
				config: func(cfg *planner.Config) {
					cfg.Search.MaxSourceShards = 1
					cfg.Churn.MaxShardFraction = 1
				},
				want: map[shard.ID]placement.Target{ids[2]: onN0},
			},
			{
				name:  "ends on MaxSourceShards after the largest shard of a node",
				hosts: []shard.NodeID{n0, n0, n1, n2},
				goals: []tiered{{goal: everyMove, weight: 1}},
				input: func(in *placement.Input) {
					in.Metrics = []placement.Metric{{Name: placementtest.CPU}, {Name: bytesMetric}, {Name: memMetric}}
					in.ShardLoad[ids[0]] = shard.Load{bytesMetric: 2000}
					in.ShardLoad[ids[1]] = shard.Load{placementtest.CPU: 3}
					in.ShardLoad[ids[2]] = shard.Load{placementtest.CPU: 1, bytesMetric: 1000}
					in.ShardLoad[ids[3]] = shard.Load{placementtest.CPU: 1, bytesMetric: 1000}
				},
				config: func(cfg *planner.Config) { cfg.Search.MaxSourceShards = 1 },
				want:   map[shard.ID]placement.Target{ids[1]: onN1},
			},
		}
		for _, tt := range limits {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				in := cell(tt.hosts...)
				if tt.input != nil {
					tt.input(in)
				}
				cfg := defaults()
				tt.config(&cfg)
				goals := tt.goals
				if goals == nil {
					goals = []tiered{{goal: count, weight: 1}}
				}
				plan, err := newPlanner(t, cfg, nil, goals...).Place(t.Context(), in)
				assert.NoError(t, err, "Place must plan the cell")
				expect.Equal(t, plan.Targets, tt.want, "Place must apply the moves before the limit")
				expect.True(t, plan.Incomplete, "the plan must report that the pass ended on its limit")
			})
		}

		t.Run(
			"ends on MaxEvaluations after one evaluation for each examined shard of a sample of 1",
			func(t *testing.T) {
				t.Parallel()
				cfg := defaults()
				cfg.Search.Sample = 1
				cfg.Search.MaxEvaluations = 2
				cfg.Churn.PerNode = 10
				cfg.Churn.MaxShardFraction = 1
				p := newPlanner(t, cfg, nil, tiered{goal: everyMove, weight: 1})
				plan, err := p.Place(t.Context(), cell(n0, n0, n0))
				assert.NoError(t, err, "Place must plan the cell")
				expect.Length(t, plan.Targets, 2, "Place must move the two shards that it examined")
				expect.True(t, plan.Incomplete, "the plan must report that the pass ended on its limit")
			},
		)

		t.Run("moves a shard to a random node of its sample", func(t *testing.T) {
			t.Parallel()
			cfg := defaults()
			cfg.Search.Sample = 1
			p := newPlanner(t, cfg, nil, tiered{goal: everyMove, weight: 1})
			seeds := rand.Seed(32)
			targets := make([]shard.NodeID, 0, seeds)
			for seed := range seeds {
				in := cell(n0, n0)
				in.Rand = pcg.New(seed + 1)
				plan, err := p.Place(t.Context(), in)
				assert.NoError(t, err, "Place must plan the cell")
				assert.Length(t, plan.Targets[ids[0]].Replicas, 1, "Place must move the first shard")
				targets = append(targets, plan.Targets[ids[0]].Replicas[0].Node)
			}
			slices.Sort(targets)
			assert.InRange(t, len(slices.Compact(targets)), 2, 3, "the passes of 32 seeds must sample different nodes")
		})

		t.Run("moves a shard to the lowest node of a sample with equal deltas", func(t *testing.T) {
			t.Parallel()
			p := newPlanner(t, defaults(), nil, tiered{goal: everyMove, weight: 1})
			for seed := range rand.Seed(32) {
				in := cell(n0, n0)
				in.Rand = pcg.New(seed + 1)
				plan, err := p.Place(t.Context(), in)
				assert.NoError(t, err, "Place must plan the cell")
				assert.Equal(t, plan.Targets, map[shard.ID]placement.Target{ids[0]: onN1},
					"Place must move the first shard to the lowest node on every seed")
			}
		})

		t.Run("returns the cost of each tier after the pass", func(t *testing.T) {
			t.Parallel()
			p := newPlanner(t, defaults(), nil, tiered{goal: count, weight: 1},
				tiered{goal: goal.Stickiness{}, tier: 1, weight: 1})
			plan, err := p.Place(t.Context(), cell(imbalanced...))
			assert.NoError(t, err, "Place must plan the cell")
			assert.Equal(t, plan.Costs, []float64{0.1875, 1},
				"Place must return the variance of the counts and the one moved replica")
		})

		measures := []struct {
			name  string
			hosts []shard.NodeID
			want  int32
		}{
			{name: "measures the costs once in a pass without a move", hosts: []shard.NodeID{n0, n1, n2, n3}, want: 1},
			{name: "measures the costs again after a move", hosts: imbalanced, want: 2},
		}
		for _, tt := range measures {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				g := counted{calls: new(atomic.Int32)}
				p := newPlanner(t, defaults(), nil, tiered{goal: count, weight: 1}, tiered{goal: g, tier: 1, weight: 1})
				_, err := p.Place(t.Context(), cell(tt.hosts...))
				assert.NoError(t, err, "Place must plan the cell")
				assert.Equal(t, g.calls.Load(), tt.want, "Place must call Cost once for each measure of the costs")
			})
		}

		t.Run("returns one cost for the goals of one tier", func(t *testing.T) {
			t.Parallel()
			p := newPlanner(
				t,
				defaults(),
				nil,
				tiered{goal: count, weight: 1},
				tiered{goal: goal.Stickiness{}, weight: 1},
			)
			plan, err := p.Place(t.Context(), cell(imbalanced...))
			assert.NoError(t, err, "Place must plan the cell")
			assert.Length(t, plan.Costs, 1, "Place must return one cost per tier")
		})

		t.Run("returns the first rejection of each constraint as the reason of a deficit", func(t *testing.T) {
			t.Parallel()
			rejections := map[shard.NodeID]error{}
			for _, n := range []shard.NodeID{n0, n1, n2, n3} {
				rejections[n] = errors.New(string(n))
			}
			in := cell("")
			in.Emergency = true
			plan, err := newPlanner(t, defaults(), []placement.Constraint{rejecting{errs: rejections}}).
				Place(t.Context(), in)
			assert.NoError(t, err, "Place must plan the cell")
			expect.That(t, plan.Targets[ids[0]].Reason).
				ErrorIs(placement.ErrNoNode, "the reason must wrap ErrNoNode").
				ErrorIs(rejections[n0], "the reason must wrap the rejection of the first node").
				ErrorIsNot(rejections[n3], "the reason must not wrap a later rejection of the same constraint")
		})

		t.Run("returns the reason of the first replica without a feasible node", func(t *testing.T) {
			t.Parallel()
			in := cell("")
			paired()(in)
			in.Emergency = true
			c := roles{errs: map[shard.Role]error{shard.RolePrimary: errPrimary, shard.RoleSecondary: errSecondary}}
			plan, err := newPlanner(t, defaults(), []placement.Constraint{c}).Place(t.Context(), in)
			assert.NoError(t, err, "Place must plan the cell")
			target := plan.Targets[ids[0]]
			expect.Equal(t, target.Deficit, 2, "the target must report both replicas as a deficit")
			expect.That(t, target.Reason).
				ErrorIs(errPrimary, "the reason must wrap the rejection of the primary").
				ErrorIsNot(errSecondary, "the reason must not wrap the rejection of the secondary")
		})

		t.Run("does not move a replica of a shard that still needs repair", func(t *testing.T) {
			t.Parallel()
			in := cell("", "")
			paired([]shard.NodeID{n0, d9}, []shard.NodeID{n0, n1})(in)
			c := roles{errs: map[shard.Role]error{shard.RoleSecondary: errSecondary}}
			plan, err := newPlanner(t, defaults(), []placement.Constraint{c}, tiered{goal: count, weight: 1}).
				Place(t.Context(), in)
			assert.NoError(t, err, "Place must plan the cell")
			expect.Equal(t, plan.Targets[ids[0]].Replicas, []shard.Replica{{Node: n0, Role: shard.RolePrimary}},
				"the shard with a kept replica on d9 must keep its primary on n0")
			expect.Equal(t, plan.Targets[ids[1]].Replicas, []shard.Replica{
				{Node: n2, Role: shard.RolePrimary},
				{Node: n1, Role: shard.RoleSecondary},
			}, "the improvement must move the primary of the other shard")
		})

		t.Run("returns ErrCost before it checks a move for a cost that is not finite", func(t *testing.T) {
			t.Parallel()
			in := cell(n0, "")
			clock := fake.New(origin)
			in.Clock = clock
			p := newPlanner(t, defaults(), []placement.Constraint{ticking{clock: clock}},
				tiered{goal: faulty{start: math.NaN()}, weight: 1})
			_, err := p.Place(t.Context(), in)
			assert.ErrorIs(t, err, placement.ErrCost, "Place must refuse a cost that is not finite")
			assert.Equal(t, clock.Time(), origin, "Place must not check a move before it measures the costs")
		})

		t.Run("returns no target for a shard that the pass moves away and back", func(t *testing.T) {
			t.Parallel()
			cfg := defaults()
			cfg.Churn.PerShard = 2
			p := newPlanner(
				t,
				cfg,
				nil,
				tiered{goal: first{}, weight: 1},
				tiered{goal: goal.Stickiness{}, tier: 1, weight: 1},
			)
			plan, err := p.Place(t.Context(), cell(n0, n0))
			assert.NoError(t, err, "Place must plan the cell")
			expect.Empty(t, plan.Targets, "the plan must not list a shard that ends where it started")
		})

		t.Run("ends on its timeout", func(t *testing.T) {
			t.Parallel()
			in := cell(imbalanced...)
			clock := fake.New(origin)
			in.Clock = clock
			p := newPlanner(
				t,
				defaults(),
				[]placement.Constraint{ticking{clock: clock}},
				tiered{goal: count, weight: 1},
			)
			plan, err := p.Place(t.Context(), in)
			assert.NoError(t, err, "Place must plan the cell")
			expect.Empty(t, plan.Targets, "Place must examine no shard after its timeout")
			expect.True(t, plan.Incomplete, "the plan must report that the pass ended on its timeout")
		})

		cancels := []struct {
			name        string
			hosts       []shard.NodeID
			parallelism int
			emergency   bool
		}{
			{name: "returns the cause of a context that ends during the pass", hosts: imbalanced, parallelism: 1},
			{
				name:        "returns the cause of a context that ends while goroutines evaluate",
				hosts:       []shard.NodeID{""},
				parallelism: 2,
			},
			{
				name:        "returns the cause of a context that ends during the repair",
				hosts:       []shard.NodeID{"", ""},
				parallelism: 1,
				emergency:   true,
			},
		}
		for _, tt := range cancels {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				in := cell(tt.hosts...)
				in.Parallelism = tt.parallelism
				in.Emergency = tt.emergency
				ctx, cancel := context.WithCancelCause(t.Context())
				defer cancel(nil)
				p := newPlanner(t, defaults(), []placement.Constraint{cancelling{cancel: cancel}},
					tiered{goal: count, weight: 1})
				_, err := p.Place(ctx, in)
				assert.ErrorIs(t, err, errStopped, "Place must return the cause of the context")
			})
		}

		faults := []struct {
			name  string
			hosts []shard.NodeID
			goals []tiered
		}{
			{
				name:  "returns ErrCost for a cost that is not finite",
				hosts: []shard.NodeID{n0},
				goals: []tiered{{goal: faulty{start: math.NaN()}, weight: 1}},
			},
			{
				name:  "returns ErrCost for a delta of an added replica that is not finite",
				hosts: []shard.NodeID{""},
				goals: []tiered{{goal: faulty{delta: math.Inf(1)}, weight: 1}},
			},
			{
				name:  "returns ErrCost for a delta of a relocated replica that is not finite",
				hosts: []shard.NodeID{g9},
				goals: []tiered{{goal: faulty{delta: math.Inf(1)}, weight: 1}},
			},
			{
				name:  "returns ErrCost for a delta of an improvement that is not finite",
				hosts: imbalanced,
				goals: []tiered{{goal: faulty{delta: math.NaN()}, weight: 1}},
			},
			{
				name:  "returns ErrCost for a delta of a higher tier that is not finite",
				hosts: imbalanced,
				goals: []tiered{{goal: count, weight: 1}, {goal: faulty{delta: math.NaN()}, tier: 1, weight: 1}},
			},
			{
				name:  "returns ErrCost for a cost after a move that is not finite",
				hosts: []shard.NodeID{""},
				goals: []tiered{{goal: faulty{moved: math.Inf(-1)}, weight: 1}},
			},
			{
				name:  "returns ErrCost for an infinite delta of a node that the pass does not choose",
				hosts: []shard.NodeID{""},
				goals: []tiered{{goal: poisoned{node: n2}, weight: 1}},
			},
		}
		for _, tt := range faults {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				_, err := newPlanner(t, defaults(), nil, tt.goals...).Place(t.Context(), cell(tt.hosts...))
				assert.ErrorIs(t, err, placement.ErrCost, "Place must refuse a cost that is not finite")
			})
		}

		t.Run("returns ErrMove for a constraint that changes the state", func(t *testing.T) {
			t.Parallel()
			applied := false
			p := newPlanner(
				t,
				defaults(),
				[]placement.Constraint{meddling{applied: &applied}},
				tiered{goal: count, weight: 1},
			)
			_, err := p.Place(t.Context(), cell(n0, n0, ""))
			assert.ErrorIs(t, err, placement.ErrMove, "Place must report the move that the state refuses")
		})

		t.Run("returns ErrMove for a constraint that changes the state during the improvement", func(t *testing.T) {
			t.Parallel()
			applied := false
			p := newPlanner(
				t,
				defaults(),
				[]placement.Constraint{meddling{applied: &applied}},
				tiered{goal: count, weight: 1},
			)
			_, err := p.Place(t.Context(), cell(imbalanced...))
			assert.ErrorIs(t, err, placement.ErrMove, "Place must report the move that the state refuses")
		})
	})
}

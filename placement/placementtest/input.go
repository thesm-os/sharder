// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest

import (
	"slices"
	"strconv"
	"time"

	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/rand/pcg"
	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// The names of the metrics, the capacities and the labels of the cells that Inputs
// generates. A test configures a constraint or a goal with them, so that the laws
// cover each branch of the strategy.
const (
	// CPU is the metric cpu, which fills the capacity of the same name.
	CPU = "cpu"
	// Bytes is the metric bytes, which fills the capacity Disk.
	Bytes = "bytes"
	// Disk is the capacity that the metric Bytes fills.
	Disk = "disk"
	// Zone is the key of a label of nodes, with the values a, b and c.
	Zone = "zone"
	// Tenant is the key of a label of shards, with the values t0 and t1.
	Tenant = "tenant"
	// Home is the key of a label of shards, with the values a and b of Zone.
	Home = "home"
)

// The bounds of the cells that Inputs generates.
const (
	// maxNodes is the most accepting nodes of a cell.
	maxNodes = 6
	// maxDraining is the most draining nodes of a cell.
	maxDraining = 2
	// maxShards is the most shards of a cell.
	maxShards = 12
	// maxCapacity is the largest capacity of a node, in quarters.
	maxCapacity = 80
	// maxLoad is the largest load of a shard on a metric, in quarters.
	maxLoad = 32
	// maxParallelism is the largest Parallelism of an input.
	maxParallelism = 4
	// quarter converts a count of quarters to a load or a capacity, so the sums of
	// loads are exact.
	quarter = 0.25
)

// The prefixes of the IDs of the nodes that Inputs generates.
const (
	// acceptingPrefix starts the ID of an accepting node.
	acceptingPrefix = "n"
	// drainingPrefix starts the ID of a draining node.
	drainingPrefix = "d"
	// gonePrefix starts the ID of a gone node.
	gonePrefix = "g"
	// idlePrefix starts the ID of a node that Input.Draining lists as false.
	idlePrefix = "x"
)

// The labels of the values that Inputs generates. A counterexample shows each value
// under its label.
const (
	labelModel       = "model"
	labelNodes       = "nodes"
	labelZone        = "zone"
	labelCPU         = "cpu capacity"
	labelDisk        = "disk capacity"
	labelOrder       = "order"
	labelDraining    = "draining"
	labelIdle        = "idle"
	labelShards      = "shards"
	labelTenant      = "tenant"
	labelHome        = "home"
	labelPlaced      = "placed"
	labelHosts       = "hosts"
	labelCount       = "count"
	labelLoad        = "load"
	labelBytes       = "bytes"
	labelTransition  = "transition"
	labelTo          = "to"
	labelFallback    = "fallback"
	labelFault       = "fault"
	labelRepairFrom  = "repair from"
	labelEmergency   = "emergency"
	labelParallelism = "parallelism"
	labelSeed        = "seed"
)

// gone lists the gone nodes of every generated cell. A replica of a generated shard
// can be on them.
var gone = []shard.NodeID{gonePrefix + "0", gonePrefix + "1"}

// origin is the time of the clock of every generated input.
var origin = time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC)

// Replicated is a replica state model with three replicas of each shard: one
// primary, which writes, and two secondaries. The caller must not modify it.
var Replicated = replication.Model{
	Name: "replicated",
	Roles: []replication.Role{
		{Name: shard.RolePrimary, Max: 1, Writer: true},
		{Name: shard.RoleSecondary, Max: 2},
	},
	Transitions: []replication.Transition{
		{From: shard.RolePrimary, To: shard.RoleOffline, Priority: 0},
		{From: shard.RoleSecondary, To: shard.RoleOffline, Priority: 0},
		{From: shard.RoleOffline, To: shard.RolePrimary, Priority: 1},
		{From: shard.RoleOffline, To: shard.RoleSecondary, Priority: 1},
	},
}

// Inputs returns a generator of the inputs of small cells that placement.NewState
// accepts. A cell has up to six accepting nodes, two draining nodes and twelve shards,
// and its model is replication.PrimaryOnly or Replicated.
//
//   - A node has the label Zone or none, and capacities of CPU and Disk or none.
//     Input.Nodes lists the nodes in a generated order.
//   - A shard has the labels Tenant and Home or none. The layout lists the shards in
//     a generated order, and the shard at position i has the ID whose last byte is i+1
//     in the order of the IDs.
//   - A replica of a placed shard is on an accepting, a draining or a gone node.
//   - A shard has a load of CPU and Bytes or none, a transition in flight of a replica
//     in the first role of the model or none, and a fault on the node of its first
//     replica or none.
//   - The clock is a fake clock that the laws do not advance. Rand is a PCG source
//     with a generated seed.
//
// Loads and capacities are multiples of a quarter, so their sums are exact.
func Inputs() prop.Generator[*placement.Input] {
	return prop.Composite(func(c *prop.Case) *placement.Input {
		model := c.Draw(prop.SampledFrom(replication.PrimaryOnly, Replicated), labelModel)
		nodes := drawNodes(c)
		draining := drawDraining(c)
		pool := make([]shard.NodeID, 0, len(nodes)+len(draining)+len(gone))
		for _, n := range nodes {
			pool = append(pool, n.ID)
		}
		for n, d := range draining {
			if d {
				pool = append(pool, n)
			}
		}
		pool = append(pool, gone...)
		// The map of draining nodes has no order. The pool is sorted by ID, so the hosts
		// of a shard depend only on the choices of the case.
		slices.Sort(pool)

		layout := drawLayout(c)
		in := &placement.Input{
			Clock:       fake.New(origin),
			Rand:        pcg.New(rand.Seed(c.Draw(prop.Integer[int64](1, 1<<62), labelSeed))),
			Transitions: make(map[shard.ID]placement.InFlight),
			Faults:      make(map[shard.ID][]shard.NodeID),
			Draining:    draining,
			ShardLoad:   make(map[shard.ID]shard.Load),
			Model:       model,
			Layout:      layout,
			Current:     shard.Assignment{Placements: make(map[shard.ID]shard.Placement)},
			Nodes:       nodes,
			Metrics:     []placement.Metric{{Name: CPU}, {Name: Bytes, Capacity: Disk}},
			Parallelism: c.Draw(prop.Integer(1, maxParallelism), labelParallelism),
			Emergency:   c.Draw(prop.Boolean(), labelEmergency),
		}
		ids := make([]shard.ID, len(layout.Shards))
		for i := range ids {
			ids[i] = shard.ID{ID128: id.ID128{15: byte(i + 1)}}
		}
		for _, sh := range ids {
			drawShard(c, in, sh, pool)
		}
		in.RepairFrom = c.Draw(prop.SampledFrom(append([]shard.ID{{}}, ids...)...), labelRepairFrom)
		return in
	})
}

// drawNodes generates the accepting nodes of a cell in a generated order.
func drawNodes(c *prop.Case) []shard.Node {
	count := c.Draw(prop.Integer(0, maxNodes), labelNodes)
	nodes := make([]shard.Node, count)
	order := make([]int, count)
	for i := range nodes {
		n := shard.Node{ID: shard.NodeID(acceptingPrefix + strconv.Itoa(i)), Capacity: map[string]float64{}}
		if zone := c.Draw(prop.SampledFrom("", "a", "b", "c"), labelZone); zone != "" {
			n.Labels = tag.Tags{{Key: Zone, Value: zone}}
		}
		if cpu := c.Draw(prop.Optional(prop.Integer(0, maxCapacity)), labelCPU); cpu != nil {
			n.Capacity[CPU] = float64(*cpu) * quarter
		}
		if disk := c.Draw(prop.Optional(prop.Integer(0, maxCapacity)), labelDisk); disk != nil {
			n.Capacity[Disk] = float64(*disk) * quarter
		}
		nodes[i] = n
		order[i] = i
	}
	ordered := make([]shard.Node, count)
	for i, k := range c.Draw(prop.Permutation(order...), labelOrder) {
		ordered[i] = nodes[k]
	}
	return ordered
}

// drawDraining generates the draining nodes of a cell, and a node that the map lists
// as not draining.
func drawDraining(c *prop.Case) map[shard.NodeID]bool {
	count := c.Draw(prop.Integer(0, maxDraining), labelDraining)
	draining := make(map[shard.NodeID]bool, count+1)
	for i := range count {
		draining[shard.NodeID(drainingPrefix+strconv.Itoa(i))] = true
	}
	if c.Draw(prop.Boolean(), labelIdle) {
		draining[idlePrefix+"0"] = false
	}
	return draining
}

// drawLayout generates the shards of a cell with their labels, in a generated order.
// The shards cover the position space, and the shard at position k starts at the byte
// k.
func drawLayout(c *prop.Case) partition.Layout {
	count := c.Draw(prop.Integer(0, maxShards), labelShards)
	shards := make([]partition.Shard, count)
	order := make([]int, count)
	for i := range shards {
		sh := partition.Shard{ID: shard.ID{ID128: id.ID128{15: byte(i + 1)}}}
		if tenant := c.Draw(prop.SampledFrom("", "t0", "t1"), labelTenant); tenant != "" {
			sh.Labels = append(sh.Labels, tag.Tag{Key: Tenant, Value: tenant})
		}
		if home := c.Draw(prop.SampledFrom("", "a", "b"), labelHome); home != "" {
			sh.Labels = append(sh.Labels, tag.Tag{Key: Home, Value: home})
		}
		shards[i] = sh
		order[i] = i
	}
	layout := partition.Layout{Shards: make([]partition.Shard, count)}
	for k, i := range c.Draw(prop.Permutation(order...), labelOrder) {
		sh := shards[i]
		if k > 0 {
			sh.Range.Start = []byte{byte(k)}
		}
		if k+1 < count {
			sh.Range.End = []byte{byte(k + 1)}
		}
		layout.Shards[k] = sh
	}
	return layout
}

// drawShard generates the placement, the load, the transition in flight and the fault
// of shard sh, and writes them into in. A replica of the shard is on a node of pool.
func drawShard(c *prop.Case, in *placement.Input, sh shard.ID, pool []shard.NodeID) {
	var replicas []shard.Replica
	if c.Draw(prop.Boolean(), labelPlaced) {
		hosts := c.Draw(prop.Permutation(pool...), labelHosts)
		replicas = []shard.Replica{}
		for _, role := range in.Model.Roles {
			count := c.Draw(prop.Integer(0, min(role.Max, len(hosts)-len(replicas))), labelCount)
			for range count {
				replicas = append(replicas, shard.Replica{Node: hosts[len(replicas)], Role: role.Name})
			}
		}
		in.Current.Placements[sh] = shard.Placement{Shard: sh, Replicas: replicas}
	}
	if load := c.Draw(prop.Optional(prop.Integer(0, maxLoad)), labelLoad); load != nil {
		l := shard.Load{CPU: float64(*load) * quarter}
		if bytes := c.Draw(prop.Optional(prop.Integer(0, maxLoad)), labelBytes); bytes != nil {
			l[Bytes] = float64(*bytes) * quarter
		}
		in.ShardLoad[sh] = l
	}
	if c.Draw(prop.Boolean(prop.Odds(1, 5)), labelTransition) {
		in.Transitions[sh] = placement.InFlight{
			To:       c.Draw(prop.SampledFrom(pool...), labelTo),
			Fallback: c.Draw(prop.SampledFrom(append([]shard.NodeID{""}, pool...)...), labelFallback),
			Role:     in.Model.Roles[0].Name,
		}
	}
	if len(replicas) > 0 && c.Draw(prop.Boolean(prop.Odds(1, 5)), labelFault) {
		in.Faults[sh] = []shard.NodeID{replicas[0].Node}
	}
}

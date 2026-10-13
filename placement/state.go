// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placement

import (
	"cmp"
	"fmt"
	"math"
	"slices"

	"go.thesmos.sh/core/idmap"
	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/shard"
)

// State is the working copy of the assignment of a cell that the planner changes, with
// indexes for constraints and goals. NewState builds it from an Input, and Apply
// applies one move at a time.
//
// A node is accepting when Input.Nodes lists it, and draining when Input.Draining
// lists it. Any other node is gone when it hosts a replica or is an endpoint of a
// transition in flight. A replica can be on a node of each kind, and a move adds a
// replica only to an accepting node.
//
// The zero State has no shards and no nodes, and Apply refuses every move.
//
// # Load
//
// The load of a shard counts once on each node where it counts. NodeLoad is the load
// of a node once the moves of the pass and the transitions in flight end: the load of
// each shard with a replica on the node, or with a transition in flight whose
// destination or fallback is the node. PeakLoad also counts each shard that had a
// replica on the node when the pass started, because a shard keeps its load on its
// source until its transition ends. A shard without an entry in Input.ShardLoad has
// the mean load of the shards with one. A node has a load of 0 on each metric that no
// shard on the node reports.
//
// # Order
//
// Shards, Nodes and OnNode return IDs in ascending order. Replicas returns the
// replicas of a shard in the order of Input.Current. A move that relocates a replica
// keeps its position, and a move that adds a replica appends it. ReplicasAt and
// InitialAt take the position of a shard in Shards, and read the shard without a
// lookup of its ID.
//
// # Concurrency
//
// Apply must not run concurrently with any other method. The other methods only
// read the state, so constraints and goals call them from many goroutines between the
// moves that the planner applies. A slice or a Load that a method returns is valid
// until the next Apply, and the caller must not modify it.
//
// # Allocation contract
//
// NewState allocates in proportion to the shards and the nodes of the cell, and gives
// the list of each node room for its shards alone. Apply allocates when it first
// changes the replicas of a shard, and when it counts a label of a shard on a node for
// the first time. It also allocates when it inserts a shard into a full list of a
// node. A list grows geometrically, as append grows a slice, so k inserts into one list
// allocate O(log k) times. The other methods do not allocate.
type State struct {
	// in is the input that NewState indexed.
	in *Input
	// index maps the ID of each shard of the layout to its position in ids.
	index *idmap.Map[shard.ID, int32]
	// nodeIndex maps the ID of each node of the state to its position in nodes.
	nodeIndex map[shard.NodeID]int32
	// labelled counts the replicas on each node of the shards with each label. A count
	// that falls to zero keeps its entry, so a later move does not insert it again.
	labelled map[labelledKey]int32
	// labelledNodes counts the nodes with a replica of a shard with each label. A count
	// that falls to zero keeps its entry.
	labelledNodes map[tag.Tag]int32
	// mean is the load of a shard without an entry in Input.ShardLoad.
	mean shard.Load
	// ids lists the IDs of the shards of the layout in ascending order.
	ids []shard.ID
	// shards contains the state of each shard, in the order of ids.
	shards []shardState
	// loads contains the load of each shard on each metric. The load of the shard at
	// position i of ids on the metric at position k of metrics is at
	// i*len(metrics)+k.
	loads []float64
	// nodes contains the state of each node. The accepting nodes come first, and the
	// draining nodes follow, each group in the order of their IDs. The gone nodes come
	// last, in the order in which indexReplicas finds them.
	nodes []nodeState
	// accepting lists the IDs of the accepting nodes in ascending order.
	accepting []shard.NodeID
	// metrics lists the metrics of the input, each with the name of its capacity.
	metrics []Metric
	// utilisation is the sum of the utilisation on each metric of the accepting nodes
	// with a positive capacity for the metric, in the order of metrics.
	utilisation []float64
	// capacious counts the accepting nodes with a positive capacity for each metric,
	// in the order of metrics.
	capacious []int
	// totals is the sum of the loads of the shards on each metric, in the order of
	// metrics.
	totals []float64
	// max is the Max of each role of the model, and zero for a role that the model
	// does not declare.
	max [shard.RoleCount]int
	// total counts the replicas on the accepting nodes in each role.
	total [shard.RoleCount]int
	// desired is the number of replicas of each shard: the sum of the Max of the roles
	// of the model.
	desired int
	// moved counts the moves that Apply applied.
	moved int
}

// shardState is the state of one shard of a State.
type shardState struct {
	// load is the load of the shard: its entry of Input.ShardLoad, or the mean load.
	load shard.Load
	// initial lists the replicas of the shard in Input.Current.
	initial []shard.Replica
	// replicas lists the replicas of the shard in the state. It shares the array of
	// initial until copied is true.
	replicas []shard.Replica
	// layout is the position of the shard in Input.Layout.Shards.
	layout int32
	// moved counts the moves that Apply applied to the replicas of the shard.
	moved int32
	// frozen reports that the shard has a transition in flight.
	frozen bool
	// copied reports that replicas has an array of its own.
	copied bool
}

// nodeState is the state of one node of a State.
type nodeState struct {
	// node is the description of an accepting node, and nil for any other node.
	node *shard.Node
	// load is the load of the node once the moves and the transitions end. It has an
	// entry for each metric.
	load shard.Load
	// peak is the load of the node while the moves run. It has an entry for each
	// metric.
	peak shard.Load
	// id is the ID of the node.
	id shard.NodeID
	// shards lists the IDs of the shards with a replica on the node in ascending order.
	shards []shard.ID
	// roles counts the replicas on the node in each role.
	roles [shard.RoleCount]int32
	// moved counts the moves that Apply applied to or from the node.
	moved int32
	// draining reports that the node drains.
	draining bool
}

// labelledKey identifies the replicas on one node of the shards with one label.
type labelledKey struct {
	// label is the label of the shards.
	label tag.Tag
	// node is the position of the node in State.nodes.
	node int32
}

// NewState builds the state of in before any move. The state refers to in, so the
// caller must not modify in while it uses the state. NewState sorts the shards and the
// nodes by ID, and reads each replica, load and transition of in once, so it runs in
// O(s log s + n log n) for s shards and n nodes.
//
// Error modes:
//   - an error that wraps ErrInput when in is nil.
//   - an error that wraps ErrInput and the error of replication.Model.Compile when the
//     model is not valid.
//   - an error that wraps ErrInput for a metric without a name, and for two metrics of
//     one name.
//   - an error that wraps ErrInput for a node without an ID, for two accepting nodes
//     of one ID, and for a node that is accepting and draining.
//   - an error that wraps ErrInput for a capacity of a metric that is negative or NaN.
//   - an error that wraps ErrInput for a shard of the layout without an ID, and for two
//     shards of one ID.
//   - an error that wraps ErrInput for a placement of a shard that the layout does not
//     list.
//   - an error that wraps ErrInput for a replica without a node, for a replica in a
//     role that the model does not declare, for a replica beyond the Max of its role,
//     and for two replicas of one shard on one node.
//   - an error that wraps ErrInput for a load of a metric that is negative or not
//     finite.
func NewState(in *Input) (*State, error) {
	if in == nil {
		return nil, fmt.Errorf("%w: the input is nil", ErrInput)
	}
	st := &State{in: in}
	if err := st.indexModel(); err != nil {
		return nil, err
	}
	if err := st.indexMetrics(); err != nil {
		return nil, err
	}
	if err := st.indexNodes(); err != nil {
		return nil, err
	}
	if err := st.indexShards(); err != nil {
		return nil, err
	}
	if err := st.indexLoads(); err != nil {
		return nil, err
	}
	st.indexReplicas()
	return st, nil
}

// Shards returns the IDs of the shards of the layout in ascending order.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) Shards() []shard.ID {
	return st.ids
}

// Shard returns the shard of the layout whose ID is id, with its range and its
// labels, and reports whether the layout lists it.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) Shard(id shard.ID) (partition.Shard, bool) {
	i, ok := st.position(id)
	if !ok {
		return partition.Shard{}, false
	}
	return st.in.Layout.Shards[st.shards[i].layout], true
}

// Replicas returns the replicas of shard id in the state. It returns nil for a shard
// without replicas, and for a shard that the layout does not list.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) Replicas(id shard.ID) []shard.Replica {
	i, ok := st.position(id)
	if !ok {
		return nil
	}
	return st.shards[i].replicas
}

// ReplicasAt returns the replicas in the state of the shard at position i of Shards. It
// does not look up an ID, so a scan of the shards in the order of Shards reads each
// shard without a lookup. It panics when i is not a position of Shards.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) ReplicasAt(i int) []shard.Replica {
	return st.shards[i].replicas
}

// Initial returns the replicas of shard id at the start of the pass, as Input.Current
// lists them. It returns nil for a shard without a placement, and for a shard that the
// layout does not list.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) Initial(id shard.ID) []shard.Replica {
	i, ok := st.position(id)
	if !ok {
		return nil
	}
	return st.shards[i].initial
}

// InitialAt returns the replicas at the start of the pass of the shard at position i of
// Shards. It does not look up an ID. It panics when i is not a position of Shards.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) InitialAt(i int) []shard.Replica {
	return st.shards[i].initial
}

// InFlight returns the transition in flight of shard id, and reports whether the
// shard has one. Apply refuses every move of such a shard.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) InFlight(id shard.ID) (InFlight, bool) {
	i, ok := st.position(id)
	if !ok || !st.shards[i].frozen {
		return InFlight{}, false
	}
	return st.in.Transitions[id], true
}

// ShardLoad returns the load of shard id: its entry of Input.ShardLoad, or the mean
// load of the shards with an entry. The mean load has an entry for each metric. It
// returns nil for a shard that the layout does not list.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) ShardLoad(id shard.ID) shard.Load {
	i, ok := st.position(id)
	if !ok {
		return nil
	}
	return st.shards[i].load
}

// Desired returns the number of replicas of each shard: the sum of the Max of the
// roles of the model.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) Desired() int {
	return st.desired
}

// Max returns the most replicas of a shard in role r: the Max of r in the model. It
// returns 0 for a role that the model does not declare.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) Max(r shard.Role) int {
	if !r.Valid() {
		return 0
	}
	return st.max[r]
}

// Nodes returns the IDs of the accepting nodes in ascending order.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) Nodes() []shard.NodeID {
	return st.accepting
}

// Node returns the description of node n, and reports whether n is accepting. The
// state has no description of a draining or a gone node.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) Node(n shard.NodeID) (shard.Node, bool) {
	x, ok := st.nodeIndex[n]
	if !ok || st.nodes[x].node == nil {
		return shard.Node{}, false
	}
	return *st.nodes[x].node, true
}

// Draining reports whether node n drains.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) Draining(n shard.NodeID) bool {
	x, ok := st.nodeIndex[n]
	return ok && st.nodes[x].draining
}

// OnNode returns the IDs of the shards with a replica on node n, in ascending order.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) OnNode(n shard.NodeID) []shard.ID {
	x, ok := st.nodeIndex[n]
	if !ok {
		return nil
	}
	return st.nodes[x].shards
}

// RoleCount returns the number of replicas in role r on node n.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) RoleCount(n shard.NodeID, r shard.Role) int {
	x, ok := st.nodeIndex[n]
	if !ok || !r.Valid() {
		return 0
	}
	return int(st.nodes[x].roles[r])
}

// RoleTotal returns the number of replicas in role r on the accepting nodes.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) RoleTotal(r shard.Role) int {
	if !r.Valid() {
		return 0
	}
	return st.total[r]
}

// NodeLoad returns the load of node n once the moves of the pass and the transitions
// in flight end. It has an entry for each metric. It returns nil for a node that the
// state does not know.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) NodeLoad(n shard.NodeID) shard.Load {
	x, ok := st.nodeIndex[n]
	if !ok {
		return nil
	}
	return st.nodes[x].load
}

// PeakLoad returns the load of node n while the moves of the pass run: NodeLoad(n) and
// the load of each shard that had a replica on n when the pass started. It has an
// entry for each metric. It returns nil for a node that the state does not know.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) PeakLoad(n shard.NodeID) shard.Load {
	x, ok := st.nodeIndex[n]
	if !ok {
		return nil
	}
	return st.nodes[x].peak
}

// PeakCounts reports whether PeakLoad(n) counts the load of shard id. It does when the
// shard had a replica on n when the pass started, when the shard has a replica on n,
// and when a transition of the shard in flight has n as its destination or its
// fallback.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) PeakCounts(n shard.NodeID, id shard.ID) bool {
	i, ok := st.position(id)
	if !ok || n == "" {
		return false
	}
	s := &st.shards[i]
	on := func(r shard.Replica) bool { return r.Node == n }
	if slices.ContainsFunc(s.initial, on) || slices.ContainsFunc(s.replicas, on) {
		return true
	}
	//dokimi:mutate-skip sbr-delete: a shard without a transition reads the zero InFlight, whose empty nodes differ from the non-empty n
	if !s.frozen {
		return false
	}
	f := st.in.Transitions[id]
	return f.To == n || f.Fallback == n
}

// Metrics returns the metrics of the input in their order. The Capacity of each
// metric is the name of the capacity that it fills, and is never empty.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) Metrics() []Metric {
	return st.metrics
}

// Capacity returns the capacity of node n for metric, and reports whether n is
// accepting and declares the capacity that metric fills. It returns 0 when it reports
// false.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) Capacity(n shard.NodeID, metric string) (float64, bool) {
	x, ok := st.nodeIndex[n]
	k := slices.IndexFunc(st.metrics, func(m Metric) bool { return m.Name == metric })
	if !ok || k < 0 || st.nodes[x].node == nil {
		return 0, false
	}
	c, ok := st.nodes[x].node.Capacity[st.metrics[k].Capacity]
	return c, ok
}

// Utilisation returns NodeLoad(n) on metric divided by the capacity of n for metric,
// and reports whether n is accepting with a positive capacity for metric.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) Utilisation(n shard.NodeID, metric string) (float64, bool) {
	c, ok := st.Capacity(n, metric)
	//dokimi:mutate-skip lcr-right: Capacity returns 0 with false, so c <= 0 is also true when ok is false
	if !ok || c <= 0 {
		return 0, false
	}
	return st.nodes[st.nodeIndex[n]].load[metric] / c, true
}

// UtilisationSum returns the sum of Utilisation on metric over the accepting nodes
// with a positive capacity for metric, and the number of those nodes. Apply updates
// the sum with the change of each move, so it can differ from a new sum of the same
// utilisations in the last bits.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) UtilisationSum(metric string) (float64, int) {
	k := slices.IndexFunc(st.metrics, func(m Metric) bool { return m.Name == metric })
	if k < 0 {
		return 0, 0
	}
	return st.utilisation[k], st.capacious[k]
}

// TotalLoad returns the sum of the loads of the shards of the layout on metric. Each
// shard counts once, with its entry of Input.ShardLoad or the mean load. It returns 0
// for a metric that the input does not list.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) TotalLoad(metric string) float64 {
	k := slices.IndexFunc(st.metrics, func(m Metric) bool { return m.Name == metric })
	if k < 0 {
		return 0
	}
	return st.totals[k]
}

// Labelled returns the number of replicas on node n of the shards with the label t.
// A shard has a label when its labels contain t as the first tag of the key of t.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) Labelled(t tag.Tag, n shard.NodeID) int {
	x, ok := st.nodeIndex[n]
	if !ok {
		return 0
	}
	return int(st.labelled[labelledKey{label: t, node: x}])
}

// LabelledNodes returns the number of nodes with at least one replica of a shard with
// the label t.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) LabelledNodes(t tag.Tag) int {
	return int(st.labelledNodes[t])
}

// Moved returns the number of moves that Apply applied.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) Moved() int {
	return st.moved
}

// MovedNode returns the number of moves that Apply applied with n as their From or
// their To.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) MovedNode(n shard.NodeID) int {
	x, ok := st.nodeIndex[n]
	if !ok {
		return 0
	}
	return int(st.nodes[x].moved)
}

// MovedShard returns the number of moves that Apply applied to the replicas of shard
// id.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) MovedShard(id shard.ID) int {
	i, ok := st.position(id)
	if !ok {
		return 0
	}
	return int(st.shards[i].moved)
}

// Apply applies m to the state. A move with a From relocates or removes the replica
// of the shard on From in the role of m, and a move without a From adds a replica in
// the role of m. Apply counts m in Moved, in MovedShard of the shard, and in MovedNode
// of From and of To.
//
// Error modes:
//   - an error that wraps ErrMove when the layout does not list the shard of m.
//   - an error that wraps ErrMove when the shard has a transition in flight.
//   - an error that wraps ErrMove when From equals To, which includes a move without
//     both.
//   - an error that wraps ErrMove when the model does not declare the role of m.
//   - an error that wraps ErrMove when the shard has no replica on From in the role of
//     m.
//   - an error that wraps ErrMove when To is not accepting, or when the shard has a
//     replica on To.
//   - an error that wraps ErrMove when m adds a replica to a role in which the shard
//     has Max replicas.
//
// # Allocation contract
//
// Apply allocates when it first changes the replicas of a shard, when it inserts the
// shard into a full list of a node, and when it counts a label of the shard on a node
// for the first time.
func (st *State) Apply(m Move) error {
	i, ok := st.position(m.Shard)
	if !ok {
		return fmt.Errorf("%w: the layout does not list shard %v", ErrMove, m.Shard)
	}
	s := &st.shards[i]
	if s.frozen {
		return fmt.Errorf("%w: shard %v has a transition in flight", ErrMove, m.Shard)
	}
	if m.From == m.To {
		return fmt.Errorf("%w: the move of shard %v has the From %q and the same To", ErrMove, m.Shard, m.From)
	}
	if !m.Role.Valid() {
		return fmt.Errorf("%w: the role %v is not valid", ErrMove, m.Role)
	}
	from := -1
	if m.From != "" {
		from = slices.Index(s.replicas, shard.Replica{Node: m.From, Role: m.Role})
		if from < 0 {
			return fmt.Errorf("%w: shard %v has no replica on node %q in the role %v", ErrMove, m.Shard, m.From, m.Role)
		}
	}
	to := int32(-1)
	if m.To != "" {
		x, ok := st.nodeIndex[m.To]
		if !ok || st.nodes[x].node == nil {
			return fmt.Errorf("%w: node %q does not accept replicas", ErrMove, m.To)
		}
		if slices.ContainsFunc(s.replicas, func(r shard.Replica) bool { return r.Node == m.To }) {
			return fmt.Errorf("%w: shard %v already has a replica on node %q", ErrMove, m.Shard, m.To)
		}
		in := 0
		for _, r := range s.replicas {
			if r.Role == m.Role {
				in++
			}
		}
		if from < 0 && in >= st.max[m.Role] {
			return fmt.Errorf("%w: shard %v has %d replicas in the role %v, and the model allows %d",
				ErrMove, m.Shard, in, m.Role, st.max[m.Role])
		}
		to = x
	}
	st.apply(i, from, to, m)
	s.moved++
	st.moved++
	if from >= 0 {
		st.nodes[st.nodeIndex[m.From]].moved++
	}
	if to >= 0 {
		st.nodes[to].moved++
	}
	return nil
}

// position returns the position in ids of the shard id, and reports whether the layout
// lists it. The zero State has no index, so position reports false for every shard.
//
// # Allocation contract
//
// Zero alloc.
func (st *State) position(id shard.ID) (int32, bool) {
	if st.index == nil {
		return 0, false
	}
	return st.index.Get(id)
}

// indexModel records the Max of each role of the model, and the number of replicas of
// each shard.
//
// Error modes:
//   - an error that wraps ErrInput and the error of replication.Model.Compile when the
//     model is not valid.
func (st *State) indexModel() error {
	if _, err := st.in.Model.Compile(); err != nil {
		return fmt.Errorf("%w: %w", ErrInput, err)
	}
	for _, r := range st.in.Model.Roles {
		st.max[r.Name] = r.Max
		st.desired += r.Max
	}
	return nil
}

// indexMetrics records the metrics of the input with the names of their capacities.
//
// Error modes:
//   - an error that wraps ErrInput for a metric without a name, and for two metrics of
//     one name.
func (st *State) indexMetrics() error {
	st.metrics = make([]Metric, len(st.in.Metrics))
	for i, m := range st.in.Metrics {
		if m.Name == "" {
			return fmt.Errorf("%w: metric %d has no name", ErrInput, i)
		}
		if slices.ContainsFunc(st.in.Metrics[:i], func(o Metric) bool { return o.Name == m.Name }) {
			return fmt.Errorf("%w: two metrics have the name %q", ErrInput, m.Name)
		}
		if m.Capacity == "" {
			m.Capacity = m.Name
		}
		st.metrics[i] = m
	}
	st.utilisation = make([]float64, len(st.metrics))
	st.capacious = make([]int, len(st.metrics))
	return nil
}

// indexNodes adds the accepting nodes in the order of their IDs, and then the draining
// nodes in the order of their IDs.
//
// Error modes:
//   - an error that wraps ErrInput for a node without an ID, for two accepting nodes of
//     one ID, and for a node that is accepting and draining.
//   - an error that wraps ErrInput for a capacity of a metric that is negative or NaN.
func (st *State) indexNodes() error {
	nodes := st.in.Nodes
	order := make([]int, len(nodes))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return cmp.Compare(nodes[a].ID, nodes[b].ID) })

	draining := make([]shard.NodeID, 0, len(st.in.Draining))
	for id, d := range st.in.Draining {
		if d {
			draining = append(draining, id)
		}
	}
	slices.Sort(draining)

	st.nodes = make([]nodeState, 0, len(nodes)+len(draining))
	st.nodeIndex = make(map[shard.NodeID]int32, len(nodes)+len(draining))
	st.accepting = make([]shard.NodeID, 0, len(nodes))
	for _, i := range order {
		n := &nodes[i]
		if n.ID == "" {
			return fmt.Errorf("%w: node %d has no ID", ErrInput, i)
		}
		if _, ok := st.nodeIndex[n.ID]; ok {
			return fmt.Errorf("%w: two nodes have the ID %q", ErrInput, n.ID)
		}
		for _, m := range st.metrics {
			if c := n.Capacity[m.Capacity]; math.IsNaN(c) || c < 0 {
				return fmt.Errorf("%w: node %q has the capacity %v of %q", ErrInput, n.ID, c, m.Capacity)
			}
		}
		st.nodeIndex[n.ID] = int32(len(st.nodes))
		st.nodes = append(st.nodes, nodeState{node: n, id: n.ID})
		st.accepting = append(st.accepting, n.ID)
	}
	for _, id := range draining {
		if id == "" {
			return fmt.Errorf("%w: a draining node has no ID", ErrInput)
		}
		if _, ok := st.nodeIndex[id]; ok {
			return fmt.Errorf("%w: node %q is accepting and draining", ErrInput, id)
		}
		st.nodeIndex[id] = int32(len(st.nodes))
		st.nodes = append(st.nodes, nodeState{id: id, draining: true})
	}
	return nil
}

// indexShards orders the shards of the layout by ID, and records the replicas of each
// shard in the current assignment.
//
// Error modes:
//   - an error that wraps ErrInput for a shard without an ID, and for two shards of one
//     ID.
//   - the errors of indexCurrent.
func (st *State) indexShards() error {
	layout := st.in.Layout.Shards
	type entry struct {
		id  shard.ID
		pos int32
	}
	entries := make([]entry, len(layout))
	for i := range layout {
		entries[i] = entry{id: layout[i].ID, pos: int32(i)}
	}
	slices.SortFunc(entries, func(a, b entry) int { return a.id.Compare(b.id) })

	st.ids = make([]shard.ID, len(entries))
	st.shards = make([]shardState, len(entries))
	st.index = idmap.NewTyped128[shard.ID, int32](len(entries))
	for i, e := range entries {
		if e.id.IsZero() {
			return fmt.Errorf("%w: shard %d of the layout has no ID", ErrInput, e.pos)
		}
		if i > 0 && e.id == entries[i-1].id {
			return fmt.Errorf("%w: two shards of the layout have the ID %v", ErrInput, e.id)
		}
		st.ids[i] = e.id
		st.shards[i].layout = e.pos
		st.index.Set(e.id, int32(i))
	}
	return st.indexCurrent()
}

// indexCurrent records the replicas of each shard in the current assignment. It reads
// the shards in the order of their IDs, so it reports the problem of the lowest ID.
//
// Error modes:
//   - an error that wraps ErrInput for a placement of a shard that the layout does not
//     list.
//   - the errors of checkReplicas.
func (st *State) indexCurrent() error {
	placements := st.in.Current.Placements
	found := 0
	for i, id := range st.ids {
		p, ok := placements[id]
		if !ok {
			continue
		}
		found++
		if err := st.checkReplicas(id, p.Replicas); err != nil {
			return err
		}
		st.shards[i].initial = p.Replicas
		st.shards[i].replicas = p.Replicas
	}
	if found == len(placements) {
		return nil
	}
	var extra shard.ID
	for id := range placements {
		//dokimi:mutate-skip ror-boundary: the keys of a map differ, so two IDs never compare equal
		if _, ok := st.index.Get(id); !ok && (extra.IsZero() || id.Compare(extra) < 0) {
			extra = id
		}
	}
	return fmt.Errorf("%w: the current assignment places shard %v, which the layout does not list", ErrInput, extra)
}

// checkReplicas checks the replicas of shard id in the current assignment against the
// model.
//
// Error modes:
//   - an error that wraps ErrInput for a replica without a node, for a replica in a
//     role that is not valid, for a replica beyond the Max of its role, and for two
//     replicas on one node. A replica in a role that the model does not declare is
//     beyond the Max of 0.
func (st *State) checkReplicas(id shard.ID, replicas []shard.Replica) error {
	var count [shard.RoleCount]int
	for i, r := range replicas {
		if r.Node == "" {
			return fmt.Errorf("%w: a replica of shard %v has no node", ErrInput, id)
		}
		if !r.Role.Valid() {
			return fmt.Errorf("%w: a replica of shard %v has the role %v, which is not valid", ErrInput, id, r.Role)
		}
		if slices.ContainsFunc(replicas[:i], func(o shard.Replica) bool { return o.Node == r.Node }) {
			return fmt.Errorf("%w: shard %v has two replicas on node %q", ErrInput, id, r.Node)
		}
		count[r.Role]++
		if count[r.Role] > st.max[r.Role] {
			return fmt.Errorf("%w: shard %v has %d replicas in the role %v, and the model allows %d",
				ErrInput, id, count[r.Role], r.Role, st.max[r.Role])
		}
	}
	return nil
}

// indexLoads records the load of each shard on each metric, and the total of each
// metric. A shard without an entry in Input.ShardLoad gets the mean load of the shards
// with one. The totals add the loads in the order of the IDs of the shards.
//
// Error modes:
//   - an error that wraps ErrInput for a load of a metric that is negative or not
//     finite.
func (st *State) indexLoads() error {
	width := len(st.metrics)
	st.loads = make([]float64, len(st.ids)*width)
	reported := make([]bool, len(st.ids))
	sums := make([]float64, width)
	count := 0
	for i, id := range st.ids {
		l, ok := st.in.ShardLoad[id]
		if !ok {
			continue
		}
		reported[i] = true
		count++
		st.shards[i].load = l
		for k, m := range st.metrics {
			v := l[m.Name]
			if math.IsNaN(v) || v < 0 || math.IsInf(v, 1) {
				return fmt.Errorf("%w: shard %v has the load %v of %q", ErrInput, id, v, m.Name)
			}
			st.loads[i*width+k] = v
			sums[k] += v
		}
	}
	st.mean = make(shard.Load, width)
	for k, m := range st.metrics {
		if count > 0 {
			sums[k] /= float64(count)
		}
		st.mean[m.Name] = sums[k]
	}
	st.totals = make([]float64, width)
	for i := range st.ids {
		if !reported[i] {
			st.shards[i].load = st.mean
			copy(st.loads[i*width:(i+1)*width], sums)
		}
		for k, v := range st.loads[i*width : (i+1)*width] {
			st.totals[k] += v
		}
	}
	return nil
}

// indexReplicas records the shards with a transition in flight, the gone nodes, and
// for each node its shards, its counts of roles, its loads and its counts of labels.
// It reads the shards in the order of their IDs, so the lists of the nodes are in
// ascending order.
func (st *State) indexReplicas() {
	for id := range st.in.Transitions {
		if i, ok := st.index.Get(id); ok {
			st.shards[i].frozen = true
		}
	}

	counts := make([]int, 0, len(st.nodes))
	replicas := 0
	for i := range st.shards {
		replicas += len(st.shards[i].replicas)
		for _, r := range st.shards[i].replicas {
			x := st.touch(r.Node)
			for int(x) >= len(counts) {
				counts = append(counts, 0)
			}
			counts[x]++
		}
		if st.shards[i].frozen {
			f := st.in.Transitions[st.ids[i]]
			for _, n := range [...]shard.NodeID{f.To, f.Fallback} {
				if n != "" {
					st.touch(n)
				}
			}
		}
	}
	//dokimi:mutate-skip ror-boundary: one more count of 0 after the last node does not change a list
	for len(counts) < len(st.nodes) {
		counts = append(counts, 0)
	}

	lists := make([]shard.ID, replicas)
	for x := range st.nodes {
		st.nodes[x].shards = lists[:0:counts[x]]
		lists = lists[counts[x]:]
	}

	width := len(st.metrics)
	sums := make([]float64, len(st.nodes)*width)
	for i := range st.shards {
		st.indexShard(int32(i), sums)
	}

	for x := range st.nodes {
		n := &st.nodes[x]
		n.load = make(shard.Load, width)
		n.peak = make(shard.Load, width)
		for k, m := range st.metrics {
			v := sums[x*width+k]
			n.load[m.Name] = v
			n.peak[m.Name] = v
			if n.node == nil {
				continue
			}
			if c := n.node.Capacity[m.Capacity]; c > 0 {
				st.utilisation[k] += v / c
				st.capacious[k]++
			}
		}
		if n.node == nil {
			continue
		}
		for r, c := range n.roles {
			st.total[r] += int(c)
		}
	}
}

// indexShard records the shard at position i of ids in the lists, the counts of roles
// and the counts of labels of the nodes of its replicas. It adds its load to sums on
// each node where it counts, once per node. sums contains the load of the node at
// position x of nodes on the metric at position k of metrics at x*len(metrics)+k.
func (st *State) indexShard(i int32, sums []float64) {
	s := &st.shards[i]
	width := len(st.metrics)
	load := st.loads[int(i)*width : int(i+1)*width]
	labels := st.in.Layout.Shards[s.layout].Labels
	add := func(x int32) {
		for k, v := range load {
			sums[int(x)*width+k] += v
		}
	}
	for _, r := range s.replicas {
		x := st.nodeIndex[r.Node]
		n := &st.nodes[x]
		n.shards = append(n.shards, st.ids[i])
		n.roles[r.Role]++
		st.label(labels, x, 1)
		add(x)
	}
	//dokimi:mutate-skip sbr-delete: a shard without a transition reads the zero InFlight, and the loop skips its empty nodes
	if !s.frozen {
		return
	}
	f := st.in.Transitions[st.ids[i]]
	for k, n := range [...]shard.NodeID{f.To, f.Fallback} {
		on := func(r shard.Replica) bool { return r.Node == n }
		if n == "" || (k == 1 && n == f.To) || slices.ContainsFunc(s.replicas, on) {
			continue
		}
		add(st.nodeIndex[n])
	}
}

// touch returns the position of node n in nodes, and adds n as a gone node when the
// state does not know it. n is not empty.
func (st *State) touch(n shard.NodeID) int32 {
	if x, ok := st.nodeIndex[n]; ok {
		return x
	}
	x := int32(len(st.nodes))
	st.nodeIndex[n] = x
	st.nodes = append(st.nodes, nodeState{id: n})
	return x
}

// label adds delta to the count of the replicas on the node at position x of nodes of
// the shards with each label of labels. A label whose key an earlier label of labels
// has does not count.
func (st *State) label(labels tag.Tags, x, delta int32) {
	for i, t := range labels {
		if slices.ContainsFunc(labels[:i], func(o tag.Tag) bool { return o.Key == t.Key }) {
			continue
		}
		if st.labelled == nil {
			st.labelled = make(map[labelledKey]int32)
			st.labelledNodes = make(map[tag.Tag]int32)
		}
		key := labelledKey{label: t, node: x}
		before := st.labelled[key]
		after := before + delta
		st.labelled[key] = after
		if before == 0 {
			st.labelledNodes[t]++
		} else if after == 0 {
			st.labelledNodes[t]--
		}
	}
}

// apply changes the replicas of the shard at position i of ids and the indexes of
// their nodes as the move m does. It neither checks m nor counts it. from is the
// position of the replica that m moves in the replicas of the shard, or -1 for a move
// that adds a replica. to is the position of To in nodes, or -1 for a move that
// removes a replica.
func (st *State) apply(i int32, from int, to int32, m Move) {
	s := &st.shards[i]
	if !s.copied {
		s.replicas = append(make([]shard.Replica, 0, max(st.desired, len(s.replicas))), s.replicas...)
		s.copied = true
	}
	initial := func(n shard.NodeID) bool {
		return slices.ContainsFunc(s.initial, func(r shard.Replica) bool { return r.Node == n })
	}
	if from >= 0 {
		st.shift(i, st.nodeIndex[m.From], m.Role, -1, !initial(m.From))
	}
	if to >= 0 {
		st.shift(i, to, m.Role, 1, !initial(m.To))
	}
	if from >= 0 && to >= 0 {
		s.replicas[from].Node = m.To
	} else if from >= 0 {
		s.replicas = slices.Delete(s.replicas, from, from+1)
	} else {
		s.replicas = append(s.replicas, shard.Replica{Node: m.To, Role: m.Role})
	}
}

// shift moves one replica of the shard at position i of ids in the role r onto the
// node at position x of nodes when sign is 1, and off it when sign is -1. It updates
// the list, the counts of roles, the loads, the utilisation and the counts of labels
// of the node. peak reports whether the peak load of the node changes too.
func (st *State) shift(i, x int32, r shard.Role, sign int32, peak bool) {
	n := &st.nodes[x]
	id := st.ids[i]
	pos, _ := slices.BinarySearchFunc(n.shards, id, shard.ID.Compare)
	//dokimi:mutate-skip ror-boundary: sign is 1 or -1, never 0
	if sign > 0 {
		n.shards = slices.Insert(n.shards, pos, id)
	} else {
		n.shards = slices.Delete(n.shards, pos, pos+1)
	}
	n.roles[r] += sign
	if n.node != nil {
		st.total[r] += int(sign)
	}

	width := len(st.metrics)
	for k, m := range st.metrics {
		v := float64(sign) * st.loads[int(i)*width+k]
		n.load[m.Name] += v
		if peak {
			n.peak[m.Name] += v
		}
		if n.node == nil {
			continue
		}
		if c := n.node.Capacity[m.Capacity]; c > 0 {
			st.utilisation[k] += v / c
		}
	}
	st.label(st.in.Layout.Shards[st.shards[i].layout].Labels, x, sign)
}

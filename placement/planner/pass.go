// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package planner

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/task"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// precision is the relative difference below which a pass treats two costs of a tier
// as equal. A cost is a sum of at most one term per node of a cell. The rounding error
// of such a sum over the 5,000 nodes of the largest cell is about 5,000 times the
// machine epsilon of float64, which is near 1e-12 of the cost. precision is a thousand
// times that, so a rounding error never counts as an improvement.
const precision = 1e-9

// pass is the state of one pass of a Planner.
type pass struct {
	// pl is the planner of the pass.
	pl *Planner
	// in is the input of the pass.
	in *placement.Input
	// st is the state that the pass changes.
	st *placement.State
	// deficits maps a shard that repair could not complete to its deficit.
	deficits map[shard.ID]*deficit
	// touched records the shards that the pass moved or left with a deficit.
	touched map[shard.ID]bool
	// placed counts the replicas that repair placed on each node.
	placed map[shard.NodeID]int
	// out counts the moves of the improvement out of each node.
	out map[shard.NodeID]int
	// improved records the shards that the improvement moved.
	improved map[shard.ID]bool
	// deadline is the time on the clock of the input at which the pass stops.
	deadline time.Time
	// costs is the cost of each tier. apply adds the deltas of each move.
	costs []float64
	// tolerances is the difference below which two costs of a tier are equal. It is
	// precision times the cost of the tier, and at least precision.
	tolerances []float64
	// scale is the reciprocal of the total load of the shards on each metric, or 0 for
	// a metric without load.
	scale []float64
	// candidates contains the candidate moves of the last step.
	candidates []placement.Move
	// results contains the result of each candidate of the last step.
	results []result
	// deltas contains the weighted delta of each tier of each candidate of the last
	// step. The delta of tier t of candidate i is at i*len(tiers)+t.
	deltas []float64
	// bestDeltas contains the weighted delta of each tier of the best move that the
	// running call of improveFrom has found.
	bestDeltas []float64
	// perm is a permutation of the positions of the accepting nodes. Each sample
	// shuffles a part of it.
	perm []int
	// maxShards is the most shards that the improvement may move.
	maxShards int
	// moves counts the moves that the pass applied.
	moves int
	// evaluations counts the candidate moves that the pass evaluated.
	evaluations int
	// sources counts the shards that the pass examined as the source of a move.
	sources int
	// repairNext is the repair cursor for the next pass.
	repairNext shard.ID
	// stopped reports that the pass stopped at its timeout or at a limit of its work
	// while work remained.
	stopped bool
}

// deficit is the part of the desired replicas of a shard that repair could not place.
type deficit struct {
	// reason is the error that explains why the first replica of the shard without a
	// feasible node has none.
	reason error
	// kept lists the nodes of the replicas that need repair and remain where they are.
	kept []shard.NodeID
}

// result is the evaluation of one candidate move.
type result struct {
	// rejection is the error of the first constraint that rejects the move, or nil for
	// a feasible move.
	rejection error
	// fault is the error of a goal that returned a delta that is not finite, or nil.
	fault error
	// constraint is the position of the constraint of rejection.
	constraint int
}

// newPass returns the pass of pl over in. st is the state of in.
func newPass(pl *Planner, in *placement.Input, st *placement.State) *pass {
	perm := make([]int, len(st.Nodes()))
	for i := range perm {
		perm[i] = i
	}
	scale := make([]float64, len(st.Metrics()))
	for k, m := range st.Metrics() {
		if total := st.TotalLoad(m.Name); total > 0 {
			scale[k] = 1 / total
		}
	}
	return &pass{
		pl:         pl,
		in:         in,
		st:         st,
		deadline:   in.Clock.Time().Add(time.Duration(pl.search.Timeout)),
		deficits:   map[shard.ID]*deficit{},
		touched:    map[shard.ID]bool{},
		placed:     map[shard.NodeID]int{},
		out:        map[shard.NodeID]int{},
		improved:   map[shard.ID]bool{},
		costs:      make([]float64, len(pl.tiers)),
		tolerances: make([]float64, len(pl.tiers)),
		bestDeltas: make([]float64, len(pl.tiers)),
		scale:      scale,
		perm:       perm,
		repairNext: in.RepairFrom,
		maxShards:  int(math.Ceil(pl.churn.MaxShardFraction * float64(len(st.Shards())))),
	}
}

// run runs the repair of the pass, and then the improvement. It skips the improvement
// in an emergency, and when the triggers do not report a node at risk.
//
// Error modes:
//   - the errors of measure, repair and improve.
func (p *pass) run(ctx context.Context) (placement.Plan, error) {
	if err := p.measure(); err != nil {
		return placement.Plan{}, err
	}
	if err := p.repair(ctx); err != nil {
		return placement.Plan{}, err
	}
	if !p.in.Emergency && p.triggered() {
		if err := p.improve(ctx); err != nil {
			return placement.Plan{}, err
		}
	}
	return p.plan()
}

// measure sets the cost and the tolerance of each tier from the costs of its goals.
//
// Error modes:
//   - an error that wraps placement.ErrCost when a goal returns a cost that is not
//     finite.
func (p *pass) measure() error {
	for t, tr := range p.pl.tiers {
		cost := 0.0
		for g, goal := range tr.goals {
			c := goal.Cost(p.st)
			if math.IsNaN(c) || math.IsInf(c, 0) {
				return fmt.Errorf("%w: goal %s returned the cost %v", placement.ErrCost, goal.Name(), c)
			}
			cost += tr.weights[g] * c
		}
		p.costs[t] = cost
		p.tolerances[t] = precision * max(1, math.Abs(cost))
	}
	return nil
}

// repair examines the shards that need repair in the order of their IDs, from the
// first ID at or above Input.RepairFrom, and continues from the lowest ID after the
// highest. It examines each shard at most once, and moves the repair cursor past each
// shard that it examines.
//
// Error modes:
//   - the errors of check and repairShard.
func (p *pass) repair(ctx context.Context) error {
	ids := p.st.Shards()
	start, _ := slices.BinarySearchFunc(ids, p.in.RepairFrom, shard.ID.Compare)
	for k := range ids {
		i := (start + k) % len(ids)
		if !p.needsRepair(ids[i], p.st.ReplicasAt(i)) {
			continue
		}
		if err := p.check(ctx); err != nil || p.stopped {
			return err
		}
		p.sources++
		p.repairNext = ids[(i+1)%len(ids)]
		if err := p.repairShard(ctx, ids[i]); err != nil {
			return err
		}
	}
	return nil
}

// needsRepair reports whether shard id of the layout, with replicas in the state, needs
// repair. A shard needs repair when it has fewer replicas than the model gives each
// shard, or a replica that bad reports. A shard with a transition in flight does not
// need repair, because the pass does not plan a move for it. Input.Transitions lists
// such a shard, and a lookup there does not read the index of every shard of the state.
func (p *pass) needsRepair(id shard.ID, replicas []shard.Replica) bool {
	if _, ok := p.in.Transitions[id]; ok {
		return false
	}
	return len(replicas) < p.st.Desired() ||
		slices.ContainsFunc(replicas, func(r shard.Replica) bool { return p.bad(id, r) })
}

// bad reports whether replica r of shard id needs repair. A replica on a gone node and
// a replica with a fault need repair. Outside an emergency, a replica on a draining
// node and a replica that a constraint rejects need repair too.
func (p *pass) bad(id shard.ID, r shard.Replica) bool {
	_, accepting := p.st.Node(r.Node)
	draining := p.st.Draining(r.Node)
	if (!accepting && !draining) || slices.Contains(p.in.Faults[id], r.Node) {
		return true
	}
	if p.in.Emergency {
		return false
	}
	if draining {
		return true
	}
	keep := placement.Move{From: r.Node, To: r.Node, Shard: id, Role: r.Role}
	return slices.ContainsFunc(
		p.pl.constraints,
		func(c placement.Constraint) bool { return c.Check(p.st, keep) != nil },
	)
}

// repairShard relocates each replica of shard id that needs repair. It then adds the
// missing replicas of each role. A replica without a feasible node remains where it
// is, and the shard records a deficit.
//
// Error modes:
//   - the errors of place.
func (p *pass) repairShard(ctx context.Context, id shard.ID) error {
	d := &deficit{}
	for {
		replicas := p.st.Replicas(id)
		k := slices.IndexFunc(replicas, func(r shard.Replica) bool {
			return !slices.Contains(d.kept, r.Node) && p.bad(id, r)
		})
		if k < 0 {
			break
		}
		r := replicas[k]
		placed, err := p.place(ctx, id, r.Node, r.Role, d)
		if err != nil {
			return err
		}
		if !placed {
			d.kept = append(d.kept, r.Node)
		}
	}
	count := func(role shard.Role) int {
		n := 0
		for _, r := range p.st.Replicas(id) {
			if r.Role == role {
				n++
			}
		}
		return n
	}
	for role := range shard.Role(shard.RoleCount) {
		for count(role) < p.st.Max(role) {
			placed, err := p.place(ctx, id, "", role, d)
			if err != nil {
				return err
			}
			if !placed {
				break
			}
		}
	}
	//dokimi:mutate-skip ror-true: a shard without a reason has no kept replica and the replicas of the model, so plan gives it the same target with or without its deficit
	if d.reason != nil {
		p.deficits[id] = d
		p.touched[id] = true
	}
	return nil
}

// place moves the replica of shard id in role from node from to the best feasible
// node, or adds a replica in role when from is empty. Every constraint filters the
// accepting nodes that have neither a replica nor a fault of the shard. The goals score
// the rest tier by tier, from the lowest tier up. place breaks a tie of two nodes in
// favour of the node that received fewer replicas from repair in the pass, and then of
// the node with the lower ID. The replicas of a gone node then spread over many nodes.
// place reports whether it found a node. When it did not, it records the reason in d.
//
// Error modes:
//   - the errors of evaluate and apply.
func (p *pass) place(ctx context.Context, id shard.ID, from shard.NodeID, role shard.Role, d *deficit) (bool, error) {
	replicas := p.st.Replicas(id)
	p.candidates = p.candidates[:0]
	for _, n := range p.st.Nodes() {
		hosts := slices.ContainsFunc(replicas, func(r shard.Replica) bool { return r.Node == n })
		if !hosts && !slices.Contains(p.in.Faults[id], n) {
			p.candidates = append(p.candidates, placement.Move{From: from, To: n, Shard: id, Role: role})
		}
	}
	if err := p.evaluate(ctx, p.candidates); err != nil {
		return false, err
	}
	tiers := len(p.pl.tiers)
	best := -1
	for i, m := range p.candidates {
		if p.results[i].rejection != nil {
			continue
		}
		order := 1
		if best >= 0 {
			order = p.compare(p.deltas[i*tiers:(i+1)*tiers], p.deltas[best*tiers:(best+1)*tiers])
		}
		if best < 0 || order < 0 || (order == 0 && p.placed[m.To] < p.placed[p.candidates[best].To]) {
			best = i
		}
	}
	if best < 0 {
		if d.reason == nil {
			d.reason = p.reason(id)
		}
		return false, nil
	}
	m := p.candidates[best]
	p.placed[m.To]++
	return true, p.apply(m, p.deltas[best*tiers:(best+1)*tiers])
}

// reason returns the error that explains why no candidate of the last step for shard
// id is feasible. It wraps placement.ErrNoNode and the first error of each constraint
// that rejected a candidate. The errors of the constraints follow in the order of the
// constraints.
func (p *pass) reason(id shard.ID) error {
	errs := make([]error, 1+len(p.pl.constraints))
	errs[0] = fmt.Errorf("%w: no accepting node can take a replica of shard %v", placement.ErrNoNode, id)
	rejections := errs[1:]
	for i := range p.candidates {
		if r := p.results[i]; rejections[r.constraint] == nil {
			rejections[r.constraint] = r.rejection
		}
	}
	return errors.Join(errs...)
}

// improve lowers the costs of the tiers with a local search. It sweeps the accepting
// nodes in the order that order returns, and applies at most one move from each node
// per sweep. It sweeps again until a sweep does not apply a move, or until the pass
// stops.
//
// Error modes:
//   - the errors of improveFrom.
func (p *pass) improve(ctx context.Context) error {
	for {
		moved := false
		for _, n := range p.order() {
			if p.out[n] >= p.pl.churn.PerNode {
				continue
			}
			applied, err := p.improveFrom(ctx, n)
			if err != nil {
				return err
			}
			moved = moved || applied
			//dokimi:mutate-skip sbr-delete: improveFrom returns at its first check once the pass has stopped, so a later node does not move a replica
			if p.stopped {
				return nil
			}
		}
		if !moved {
			return nil
		}
	}
}

// improveFrom examines up to Search.Sample shards of node n that the improvement may
// move, from the largest shard to the smallest. It evaluates the move of each examined
// shard to a sample of the accepting nodes, in every tier. Among the moves that improve
// the costs, it applies the move whose deltas compare lowest, so a move that lowers a
// lower tier takes precedence over every move that lowers only a higher tier.
// improveFrom breaks a tie of two moves in favour of the lower shard ID, and then of the
// lower node ID. It reports whether it applied a move.
//
// Error modes:
//   - the errors of check, evaluate and apply.
func (p *pass) improveFrom(ctx context.Context, n shard.NodeID) (bool, error) {
	tiers := len(p.pl.tiers)
	var best placement.Move
	found, examined := false, 0
	for _, id := range p.largest(n) {
		if examined == p.pl.search.Sample {
			break
		}
		if !p.movable(id) {
			continue
		}
		if err := p.check(ctx); err != nil {
			return false, err
		}
		if p.stopped {
			break
		}
		examined++
		p.sources++
		replicas := p.st.Replicas(id)
		role := replicas[slices.IndexFunc(replicas, func(r shard.Replica) bool { return r.Node == n })].Role
		p.candidates = p.candidates[:0]
		for _, to := range p.sample(id) {
			p.candidates = append(p.candidates, placement.Move{From: n, To: to, Shard: id, Role: role})
		}
		if err := p.evaluate(ctx, p.candidates); err != nil {
			return false, err
		}
		for i, m := range p.candidates {
			deltas := p.deltas[i*tiers : (i+1)*tiers]
			if p.results[i].rejection != nil || !p.improves(deltas) {
				continue
			}
			// The candidates of one shard are in the order of their nodes, so a tie keeps
			// the lower node.
			order := -1
			if found {
				order = p.compare(deltas, p.bestDeltas)
			}
			if order < 0 || (order == 0 && m.Shard.Compare(best.Shard) < 0) {
				best, found = m, true
				copy(p.bestDeltas, deltas)
			}
		}
	}
	if !found {
		return false, nil
	}
	if err := p.apply(best, p.bestDeltas); err != nil {
		return false, err
	}
	p.out[n]++
	p.improved[best.Shard] = true
	return true, nil
}

// movable reports whether the improvement may move shard id. The improvement may move
// a shard when each of these is true:
//
//   - the shard does not have a transition in flight.
//   - the shard does not need repair.
//   - the shard has fewer moves in the pass than Churn.PerShard allows.
//   - the improvement moved the shard before, or Churn.MaxShardFraction allows one
//     more shard.
func (p *pass) movable(id shard.ID) bool {
	if p.st.MovedShard(id) >= p.pl.churn.PerShard || (!p.improved[id] && len(p.improved) >= p.maxShards) {
		return false
	}
	_, frozen := p.st.InFlight(id)
	return !frozen && !p.needsRepair(id, p.st.Replicas(id))
}

// improves reports whether a move with the weighted delta of each tier in deltas
// improves the costs. Such a move lowers the cost of a tier by more than the tolerance
// of the tier. It does not raise the cost of a lower tier by more than the tolerance of
// that tier.
func (p *pass) improves(deltas []float64) bool {
	for t, d := range deltas {
		//dokimi:mutate-skip ror-boundary: the tolerance absorbs rounding error, so a delta of exactly the tolerance is noise
		if d < -p.tolerances[t] {
			return true
		}
		//dokimi:mutate-skip ror-boundary: the tolerance absorbs rounding error, so a delta of exactly the tolerance is noise
		if d > p.tolerances[t] {
			return false
		}
	}
	return false
}

// compare compares a and b, the weighted deltas of each tier of two moves, from the
// lowest tier up. It compares them in the first tier where they differ by more than the
// tolerance of the tier, and returns:
//
//   - -1 when the delta of a is lower there.
//   - 1 when the delta of b is lower there.
//   - 0 when the deltas do not differ by more than the tolerance in any tier.
func (p *pass) compare(a, b []float64) int {
	for t := range a {
		//dokimi:mutate-skip ror-boundary: the tolerance absorbs rounding error, so a difference of exactly the tolerance is noise
		if a[t] < b[t]-p.tolerances[t] {
			return -1
		}
		//dokimi:mutate-skip ror-boundary: the tolerance absorbs rounding error, so a difference of exactly the tolerance is noise
		if a[t] > b[t]+p.tolerances[t] {
			return 1
		}
	}
	return 0
}

// evaluate checks each candidate against the constraints, in their order, and adds up
// the weighted deltas of each feasible candidate in each tier. It counts each candidate
// as an evaluation, and runs the evaluations on up to Input.Parallelism goroutines. The
// results do not depend on the number of goroutines.
//
// Error modes:
//   - an error that wraps the cause of ctx when ctx ends while goroutines evaluate.
//   - an error that wraps placement.ErrCost when a goal returns a delta that is not
//     finite. The error of the first such candidate is returned.
func (p *pass) evaluate(ctx context.Context, candidates []placement.Move) error {
	p.evaluations += len(candidates)
	p.results = slices.Grow(p.results[:0], len(candidates))[:len(candidates)]
	width := len(candidates) * len(p.pl.tiers)
	p.deltas = slices.Grow(p.deltas[:0], width)[:width]
	//dokimi:mutate-skip ror-false: task.Each with a limit of 1 evaluates the candidates on one goroutine, as the loop does
	if p.in.Parallelism == 1 {
		for i, m := range candidates {
			p.evaluateOne(i, m)
		}
	} else if err := task.Each(ctx, p.in.Parallelism, candidates,
		func(_ context.Context, i int, m placement.Move) error {
			p.evaluateOne(i, m)
			return nil
		}); err != nil {
		return fmt.Errorf("planner: evaluation of %d candidate moves: %w", len(candidates), err)
	}
	for i := range candidates {
		if p.results[i].fault != nil {
			return p.results[i].fault
		}
	}
	return nil
}

// evaluateOne evaluates the candidate m at position i of the step in every tier, and
// writes its result and its deltas at position i. It only reads the state, so many
// goroutines run it at once.
func (p *pass) evaluateOne(i int, m placement.Move) {
	r := &p.results[i]
	*r = result{}
	for k, c := range p.pl.constraints {
		if err := c.Check(p.st, m); err != nil {
			r.rejection, r.constraint = err, k
			return
		}
	}
	width := len(p.pl.tiers)
	for t, tr := range p.pl.tiers {
		sum := 0.0
		for g, goal := range tr.goals {
			delta := goal.Delta(p.st, m)
			if math.IsNaN(delta) || math.IsInf(delta, 0) {
				r.fault = fmt.Errorf("%w: goal %s returned the delta %v", placement.ErrCost, goal.Name(), delta)
				return
			}
			sum += tr.weights[g] * delta
		}
		p.deltas[i*width+t] = sum
	}
}

// apply applies m to the state and records the shard of m as touched. It adds deltas to
// the cost of each tier. deltas contains the weighted delta of each tier of m, as
// evaluate computed it before the move.
//
// Error modes:
//   - an error that wraps placement.ErrMove when the state refuses m, which happens
//     only when a constraint or a goal changed the state.
func (p *pass) apply(m placement.Move, deltas []float64) error {
	for t, d := range deltas {
		p.costs[t] += d
		p.tolerances[t] = precision * max(1, math.Abs(p.costs[t]))
	}
	if err := p.st.Apply(m); err != nil {
		return fmt.Errorf("planner: a constraint or a goal changed the state of the pass: %w", err)
	}
	p.moves++
	p.touched[m.Shard] = true
	return nil
}

// check returns the cause of ctx when ctx has ended. It stops the pass at its timeout
// or at a limit of its work, before the pass examines another shard. The work of the
// last shard that a pass examines can take the pass past MaxMoves and MaxEvaluations.
func (p *pass) check(ctx context.Context) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	s := p.pl.search
	if p.moves >= s.MaxMoves || p.evaluations >= s.MaxEvaluations || p.sources >= s.MaxSourceShards ||
		!p.in.Clock.Time().Before(p.deadline) {

		p.stopped = true
	}
	return nil
}

// triggered reports whether atRisk finds a node of the cell at risk on a load metric
// or on the replica count.
func (p *pass) triggered() bool {
	for _, m := range p.st.Metrics() {
		if p.atRisk(m.Name, func(n shard.NodeID) float64 { return p.st.NodeLoad(n)[m.Name] }) {
			return true
		}
	}
	return p.atRisk("", func(n shard.NodeID) float64 { return float64(len(p.st.OnNode(n))) })
}

// atRisk reports whether the values of the accepting nodes on metric show a node at
// risk. The cell must be busy: on a metric whose nodes declare a capacity, a node is at
// or above Triggers.ActiveAbove of its capacity. The load must be imbalanced: the
// highest value is at least Triggers.MinImbalance times the mean, or the mean is above
// Triggers.MinImbalance times the lowest value. A cell without nodes is not at risk,
// and neither is a metric with a mean of 0. The empty metric is the replica count, and
// the nodes do not declare a capacity of it.
func (p *pass) atRisk(metric string, value func(shard.NodeID) float64) bool {
	nodes := p.st.Nodes()
	//dokimi:mutate-skip sbr-delete,ror-false: without nodes the mean is NaN, and every comparison with NaN below is false
	if len(nodes) == 0 {
		return false
	}
	high, low, sum := math.Inf(-1), math.Inf(1), 0.0
	capacity, busy := false, false
	for _, n := range nodes {
		v := value(n)
		high, low, sum = max(high, v), min(low, v), sum+v
		if u, ok := p.st.Utilisation(n, metric); ok {
			capacity = true
			busy = busy || u >= p.pl.triggers.ActiveAbove
		}
	}
	mean := sum / float64(len(nodes))
	if mean <= 0 || (capacity && !busy) {
		return false
	}
	return high >= p.pl.triggers.MinImbalance*mean || mean > p.pl.triggers.MinImbalance*low
}

// order returns the accepting nodes from the highest pressure to the lowest, in the
// order of their IDs at equal pressure. The pressure of a node is the highest of its
// shares of the totals of the accepting nodes, over its replica count and its load on
// each metric. A share orders the nodes as the ratio to the mean does.
func (p *pass) order() []shard.NodeID {
	nodes := p.st.Nodes()
	metrics := p.st.Metrics()
	totals := make([]float64, len(metrics)+1)
	for _, n := range nodes {
		totals[0] += float64(len(p.st.OnNode(n)))
		for k, m := range metrics {
			totals[k+1] += p.st.NodeLoad(n)[m.Name]
		}
	}
	type pressured struct {
		node     shard.NodeID
		pressure float64
	}
	list := make([]pressured, len(nodes))
	for i, n := range nodes {
		list[i].node = n
		for k := range totals {
			value := float64(len(p.st.OnNode(n)))
			if k > 0 {
				value = p.st.NodeLoad(n)[metrics[k-1].Name]
			}
			if totals[k] > 0 {
				list[i].pressure = max(list[i].pressure, value/totals[k])
			}
		}
	}
	slices.SortStableFunc(list, func(a, b pressured) int { return cmp.Compare(b.pressure, a.pressure) })
	order := make([]shard.NodeID, len(list))
	for i, l := range list {
		order[i] = l.node
	}
	return order
}

// largest returns the shards of node n from the largest to the smallest, in the order
// of their IDs at equal size. The size of a shard is the highest of its shares of the
// total load of the shards, over the metrics.
func (p *pass) largest(n shard.NodeID) []shard.ID {
	type sized struct {
		id   shard.ID
		size float64
	}
	shards := p.st.OnNode(n)
	list := make([]sized, len(shards))
	for i, sh := range shards {
		list[i].id = sh
		for k, m := range p.st.Metrics() {
			list[i].size = max(list[i].size, p.st.ShardLoad(sh)[m.Name]*p.scale[k])
		}
	}
	slices.SortStableFunc(list, func(a, b sized) int { return cmp.Compare(b.size, a.size) })
	ids := make([]shard.ID, len(list))
	for i, l := range list {
		ids[i] = l.id
	}
	return ids
}

// sample returns up to Search.Sample accepting nodes that have neither a replica nor a
// fault of shard id, in the order of their IDs. It selects the nodes without
// replacement with a partial Fisher-Yates shuffle of perm. The shuffle takes its
// random numbers from Input.Rand. sample returns every such node when there are no
// more than Search.Sample of them.
func (p *pass) sample(id shard.ID) []shard.NodeID {
	nodes := p.st.Nodes()
	replicas := p.st.Replicas(id)
	faults := p.in.Faults[id]
	chosen := make([]shard.NodeID, 0, min(p.pl.search.Sample, len(nodes)))
	for j := 0; j < len(p.perm) && len(chosen) < p.pl.search.Sample; j++ {
		r := j + int(rand.Uint64N(p.in.Rand, uint64(len(p.perm)-j)))
		p.perm[j], p.perm[r] = p.perm[r], p.perm[j]
		n := nodes[p.perm[j]]
		hosts := slices.ContainsFunc(replicas, func(x shard.Replica) bool { return x.Node == n })
		if !hosts && !slices.Contains(faults, n) {
			chosen = append(chosen, n)
		}
	}
	slices.Sort(chosen)
	return chosen
}

// plan returns the plan of the pass. The plan has a target for each shard whose
// replicas differ from the start of the pass, and for each shard with a deficit. Its
// Costs are the final costs of the tiers. plan measures them again when the pass applied
// a move, and a pass without a move keeps the costs of its start.
//
// Error modes:
//   - the errors of measure.
func (p *pass) plan() (placement.Plan, error) {
	if p.moves > 0 {
		if err := p.measure(); err != nil {
			return placement.Plan{}, err
		}
	}
	targets := make(map[shard.ID]placement.Target, len(p.touched))
	for id := range p.touched {
		replicas := p.st.Replicas(id)
		initial := p.st.Initial(id)
		d := p.deficits[id]
		// A pass never removes a replica without placing it on another node, so a shard
		// whose replicas are all initial replicas has its initial replicas.
		same := !slices.ContainsFunc(replicas, func(r shard.Replica) bool { return !slices.Contains(initial, r) })
		if d == nil && same {
			continue
		}
		target := placement.Target{Replicas: slices.Clone(replicas)}
		if d != nil {
			target.Replicas = slices.DeleteFunc(target.Replicas,
				func(r shard.Replica) bool { return slices.Contains(d.kept, r.Node) })
			target.Deficit = p.st.Desired() - len(target.Replicas)
			target.Reason = d.reason
		}
		targets[id] = target
	}
	return placement.Plan{
		Targets:    targets,
		Costs:      slices.Clone(p.costs),
		RepairNext: p.repairNext,
		Incomplete: p.stopped,
	}, nil
}

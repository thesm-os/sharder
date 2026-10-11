// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placement

import (
	"fmt"
	"maps"
	"slices"

	"go.thesmos.sh/sharder/shard"
)

// Violation is a replica that a plan keeps and that a constraint rejects. The core
// reports a violation and does not reject the plan for it.
//
// # Concurrency
//
// A Violation is a value, and a copy shares its Err with the original.
type Violation struct {
	// Err is the error that the constraint returned.
	Err error
	// Constraint is the name of the constraint.
	Constraint shard.StrategyName
	// Node is the node of the replica.
	Node shard.NodeID
	// Shard is the shard of the replica.
	Shard shard.ID
	// Role is the role of the replica.
	Role shard.Role
}

// Validate checks the plan p of a pass over in against the constraints cs, as the
// core checks every plan before it commits it, whatever placer returned it. It returns
// the violations of the replicas that p keeps, or an error for a plan that it rejects
// whole.
//
// Validate builds the State of in, and applies each target of p as the moves that
// remove the current replicas that the target does not list and add the replicas that
// the target adds. The constraints see the result, which is the state of the whole
// plan. Validate checks each replica that p adds as the move that adds it to the
// result without it. It checks each replica that p keeps as a move whose From equals
// its To. The load of a shard counts on each node of its current replicas, of its
// target, and of its transition in flight, so two targets that each fit the last free
// capacity of a node fail together. A replica that p adds must pass every constraint.
// A replica that p keeps and that a constraint rejects is a violation. Validate
// returns the violations in the order of the IDs of their shards, then of the replicas
// of the shard, then of cs.
//
// Validate checks what the inputs of placement state. The engine checks the transfers
// of a plan and their fallbacks before it starts any of them.
//
// Error modes:
//   - the errors of NewState.
//   - an error that wraps ErrPlan for a target of a shard that the layout does not
//     list.
//   - an error that wraps ErrPlan for a target with a negative deficit, and for a
//     target whose replicas and deficit do not add up to State.Desired.
//   - an error that wraps ErrPlan and ErrMove for a target that changes a shard with a
//     transition in flight, that adds a replica to a node that is not accepting or
//     already has a replica of the shard, that adds a replica in a role that the model
//     does not declare, or that puts more replicas in a role than its Max allows.
//   - an error that wraps ErrPlan for a target that adds a replica to a node that
//     Input.Faults lists for the shard.
//   - an error that wraps ErrPlan and the error of the constraint for a replica that
//     the plan adds and that a constraint rejects.
func Validate(in *Input, p Plan, cs []Constraint) ([]Violation, error) {
	st, err := NewState(in)
	if err != nil {
		return nil, err
	}
	targets := slices.SortedFunc(maps.Keys(p.Targets), func(a, b shard.ID) int { return a.Compare(b.ID) })
	for _, id := range targets {
		if err := st.applyTarget(id, p.Targets[id]); err != nil {
			return nil, err
		}
	}

	for _, id := range targets {
		i := st.index[id]
		initial := st.Initial(id)
		for _, r := range p.Targets[id].Replicas {
			if slices.Contains(initial, r) {
				continue
			}
			add := Move{To: r.Node, Shard: id, Role: r.Role}
			st.apply(i, slices.Index(st.shards[i].replicas, r), -1, Move{From: r.Node, Shard: id, Role: r.Role})
			for _, c := range cs {
				if err := c.Check(st, add); err != nil {
					return nil, fmt.Errorf("%w: constraint %s rejects the replica of shard %v on node %q: %w",
						ErrPlan, c.Name(), id, r.Node, err)
				}
			}
			st.apply(i, -1, st.nodeIndex[r.Node], add)
		}
	}

	var violations []Violation
	for _, id := range st.ids {
		initial := st.Initial(id)
		for _, r := range st.Replicas(id) {
			if !slices.Contains(initial, r) {
				continue
			}
			m := Move{From: r.Node, To: r.Node, Shard: id, Role: r.Role}
			for _, c := range cs {
				if err := c.Check(st, m); err != nil {
					violations = append(violations,
						Violation{Err: err, Constraint: c.Name(), Node: r.Node, Shard: id, Role: r.Role})
				}
			}
		}
	}
	return violations, nil
}

// applyTarget applies the target t of shard id to the state. It removes each current
// replica that t does not list, and then adds each replica of t that the shard does
// not have. A State refuses each move that a valid target cannot imply, such as a
// move of a shard with a transition in flight.
//
// Error modes:
//   - an error that wraps ErrPlan for a shard that the layout does not list.
//   - an error that wraps ErrPlan for a negative deficit, and for replicas and a
//     deficit that do not add up to the replicas of each shard.
//   - an error that wraps ErrPlan and ErrMove for a move that the state refuses.
//   - an error that wraps ErrPlan for a replica on a node that Input.Faults lists for
//     the shard.
func (st *State) applyTarget(id shard.ID, t Target) error {
	if _, ok := st.index[id]; !ok {
		return fmt.Errorf("%w: a target of shard %v, which the layout does not list", ErrPlan, id)
	}
	if t.Deficit < 0 {
		return fmt.Errorf("%w: the target of shard %v has the deficit %d", ErrPlan, id, t.Deficit)
	}
	if len(t.Replicas)+t.Deficit != st.desired {
		return fmt.Errorf("%w: the target of shard %v has %d replicas and a deficit of %d, and a shard has %d replicas",
			ErrPlan, id, len(t.Replicas), t.Deficit, st.desired)
	}
	initial := st.Initial(id)
	for _, r := range initial {
		if slices.Contains(t.Replicas, r) {
			continue
		}
		if err := st.Apply(Move{From: r.Node, Shard: id, Role: r.Role}); err != nil {
			return fmt.Errorf("%w: the target of shard %v removes a replica: %w", ErrPlan, id, err)
		}
	}
	for _, r := range t.Replicas {
		if slices.Contains(initial, r) {
			continue
		}
		if slices.Contains(st.in.Faults[id], r.Node) {
			return fmt.Errorf("%w: the target of shard %v adds a replica to node %q, which has a fault of the shard",
				ErrPlan, id, r.Node)
		}
		if err := st.Apply(Move{To: r.Node, Shard: id, Role: r.Role}); err != nil {
			return fmt.Errorf("%w: the target of shard %v adds a replica: %w", ErrPlan, id, err)
		}
	}
	return nil
}

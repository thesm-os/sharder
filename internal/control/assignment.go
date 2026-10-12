// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control

import (
	"fmt"
	"maps"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"

	"go.thesmos.sh/sharder/internal/scope"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// hosting is a node and a role, the key of the count of the replicas of a role on a node.
type hosting struct {
	// node is the node.
	node shard.NodeID
	// role is the role.
	role shard.Role
}

// assignment is the committed state of the scope that the controller keeps. It has the
// layout and the placements of the shards of the scope, the records of the transitions in
// flight and the generation of the scope. The controller changes it only with the mutation
// of a commit that has applied, so a pass reads a copy of the committed assignment.
type assignment struct {
	// placements maps each shard with a committed placement to the placement.
	placements map[shard.ID]shard.Placement
	// transitions maps each shard with a transition in flight to its committed record. The
	// record is in the snapshot or in the mutation of a commit, and nothing modifies it.
	transitions map[shard.ID]*store.Transition
	// index maps the ID of each shard of the layout to its position in the layout.
	index map[shard.ID]int
	// hosted counts the replicas of each role on each node.
	hosted map[hosting]int
	// layout lists the shards of the scope in position order, each with its ID.
	layout partition.Layout
	// generation is the generation of the scope that the assignment is at.
	generation epoch.Epoch
	// unplaced counts the shards of the layout without a replica in the writer role.
	unplaced int
	// writer is the writer role of the replica state model. A spec edit does not change
	// the model.
	writer shard.Role
}

// newAssignment returns the assignment of the snapshot snap of the scope of the cell cell,
// whose shards are spans and whose writer role is role. The cell of the root scope is
// empty. A span whose range starts where the range of a placement of snap starts has the
// ID of that shard. Every other span gets a new ID, a ULID from ids.
//
// Error modes:
//   - an error that wraps ErrSpec for a placement whose range does not start where a span
//     starts, because the shards of the scope then differ from the spans of its partitioner.
func newAssignment(snap *scope.Snapshot, cell shard.CellID, spans []partition.Span, ids *ulid.Generator,
	role shard.Role,
) (*assignment, error) {
	a := &assignment{
		placements:  make(map[shard.ID]shard.Placement, len(snap.Placements)),
		transitions: make(map[shard.ID]*store.Transition, len(snap.State.Transitions)),
		index:       make(map[shard.ID]int, len(spans)),
		hosted:      make(map[hosting]int),
		layout:      partition.Layout{Cell: cell, Shards: make([]partition.Shard, 0, len(spans))},
		generation:  snap.State.Head.Generation,
		unplaced:    len(spans),
		writer:      role,
	}
	starts := make(map[string]shard.ID, len(snap.Placements))
	for i := range snap.Placements {
		p := &snap.Placements[i]
		a.put(p)
		starts[string(p.Range.Start)] = p.Shard
	}
	for i := range snap.State.Transitions {
		t := &snap.State.Transitions[i]
		a.transitions[t.Operation.Shard] = t
	}
	for _, s := range spans {
		sid, ok := starts[string(s.Range.Start)]
		delete(starts, string(s.Range.Start))
		if !ok {
			var k id.ID128
			copy(k[:], ids.Generate().Bytes())
			sid = shard.ID{ID128: k}
		}
		a.index[sid] = len(a.layout.Shards)
		a.layout.Shards = append(a.layout.Shards, partition.Shard{Span: s, ID: sid})
	}
	for _, p := range snap.Placements {
		if _, ok := starts[string(p.Range.Start)]; ok {
			return nil, fmt.Errorf(
				"%w: the placement of shard %v starts at %x, where no span of the partitioner starts",
				ErrSpec, p.Shard, p.Range.Start,
			)
		}
	}
	return a, nil
}

// apply applies the mutation m of the commit that created the generation gen. It ends the
// transitions of m.Done before it applies m.Transitions, as a store does.
func (a *assignment) apply(m *store.Mutation, gen epoch.Epoch) {
	for _, sid := range m.Done {
		delete(a.transitions, sid)
	}
	for i := range m.Transitions {
		t := &m.Transitions[i]
		a.transitions[t.Operation.Shard] = t
	}
	for i := range m.Put {
		a.put(&m.Put[i])
	}
	a.generation = gen
}

// put replaces the placement of the shard of p with p, and counts the replicas of p in
// place of the replicas of the placement that p replaces.
func (a *assignment) put(p *shard.Placement) {
	old := a.placements[p.Shard]
	for _, r := range old.Replicas {
		a.hosted[hosting{node: r.Node, role: r.Role}]--
	}
	for _, r := range p.Replicas {
		a.hosted[hosting{node: r.Node, role: r.Role}]++
	}
	_, before := writer(&old, a.writer)
	_, after := writer(p, a.writer)
	if before {
		a.unplaced++
	}
	if after {
		a.unplaced--
	}
	a.placements[p.Shard] = *p
}

// placement returns the placement of the shard sid of the layout: its committed placement,
// or a placement without replicas and epochs for a shard without one.
func (a *assignment) placement(sid shard.ID) shard.Placement {
	if p, ok := a.placements[sid]; ok {
		return p
	}
	return shard.Placement{Shard: sid, Range: a.layout.Shards[a.index[sid]].Range}
}

// hosts counts the replicas on the node n in the roles roles.
func (a *assignment) hosts(n shard.NodeID, roles []shard.Role) int {
	count := 0
	for _, r := range roles {
		count += a.hosted[hosting{node: n, role: r}]
	}
	return count
}

// current returns the committed assignment of the scope s as a copy, which a pass reads
// while the controller changes a. The copy costs O(n) for n placements.
func (a *assignment) current(s shard.Scope) shard.Assignment {
	return shard.Assignment{Placements: maps.Clone(a.placements), Scope: s, Generation: a.generation}
}

// writer returns the replica of p in the role writer, and reports whether p has one.
func writer(p *shard.Placement, role shard.Role) (shard.Replica, bool) {
	for _, r := range p.Replicas {
		if r.Role == role {
			return r, true
		}
	}
	return shard.Replica{}, false
}

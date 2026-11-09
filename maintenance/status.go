// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package maintenance

import (
	"cmp"
	"context"
	"slices"
	"time"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id/ulid"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// StatusRequest is the request of Client.Status.
type StatusRequest struct {
	_ incomparable
	// Namespace is the namespace of the status.
	Namespace shard.Namespace
}

// Replica is one replica on a node.
type Replica struct {
	// Shard is the shard of the replica.
	Shard shard.ID
	// Role is the role of the replica.
	Role shard.Role
}

// NodeStatus is the status of one node of a cell.
type NodeStatus struct {
	// Replicas lists the replicas on the node, in the position order of their shards.
	Replicas []Replica
	// Node is the node that the view of the cell lists, or a node with its ID alone when the
	// view does not list it.
	Node shard.Node
	// Live reports whether the view of the cell lists the node.
	Live bool
	// Draining reports whether the cell has a drain of the node. A node with a drain does not
	// receive new replicas.
	Draining bool
}

// ScopeStatus is the status of one scope: its record and its header.
type ScopeStatus struct {
	// Applied maps the ID of each proposal that the scope has applied to its Expires. The scope
	// forgets an ID once its Expires has passed, so an ID that Applied does not contain may have
	// applied.
	Applied map[proposal.ID]time.Time
	// Scope is the scope.
	Scope shard.Scope
	// Record is the record of the controller of the scope: its address, its epoch and the
	// version of the spec that it applies. The record of the root scope is the record of the
	// leader of the root.
	Record shard.Cell
	// Generation is the generation of the scope.
	Generation epoch.Epoch
}

// CellStatus is the status of one cell.
type CellStatus struct {
	// Nodes lists each node of the view of the cell and each node with a replica of the cell, in
	// the order of their IDs.
	Nodes []NodeStatus
	// Unplaced lists each shard whose placement has no assigned writer, in position order. A
	// shard that a controller has never placed has no placement in the store, so Unplaced does
	// not list it.
	Unplaced []shard.ID
	// Transitions lists the transitions in flight, in the order of their shards.
	Transitions []store.Transition
	// Reservations lists the disruptions that the budget of the cell deferred or admitted and
	// that have not ended. A reservation whose Admitted is zero is pending.
	Reservations []budget.Reservation
	// Drains lists every drain of the cell.
	Drains []store.Drain
	// ScopeStatus is the status of the scope of the cell.
	ScopeStatus
}

// cellStatus returns the status of the cell of the scope s, from its state st, its placements
// and its view. The nodes of the view come first, then each other node with a replica, and
// cellStatus then sorts them by their IDs.
func cellStatus(s shard.Scope, st *store.State, placements []shard.Placement, view *membership.View) CellStatus {
	cs := CellStatus{
		Nodes:        make([]NodeStatus, 0, len(view.Nodes)),
		Transitions:  st.Transitions,
		Reservations: st.Reservations,
		Drains:       st.Drains,
		Applied:      st.Applied,
		Scope:        s,
		Record:       st.Cell,
		Generation:   st.Head.Generation,
	}
	index := make(map[shard.NodeID]int, len(view.Nodes))
	for _, n := range view.Nodes {
		index[n.ID] = len(cs.Nodes)
		cs.Nodes = append(cs.Nodes, NodeStatus{Node: n, Live: true})
	}
	for _, p := range placements {
		if p.Epoch == epoch.Zero {
			cs.Unplaced = append(cs.Unplaced, p.Shard)
		}
		for _, r := range p.Replicas {
			i, ok := index[r.Node]
			if !ok {
				i = len(cs.Nodes)
				index[r.Node] = i
				cs.Nodes = append(cs.Nodes, NodeStatus{Node: shard.Node{ID: r.Node}})
			}
			cs.Nodes[i].Replicas = append(cs.Nodes[i].Replicas, Replica{Shard: p.Shard, Role: r.Role})
		}
	}
	for _, d := range st.Drains {
		if i, ok := index[d.Node]; ok {
			cs.Nodes[i].Draining = true
		}
	}
	slices.SortFunc(cs.Nodes, func(a, b NodeStatus) int { return cmp.Compare(a.Node.ID, b.Node.ID) })
	return cs
}

// StatusResult is the result of Client.Status. It shares the maps and the slices of the
// states that the store returned, so the caller does not modify them.
type StatusResult struct {
	_ incomparable
	// Cells lists the status of each cell, in the order of the placements of the root scope.
	Cells []CellStatus
	// Root is the status of the root scope.
	Root ScopeStatus
	// Incarnation is the namespace incarnation.
	Incarnation shard.NamespaceIncarnation
	// Version is the version of the accepted spec. The record of each scope has the version
	// that its controller applies.
	Version epoch.Epoch
}

// Status returns the status of the namespace req.Namespace. It reads the root scope, the scope
// of each cell with every page of its placements, and the current view of each cell. Each read
// goes to the store or the membership, and none goes to a controller.
//
// Error modes:
//   - an error that wraps ErrNoNamespace for a namespace that the root has not created.
//   - an error that wraps the error of a read or of a watch, such as shard.ErrIncarnation for
//     a cell of another namespace incarnation.
//   - an error for an accepted spec that does not decode, and for a watch that ends before
//     its first view.
func (c *Client) Status(ctx context.Context, req *StatusRequest) (StatusResult, error) {
	root, cells, err := c.root(ctx, req.Namespace)
	if err != nil {
		return StatusResult{}, err
	}
	version, err := accepted(req.Namespace, &root)
	if err != nil {
		return StatusResult{}, err
	}
	res := StatusResult{
		Cells: make([]CellStatus, 0, len(cells)),
		Root: ScopeStatus{
			Applied:    root.Applied,
			Scope:      shard.Scope{Namespace: req.Namespace},
			Record:     root.Cell,
			Generation: root.Head.Generation,
		},
		Incarnation: root.Head.Incarnation,
		Version:     version,
	}
	for i := range cells {
		s := shard.Scope{Namespace: req.Namespace, Cell: shard.CellID(ulid.Format(cells[i].Shard.ID()))}
		st, placements, err := c.read(ctx, s, res.Incarnation)
		if err != nil {
			return StatusResult{}, err
		}
		view, err := c.view(ctx, s, res.Incarnation)
		if err != nil {
			return StatusResult{}, err
		}
		res.Cells = append(res.Cells, cellStatus(s, &st, placements, &view))
	}
	return res, nil
}

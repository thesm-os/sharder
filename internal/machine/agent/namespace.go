// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"fmt"
	"slices"

	"go.thesmos.sh/core/id/ulid"

	"go.thesmos.sh/sharder/internal/follow"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/shard"
)

// namespace is what the agent reads from the root scope before it joins its cell: the spec,
// the incarnation and the cells of the namespace.
type namespace struct {
	// spec is the accepted spec of the namespace as JSON, and nil before the root has
	// created the namespace.
	spec []byte
	// cells lists each cell of the namespace that the agent has read once, in the order of
	// the reads. The cells are the shards of the root scope.
	cells []shard.ID
	// incarnation is the namespace incarnation, and zero before the root has created the
	// namespace.
	incarnation shard.NamespaceIncarnation
}

// add appends the cell sid to the cells when the list does not have it. The follower reads a
// snapshot again after a compaction, and the root can put a cell again.
func (ns *namespace) add(sid shard.ID) {
	if !slices.Contains(ns.cells, sid) {
		ns.cells = append(ns.cells, sid)
	}
}

// rooted takes what the follower of the root scope passed. The last page of a snapshot and
// each later generation of the root scope can complete the namespace and the cells that the
// agent waits for.
//
// Error modes:
//   - an error that wraps the error that stopped the follower.
//   - the error of discover.
func (m *Machine) rooted(res *follow.Result, out []machine.Effect) ([]machine.Effect, error) {
	if res.Err != nil {
		return out, fmt.Errorf("agent: the root scope of %s: %w", m.rootScope.Namespace, res.Err)
	}
	if p := res.Page; p != nil {
		for i := range p.Placements {
			m.ns.add(p.Placements[i].Shard)
		}
		if !p.Last {
			return out, nil
		}
		m.ns.spec, m.ns.incarnation = p.State.Spec, p.State.Head.Incarnation
		return m.discover(out)
	}
	mu := &res.Event.Mutation
	if mu.Spec != nil {
		m.ns.spec, m.ns.incarnation = mu.Spec, mu.ID.Incarnation
	}
	for i := range mu.Put {
		m.ns.add(mu.Put[i].Shard)
	}
	return m.discover(out)
}

// discover joins the cell of the node once the root scope has the spec of the namespace and
// a cell. A node without a cell takes the only cell of the namespace. The agent then stops
// reading the root scope, joins the cell, and reads the scope of the cell.
//
// Error modes:
//   - an error that wraps ErrConfig for a cell of the node that the namespace does not
//     have, and for a node without a cell in a namespace of more than one cell.
//   - the error of resolve for a spec that the agent cannot apply.
func (m *Machine) discover(out []machine.Effect) ([]machine.Effect, error) {
	if m.ns.spec == nil || len(m.ns.cells) == 0 {
		return out, nil
	}
	cell := m.want
	if cell.IsZero() && len(m.ns.cells) > 1 {
		return out, fmt.Errorf("%w: node %s has no cell, and namespace %s has %d cells",
			ErrConfig, m.node.ID, m.rootScope.Namespace, len(m.ns.cells))
	}
	if cell.IsZero() {
		cell = m.ns.cells[0]
	}
	if !slices.Contains(m.ns.cells, cell) {
		return out, fmt.Errorf("%w: namespace %s has no cell %s", ErrConfig, m.rootScope.Namespace, m.node.Cell)
	}
	s, err := resolve(m.registry, m.provided, m.ns.spec)
	if err == nil {
		m.cellScope = shard.Scope{Namespace: m.rootScope.Namespace, Cell: shard.CellID(ulid.Format(cell.ID()))}
		m.cell, err = follow.New(&follow.Config{
			Rand:        m.rand,
			Tokens:      &m.tokens,
			Scope:       m.cellScope,
			Node:        m.node.ID,
			Incarnation: m.ns.incarnation,
		})
	}
	if err == nil {
		err = m.fire(eventRooted)
	}
	if err != nil {
		return out, err
	}
	m.settings, m.incarnation = s, m.ns.incarnation
	out = m.join(m.root.Stop(out))
	return m.cell.Start(out), nil
}

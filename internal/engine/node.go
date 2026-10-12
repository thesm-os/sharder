// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package engine

import (
	"slices"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/shard"
)

// node is a node that the callbacks of an Engine address. It has Config.NodeCallbacks
// permits, and the callbacks without a permit wait in its queue.
type node struct {
	// queue lists the transitions whose callbacks wait for a permit of the node, the
	// oldest first.
	queue []*transition
	// busy counts the callbacks in flight to the node. Each of them takes one permit.
	busy int
}

// node returns the node id, and adds it to the nodes of e on its first call.
func (e *Engine) node(id shard.NodeID) *node {
	n, ok := e.nodes[id]
	if !ok {
		n = &node{}
		e.nodes[id] = n
		e.nodeOrder = append(e.nodeOrder, n)
	}
	return n
}

// release returns the permit of a callback to the node id, and appends the callbacks that
// the permit lets the queue of the node send to out.
func (e *Engine) release(id shard.NodeID, out []machine.Effect) []machine.Effect {
	n := e.nodes[id]
	n.busy--
	return e.drain(n, out)
}

// drain sends the callbacks of the queue of n while n has a free permit, the oldest first,
// and appends them to out.
func (e *Engine) drain(n *node, out []machine.Effect) []machine.Effect {
	for n.busy < e.nodeCallbacks && len(n.queue) > 0 {
		t := n.queue[0]
		n.queue = slices.Delete(n.queue, 0, 1)
		out = e.dispatch(t, out)
	}
	return out
}

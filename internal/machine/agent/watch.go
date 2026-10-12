// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"fmt"
	"slices"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/internal/follow"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// watched is the state of the scope of the cell that concerns the node, as the follower of
// the scope reads it: the placements that name the node, and the record of the controller of
// the cell. The state of each shard marks the operation whose transition the watch shows in
// flight.
type watched struct {
	// placements maps each shard whose placement names the node to the placement.
	placements map[shard.ID]shard.Placement
	// next maps each shard of the snapshot that the follower reads to its placement. It is
	// placements once the agent has installed the snapshot.
	next map[shard.ID]shard.Placement
	// record is the record of the controller of the cell, and the zero Cell before the
	// controller has acquired the scope.
	record shard.Cell
	// generation is the last generation of the scope that the agent has applied.
	generation epoch.Epoch
	// installed is true once the agent has installed a snapshot of the scope.
	installed bool
}

// watch takes what the follower of the scope of the cell passed: a page of a snapshot, or a
// later generation of the scope. The last page of a snapshot replaces the watched state. A
// Result without a page and without a generation, such as the retry of a read, changes
// nothing.
//
// Error modes:
//   - an error that wraps the error that stopped the follower, such as
//     shard.ErrIncarnation for a namespace that the root has created again.
//   - the error of install.
func (m *Machine) watch(res *follow.Result, out []machine.Effect) ([]machine.Effect, error) {
	if res.Err != nil {
		return out, fmt.Errorf("agent: the scope of %v: %w", m.cellScope, res.Err)
	}
	if p := res.Page; p != nil {
		if p.First {
			m.watched.next = make(map[shard.ID]shard.Placement, len(p.Placements))
		}
		for i := range p.Placements {
			m.watched.next[p.Placements[i].Shard] = p.Placements[i]
		}
		if !p.Last {
			return out, nil
		}
		return m.install(&p.State, out)
	}
	if res.Event.Generation == 0 {
		return out, nil
	}
	return m.apply(&res.Event, out), nil
}

// install replaces the watched state with the snapshot that the follower read and the state
// s of the scope. An operation whose transition the watch showed in flight has ended when
// the snapshot does not have the transition. The agent then compares its tenures with the
// placements of the snapshot, so it cleans up each tenure that the scope removed while the
// watch did not run. The first snapshot serves once the first session of the node has
// started. install costs O(n) for the n shards that the agent has seen.
//
// Error modes:
//   - the error of serve.
func (m *Machine) install(s *store.State, out []machine.Effect) ([]machine.Effect, error) {
	m.watched.placements = m.watched.next
	var shown []*shardState
	for _, st := range m.shards.All() {
		if st.op.watched {
			st.op.watched = false
			shown = append(shown, st)
		}
	}
	for i := range s.Transitions {
		m.track(&s.Transitions[i])
	}
	for _, st := range shown {
		if !st.op.watched {
			st.op = operation{id: st.op.id, ended: true}
		}
	}
	m.watched.generation = s.Head.Generation
	m.watched.installed = true
	out = m.settle(m.record(&s.Cell, out))
	if m.lc.State() == stateJoining {
		return m.serve(out)
	}
	return out, nil
}

// apply applies the generation ev of the watch to the watched state:
//   - The end of a transition ends the newest operation of its shard when the watch showed
//     the transition of that operation in flight.
//   - The agent tracks each transition that the generation creates or advances.
//   - The placements that the generation puts replace the watched placements, and a shard in
//     ev.Removed loses its placement.
//   - A record of the controller raises the controller epoch.
//
// The agent then compares its tenures with the placements.
func (m *Machine) apply(ev *store.Event, out []machine.Effect) []machine.Effect {
	m.watched.generation = ev.Generation
	mu := &ev.Mutation
	for _, sid := range mu.Done {
		if st := m.state(sid); st.op.watched {
			st.op = operation{id: st.op.id, ended: true}
		}
	}
	for i := range mu.Transitions {
		m.track(&mu.Transitions[i])
	}
	for i := range mu.Put {
		m.watched.placements[mu.Put[i].Shard] = mu.Put[i]
	}
	for _, sid := range ev.Removed {
		delete(m.watched.placements, sid)
	}
	if mu.Cell != nil {
		out = m.record(mu.Cell, out)
	}
	return m.settle(out)
}

// track takes the transition t of the scope when one of its endpoints is the node. The
// operation of t replaces an older operation of its shard. When t is of the newest operation
// of its shard, the agent marks the operation in flight on the watch. A transition in the
// phase shard.PhaseAborting puts the operation on its abort path.
func (m *Machine) track(t *store.Transition) {
	n := m.node.ID
	if t.From.Node != n && t.To.Node != n && t.Fallback.Node != n {
		return
	}
	st := m.state(t.Operation.Shard)
	st.newer(t.Operation)
	if t.Operation != st.op.id {
		return
	}
	st.op.watched = true
	if t.Phase == shard.PhaseAborting {
		st.op.aborted = true
	}
}

// record takes c, the record of the controller of the cell. The epoch of c raises the
// controller epoch of the agent. A record of a newer controller makes the agent report its
// replicas to that controller.
func (m *Machine) record(c *shard.Cell, out []machine.Effect) []machine.Effect {
	newer := c.Epoch > m.watched.record.Epoch
	m.watched.record = *c
	m.epoch = max(m.epoch, c.Epoch)
	if !newer {
		return out
	}
	return m.report(out)
}

// settle compares the tenures of the node with the watched placements. The only replica of
// a placement is its writer, and each new writer has a new epoch. A placement lists a tenure
// when it has the epoch of the tenure. settle queues the cleanup of each tenure that
// its placement does not list. It queues each callback whose placement now lists its tenure.
// It runs the queue of each shard in the order of the shard IDs. The effects of a step then
// do not depend on the order of a map. settle costs O(n) for the n shards that the agent has
// seen.
func (m *Machine) settle(out []machine.Effect) []machine.Effect {
	for sid, st := range m.shards.All() {
		listed := m.watched.placements[sid].Epoch
		if t := st.tenure; t != nil && !t.cleaned && t.request.Epoch != listed {
			st.cleanup(t)
		}
		for _, w := range st.listing {
			if w.request.Epoch == listed {
				st.queue = append(st.queue, w)
			}
		}
		st.listing = slices.DeleteFunc(st.listing, func(w *work) bool { return w.request.Epoch == listed })
		out = m.run(st, out)
	}
	return out
}

// serve starts to serve once the first session of the node has started and the first
// snapshot of the scope of its cell is installed. The agent then replies to Started and
// reports its replicas.
//
// Error modes:
//   - the error of fsm.Machine.Fire.
func (m *Machine) serve(out []machine.Effect) ([]machine.Effect, error) {
	if !m.session.live || !m.watched.installed {
		return out, nil
	}
	err := m.fire(eventServed)
	return m.report(m.answer(nil, out)), err
}

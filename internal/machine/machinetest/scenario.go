// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machinetest

import (
	"slices"
	"strings"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/history"
	"go.dokimi.dev/sim"

	"go.thesmos.sh/sharder/internal/engine"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store/memory"
)

// handlerDependency is the dependency of the effect Handle, as the anatomy of the simulation
// names it.
const handlerDependency = "driver.Handler"

// firstMove settles the simplest case until the first move of its shard has ended: the root
// places the cell, the cell machine starts, and its controller moves the shard to the node of
// the host.
//
//dokimi:scenario
func firstMove(w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	s.settle(w, "the first move of the shard ends within the bound of the convergence", s.moved)
}

// abortedMove loses the reply of each attempt of the add of the first move of the shard of the
// simplest case after the handler has served it, so the move runs its abort path once its
// round of attempts ends. The losses strike the handler once the cell machine has started, so
// they leave the add of the cell machine itself alone. The scenario then settles until a later
// move of the shard has ended.
//
//dokimi:scenario
func abortedMove(w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	s.settle(w, "the cell machine starts within the bound of the convergence", s.launched)
	for range engine.Attempts {
		w.LoseAfter(handlerDependency)
	}
	s.settle(w, "a move of the shard ends within the bound of the convergence", s.moved)
}

// settledCommit loses the reply of the commit that starts the first move of the shard of the
// simplest case, after the store has applied the commit, so the controller of the cell settles
// the unknown outcome of a commit with a transition in flight. The loss strikes once the
// controller of the cell serves and the root has ended the move of the cell, so it leaves the
// commits of the root alone. The scenario then settles until the first move of the shard has
// ended.
//
//dokimi:scenario
func settledCommit(w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	s.settle(w, "the controller of the cell serves within the bound of the convergence", s.serves)
	s.store.FailAt(memory.PointCommitAck)
	s.settle(w, "the first move of the shard ends within the bound of the convergence", s.moved)
}

// editedSpec settles the simplest case until the first move of each shard has ended. An operator
// then submits a spec edit of the first namespace to the root of the first host, which selects the
// placement of the edits of the system. The scenario settles until the controller of the cell has
// taken the result of a planning pass under the spec of the edit, or until the call of the edit
// has ended while the root scope keeps the first version of its spec, as a crash of the root
// before its commit ends the call.
//
//dokimi:scenario
func editedSpec(w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	s.settle(w, "the first move of the shard ends within the bound of the convergence", s.moved)
	ns := s.specs[0].Namespace
	s.propose(w, editor, ns, s.hosts[0], proposal.SpecChange, operator)
	adopted, from := false, 0
	s.settle(w, "the controller of the cell takes a pass under the spec edit within the bound of the convergence",
		func(w *world) bool {
			ctx := w.Context()
			log := w.Log()
			if !adopted {
				_, st, _, placed := s.cellOf(ctx, ns)
				adopted, from = placed && st.Cell.SpecVersion > firstVersion, len(log)
				return !adopted && answered(w, editor) && s.version(ctx, ns) == firstVersion
			}
			return slices.ContainsFunc(log[from:], func(e sim.Entry[machine.Event, machine.Effect]) bool {
				_, planned := e.Event.(machine.Planned)
				return planned && !e.Dropped && sim.Dependency[role](w, w.Process(e.Process)) == roleCell
			})
		})
}

// editor is the client whose call submits the spec edit of the scenario editedSpec.
const editor = 0

// answered reports whether the last call of the client of w has ended: the last event of the
// client in the history of w is not an invocation. It reports false for a client without a call.
func answered(w *world, client int) bool {
	events := w.History().Events()
	for i := range slices.Backward(events) {
		if events[i].Client == client {
			return events[i].Kind != history.Invoke
		}
	}
	return false
}

// settle steps the process of w with the first pending event, and advances the time of w to
// its next timer, until done reports true. It fails the case with the statement what once the
// bound of the convergence has passed in the time of the run without it.
func (s *env) settle(w *world, what string, done func(w *world) bool) {
	deadline := w.Now().Add(settled())
	for !done(w) {
		if pending := w.Pending(); len(pending) > 0 {
			w.Step(pending[0])
			continue
		}
		gap, ok := w.Next()
		if !ok || w.Now().Add(gap).After(deadline) {
			assert.True(s.tb, done(w), what)
			return
		}
		w.Advance(gap)
	}
}

// launched reports whether a host of s has started a cell machine of the first namespace of w
// for the current epoch of its agent of the root.
func (s *env) launched(w *world) bool {
	ns := s.specs[0].Namespace
	return slices.ContainsFunc(s.hosts, func(h *host) bool {
		starter := w.Process(address{host: h.name, namespace: ns, role: roleRootAgent}.String())
		if starter == nil {
			return false
		}
		prefix := address{host: h.name, namespace: ns, role: roleCell}.String() + "/"
		for name, c := range h.cells {
			if c.machine != nil && c.by == starter.Epoch() && strings.HasPrefix(name, prefix) {
				return true
			}
		}
		return false
	})
}

// serves reports whether a controller of the cell of the first namespace of w serves, and the
// root of the namespace has no transition in flight.
func (s *env) serves(w *world) bool {
	ctx := w.Context()
	ns := s.specs[0].Namespace
	top, _ := s.stateOf(ctx, shard.Scope{Namespace: ns}, s.incarnationOf(ctx, ns))
	scope, _, _, placed := s.cellOf(ctx, ns)
	return placed && len(top.Transitions) == 0 && len(s.controllers(w, scope)) > 0
}

// moved reports whether each shard of the cell of the first namespace of w has a writer whose
// agent has a tenure at the epoch of its placement, and the cell has no transition in flight.
func (s *env) moved(w *world) bool {
	ns := s.specs[0].Namespace
	_, st, placements, placed := s.cellOf(w.Context(), ns)
	if !placed || len(placements) == 0 || len(st.Transitions) > 0 {
		return false
	}
	for i := range placements {
		p := &placements[i]
		if len(p.Replicas) != 1 {
			return false
		}
		at := slices.IndexFunc(s.hosts, func(h *host) bool { return h.node.ID == p.Replicas[0].Node })
		if at < 0 || s.hosts[at].parts[ns].agent == nil {
			return false
		}
		if token, err := s.hosts[at].parts[ns].agent.Token(p.Shard); err != nil || token.Epoch != p.Epoch {
			return false
		}
	}
	return true
}

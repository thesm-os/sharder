// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machinetest

import (
	"encoding/json"
	"errors"
	"math"
	"slices"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/sim"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/version"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/cell"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// settling is the state of the oracle of unknown outcomes: the entries of the log that it has
// read, the commit of each Commit and Outcome in flight, and the commit of each process whose
// last attempt has an unknown outcome.
type settling struct {
	// commits maps each Commit and Outcome in flight to the ID of its commit.
	commits map[waitKey]store.CommitID
	// unknown maps each epoch of a process to the commit whose last attempt failed with
	// version.ErrOutcomeUnknown, until the process settles it.
	unknown map[procKey]store.CommitID
	// log is the number of the entries of the log that the oracle has read.
	log int
}

// isolation is the state of the oracle of namespace incarnations: the commits, the sessions
// and the entries of the log that it has read, and the epochs that the commits of each scope
// put for each shard.
type isolation struct {
	// put maps each scope to each shard that a commit of the scope put, and each such shard to
	// the epochs of its placements.
	put map[scopeKey]map[shard.ID][]epoch.Epoch
	// commits is the number of the commits of the env that the oracle has read.
	commits int
	// joined is the number of the sessions of the env that the oracle has read.
	joined int
	// log is the number of the entries of the log that the oracle has read.
	log int
}

// callbackKey is a node incarnation of the callbacks of one epoch of the process of a controller.
type callbackKey struct {
	// node is the node of the callbacks.
	node shard.NodeID
	// proc is the epoch of the process of the controller.
	proc procKey
	// incarnation is the incarnation of the node that the callbacks address.
	incarnation epoch.Epoch
}

// bounding is the state of the oracle of the bounds on work in flight: the commits and the
// entries of the log that it has read, the transitions of each scope, and the callbacks in
// flight of each controller.
type bounding struct {
	// callbacks maps each Callback in flight to its node of its controller.
	callbacks map[waitKey]callbackKey
	// inflight maps each node of each controller to its callbacks in flight.
	inflight map[callbackKey]int
	// scopes replays the commits of each scope.
	scopes assignments
	// log is the number of the entries of the log that the oracle has read.
	log int
}

// abortsAwaitOutcomes checks that no abort path starts while the outcome of a commit is unknown.
// A machine whose attempt of a commit fails with version.ErrOutcomeUnknown settles the commit by
// a retry with the same commit ID or by Outcome. Until it has settled the commit, it commits no
// other mutation of the term of the commit that moves a transition onto its abort path or ends a
// transition there. A root that takes a new term acquires its scope with a commit of that term,
// and reads the outcome from the state of the scope. The store does not apply a commit of the
// earlier term after that acquisition.
//
//dokimi:invariant defect=presumedRejections
func abortsAwaitOutcomes(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	st := &s.settling
	log := w.Log()
	for ; st.log < len(log); st.log++ {
		e := &log[st.log]
		if e.Dropped {
			continue
		}
		proc := procKey{process: e.Process, epoch: e.Epoch}
		switch ev := e.Event.(type) {
		case machine.Committed:
			at := waitKey{process: e.Process, epoch: e.Epoch, token: ev.Token}
			id, sent := st.commits[at]
			delete(st.commits, at)
			switch {
			case !sent:
			case errors.Is(ev.Err, version.ErrOutcomeUnknown):
				st.unknown[proc] = id
			case ev.Err == nil && st.unknown[proc] == id:
				delete(st.unknown, proc)
			}
		case machine.OutcomeRead:
			at := waitKey{process: e.Process, epoch: e.Epoch, token: ev.Token}
			if id, sent := st.commits[at]; sent && st.unknown[proc] == id {
				delete(st.unknown, proc)
			}
			delete(st.commits, at)
		}
		for _, f := range e.Effects {
			switch f := f.(type) {
			case machine.Commit:
				id, unknown := st.unknown[proc]
				if unknown && f.Mutation.ID != id && f.Mutation.ID.Term == id.Term {
					assert.False(h, aborts(f.Mutation), h.Statement())
				}
				st.commits[waitKey{process: e.Process, epoch: e.Epoch, token: f.Token}] = f.Mutation.ID
			case machine.Outcome:
				st.commits[waitKey{process: e.Process, epoch: e.Epoch, token: f.Token}] = f.ID
			}
		}
	}
}

// aborts reports whether m moves a transition onto its abort path or ends a transition there: a
// transition in the phase shard.PhaseAborting, or the end of a transition with a placement
// without a writer.
func aborts(m *store.Mutation) bool {
	if slices.ContainsFunc(m.Transitions, func(t store.Transition) bool { return t.Phase == shard.PhaseAborting }) {
		return true
	}
	return slices.ContainsFunc(m.Put, func(p shard.Placement) bool {
		return len(p.Replicas) == 0 && slices.Contains(m.Done, p.Shard)
	})
}

// presumedRejections is the planted defect of abortsAwaitOutcomes: a cell machine that presumes
// the rejection of a commit whose outcome is unknown. When an attempt of a commit with a running
// transition fails with version.ErrOutcomeUnknown, the cell machine also commits the abort path
// of each such transition under a commit ID of its own.
func presumedRejections(c *prop.Case) func(w *world) {
	return build(c, c.Draw(presets, "preset"), &variant{cell: func(_ *env, m *cell.Machine) machine.Machine {
		sent := make(map[machine.Token]machine.Commit)
		planted := plantedTokens
		step := func(ev machine.Event, out []machine.Effect) []machine.Effect {
			from := len(out)
			out = m.Step(ev, out)
			for _, f := range out[from:] {
				if cm, isCommit := f.(machine.Commit); isCommit {
					sent[cm.Token] = cm
				}
			}
			cd, isResult := ev.(machine.Committed)
			cm, known := sent[cd.Token]
			if !isResult || !known || !errors.Is(cd.Err, version.ErrOutcomeUnknown) {
				return out
			}
			abort := cm
			abort.Mutation = cm.Mutation.CloneKanon()
			running := false
			for i := range abort.Mutation.Transitions {
				if t := &abort.Mutation.Transitions[i]; t.Phase == shard.PhaseRunning {
					t.Phase, t.Failed, t.Step = shard.PhaseAborting, t.Step, 0
					running = true
				}
			}
			if !running {
				return out
			}
			planted++
			abort.Mutation.ID.Seq = math.MaxUint64 - uint64(planted-plantedTokens)
			abort.Token = planted
			return append(out, abort)
		}
		return NewMachineStub(nil, MachineStubBenchMode(), MachineStubDelegateTo(m), WithMachineStep(step))
	}})
}

// incarnationsAreIsolated checks that no commit, callback, session or route of one namespace
// incarnation changes another incarnation. Each transition and each reservation of a commit
// has the namespace incarnation of the writer of the commit. Each call of the handler has a
// request and a fencing token of one incarnation. The membership grants each session in the
// incarnation that the header of its scope has. Each route of a router has a placement that a
// commit of the incarnation of the route put.
//
//dokimi:invariant defect=foreignTransitions
func incarnationsAreIsolated(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	is := &s.isolation
	for ; is.commits < len(s.commits); is.commits++ {
		c := &s.commits[is.commits]
		for i := range c.mutation.Transitions {
			assert.Equal(h, c.mutation.Transitions[i].Operation.Incarnation, c.incarnation, h.Statement())
		}
		for i := range c.mutation.Reserve {
			assert.Equal(h, c.mutation.Reserve[i].Disruption.Incarnation, c.incarnation, h.Statement())
		}
		k := scopeKey{scope: c.scope, incarnation: c.incarnation}
		if is.put[k] == nil {
			is.put[k] = make(map[shard.ID][]epoch.Epoch)
		}
		for i := range c.mutation.Put {
			p := &c.mutation.Put[i]
			is.put[k][p.Shard] = append(is.put[k][p.Shard], p.Epoch)
		}
	}
	for ; is.joined < len(s.joined); is.joined++ {
		if j := &s.joined[is.joined]; !j.current.IsZero() {
			assert.Equal(h, j.incarnation, j.current, h.Statement())
		}
	}
	log := w.Log()
	for ; is.log < len(log); is.log++ {
		for _, f := range log[is.log].Effects {
			if hd, ok := f.(machine.Handle); ok {
				assert.Equal(h, hd.Request.Operation.Incarnation, hd.Fence.Incarnation, h.Statement())
			}
		}
	}
	for _, u := range s.sight(w) {
		for i := range u.after.routes {
			r := &u.after.routes[i].route
			if u.after.routes[i].err != nil {
				continue
			}
			k := scopeKey{scope: shard.Scope{Namespace: u.namespace, Cell: r.Cell}, incarnation: r.Incarnation}
			assert.True(h, slices.Contains(is.put[k][r.Shard], r.Epoch), h.Statement())
		}
	}
}

// foreignTransitions is the planted defect of incarnationsAreIsolated: a cell machine whose
// commits have the transitions of another namespace incarnation. Each transition that a commit
// of the cell machine creates or advances has an operation of an incarnation that no namespace
// of the world has.
func foreignTransitions(c *prop.Case) func(w *world) {
	return build(c, c.Draw(presets, "preset"), &variant{cell: func(s *env, m *cell.Machine) machine.Machine {
		foreign := shard.NamespaceIncarnation{ID: s.nextID()}
		step := func(ev machine.Event, out []machine.Effect) []machine.Effect {
			from := len(out)
			out = m.Step(ev, out)
			for i, f := range out[from:] {
				cm, isCommit := f.(machine.Commit)
				if !isCommit || len(cm.Mutation.Transitions) == 0 {
					continue
				}
				cm.Mutation = cm.Mutation.CloneKanon()
				for j := range cm.Mutation.Transitions {
					cm.Mutation.Transitions[j].Operation.Incarnation = foreign
				}
				out[from+i] = cm
			}
			return out
		}
		return NewMachineStub(nil, MachineStubBenchMode(), MachineStubDelegateTo(m), WithMachineStep(step))
	}})
}

// transitionsAreBounded checks that the transitions in flight remain within maxTransitions and
// maxNodeTransitions, and that the callbacks in flight to a node remain within nodeCallbacks.
// The transitions of the scope of a cell, as its commits leave them, number at most the
// maxTransitions of its namespace. At most maxNodeTransitions of them have a node as their
// destination or their fallback. The callbacks to an incarnation of a node that one controller
// has sent, and whose results it has not taken, number at most nodeCallbacks. A view that ends
// the incarnation of a node ends the wait of the controller for each callback to it, so the
// callbacks to the next incarnation count apart.
//
//dokimi:invariant defect=boundlessMoves
func transitionsAreBounded(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	b := &s.bounding
	for ; b.scopes.commits < len(s.commits); b.scopes.commits++ {
		c := &s.commits[b.scopes.commits]
		sc, _ := b.scopes.apply(c)
		if c.scope.Cell == "" {
			continue
		}
		budget := s.budgets[c.scope.Namespace]
		assert.True(h, len(sc.transitions) <= *budget.MaxTransitions, h.Statement())
		loads := make(map[shard.NodeID]int)
		for _, t := range sc.transitions {
			for _, n := range [...]shard.NodeID{t.To.Node, t.Fallback.Node} {
				if n != "" {
					loads[n]++
				}
			}
		}
		for _, load := range loads {
			assert.True(h, load <= *budget.MaxNodeTransitions, h.Statement())
		}
	}
	log := w.Log()
	for ; b.log < len(log); b.log++ {
		e := &log[b.log]
		if ev, ok := e.Event.(machine.Called); ok && !e.Dropped {
			at := waitKey{process: e.Process, epoch: e.Epoch, token: ev.Token}
			if k, sent := b.callbacks[at]; sent {
				b.inflight[k]--
				delete(b.callbacks, at)
			}
		}
		for _, f := range e.Effects {
			cb, ok := f.(machine.Callback)
			if !ok || cb.Request.Scope.Cell == "" {
				continue
			}
			k := callbackKey{
				proc:        procKey{process: e.Process, epoch: e.Epoch},
				node:        cb.Node.ID,
				incarnation: cb.Request.NodeIncarnation,
			}
			b.callbacks[waitKey{process: e.Process, epoch: e.Epoch, token: cb.Token}] = k
			b.inflight[k]++
			assert.True(h, b.inflight[k] <= *s.budgets[cb.Request.Scope.Namespace].NodeCallbacks, h.Statement())
		}
	}
}

// boundlessMoves is the planted defect of transitionsAreBounded. Its cell machine admits moves
// beyond the bounds of its budget. The specs of the world bound the transitions in flight and
// the callbacks to a node to one, and the cell machine reads each spec of the root scope without
// those bounds, which then take the defaults of its preset.
func boundlessMoves(c *prop.Case) func(w *world) {
	boundless := func(s *env, m *cell.Machine) machine.Machine {
		step := func(ev machine.Event, out []machine.Effect) []machine.Effect {
			switch e := ev.(type) {
			case machine.PageRead:
				e.State.Spec = loosened(s.tb, e.State.Spec)
				ev = e
			case machine.Watched:
				e.Event.Mutation.Spec = loosened(s.tb, e.Event.Mutation.Spec)
				ev = e
			}
			return m.Step(ev, out)
		}
		return NewMachineStub(nil, MachineStubBenchMode(), MachineStubDelegateTo(m), WithMachineStep(step))
	}
	return build(c, c.Draw(presets, "preset"), &variant{tight: true, cell: boundless})
}

// loosened returns the spec raw without its bounds on the transitions in flight and on the
// callbacks to a node, and an empty raw unchanged. It fails tb for a spec that does not decode
// or encode.
func loosened(tb assert.TB, raw []byte) []byte {
	if len(raw) == 0 {
		return raw
	}
	var spec config.Spec
	assert.NoError(tb, json.Unmarshal(raw, &spec), "the spec of a root scope must decode")
	spec.Budget.MaxTransitions, spec.Budget.MaxNodeTransitions, spec.Budget.NodeCallbacks = nil, nil, nil
	out, err := json.Marshal(&spec)
	assert.NoError(tb, err, "a spec must encode")
	return out
}

// schemasAreRefused checks that a controller refuses a scope of a newer schema before its first
// commit: no commit applies to a scope whose header has a schema above the schema that this
// release writes.
//
//dokimi:invariant defect=hiddenSchemas
func schemasAreRefused(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	for ; s.schemas < len(s.commits); s.schemas++ {
		assert.True(h, s.commits[s.schemas].before.Schema <= store.Schema, h.Statement())
	}
}

// hiddenSchemas is the planted defect of schemasAreRefused. Its store returns each header of a
// scope with at most the schema that this release writes, so a controller reads the header of an
// upgraded scope as a header of its own release.
func hiddenSchemas(c *prop.Case) func(w *world) {
	return build(c, c.Draw(presets, "preset"), &variant{schemaless: true})
}

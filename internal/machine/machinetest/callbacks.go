// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machinetest

import (
	"context"
	"slices"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/sim"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// keptKey is a callback key in one epoch of the process of an agent.
type keptKey struct {
	// proc is the epoch of the process of the agent.
	proc procKey
	// key is the callback key.
	key protocol.Key
}

// keeping is the state of the oracle of the effects of callbacks: the entries of the log that
// it has read, the key of each call of the handler in flight, and the keys whose results the
// agents keep.
type keeping struct {
	// handles maps each Handle in flight to the key of its request.
	handles map[waitKey]protocol.Key
	// kept contains each key whose call of the handler the agent of one epoch of a process ended
	// without a transient error.
	kept map[keptKey]bool
	// log is the number of the entries of the log that the oracle has read.
	log int
}

// cleanupKey is the tenure of a cleanup: the fields of its callback key that the tenure fixes.
type cleanupKey struct {
	// node is the node of the tenure.
	node shard.NodeID
	// operation is the operation of the callback that started the tenure.
	operation shard.OperationID
	// incarnation is the incarnation of the node.
	incarnation epoch.Epoch
	// epoch is the epoch of the tenure.
	epoch epoch.Epoch
}

// coverage is the state of the oracle of the tenures: the entries of the log that it has read,
// and the keys under which the cleanups of each tenure took effect in the fenced store.
type coverage struct {
	// cleanups maps each tenure to the keys of its cleanups that took effect.
	cleanups map[cleanupKey][]protocol.Key
	// log is the number of the entries of the log that the oracle has read.
	log int
}

// callbacksTakeEffectOnce checks that each callback key has one effect in the fenced store, and
// that a resent key returns the kept result without a second call of the handler. An agent keeps
// the result of each call of the handler that ends without a transient error, so a Handle of
// a key whose result the agent of the same epoch keeps is a second call.
//
//dokimi:invariant defect=forgottenResults
func callbacksTakeEffectOnce(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	k := &s.keeping
	log := w.Log()
	for ; k.log < len(log); k.log++ {
		e := &log[k.log]
		proc := procKey{process: e.Process, epoch: e.Epoch}
		if ev, ok := e.Event.(machine.Handled); ok && !e.Dropped {
			at := waitKey{process: e.Process, epoch: e.Epoch, token: ev.Token}
			if key, running := k.handles[at]; running {
				delete(k.handles, at)
				if !errs.Retryable(machine.ClassifyAttempt(ev.Err)) {
					k.kept[keptKey{proc: proc, key: key}] = true
				}
			}
		}
		for _, f := range e.Effects {
			hd, ok := f.(machine.Handle)
			if !ok {
				continue
			}
			key := hd.Request.Key()
			assert.False(h, k.kept[keptKey{proc: proc, key: key}], h.Statement())
			assert.True(h, s.app.Effects(&key) <= 1, h.Statement())
			k.handles[waitKey{process: e.Process, epoch: e.Epoch, token: hd.Token}] = key
		}
	}
}

// forgottenResults is the planted defect of callbacksTakeEffectOnce: an agent that keeps no
// result of the handler. The agent of each cell takes each call of the handler that succeeded
// as one that ran past its deadline, so a resent key calls the handler again.
func forgottenResults(c *prop.Case) func(w *world) {
	return build(c, c.Draw(presets, "preset"), &variant{agent: func(_ *env, m *agent.Machine) machine.Machine {
		step := func(ev machine.Event, out []machine.Effect) []machine.Effect {
			if hd, ok := ev.(machine.Handled); ok && hd.Err == nil {
				hd.Err = context.DeadlineExceeded
				ev = hd
			}
			return m.Step(ev, out)
		}
		return NewMachineStub(nil, MachineStubBenchMode(), MachineStubDelegateTo(m), WithMachineStep(step))
	}})
}

// forgeriesAreRefused checks that a callback of an earlier node incarnation, of a caller that is
// not the controller, or with a key that another payload used, does not call a handler. The
// action forge sends such callbacks, and the step at which an agent takes one has no effect
// other than a reply with an error to it. An agent that has stopped takes the callback without
// an effect.
//
//dokimi:invariant defect=swallowedRefusals
func forgeriesAreRefused(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	log := w.Log()
	for ; s.forgeries < len(log); s.forgeries++ {
		e := &log[s.forgeries]
		do, ok := e.Event.(machine.Do)
		if !ok || e.Dropped || !s.forged[do.Request] {
			continue
		}
		for _, f := range e.Effects {
			r, isReply := f.(machine.Reply[struct{}])
			assert.True(h, isReply, h.Statement())
			assert.HasError(h, r.Err, h.Statement())
		}
	}
}

// swallowedRefusals is the planted defect of forgeriesAreRefused. Each agent, of a cell and of the
// root, replies to each callback without the error of its refusal, so a refused callback gets a
// reply with success.
func swallowedRefusals(c *prop.Case) func(w *world) {
	swallowing := func(_ *env, m *agent.Machine) machine.Machine {
		step := func(ev machine.Event, out []machine.Effect) []machine.Effect {
			from := len(out)
			out = m.Step(ev, out)
			do, isCall := ev.(machine.Do)
			if !isCall {
				return out
			}
			for i, f := range out[from:] {
				if r, ok := f.(machine.Reply[struct{}]); ok && r.To == do.Reply {
					r.Err = nil
					out[from+i] = r
				}
			}
			return out
		}
		return NewMachineStub(nil, MachineStubBenchMode(), MachineStubDelegateTo(m), WithMachineStep(step))
	}
	return build(c, c.Draw(presets, "preset"), &variant{agent: swallowing, rootAgent: swallowing})
}

// tenuresAreCovered checks that no tenure starts without a comparison that covers it, and that
// a cleanup applies once under its key. An agent of a cell acquires the fence of a tenure only
// while the placement of its shard at the last generation that the agent has applied lists the
// node at the epoch of the tenure. Each later generation that removes the placement and each
// snapshot then compare the tenure. The cleanups of one tenure take effect in the fenced store
// under one key. The oracle checks an acquisition at the last step of its process that it reads,
// because the agent has applied no later generation by then.
//
//dokimi:invariant defect=doubledCleanups
func tenuresAreCovered(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	c := &s.coverage
	log := w.Log()
	last := make(map[procKey]int)
	for i := c.log; i < len(log); i++ {
		last[procKey{process: log[i].Process, epoch: log[i].Epoch}] = i
	}
	for ; c.log < len(log); c.log++ {
		e := &log[c.log]
		for _, f := range e.Effects {
			switch f := f.(type) {
			case machine.Acquire:
				if last[procKey{process: e.Process, epoch: e.Epoch}] == c.log {
					s.covered(h, w, e.Process, e.Epoch, &f)
				}
			case machine.Handle:
				key := f.Request.Key()
				if f.Request.Path != protocol.Cleanup || s.app.Effects(&key) == 0 {
					continue
				}
				t := cleanupKey{
					node: key.Node, operation: key.Operation, incarnation: key.NodeIncarnation, epoch: key.Epoch,
				}
				if !slices.Contains(c.cleanups[t], key) {
					c.cleanups[t] = append(c.cleanups[t], key)
				}
				assert.Length(h, c.cleanups[t], 1, h.Statement())
			}
		}
	}
}

// covered checks the acquisition f of the agent of the process name at the epoch of the
// process. The placement of the shard of f at the generation that the agent has applied lists
// the node of the agent at the epoch of f. An agent of a process that has restarted since, or
// that no longer serves, is not checked.
func (s *env) covered(h *sim.Check, w *world, name string, epoch uint64, f *machine.Acquire) {
	p := w.Process(name)
	if p == nil || p.Epoch() != epoch {
		return
	}
	hh := sim.Dependency[*host](w, p)
	a := hh.parts[f.Fence.Namespace].agent
	o, err := a.Observed()
	if err != nil {
		return
	}
	scope := shard.Scope{Namespace: f.Fence.Namespace, Cell: o.Cell}
	pl, ok := s.placementAt(scopeKey{scope: scope, incarnation: o.Incarnation}, f.Fence.Shard, o.Generation)
	assert.True(h, ok, h.Statement())
	assert.Equal(h, pl.Epoch, f.Fence.Epoch, h.Statement())
	assert.True(h, slices.ContainsFunc(pl.Replicas, func(r shard.Replica) bool { return r.Node == hh.node.ID }),
		h.Statement())
}

// placementAt returns the placement of the shard sid that the commits of the scope k with a
// generation of at most g left, and reports false for a shard that none of them put.
func (s *env) placementAt(k scopeKey, sid shard.ID, g epoch.Epoch) (shard.Placement, bool) {
	var p shard.Placement
	found := false
	for i := range s.commits {
		c := &s.commits[i]
		if c.scope != k.scope || c.incarnation != k.incarnation || c.generation > g {
			continue
		}
		for j := range c.mutation.Put {
			if c.mutation.Put[j].Shard == sid {
				p, found = c.mutation.Put[j], true
			}
		}
	}
	return p, found
}

// doubledCleanups is the planted defect of tenuresAreCovered. The agent of each cell runs each
// cleanup twice under two keys: it calls the handler with each cleanup again, at the next step
// of the path of the cleanup.
func doubledCleanups(c *prop.Case) func(w *world) {
	return build(c, c.Draw(presets, "preset"), &variant{agent: func(_ *env, m *agent.Machine) machine.Machine {
		planted := plantedTokens
		step := func(ev machine.Event, out []machine.Effect) []machine.Effect {
			from := len(out)
			out = m.Step(ev, out)
			for _, f := range out[from:] {
				hd, ok := f.(machine.Handle)
				if !ok || hd.Request.Path != protocol.Cleanup {
					continue
				}
				again := hd.Request.CloneKanon()
				again.Step++
				planted++
				hd.Request, hd.Token = again, planted
				out = append(out, hd)
			}
			return out
		}
		return NewMachineStub(nil, MachineStubBenchMode(), MachineStubDelegateTo(m), WithMachineStep(step))
	}})
}

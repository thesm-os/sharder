// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machinetest

import (
	"context"
	"maps"
	"math"
	"slices"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/sim"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/cell"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/storetest"
)

// shardKey is a shard of one scope in one namespace incarnation.
type shardKey struct {
	// scope is the scope that places the shard.
	scope shard.Scope
	// incarnation is the namespace incarnation of the scope.
	incarnation shard.NamespaceIncarnation
	// shard is the shard.
	shard shard.ID
}

// epochs is the state of the invariant of epoch uniqueness: the commits and the callbacks that
// it has read, the epochs that the commits reserved for each shard, and the last transition of
// each shard.
type epochs struct {
	// reserved maps each shard to the set of the epochs that the commits reserved for it.
	reserved map[shardKey]map[epoch.Epoch]bool
	// highest maps each shard to the highest epoch that a commit reserved for it.
	highest map[shardKey]epoch.Epoch
	// operations maps each shard to the operation of its last transition.
	operations map[shardKey]shard.OperationID
	// commits is the number of the commits of the env that the invariant has read.
	commits int
	// callbacks is the number of the callbacks of the env that the invariant has read.
	callbacks int
}

// read reads each commit of commits that e has not read, and checks the epochs that its new
// transitions reserve. A transition is new when its operation differs from the operation of
// the last transition of its shard.
func (e *epochs) read(h *sim.Check, commits []commit) {
	for ; e.commits < len(commits); e.commits++ {
		c := &commits[e.commits]
		for i := range c.mutation.Transitions {
			t := &c.mutation.Transitions[i]
			k := shardKey{scope: c.scope, incarnation: t.Operation.Incarnation, shard: t.Operation.Shard}
			if e.operations[k] == t.Operation {
				continue
			}
			e.operations[k] = t.Operation
			if t.To.Node != "" {
				assert.NotEqual(h, t.NewEpoch, epoch.Zero, h.Statement())
			}
			if t.NewEpoch != epoch.Zero && t.AbortEpoch != epoch.Zero {
				assert.NotEqual(h, t.NewEpoch, t.AbortEpoch, h.Statement())
			}
			before := e.highest[k]
			set := e.reserved[k]
			if set == nil {
				set = make(map[epoch.Epoch]bool)
				e.reserved[k] = set
			}
			for _, reserved := range [...]epoch.Epoch{t.NewEpoch, t.AbortEpoch} {
				if reserved == epoch.Zero {
					continue
				}
				assert.True(h, reserved > before, h.Statement())
				set[reserved] = true
				e.highest[k] = max(e.highest[k], reserved)
			}
		}
	}
}

// scopeKey is one scope in one namespace incarnation.
type scopeKey struct {
	// scope is the scope.
	scope shard.Scope
	// incarnation is the namespace incarnation of the scope.
	incarnation shard.NamespaceIncarnation
}

// assignment is the placement and the transition of each shard of one scope, and the
// reservations of the scope, as the commits of the scope left them.
type assignment struct {
	// placements maps each shard to its placement.
	placements map[shard.ID]shard.Placement
	// transitions maps each shard with a transition to a copy of the record of its
	// transition.
	transitions map[shard.ID]*store.Transition
	// reservations maps the ID of each reservation of the scope to a copy of the reservation.
	reservations map[budget.ID]*budget.Reservation
}

// assignments is the state of an invariant that replays the commits of each scope: the
// commits that it has read, and the assignment of each scope.
type assignments struct {
	// scopes maps each scope to its assignment.
	scopes map[scopeKey]*assignment
	// commits is the number of the commits of the env that the invariant has read.
	commits int
}

// apply applies the commit c to the assignment of its scope, as the store applies it: the
// ends of the transitions, then the transitions, the placements, the reservations and the
// releases. It returns the assignment and the shards that c changes.
func (a *assignments) apply(c *commit) (*assignment, []shard.ID) {
	k := scopeKey{scope: c.scope, incarnation: c.incarnation}
	sc := a.scopes[k]
	if sc == nil {
		sc = &assignment{
			placements:   make(map[shard.ID]shard.Placement),
			transitions:  make(map[shard.ID]*store.Transition),
			reservations: make(map[budget.ID]*budget.Reservation),
		}
		a.scopes[k] = sc
	}
	m := c.mutation
	touched := slices.Clone(m.Done)
	for _, id := range m.Done {
		delete(sc.transitions, id)
	}
	for i := range m.Transitions {
		t := m.Transitions[i]
		sc.transitions[t.Operation.Shard] = &t
		touched = append(touched, t.Operation.Shard)
	}
	for i := range m.Put {
		sc.placements[m.Put[i].Shard] = m.Put[i]
		touched = append(touched, m.Put[i].Shard)
	}
	for i := range m.Reserve {
		r := m.Reserve[i]
		sc.reservations[r.Disruption.ID] = &r
	}
	for _, id := range m.Release {
		delete(sc.reservations, id)
	}
	return sc, touched
}

// fences is the state of the invariant of writer safety: the records and the acquisitions that
// it has read, the highest epoch that the fence reported as acquired for each fence key, and
// the tenures whose acquisitions the reference application recorded.
type fences struct {
	// acquired maps each fence key to the highest epoch of an acquisition that the fence
	// reported to an agent as done.
	acquired map[fenceKey]epoch.Epoch
	// recorded contains each tenure whose acquisition the reference application recorded.
	recorded map[tenure]bool
	// records is the number of the records of the reference application that the invariant
	// has read.
	records int
	// acquisitions is the number of the acquisitions of the env that the invariant has read.
	acquisitions int
}

// acquire reads each acquisition of acquisitions that f has not read and that the fence
// reported while the reference application had at most at records.
func (f *fences) acquire(acquisitions []acquisition, at int) {
	for ; f.acquisitions < len(acquisitions) && acquisitions[f.acquisitions].records <= at; f.acquisitions++ {
		a := &acquisitions[f.acquisitions]
		k := fenceKey{incarnation: a.token.Incarnation, shard: a.token.Shard}
		f.acquired[k] = max(f.acquired[k], a.token.Epoch)
	}
}

// reporting is the state of the oracle of the violations that the planning passes report: the
// input of each planning pass in flight, and the entries of the log that the oracle has read.
type reporting struct {
	// passes maps each Plan in flight to its input.
	passes map[waitKey]*placement.Input
	// log is the number of the entries of the log that the oracle has read.
	log int
}

// epochsAreUnique checks that no commit reserves an epoch of a shard twice, and that a commit
// reserves each epoch before a callback names it. A commit reserves the new epoch and the abort
// epoch of each transition that it creates. Each epoch that a new transition reserves is above
// every epoch that a commit reserved for its shard before, and the transition of a move to a
// node reserves an epoch for that node. Each callback that names an epoch names one that a
// commit reserved for its shard before the callback.
//
//dokimi:invariant defect=reusedEpochs
func epochsAreUnique(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	e := &s.epochs
	for ; e.callbacks < len(s.callbacks); e.callbacks++ {
		cb := &s.callbacks[e.callbacks]
		e.read(h, s.commits[:cb.commits])
		r := cb.request
		if r.Epoch == epoch.Zero {
			continue
		}
		k := shardKey{scope: r.Scope, incarnation: r.Operation.Incarnation, shard: r.Shard}
		assert.True(h, e.reserved[k][r.Epoch], h.Statement())
	}
	e.read(h, s.commits)
}

// reusedEpochs is the planted defect of epochsAreUnique. Its cell machine reserves the epoch of
// the placement again: each transition that a commit of the cell machine creates or advances has
// the epoch of its shard before the move as its new epoch.
func reusedEpochs(c *prop.Case) func(w *world) {
	return build(c, c.Draw(presets, "preset"), &variant{cell: func(_ *env, m *cell.Machine) machine.Machine {
		step := func(ev machine.Event, out []machine.Effect) []machine.Effect {
			n := len(out)
			out = m.Step(ev, out)
			for i, e := range out[n:] {
				cm, isCommit := e.(machine.Commit)
				if !isCommit || len(cm.Mutation.Transitions) == 0 {
					continue
				}
				cm.Mutation = cm.Mutation.CloneKanon()
				for j := range cm.Mutation.Transitions {
					cm.Mutation.Transitions[j].NewEpoch = cm.Mutation.Transitions[j].OldEpoch
				}
				out[n+i] = cm
			}
			return out
		}
		return NewMachineStub(nil, MachineStubBenchMode(), MachineStubDelegateTo(m), WithMachineStep(step))
	}})
}

// controlIsSafe checks that each commit of a scope is in the namespace incarnation of the
// scope, names the generation that it replaces, and has an epoch at or above the watermark of
// the scope. A commit to a scope without a namespace incarnation creates the scope in the
// incarnation of its writer.
//
//dokimi:invariant defect=admittingWriter
func controlIsSafe(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	for ; s.controls < len(s.commits); s.controls++ {
		c := &s.commits[s.controls]
		if !c.before.Incarnation.IsZero() {
			assert.Equal(h, c.incarnation, c.before.Incarnation, h.Statement())
		}
		assert.Equal(h, c.expect, c.before.Generation, h.Statement())
		assert.True(h, c.term >= c.before.Watermark, h.Statement())
	}
}

// admittingWriter is the planted defect of controlIsSafe. Its store writer admits each commit,
// also one of another generation or below the watermark of its scope, and applies a retry of a
// commit again. The writer commits at the generation and the watermark of the
// header of the scope before the commit, and under a commit ID of its own for a retry.
func admittingWriter(c *prop.Case) func(w *world) {
	committed := make(map[store.CommitID]bool)
	retries := uint64(0)
	return build(c, c.Draw(presets, "preset"), &variant{
		writer: func(s *env, f *machine.Commit, before store.Head) store.Writer {
			admitted := s.store.Writer(f.Scope, f.Incarnation, max(f.Term, before.Watermark))
			commit := func(ctx context.Context, _ epoch.Epoch, m *store.Mutation) (epoch.Epoch, error) {
				if committed[m.ID] {
					m = m.CloneKanon()
					retries++
					m.ID.Seq = math.MaxUint64 - retries
				}
				committed[m.ID] = true
				return admitted.Commit(ctx, before.Generation, m)
			}
			return storetest.NewWriterStub(nil,
				storetest.WriterStubBenchMode(),
				storetest.WriterStubDelegateTo(admitted),
				storetest.WithWriterCommit(commit),
			)
		},
	})
}

// writersAreSafe checks that each tenure starts after an acquisition of the fence of its shard
// at its epoch, and that no write of a tenure commits after a newer tenure of its shard has
// acquired the fence. The reference application records the start of a tenure as a Started
// record, each acquisition of its fence as an Acquired record, and each write as a Committed
// record. An acquisition counts from the report of the fence to the agent, and a tenure or a
// write at epoch.Zero is unfenced.
//
//dokimi:invariant defect=unfencedAcquire
func writersAreSafe(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	f := &s.fences
	records := s.app.Records()
	for ; f.records < len(records); f.records++ {
		f.acquire(s.acquisitions, f.records)
		r := &records[f.records]
		k := fenceKey{incarnation: r.Token.Incarnation, shard: r.Token.Shard}
		switch {
		case r.Token.Epoch == epoch.Zero:
		case r.Kind == Acquired:
			f.recorded[tenure{key: k, epoch: r.Token.Epoch}] = true
		case r.Kind == Started:
			assert.True(h, f.recorded[tenure{key: k, epoch: r.Token.Epoch}], h.Statement())
			assert.True(h, r.Token.Epoch >= f.acquired[k], h.Statement())
		case r.Kind == Committed:
			assert.True(h, r.Token.Epoch >= f.acquired[k], h.Statement())
		}
	}
	f.acquire(s.acquisitions, len(records))
}

// unfencedAcquire is the planted defect of writersAreSafe. Its agents end each acquisition of the
// fence of a shard as done without a call of the fence.
func unfencedAcquire(c *prop.Case) func(w *world) {
	return build(c, c.Draw(presets, "preset"), &variant{unfenced: true})
}

// rolesAreSafe checks that the replicas of each role of each shard, with the started
// transitions that add a replica to the role, do not exceed the bound of the role. A started
// transition adds a replica to a role when its destination has the role and its source does
// not, until the placement of its shard lists the destination in the role. A move inside a
// role keeps the count of the role. The root places each cell with the roles of primary-only.
//
//dokimi:invariant defect=earlyAdds
func rolesAreSafe(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	a := &s.assignments
	for ; a.commits < len(s.commits); a.commits++ {
		c := &s.commits[a.commits]
		sc, touched := a.apply(c)
		roles := s.roles[c.scope.Namespace]
		if c.scope.Cell == "" {
			roles = replication.PrimaryOnly.Roles
		}
		for _, id := range touched {
			p := sc.placements[id]
			t, moving := sc.transitions[id]
			for _, r := range roles {
				count := 0
				for _, rep := range p.Replicas {
					if rep.Role == r.Name {
						count++
					}
				}
				adds := moving && t.To.Role == r.Name && t.From.Role != r.Name
				if adds && !slices.Contains(p.Replicas, shard.Replica{Node: t.To.Node, Role: r.Name}) {
					count++
				}
				assert.True(h, count <= r.Max, h.Statement())
			}
		}
	}
}

// admissionsAreBounded checks that each admitted disruption of a cell keeps the unavailable
// replicas and the reservations within the budget of its namespace. The drain roles of the
// support matrix are every role, so a negotiable disruption that the budget admits leaves no
// replica of the drain roles on its nodes, and no transition in flight to its nodes in a
// drain role. A commit that admits a negotiable disruption leaves at most maxConcurrentNodes
// nodes that host a replica under the admitted disruptions. A disruption that is not
// negotiable starts at its Start under any decision, and one move makes the one replica of
// its shard unavailable, which the maxUnavailable of 1 admits.
//
//dokimi:invariant defect=eagerAdmissions
func admissionsAreBounded(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	a := &s.admissions
	for ; a.commits < len(s.commits); a.commits++ {
		c := &s.commits[a.commits]
		sc, _ := a.apply(c)
		if c.scope.Cell == "" {
			continue
		}
		b := s.budgets[c.scope.Namespace]
		covered := make(map[shard.NodeID]bool)
		for _, r := range sc.reservations {
			if r.Admitted.IsZero() {
				continue
			}
			for _, n := range r.Disruption.Nodes {
				hosted, drained, inbound := 0, 0, 0
				for _, p := range sc.placements {
					for _, rep := range p.Replicas {
						if rep.Node != n {
							continue
						}
						hosted++
						if slices.Contains(b.DrainRoles, rep.Role) {
							drained++
						}
					}
				}
				for _, t := range sc.transitions {
					to := t.To.Node == n && slices.Contains(b.DrainRoles, t.To.Role)
					if to || t.Fallback.Node == n && slices.Contains(b.DrainRoles, t.Fallback.Role) {
						inbound++
					}
				}
				if r.Disruption.Negotiable {
					assert.Equal(h, drained, 0, h.Statement())
					assert.Equal(h, inbound, 0, h.Statement())
				}
				if hosted > 0 {
					covered[n] = true
				}
			}
		}
		admits := slices.ContainsFunc(c.mutation.Reserve, func(r budget.Reservation) bool {
			return !r.Admitted.IsZero() && r.Disruption.Negotiable
		})
		if admits {
			assert.True(h, len(covered) <= *b.MaxConcurrentNodes, h.Statement())
		}
	}
}

// eagerAdmissions is the planted defect of admissionsAreBounded: a cell machine that admits
// every disruption. Each reservation that a commit of a cell machine reserves is admitted at
// the Start of its disruption.
func eagerAdmissions(c *prop.Case) func(w *world) {
	return build(c, c.Draw(presets, "preset"), &variant{cell: func(_ *env, m *cell.Machine) machine.Machine {
		step := func(ev machine.Event, out []machine.Effect) []machine.Effect {
			n := len(out)
			out = m.Step(ev, out)
			for i, e := range out[n:] {
				cm, isCommit := e.(machine.Commit)
				if !isCommit || len(cm.Mutation.Reserve) == 0 {
					continue
				}
				cm.Mutation = cm.Mutation.CloneKanon()
				for j := range cm.Mutation.Reserve {
					if r := &cm.Mutation.Reserve[j]; r.Admitted.IsZero() {
						r.Admitted = r.Disruption.Start
					}
				}
				out[n+i] = cm
			}
			return out
		}
		return NewMachineStub(nil, MachineStubBenchMode(), MachineStubDelegateTo(m), WithMachineStep(step))
	}})
}

// constraintsHold checks that each replica that a commit of a cell adds or moves satisfies
// every hard constraint of the spec version that planned its move. The specs of a simulation
// carry one hard constraint: labels with the rules of zoneRules, so no replica runs on a node of
// the zone west. The first version of a spec of an unconstrained variant has none, so a move of
// that version can place a replica in the zone west. A replica of a placement is on the
// incarnation of its node that the placement names, and has the labels with which that
// incarnation joined the cell. A replica that a changed node makes invalid stays until repair
// moves it, so the invariant checks a replica only in the commit that adds it.
//
//dokimi:invariant defect=misplacedCommits
func constraintsHold(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	a := &s.constraints
	for ; a.commits < len(s.commits); a.commits++ {
		c := &s.commits[a.commits]
		olds := make([]shard.Placement, len(c.mutation.Put))
		versions := make(map[shard.ID]epoch.Epoch, len(c.mutation.Put))
		if sc := a.scopes[scopeKey{scope: c.scope, incarnation: c.incarnation}]; sc != nil {
			for i := range c.mutation.Put {
				sid := c.mutation.Put[i].Shard
				olds[i] = sc.placements[sid]
				if t := sc.transitions[sid]; t != nil {
					versions[sid] = t.SpecVersion
				}
			}
		}
		for i := range c.mutation.Transitions {
			t := &c.mutation.Transitions[i]
			versions[t.Operation.Shard] = t.SpecVersion
		}
		a.apply(c)
		if c.scope.Cell == "" {
			continue
		}
		for i := range c.mutation.Put {
			p := &c.mutation.Put[i]
			if s.variant.unconstrained && versions[p.Shard] <= firstVersion {
				continue
			}
			for _, r := range p.Replicas {
				labels, joined := s.labels[nodeKey{scope: c.scope, node: r.Node, incarnation: p.Incarnation}]
				if slices.Contains(olds[i].Replicas, r) || !joined {
					continue
				}
				for _, rule := range zoneRules {
					if rule.Role != shard.RoleNone && rule.Role != r.Role {
						continue
					}
					label, has := labels.Find(rule.Key)
					var admitted bool
					switch rule.Operator {
					case constraint.In:
						admitted = has && slices.Contains(rule.Values, label.Value)
					case constraint.NotIn:
						admitted = !has || !slices.Contains(rule.Values, label.Value)
					case constraint.Exists:
						admitted = has
					default:
						admitted = !has
					}
					assert.True(h, admitted, h.Statement())
				}
			}
		}
	}
}

// misplacedCommits is the planted defect of constraintsHold. Its cell machine commits a replica
// against a constraint. Each placement that a commit of the cell machine puts with a transition
// has its first replica on the node of the zone west with the lowest ID among the
// nodes that joined the cell, at its latest incarnation in the cell.
func misplacedCommits(c *prop.Case) func(w *world) {
	return build(c, c.Draw(presets, "preset"), &variant{cell: func(s *env, m *cell.Machine) machine.Machine {
		step := func(ev machine.Event, out []machine.Effect) []machine.Effect {
			n := len(out)
			out = m.Step(ev, out)
			for i, e := range out[n:] {
				cm, isCommit := e.(machine.Commit)
				if !isCommit || len(cm.Mutation.Transitions) == 0 {
					continue
				}
				var west nodeKey
				for k, labels := range s.labels {
					if k.scope != cm.Scope || labels.Get(zoneLabel) != zoneWest {
						continue
					}
					first := west.node == "" || k.node < west.node
					if first || k.node == west.node && k.incarnation > west.incarnation {
						west = k
					}
				}
				if west.node == "" {
					continue
				}
				cm.Mutation = cm.Mutation.CloneKanon()
				for j := range cm.Mutation.Put {
					if p := &cm.Mutation.Put[j]; len(p.Replicas) > 0 {
						p.Replicas[0].Node, p.Incarnation = west.node, west.incarnation
					}
				}
				out[n+i] = cm
			}
			return out
		}
		return NewMachineStub(nil, MachineStubBenchMode(), MachineStubDelegateTo(m), WithMachineStep(step))
	}})
}

// violationsAreReported checks that each planning pass of a cell machine reports as a violation
// each replica that its plan keeps against the constraint of the zones. A plan keeps the replicas
// of a shard that it has no target for, and each replica that the target of its shard lists. The
// oracle checks each such replica on a node of the input of the pass in the zone west, under a
// spec version with the constraint. The first version of a spec of an unconstrained variant has no
// constraint. The root places the cells under a spec of its own, which has the constraint of
// capacity alone, so the oracle does not check the passes of a root.
//
//dokimi:invariant defect=unreportedViolations
func violationsAreReported(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	r := &s.reporting
	log := w.Log()
	for ; r.log < len(log); r.log++ {
		e := &log[r.log]
		if ev, planned := e.Event.(machine.Planned); planned {
			at := waitKey{process: e.Process, epoch: e.Epoch, token: ev.Token}
			if in := r.passes[at]; in != nil && ev.Err == nil && !e.Dropped {
				s.reported(h, in, &ev)
			}
			delete(r.passes, at)
		}
		for _, f := range e.Effects {
			p, isPlan := f.(machine.Plan)
			if isPlan && sim.Dependency[role](w, w.Process(e.Process)) == roleCell {
				r.passes[waitKey{process: e.Process, epoch: e.Epoch, token: p.Token}] = p.Input
			}
		}
	}
}

// unreportedViolations is the planted defect of violationsAreReported. Its runtime of each
// planning pass drops the violations of the pass. Its system starts with one host of the zone
// west and the most slots in each namespace. The first version of each spec has no constraint,
// so the first placement puts every shard on the node of that host. Each pass examines one shard
// as the source of a move, so each pass after the spec edit that adds the constraint keeps the
// other shards in the zone west. The scenario editedSpec submits that edit.
func unreportedViolations(c *prop.Case) func(w *world) {
	return build(c, c.Draw(presets, "preset"),
		&variant{unreported: true, unconstrained: true, sources: 1, crowd: zoneWest})
}

// earlyAdds is the planted defect of rolesAreSafe. Its cell machine adds the new writer of a move
// before it drops the old one: each placement that a commit of the cell machine puts with a
// transition of its shard also lists the source of the transition.
func earlyAdds(c *prop.Case) func(w *world) {
	return build(c, c.Draw(presets, "preset"), &variant{cell: func(_ *env, m *cell.Machine) machine.Machine {
		step := func(ev machine.Event, out []machine.Effect) []machine.Effect {
			n := len(out)
			out = m.Step(ev, out)
			for i, e := range out[n:] {
				cm, isCommit := e.(machine.Commit)
				if !isCommit || len(cm.Mutation.Transitions) == 0 {
					continue
				}
				cm.Mutation = cm.Mutation.CloneKanon()
				for j := range cm.Mutation.Put {
					p := &cm.Mutation.Put[j]
					at := slices.IndexFunc(cm.Mutation.Transitions, func(t store.Transition) bool {
						return t.Operation.Shard == p.Shard
					})
					if at < 0 {
						continue
					}
					from := cm.Mutation.Transitions[at].From
					listed := slices.ContainsFunc(p.Replicas, func(r shard.Replica) bool { return r.Node == from.Node })
					if from.Node != "" && !listed {
						p.Replicas = append(p.Replicas, shard.Replica{Node: from.Node, Role: from.Role})
					}
				}
				out[n+i] = cm
			}
			return out
		}
		return NewMachineStub(nil, MachineStubBenchMode(), MachineStubDelegateTo(m), WithMachineStep(step))
	}})
}

// reported checks the violations of the planning pass ev over the input in. Each replica that the
// plan of ev keeps on a node of the zone west that the input lists is a violation of ev, when the
// spec version of the input has the constraint of the zones.
func (s *env) reported(h *sim.Check, in *placement.Input, ev *machine.Planned) {
	if s.variant.unconstrained && in.Snapshot.SpecVersion <= firstVersion {
		return
	}
	west := make(map[shard.NodeID]bool, len(in.Nodes))
	for _, n := range in.Nodes {
		west[n.ID] = slices.Contains(n.Labels, tag.Tag{Key: zoneLabel, Value: zoneWest})
	}
	for _, sid := range slices.SortedFunc(maps.Keys(in.Current.Placements), shard.ID.Compare) {
		t, changed := ev.Plan.Targets[sid]
		for _, r := range in.Current.Placements[sid].Replicas {
			if !west[r.Node] || changed && !slices.Contains(t.Replicas, r) {
				continue
			}
			assert.True(h, slices.ContainsFunc(ev.Violations, func(v placement.Violation) bool {
				return v.Shard == sid && v.Node == r.Node
			}), h.Statement())
		}
	}
}

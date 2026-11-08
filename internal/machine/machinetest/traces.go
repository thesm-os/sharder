// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machinetest

import (
	"slices"

	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/sim"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/proposal"
)

// The names of the actions that the traces take: the actions of MachineSimulation, which dokimi
// names after their functions, and the action of a trace alone.
const (
	actionChange      = "change"
	actionCloseAgent  = "closeAgent"
	actionCompact     = "compact"
	actionDisrupt     = "disrupt"
	actionDone        = "done"
	actionFault       = "fault"
	actionForge       = "forge"
	actionJoin        = "join"
	actionLoseRenewal = "loseRenewal"
	actionReadRoutes  = "readRoutes"
	actionRecreate    = "recreate"
	actionRefresh     = "refresh"
	actionSubmit      = "submit"
	actionUpgrade     = "upgrade"
	actionWrite       = "write"
	actionDrainFirst  = "drainFirst"
)

// drainFirst drains the node of the first host from the cell of a namespace, as a call of an
// operator. The case chooses the namespace. The trace of a crowded system takes it, and
// MachineSimulation does not.
var drainFirst = action{
	Name:  actionDrainFirst,
	Call:  true,
	Input: func(c *prop.Case) any { return c.Draw(prop.Integer(0, maxNamespaces-1), "namespace") },
	Run: func(w *world, client int, in any) {
		at, _ := in.(int)
		s := sim.Dependency[*env](w, w.Process(firstProcess))
		if at < len(s.specs) {
			s.propose(w, client, s.specs[at].Namespace, s.hosts[0], proposal.Drain, operator)
		}
	},
}

// Trace is a failure trace of a namespace: the behaviour that it checks, the actions that it
// takes, the faults that it names and the inputs of its system. [Trace.Simulation] returns its
// simulation over a preset.
//
// # Concurrency
//
// A Trace is safe for concurrent use, because its methods do not change it.
type Trace struct {
	// Name states the behaviour that the trace checks, as the name of a case of a test.
	Name string
	// actions are the names of the actions that the trace takes.
	actions []string
	// faults are the faults that the trace names.
	faults []sim.Fault
	// variant fixes the inputs of the system of the trace.
	variant variant
	// crashes reports that the trace runs the scenarios of MachineSimulation and their crash
	// points.
	crashes bool
}

// Simulation returns the simulation of t over the preset p: MachineSimulation with the system of
// p and the inputs of t, the actions of t, and the faults of t with their bounds. It requires the
// share of MachineSimulation for each of those faults that MachineSimulation requires, and
// converges within the bound of the inputs of t. It keeps the scenarios of MachineSimulation for
// a trace that runs the crash points.
func (t *Trace) Simulation(p config.PresetName) sim.Simulation[machine.Event, machine.Effect, struct{}] {
	s := MachineSimulation()
	v := t.variant
	s.System = func(c *prop.Case) func(w *world) { return build(c, p, &v) }
	s.Actions = slices.DeleteFunc(append(s.Actions, drainFirst), func(a action) bool {
		return !slices.Contains(t.actions, a.Name)
	})
	tolerate, require := make(map[sim.Fault]int, len(t.faults)), make(map[sim.Fault]float64, len(t.faults))
	for _, f := range t.faults {
		tolerate[f] = s.Tolerate[f]
		if share, ok := s.Require[f]; ok {
			require[f] = share
		}
	}
	s.Tolerate, s.Require = tolerate, require
	within := v.bound()
	for i := range s.Converges {
		s.Converges[i].Within = within
	}
	if !t.crashes {
		s.Scenarios = nil
	}
	return s
}

// Traces returns the failure traces of a namespace that the simulation runs, each with a
// simulation over every preset. Each trace runs the invariants after every step and the
// convergence once its faults have ended. The retries of router.Do have no trace, because
// router.Do is not a machine.
func Traces() []Trace {
	return []Trace{
		{
			Name:    "resumes each move after a crash of its controller",
			actions: []string{actionWrite},
			faults:  []sim.Fault{sim.FaultCrash, sim.FaultRestart},
			crashes: true,
		},
		{
			Name:   "abandons the abort path of a fallback that restarts",
			faults: []sim.Fault{sim.FaultCrash, sim.FaultRestart, sim.FaultLoseAfter},
		},
		{
			Name:   "completes an abort drop to a node that restarted",
			faults: []sim.Fault{sim.FaultCrash, sim.FaultRestart},
		},
		{
			Name:   "ends the first placement of a shard whose add fails",
			faults: []sim.Fault{sim.FaultCrash, sim.FaultRestart, sim.FaultLoseBefore, sim.FaultLoseAfter},
		},
		{
			Name:    "settles a commit whose reply is lost",
			actions: []string{actionCompact},
			faults:  []sim.Fault{sim.FaultLoseBefore, sim.FaultLoseAfter},
		},
		{
			Name:    "fences the tenure of an add whose reply is lost",
			actions: []string{actionWrite},
			faults:  []sim.Fault{sim.FaultLoseAfter},
		},
		{
			Name:    "refuses a callback of an earlier incarnation of its node",
			actions: []string{actionForge},
			faults:  []sim.Fault{sim.FaultCrash, sim.FaultRestart, sim.FaultDelay},
		},
		{
			Name:    "refuses a callback key that arrives with another payload",
			actions: []string{actionForge},
			faults:  []sim.Fault{sim.FaultLoseAfter},
		},
		{
			Name:    "fences the writes of a writer that paused beyond its TTL",
			actions: []string{actionWrite},
			faults:  []sim.Fault{sim.FaultPause},
		},
		{
			Name:    "keeps a granted disruption across a failure of its controller",
			actions: []string{actionDisrupt},
			faults:  []sim.Fault{sim.FaultCrash, sim.FaultRestart},
		},
		{
			Name:    "continues a deferred drain across a failure of its controller",
			actions: []string{actionDisrupt, actionDone},
			faults:  []sim.Fault{sim.FaultCrash, sim.FaultRestart},
		},
		{
			Name:   "starts one transition into the last capacity of a node",
			faults: []sim.Fault{sim.FaultCrash, sim.FaultRestart, sim.FaultPause, sim.FaultLoseAfter},
		},
		{
			Name:    "moves the shard of a replica with a permanent fault",
			actions: []string{actionFault},
		},
		{
			Name:    "drains every shard of a node within the bounds of the budget",
			actions: []string{actionJoin, actionDrainFirst},
			variant: variant{tight: true, crowd: zoneEast},
		},
		{
			Name:    "gives replicas to a node that joins the cell",
			actions: []string{actionSubmit, actionJoin},
			variant: variant{tight: true},
		},
		{
			Name:    "counts the transitions in flight apart from the shards without a writer",
			faults:  []sim.Fault{sim.FaultLoseAfter},
			variant: variant{tight: true},
		},
		{
			Name:    "repairs the shards of a pass that examines one source shard",
			actions: []string{actionSubmit},
			variant: variant{sources: 1},
		},
		{
			Name:    "admits two planned disruptions that race an unplanned failure",
			actions: []string{actionDisrupt},
			faults:  []sim.Fault{sim.FaultCrash},
		},
		{
			Name:    "drains three stopping nodes within the bounds of the budget",
			actions: []string{actionDisrupt},
			variant: variant{tight: true},
		},
		{
			Name: "completes the first move under the default budget",
		},
		{
			Name:    "keeps the views of reports that arrive twice",
			actions: []string{actionReadRoutes},
			faults:  []sim.Fault{sim.FaultDelay, sim.FaultDuplicate},
		},
		{
			Name:    "isolates two namespaces that reuse every identifier",
			actions: []string{actionRecreate},
			faults:  []sim.Fault{sim.FaultDelay},
			variant: variant{paired: true},
		},
		{
			Name:    "refuses the calls of an identity that is not an operator",
			actions: []string{actionSubmit, actionDisrupt, actionDone, actionForge},
		},
		{
			Name:    "keeps the reservation of a close whose context expires",
			actions: []string{actionCloseAgent, actionDisrupt},
		},
		{
			Name:   "serves the writers through an outage of the control plane",
			faults: []sim.Fault{sim.FaultCrash, sim.FaultPause, sim.FaultPartition, sim.FaultOutage},
		},
		{
			Name:    "repairs an assignment that a change of labels invalidates",
			actions: []string{actionSubmit, actionChange},
			faults:  []sim.Fault{sim.FaultCrash, sim.FaultRestart},
		},
		{
			Name:    "completes a transition in flight when a spec edit adds a constraint",
			actions: []string{actionSubmit},
			variant: variant{unconstrained: true},
		},
		{
			Name:    "refuses a scope of a newer schema before any commit",
			actions: []string{actionUpgrade},
		},
		{
			Name:    "reports no rejection of an unknown commit after a takeover",
			actions: []string{actionCompact},
			faults:  []sim.Fault{sim.FaultCrash, sim.FaultLoseAfter},
		},
		{
			Name:    "refuses a read that resumes across a recreation",
			actions: []string{actionRecreate},
			faults:  []sim.Fault{sim.FaultDelay},
		},
		{
			Name:    "retries a callback that arrives before the record of its controller",
			actions: []string{actionForge},
			faults:  []sim.Fault{sim.FaultCrash, sim.FaultDelay},
		},
		{
			Name:    "ignores a removal that arrives after a newer add",
			actions: []string{actionReadRoutes},
			faults:  []sim.Fault{sim.FaultDelay, sim.FaultLoseAfter},
		},
		{
			Name:    "applies a cleanup once across a replacement snapshot",
			actions: []string{actionCompact},
			faults:  []sim.Fault{sim.FaultLoseBefore, sim.FaultLoseAfter},
		},
		{
			Name:    "drops an obsolete add at the comparison of a replacement snapshot",
			actions: []string{actionCompact},
			faults:  []sim.Fault{sim.FaultDelay},
		},
		{
			Name:    "covers each tenure that starts during a replacement snapshot",
			actions: []string{actionCompact},
		},
		{
			Name:    "routes with the new mapping after a recreation",
			actions: []string{actionRecreate, actionRefresh},
		},
		{
			Name:    "keeps a node draining until its last drain ends",
			actions: []string{actionSubmit, actionDisrupt, actionDone},
			faults:  []sim.Fault{sim.FaultCrash},
		},
		{
			Name:    "ends a session under stop at the deadline of its last accepted renewal",
			actions: []string{actionLoseRenewal},
			variant: variant{stop: true},
		},
	}
}

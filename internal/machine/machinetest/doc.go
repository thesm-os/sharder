// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package machinetest contains the double of machine.Machine and the deterministic simulation
// of the machines of a namespace, which dokimi generates from the directives of
// internal/machine.
//
// # Doubles
//
// [MachineStub] is a recording double of [machine.Machine]. Each method of the double
// returns the results of a function, fixed results or an armed fault.
//
// # Simulation
//
// [MachineSimulation] is the simulation of the machines of the namespaces of a case on the
// hosts of the topology embedded. Its system builds the hosts with their machines, one memory
// store in its manual mode and one reference application. Its performers perform the effects
// of the machines against them, and its actions are the calls and the changes of operators,
// applications and the environment. Its invariants are the safety properties of the namespace
// and the oracles of the failure traces, each with a planted defect. Its convergence checks the
// settled state once the faults have ended, and its scenarios are the runs that the crash
// points cover. The machines of an incarnation of a host read the time through a hybrid logical
// clock over the clock of the host, and report their instruments to a recorder of the
// incarnation. The store reads the time of the run.
//
// [Traces] returns the failure traces of a namespace. [Trace.Simulation] returns the simulation
// of a trace over a preset: [MachineSimulation] with the actions of the trace, the faults that it
// names and the inputs of its system.
//
// # Reference application
//
// [FencedStore] is the storage of the reference application. It keeps a fence for each
// shard, the write transactions of the tenures, and the keys of the callbacks with their
// effects. [FencedStore.Acquire] waits for the write transactions that checked the fence
// before it without blocking, and [FencedStore.Records] lists each acquisition, tenure and
// write in order.
//
// # Errors
//
// The errors of the package wrap [ErrEnded] and [ErrUnknownRecordKind].
//
// # Dependency position
//
// machinetest imports the standard library, go.dokimi.dev/assert with its packages expect and
// prop, go.dokimi.dev/sim and go.dokimi.dev/stub. It imports the packages clock, clock/fake,
// clock/hlc, coretest/telemetrytest, epoch, errs, id, id/ulid, page, rand, rand/seeded, tag,
// telemetry and version of go.thesmos.sh/core. It imports the packages budget, builtin, config,
// config/configtest, fence, handoff, internal/control, internal/engine, internal/machine with
// agent, cell, root and routes, membership, ownership, partition with fixedslots, placement with
// constraint and planner, proposal, protocol, replication, shard, store, store/memory and
// store/storetest of sharder.
//
//dokimi:sentinel
package machinetest

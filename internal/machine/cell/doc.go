// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package cell runs the controller of one cell of a namespace as a state machine.
//
// The controller of a cell places the shards of the cell on its nodes and runs the moves of
// their replicas. It also admits the planned disruptions of the nodes. [Machine] is that
// controller as a machine of package go.thesmos.sh/sharder/internal/machine. It appends
// effects and takes their results as events, so a runtime performs every effect and a
// simulation replays a run. The machine reads the root scope, and runs the controller of
// package go.thesmos.sh/sharder/internal/control for the scope of the cell.
//
// # Lifecycle
//
// A table of core's fsm declares the states of a Machine and the event of each edge
// between them. After Started, the machine reads the root scope. The first snapshot gives
// it the range of the cell, the mapping that the root recorded and the accepted spec. The
// root places the cell as a shard whose ID has the ULID of the cell ID. The controller then
// acquires the scope of the cell at Config.Term, continues every transition of the scope,
// and serves. The machine replies to Started once the controller serves. An error stops
// the machine in a terminal state.
//
// # Specs
//
// The machine resolves each accepted spec with config.Resolve and builds its strategies
// with the registry of Config. A spec must resolve to the mapping that the root recorded.
// The machine passes the strategies of each newer spec to its controller, which adopts
// them at the next pass. The machine logs a newer spec that does not apply, and keeps its
// own.
//
// # Bounds
//
// The machine logs the cell once when its view rises above cells.maxNodes of the last spec
// that it resolved, and once when its layout rises above cells.maxShards. The root does
// not split cells.
//
// # Proposals
//
// The controller of a cell applies the proposals of the kinds move, drain, undrain and
// explain, and refuses a spec proposal.
//
// # Errors
//
// [ErrConfig] classifies as errs.Invalid, and [ErrRoot] classifies as errs.Conflict. A
// call before the controller serves gets control.ErrNotServing, and so does a call that
// waits for its reply when the machine stops. Started gets the error that stopped the
// machine.
//
// # Concurrency
//
// A Machine is not safe for concurrent use. A runtime calls Step from one goroutine at a
// time. [Machine.Err] is safe for concurrent use.
//
// # Dependency position
//
// cell imports the standard library, the packages clock, epoch, errs, fsm, rand and
// telemetry of go.thesmos.sh/core, and the packages config, internal/control,
// internal/follow, internal/machine, proposal, shard and store of go.thesmos.sh/sharder.
//
//dokimi:sentinel
package cell

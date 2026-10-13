// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package controller runs one controller process: the root of each namespace of the process,
// and the controllers of the cells that the roots place on it.
//
// A [Controller] serves every namespace of its process. [New] takes the store, the
// membership, the client of the callbacks and the dependencies of the process in a [Config].
// [Controller.Run] takes the spec of each namespace. The registry of the Config builds the
// strategies of each namespace from the spec that its root scope stores, with the
// capabilities that the store and the membership provide.
//
// # Namespaces
//
// For each namespace, the process runs three kinds of machines of package
// go.thesmos.sh/sharder/internal/machine, each under a driver of its own:
//
//   - The root, which campaigns for the root scope. The root that leads creates the namespace
//     when the root scope has no commit, and places the cells of the namespace on the
//     controller processes.
//   - The agent of the root, which joins the root scope with the node of the process and its
//     cell capacity. It checks each callback of the root as the agent of a node checks a
//     callback of the controller of its cell, and runs the callbacks of one cell one at a time.
//   - The controller of each cell that the root places on the process.
//
// The drivers of the three kinds run as the tasks of one call of core's task.Run, so Run
// returns once the goroutines of the process have returned.
//
// # Cells
//
// An add of the root starts the controller of its cell at the epoch of the add, and returns
// once the acquisition of the scope of the cell has applied. That acquisition fences every
// controller of the cell at an older term. A controller of the cell at the same term serves
// a repeated add, and a controller at another term stops before the new one runs. A drop
// returns once the controller of the cell has stopped.
//
// A controller of a cell that ends before it serves, such as one whose acquisition a newer
// controller fences, ends without the run, and the add returns its error. A controller that
// a newer controller fences after it served ends without the run too, because the root
// moves the cell. Every other error of a controller of a cell stops the run, as the error of
// a root or of an agent of a root does.
//
// # Calls
//
// [Controller.Do] takes the callbacks of the roots, which protocol.Dispatch passes to it.
// [Controller.Report], [Controller.Submit], [Controller.Disrupt] and [Controller.Done] pass
// a call for the root scope to the root, and a call for the scope of a cell to the
// controller of that cell. Each method calls Metadata.Check first, and passes a copy of its
// message, so the caller can reuse the message once the method returns.
//
// # Errors
//
// [ErrConfig], [ErrRun] and [ErrScope] classify as errs.Invalid. [ErrNotServing] classifies as
// errs.Transient. A call returns the error of the machine that serves it, wrapped.
//
// # Concurrency
//
// A Controller is safe for concurrent use. Run runs once, and any number of goroutines call
// the methods of the servers.
//
// # Dependency position
//
// controller imports the standard library, the packages clock, epoch, id/ulid, rand, task
// and telemetry of go.thesmos.sh/core, and the packages budget, config, internal/driver,
// internal/machine, internal/machine/agent, internal/machine/cell, internal/machine/root,
// membership, proposal, protocol, shard and store of go.thesmos.sh/sharder.
package controller

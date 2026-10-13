// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package embedded runs an application process of the topology embedded: the controller of the
// process, and the agent and the router of each namespace that the process serves.
//
// In the topology embedded, every application process is also a controller process. [New]
// takes the node of the process, its dependencies, and the spec and the handler of each
// namespace in a [Config]. It builds the controller of the process and the agent of each
// namespace. [Process.AgentServer] returns the server of the callbacks of the process, and
// [Process.ControllerServer] returns the controller. The embedder serves both through its
// transport before [Process.Start]. A handler that runs work for each tenure of a shard is a
// replica.Runner.
//
// # Start
//
// [Process.Start] runs the controller with the spec of each namespace, then starts the agent
// of each namespace, then the router of each namespace whose Route is set. Each component
// starts once the component before it serves. Start returns once every agent serves and
// every router has read its namespace. When the context of Start ends first, or a component
// fails to start, Start stops every component that it started and returns the error.
//
// # End of the run
//
// The process runs until [Process.Close], until the context of Start ends, or until a
// component stops itself, such as an agent after the root has created its namespace again.
// The end of one component stops every component. Close moves the replicas of the node away
// through the agent of each namespace. It then ends the sessions of the process in the root
// scopes through controller.Controller.Close, which stops the controller, and stops the
// routers. The roots then place the cells of the process on the other processes without
// waiting for the sessions to expire. [Process.Done] is closed once every goroutine of the
// process has returned. [Process.Err] then returns the error that ended the run, or nil after
// a Close.
//
// # Errors
//
// [ErrConfig] and [ErrStarted] classify as errs.Invalid.
//
// # Concurrency
//
// A Process is safe for concurrent use. Start runs once.
//
// # Dependency position
//
// embedded imports the standard library, the packages clock, errs, rand, task and telemetry of
// go.thesmos.sh/core, and the packages config, controller, fence, membership, protocol,
// replica, router, shard and store of go.thesmos.sh/sharder.
package embedded

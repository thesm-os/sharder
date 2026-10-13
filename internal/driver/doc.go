// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package driver runs a machine of package go.thesmos.sh/sharder/internal/machine in
// production.
//
// The root of a namespace, the controller of a cell, the agent and the router are state
// machines that do not perform I/O. A [Driver] passes the events of one machine one at a
// time, and performs the effects of each step on goroutines of core's task.Run. The
// simulation runs the same machines without a driver.
//
// # Effects
//
// [Driver.Run] passes Started first. It performs each effect as a task of the run, and the
// task passes the result of the effect to the run as a later event of the machine:
//
//   - Head, Read and Watch call the store reader. A Read passes the placements of one page
//     and the token of the next page, and closes the cursor.
//   - Commit, Outcome and Campaign call the store.
//   - Join and WatchViews call the membership.
//   - Callback calls the agent client. SendReport, SendDisrupt and SendDone call the
//     controller client. A SendReport sends the load of each replica that the
//     [LoadReporter] reports.
//   - Handle calls the [Handler], and Acquire calls the fence guard.
//   - Plan runs the placer of the effect, and then placement.Validate.
//   - Timer waits on the clock of the [Config].
//
// The Timeout of an effect bounds its call. The driver cancels the context of such a call
// once the call returns. An effect gets a result with an error that wraps [ErrConfig] when
// the Config does not have its dependency.
//
// A Watch and a WatchViews pass one event per item, until an error ends the watch or the
// Unwatch of its token cancels it. The driver passes a last item with [ErrEnded] when the
// iteration of a watch ends while the watch continues.
//
// A Campaign and a Join pass a second event once the term or the session ends. Their Resign
// and Leave release the term or the session, and then cancel the context of the effect. The
// driver logs a failed Resign or Leave, because neither has a result of its own. The driver
// cancels the context of a Watch, a WatchViews, a Campaign or a Join once the effect
// returns.
//
// # Inbound calls
//
// [Driver.Call] passes the event of an inbound call with a token of its own, and waits for
// the Reply of the machine with that token. A call whose context ends first returns the
// cause of the context, and the run passes a Cancelled of the call. [Driver.Ready] waits
// for the reply to Started.
//
// # Stop
//
// Once the context of Run ends, the run cancels every effect in flight, passes Stopped, and
// performs only the replies of Stopped. A call that the machine does not reply to returns
// [ErrStopped]. Run returns once every goroutine of the run has returned. The run stops the
// same way in these cases:
//
//   - The machine replies to Started with an error.
//   - The machine has stopped itself with an error.
//   - The machine returns an effect of a type that the driver does not perform.
//
// # Errors
//
// [ErrConfig], [ErrRun], [ErrStopped], [ErrReply] and [ErrEffect] classify as errs.Invalid.
// [ErrEnded] classifies as errs.Transient.
//
// # Concurrency
//
// A Driver is safe for concurrent use. Run runs on the goroutine of its caller, and calls
// Step of the machine from that goroutine alone.
//
// # Allocation contract
//
// A call that the machine replies to at once allocates only its event and the reply of the
// machine. Each effect that does I/O or waits costs a goroutine, its closure and the
// contexts that the driver derives for it.
//
// # Dependency position
//
// driver imports the standard library, the packages clock, errs, pool and task of
// go.thesmos.sh/core, and the packages budget, fence, internal/machine, membership,
// placement, protocol, shard and store of go.thesmos.sh/sharder.
package driver

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package machine defines the events that the state machines of sharder take and the
// effects that they return.
//
// [Machine] is the state machine of the root of a namespace, of the controller of a cell,
// of an agent or of a router. [Machine.Step] takes one [Event] and returns the [Effect]
// values of the event, such as a commit to the store or a callback to a node. Step does
// not perform the effects. A runtime performs them and passes each result back to the
// machine as an event. In production, it performs the effects on goroutines. In a
// simulation, it delivers their results in an order that the seed of the simulation
// selects, so the simulation tests the logic that runs in production.
//
// # Effects
//
// A runtime performs each effect with one call or one wait:
//
//   - For [Head], [Read], [Watch], [Commit], [Outcome] and [Campaign], it calls
//     store.Store.
//   - For [Join] and [WatchViews], it calls membership.Membership.
//   - For [Callback], it calls protocol.AgentClient. For [SendReport], [SendDisrupt] and
//     [SendDone], it calls protocol.ControllerClient.
//   - For [Handle], it calls the handler of the application. For [Acquire], it calls the
//     fence.Acquirer of the application.
//   - For [Plan], it runs placement.Placer.Place and then placement.Validate.
//   - For [Timer], it waits on the clock of the process.
//   - For [Unwatch], [Resign] and [Leave], it ends the watch, the term or the session of an
//     earlier effect.
//   - For [Reply], it replies to the caller of an inbound call.
//
// # Tokens
//
// The machine gives each effect a [Token], and the runtime passes the result of the effect
// as an event with the same token, such as a [Committed] for a Commit. The runtime passes
// one event per item of a Watch or a WatchViews, until an error or an Unwatch ends the
// watch. For a Campaign and a Join, it passes a second event when the term or the session
// ends. Unwatch, Resign and Leave have the token of the effect that started the watch, the
// term or the session. Unwatch and Resign have no result of their own. The runtime passes
// the result of a Leave as a [Left] before the end of the session.
//
// A server or the application calls a machine through an event with a [ReplyToken], such
// as a [Do] or a [Close]. The machine replies with a Reply that has the same token. It
// appends the Reply once the effects that the reply depends on have completed, such as
// the commit of a deferral before the reply to a [Disrupt].
//
// # Runtimes
//
// A runtime passes the events of one machine and performs its effects by these rules:
//
//   - It passes [Started] first, and passes one event at a time.
//   - It can perform the effects of one step in any order and concurrently. A machine
//     appends an effect that depends on another effect only after the result of the other
//     effect has arrived.
//   - It passes the result of an effect in a later step than the step that returned the
//     effect. The items of one watch arrive in the order of the watch.
//   - It bounds a call with the Timeout of its effect. Only the calls to other processes
//     and to the application have a Timeout. A call with a zero Timeout is bounded by the
//     run alone.
//   - It replies to the caller of an inbound call itself when the context of the call ends
//     before the machine replies. It then passes [Cancelled], and ignores a later Reply to
//     the call.
//   - It cancels every effect in flight when the context of the run ends, and then passes
//     [Stopped]. It performs only the replies that the machine returns for Stopped, and it
//     does not pass a later event.
//   - It stops the run in the same way once Machine.Err returns an error after a step.
//
// # Concurrency
//
// The events and the effects are value types. A copy of one shares its slices, its maps
// and the values of its pointers with the original. Neither the machine nor the runtime
// modifies the memory of an event or an effect after it passes it. A machine can keep that
// memory and read it, as it does to commit the mutation of a Commit again. The runtime
// passes a copy of the message of each inbound call, so the machine can keep the message.
//
// # Allocation contract
//
// An Event or an Effect that escapes costs at most one allocation for the value that it
// stores. An element of the slice that Machine.Step returns escapes. A Stopped does not
// cost an allocation, because it has no fields.
//
// # Dependency position
//
// machine imports the standard library, the packages epoch and page of
// go.thesmos.sh/core, and the packages budget, fence, membership, ownership, placement,
// proposal, protocol, shard and store of go.thesmos.sh/sharder.
package machine

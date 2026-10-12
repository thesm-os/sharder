// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine

// Token is the identity of one effect of a machine. The events of the result of the effect
// have the same token, so the machine passes each result to the part of the machine that
// appended the effect. A machine numbers the tokens of its effects from one with a single
// counter. Its parts share the counter, so no two effects of one machine have the same
// token.
//
// The zero Token is no effect.
type Token uint64

// ReplyToken is the identity of one inbound call of a machine. The runtime numbers the
// inbound calls of a run from one, and passes the token of a call in the event of the
// call. The machine replies to the call with a [Reply] that has the same token.
//
// The zero ReplyToken is no call.
type ReplyToken uint64

// Event is an input of a machine. An event is the result of an effect, an inbound call,
// the [Cancelled] of an inbound call, [Started] or [Stopped]. Only the types of this
// package implement Event, so the set of events is closed. No type of the package is both
// an Event and an [Effect].
type Event interface {
	// event marks a type of this package as an Event.
	event()
}

// Effect is an output of a machine. The runtime performs each effect, such as a commit to
// the store or a [Reply] to an inbound call. Only the types of this package implement
// Effect, so the set of effects is closed.
type Effect interface {
	// effect marks a type of this package as an Effect.
	effect()
}

// Machine is the state machine of the root of a namespace, of the controller of a cell,
// of an agent or of a router. A runtime passes it one event at a time, and performs the
// effects that it returns.
//
// # Concurrency
//
// A runtime calls Step from one goroutine at a time. Other goroutines read some values of
// a machine while it runs, such as the fencing tokens of an agent or the routes of a
// router. The machine publishes each such value through an atomic pointer during Step, so
// a reader does not wait for a step.
type Machine interface {
	// Step applies ev to the machine and appends the effects of ev to out. It returns the
	// extended slice, as append does. It does not modify the elements of out, and it does
	// not keep out after it returns.
	//
	// Step does not block, start a goroutine or perform I/O. It measures each wait with a
	// [Timer], and it reads its clock only for timestamps and for the windows of budgets,
	// such as the window of a retry budget. It reports metrics and logs during the step,
	// because neither changes its state. The effects of a step depend only on the state of
	// the machine, on ev, and on the readings of its clock and its source of randomness. A
	// simulation that passes the same events in the same order replays the same effects.
	//
	// Step ignores an event whose token it does not wait for, such as a [Watched] that
	// arrives after the [Unwatch] of its watch.
	Step(ev Event, out []Effect) []Effect
}

// Started is the first event of a run. It is also the inbound call of the constructor of
// the component, such as the start of an agent. The constructor waits for the reply. The
// machine replies with a Reply[struct{}] once it serves, or with the error that keeps it
// from serving. A runtime stops a machine whose reply to Started has an error.
type Started struct {
	// Reply is the token of the call.
	Reply ReplyToken
}

var _ Event = Started{}

// event marks Started as an Event.
func (Started) event() {}

// Stopped is the last event of a run. A runtime cancels every effect in flight when the
// context of the run ends, and then passes Stopped. The machine replies to each inbound
// call that it has not replied to, and does not return another effect.
type Stopped struct{}

var _ Event = Stopped{}

// event marks Stopped as an Event.
func (Stopped) event() {}

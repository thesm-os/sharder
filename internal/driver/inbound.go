// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver

import (
	"context"
	"fmt"

	"go.thesmos.sh/sharder/internal/machine"
)

// call is one inbound call that Call passes to the run.
type call struct {
	// event is the event of the call, with the token of the call.
	event machine.Event
	// reply receives the Reply of the machine to the call. It has room for one reply, so the
	// run never waits on it. The run closes it when it stops without a reply to the call.
	reply chan<- machine.Effect
	// token is the token of the call.
	token machine.ReplyToken
}

// Call passes the inbound call that event returns to the machine, and waits for the
// reply. event receives the token of the call, and returns the event of the call with
// that token, such as a machine.Do. Call returns the Value and the Err of the Reply[T] of
// the machine.
//
// When ctx ends before the reply, Call returns the cause of ctx, and the run passes a
// Cancelled of the call unless the machine has replied by then. The run drops a later
// reply to the call.
//
// Error modes:
//   - the Err of the reply.
//   - the cause of ctx when ctx ends before the reply.
//   - ErrStopped when the run has stopped, and when Run returns without a reply to the
//     call.
//   - an error that wraps ErrReply when the machine replies with another type than
//     Reply[T].
//
// # Allocation contract
//
// Zero alloc, apart from the event that event returns, which escapes. Call takes the
// channel of its reply from a pool, and returns it to the pool once it has received the
// reply. A call that ends without its reply leaves its channel to the garbage collector.
func (d *Driver) Call[T machine.Result](ctx context.Context, event func(machine.ReplyToken) machine.Event) (T, error) {
	var zero T
	tok := machine.ReplyToken(d.tokens.Add(1))
	reply := d.replies.Get()
	select {
	case d.calls <- call{event: event(tok), reply: reply, token: tok}:
	case <-ctx.Done():
		return zero, context.Cause(ctx)
	case <-d.stopping:
		return zero, ErrStopped
	}
	select {
	case e, ok := <-reply:
		if !ok {
			return zero, ErrStopped
		}
		d.replies.Put(reply)
		return value[T](e)
	case <-ctx.Done():
	}
	select {
	case d.cancels <- tok:
	case <-d.stopping:
	}
	return zero, context.Cause(ctx)
}

// value returns the Value and the Err of e, the reply to a call that expects a Reply[T].
// It returns an error that wraps ErrReply when e is a Reply of another type.
func value[T machine.Result](e machine.Effect) (T, error) {
	r, ok := e.(machine.Reply[T])
	if !ok {
		var zero T
		return zero, fmt.Errorf("%w: %T", ErrReply, e)
	}
	return r.Value, r.Err
}

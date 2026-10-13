// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver

import (
	"context"
	"fmt"
	"maps"
	"time"

	"go.thesmos.sh/core/task"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/protocol"
)

// run is the state of the run of a Driver. Only the goroutine of the run reads and writes
// it.
type run struct {
	// d is the driver of the run.
	d *Driver
	// group starts the tasks that perform the effects.
	group *task.Group
	// pending maps the token of each call that waits for its reply to the channel of the
	// reply. The token of Started maps to nil, because Ready reads its reply.
	pending map[machine.ReplyToken]chan<- machine.Effect
	// ends maps the token of each Watch, WatchViews, Campaign and Join that has begun to
	// its ending. begin forgets the effects whose tasks have returned.
	ends map[machine.Token]ending
	// err is the error that stops the run, or nil while the run continues.
	err error
	// out receives the effects of each step. The run clears it after each step, so it does
	// not keep an effect that it has performed.
	out []machine.Effect
	// start is the token of Started.
	start machine.ReplyToken
}

// ending is the end of a Watch, a WatchViews, a Campaign or a Join.
type ending struct {
	// ended returns the error of the context of the effect, which the Unwatch, the Resign
	// or the Leave of the effect ends. The task of the effect ends it too when it returns.
	ended func() error
	// stop ends the context of the effect.
	stop context.CancelFunc
}

// loop passes Started, and then each result, each call and each Cancelled until the run
// stops. It then stops the machine, and returns the error that stopped the run. A call
// without a reply after Stopped gets ErrStopped: Ready for Started, and Call through the
// closed channel of its reply. ctx is the context of the group of the run.
func (r *run) loop(ctx context.Context) error {
	effects, cancel := context.WithCancel(ctx)
	r.pending[r.start] = nil
	r.step(effects, machine.Started{Reply: r.start})
	for r.err == nil {
		select {
		case ev := <-r.d.results:
			r.step(effects, ev)
		case c := <-r.d.calls:
			r.pending[c.token] = c.reply
			r.step(effects, c.event)
		case tok := <-r.d.cancels:
			if _, ok := r.pending[tok]; ok {
				//dokimi:mutate-skip sbr-delete: the entry of a cancelled call keeps only memory until the run stops, because the run drops its reply and closes its channel at the stop
				delete(r.pending, tok)
				r.step(effects, machine.Cancelled{Reply: tok})
			}
		case <-ctx.Done():
			r.fail(context.Cause(ctx))
		}
	}
	close(r.d.stopping)
	cancel()
	for _, e := range r.d.machine.Step(machine.Stopped{}, r.out[:0]) {
		r.reply(e)
	}
	for tok, ch := range r.pending {
		if tok == r.start {
			r.ready(machine.Reply[struct{}]{To: tok, Err: ErrStopped})
			continue
		}
		close(ch)
	}
	return r.err
}

// step passes ev to the machine, and performs the effects of the step. A machine that has
// stopped itself in the step stops the run with its error.
func (r *run) step(ctx context.Context, ev machine.Event) {
	r.out = r.d.machine.Step(ev, r.out[:0])
	for _, e := range r.out {
		r.perform(ctx, e)
	}
	clear(r.out)
	r.fail(r.d.machine.Err())
}

// perform performs the effect e of a step. It passes a Reply to the call that waits for
// it, ends the effect of an Unwatch, a Resign or a Leave, and starts a task for each other
// effect. ctx is the context of the effects, which the run cancels when it stops.
func (r *run) perform(ctx context.Context, e machine.Effect) {
	if r.reply(e) {
		return
	}
	switch e := e.(type) {
	case machine.Head:
		r.spawn(ctx, 0, e, (*Driver).head)
	case machine.Read:
		r.spawn(ctx, 0, e, (*Driver).read)
	case machine.Watch:
		r.begin(ctx, e.Token, e, (*Driver).watch)
	case machine.Unwatch:
		r.end(e.Token)
	case machine.Commit:
		r.spawn(ctx, 0, e, (*Driver).commit)
	case machine.Outcome:
		r.spawn(ctx, 0, e, (*Driver).outcome)
	case machine.Campaign:
		r.begin(ctx, e.Token, e, (*Driver).campaign)
	case machine.Resign:
		r.end(e.Token)
	case machine.Join:
		r.begin(ctx, e.Token, e, (*Driver).join)
	case machine.Leave:
		r.end(e.Token)
	case machine.WatchViews:
		r.begin(ctx, e.Token, e, (*Driver).watchViews)
	case machine.Callback:
		r.spawn(ctx, e.Timeout, e, (*Driver).callback)
	case machine.SendReport:
		r.spawn(ctx, e.Timeout, e, (*Driver).sendReport)
	case machine.SendDisrupt:
		r.spawn(ctx, e.Timeout, e, (*Driver).sendDisrupt)
	case machine.SendDone:
		r.spawn(ctx, e.Timeout, e, (*Driver).sendDone)
	case machine.Handle:
		r.spawn(ctx, e.Timeout, e, (*Driver).handle)
	case machine.Acquire:
		r.spawn(ctx, e.Timeout, e, (*Driver).acquire)
	case machine.Plan:
		r.spawn(ctx, 0, e, (*Driver).plan)
	case machine.Timer:
		r.fail(r.group.Go(func(context.Context) error {
			r.d.timer(ctx, &e)
			return nil
		}))
	default:
		r.fail(fmt.Errorf("%w: %T", ErrEffect, e))
	}
}

// end ends the Watch, the WatchViews, the Campaign or the Join of tok. It does nothing for
// a token whose effect begin has forgotten.
func (r *run) end(tok machine.Token) {
	if en, ok := r.ends[tok]; ok {
		en.stop()
	}
}

// reply passes e to the call that waits for it when e is a Reply, and reports whether e is
// a Reply. The run drops a reply to a call that does not wait, such as a call whose
// Cancelled the run has passed.
func (r *run) reply(e machine.Effect) bool {
	var to machine.ReplyToken
	switch e := e.(type) {
	case machine.Reply[struct{}]:
		to = e.To
	case machine.Reply[protocol.Ack]:
		to = e.To
	case machine.Reply[protocol.Receipt]:
		to = e.To
	case machine.Reply[budget.Decision]:
		to = e.To
	default:
		return false
	}
	ch, ok := r.pending[to]
	if !ok {
		return true
	}
	delete(r.pending, to)
	if to == r.start {
		r.ready(e)
		return true
	}
	ch <- e
	return true
}

// ready records e, the reply to Started, for Ready. A reply with an error stops the run,
// and so does a reply of another type than Reply[struct{}].
func (r *run) ready(e machine.Effect) {
	rep, ok := e.(machine.Reply[struct{}])
	err := rep.Err
	if !ok {
		err = fmt.Errorf("%w: %T to Started", ErrReply, e)
	}
	r.d.startErr = err
	close(r.d.started)
	r.fail(err)
}

// spawn starts a task that performs the effect e with perform, and passes the result that
// perform returns. ctx is the context of the effects. A timeout above zero, the Timeout of
// e, bounds the context of the call of perform, and the task releases the bound once
// perform returns. A zero timeout leaves the call bounded by the run alone. A task that the
// group refuses, because the context of the group has ended, stops the run.
func (r *run) spawn[E any](ctx context.Context, timeout time.Duration, e E,
	perform func(d *Driver, ctx context.Context, e *E) machine.Event,
) {
	r.fail(r.group.Go(func(context.Context) error {
		call := ctx
		if timeout != 0 {
			var cancel context.CancelFunc
			call, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		r.d.post(ctx, perform(r.d, call, &e))
		return nil
	}))
}

// begin starts a task that performs the effect e of the token tok with perform: a Watch, a
// WatchViews, a Campaign or a Join. perform receives ctx, the context of the effects, and
// end, a context of ctx that the Unwatch, the Resign or the Leave of tok ends. The task
// ends end when it returns, and begin forgets each effect whose end has ended. A task that
// the group refuses, because the context of the group has ended, stops the run.
func (r *run) begin[E any](ctx context.Context, tok machine.Token, e E,
	perform func(d *Driver, ctx, end context.Context, e *E),
) {
	//dokimi:mutate-skip sbr-delete,ror-false,sbr-zero: the ending of an effect whose task has returned keeps only memory until the run returns
	maps.DeleteFunc(r.ends, func(_ machine.Token, en ending) bool { return en.ended() != nil })
	end, stop := context.WithCancel(ctx)
	r.ends[tok] = ending{ended: end.Err, stop: stop}
	r.fail(r.group.Go(func(context.Context) error {
		defer stop()
		perform(r.d, ctx, end, &e)
		return nil
	}))
}

// fail stops the run with err, unless the run has an error already. A nil err does not
// stop the run.
func (r *run) fail(err error) {
	if r.err == nil {
		r.err = err
	}
}

// post passes ev, the result of an effect, to the run, and reports whether it did. It
// drops ev and reports false once ctx has ended: the run has stopped, or the machine has
// ended the effect.
func (d *Driver) post(ctx context.Context, ev machine.Event) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case d.results <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

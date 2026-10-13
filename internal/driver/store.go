// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// head performs e through the reader, and returns the HeadRead of e.
func (d *Driver) head(ctx context.Context, e *machine.Head) machine.Event {
	h, err := d.reader.Head(ctx, e.Scope)
	return machine.HeadRead{Head: h, Err: err, Token: e.Token}
}

// read performs e through the reader. It reads the placements of the page and the token of
// the next page from the cursor of the read, and then closes the cursor. It returns the
// PageRead of e. A PageRead with an error has only the error and the token.
func (d *Driver) read(ctx context.Context, e *machine.Read) machine.Event {
	st, cur, err := d.reader.Read(ctx, e.Scope, e.Incarnation, e.Query, e.Page)
	if err != nil {
		return machine.PageRead{Err: err, Token: e.Token}
	}
	var placements []shard.Placement
	for p, perr := range cur.Seq(ctx) {
		if perr != nil {
			err = perr
			break
		}
		placements = append(placements, p)
	}
	next := cur.NextPage()
	if cerr := cur.Close(); cerr != nil {
		err = errors.Join(err, cerr)
	}
	if err != nil {
		return machine.PageRead{Err: err, Token: e.Token}
	}
	return machine.PageRead{State: st, Placements: placements, Next: next, Token: e.Token}
}

// watch performs e through the reader, and passes each item of the watch as a Watched. The
// Unwatch of e ends end, which ends the watch.
func (d *Driver) watch(_, end context.Context, e *machine.Watch) {
	d.stream(end, d.reader.Watch(end, e.Scope, e.Incarnation, e.From, e.Node),
		fmt.Errorf("%w: the generations of %v", ErrEnded, e.Scope),
		func(ev store.Event, err error) machine.Event {
			return machine.Watched{Event: ev, Err: err, Token: e.Token}
		})
}

// commit performs e through the writer of the store for the scope, the incarnation and the
// term of e, and returns the Committed of e.
func (d *Driver) commit(ctx context.Context, e *machine.Commit) machine.Event {
	if d.store == nil {
		return machine.Committed{Err: fmt.Errorf("%w: a Commit without a store", ErrConfig), Token: e.Token}
	}
	g, err := d.store.Writer(e.Scope, e.Incarnation, e.Term).Commit(ctx, e.Expect, e.Mutation)
	return machine.Committed{Generation: g, Err: err, Token: e.Token}
}

// outcome performs e through the store, and returns the OutcomeRead of e.
func (d *Driver) outcome(ctx context.Context, e *machine.Outcome) machine.Event {
	if d.store == nil {
		return machine.OutcomeRead{Err: fmt.Errorf("%w: an Outcome without a store", ErrConfig), Token: e.Token}
	}
	r, found, err := d.store.Outcome(ctx, e.Scope, e.ID)
	return machine.OutcomeRead{Receipt: r, Found: found, Err: err, Token: e.Token}
}

// stream passes the event that item returns for each value and error of seq, until an
// error ends seq or ctx ends. An iteration that ends while ctx has not ended passes a last
// event with ended, an error that wraps ErrEnded, so the machine watches again. It passes
// nothing once ctx has ended.
func (d *Driver) stream[T any](ctx context.Context, seq iter.Seq2[T, error], ended error,
	item func(v T, err error) machine.Event,
) {
	for v, err := range seq {
		if !d.post(ctx, item(v, err)) || err != nil {
			return
		}
	}
	var zero T
	d.post(ctx, item(zero, ended))
}

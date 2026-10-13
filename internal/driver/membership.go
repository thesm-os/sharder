// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver

import (
	"context"
	"fmt"
	"log/slog"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/membership"
)

// join performs e through the membership. It passes a Joined once the membership starts a
// session or the join fails, and a SessionEnded once a started session ends. The Leave of
// e ends end. Before the session starts, that ends the wait of the join. After the start,
// the driver leaves the session through the membership, and then cancels the context of
// the join, so a session whose leave failed still ends at its deadline. The driver logs a
// failed leave, and passes the result of the leave as a Left before the SessionEnded.
func (d *Driver) join(ctx, end context.Context, e *machine.Join) {
	jctx, cancel := context.WithCancel(ctx)
	defer cancel()
	unhook := context.AfterFunc(end, cancel)
	s, err := d.membership.Join(jctx, e.Scope, e.Incarnation, e.Node, e.Lease)
	waiting := unhook()
	if err != nil {
		d.post(ctx, machine.Joined{Err: err, Token: e.Token})
		return
	}
	if d.post(ctx, machine.Joined{Incarnation: s.Incarnation(), Token: e.Token}) {
		d.keep(ctx, end, waiting, &grant{
			done:    s.Done(),
			release: s.Leave,
			cancel:  cancel,
			released: func(err error) machine.Event {
				return machine.Left{Err: err, Token: e.Token}
			},
			ended:   machine.SessionEnded{Token: e.Token},
			failure: msgLeave,
			attrs:   []slog.Attr{slog.Any(attrScope, e.Scope)},
		})
	}
}

// watchViews performs e through the membership, and passes each view of the watch as a
// Viewed. The Unwatch of e ends end, which ends the watch.
func (d *Driver) watchViews(_, end context.Context, e *machine.WatchViews) {
	d.stream(end, d.membership.Watch(end, e.Scope, e.Incarnation),
		fmt.Errorf("%w: the views of %v", ErrEnded, e.Scope),
		func(v membership.View, err error) machine.Event {
			return machine.Viewed{View: v, Err: err, Token: e.Token}
		})
}

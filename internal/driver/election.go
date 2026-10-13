// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver

import (
	"context"
	"fmt"
	"log/slog"

	"go.thesmos.sh/sharder/internal/machine"
)

// The messages and the attributes of the logs of the driver.
const (
	// msgResign is the message of a failed resignation of a term.
	msgResign = "driver: a resignation failed"
	// msgLeave is the message of a failed leave of a session.
	msgLeave = "driver: a leave failed"
	// attrError is the attribute of the error of a failure.
	attrError = "error"
	// attrScope is the attribute of the scope of a failure.
	attrScope = "scope"
)

// campaign performs e through the store. It passes a Campaigned once the store grants a
// term or the campaign fails, and a TermEnded once a granted term ends. The Resign of e
// ends end. Before the grant, that ends the wait of the campaign. After the grant, the
// driver resigns the term through the store, and then cancels the context of the
// campaign, so a failed resignation still ends the term. The driver logs a failed
// resignation.
func (d *Driver) campaign(ctx, end context.Context, e *machine.Campaign) {
	if d.store == nil {
		d.post(ctx, machine.Campaigned{Err: fmt.Errorf("%w: a Campaign without a store", ErrConfig), Token: e.Token})
		return
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	unhook := context.AfterFunc(end, cancel)
	l, err := d.store.Campaign(cctx, e.Scope)
	waiting := unhook()
	if err != nil {
		d.post(ctx, machine.Campaigned{Err: err, Token: e.Token})
		return
	}
	if d.post(ctx, machine.Campaigned{Term: l.Term(), Token: e.Token}) {
		d.keep(ctx, end, waiting, &grant{
			done:    l.Done(),
			release: l.Resign,
			cancel:  cancel,
			ended:   machine.TermEnded{Token: e.Token},
			failure: msgResign,
			attrs:   []slog.Attr{slog.Any(attrScope, e.Scope)},
		})
	}
}

// grant is a term that the store has granted to a Campaign, or a session that the
// membership has started for a Join. The run keeps it until it ends.
type grant struct {
	// done is closed when the term or the session ends.
	done <-chan struct{}
	// release resigns the term, or leaves the session.
	release func(ctx context.Context) error
	// cancel cancels the context of the Campaign or the Join.
	cancel context.CancelFunc
	// ended is the last event of the effect: a TermEnded or a SessionEnded.
	ended machine.Event
	// failure is the message of the log of a failed release.
	failure string
	// attrs are the attributes of the log of a failed release.
	attrs []slog.Attr
}

// keep waits until the term or the session of g ends, and then passes its last event. When
// end ends first, keep releases g, logs a failed release, and then cancels the context of
// the effect, so a failed release still ends the term or the session. waiting reports that
// end had not ended when the effect acquired g. keep passes nothing once ctx has ended.
func (d *Driver) keep(ctx, end context.Context, waiting bool, g *grant) {
	if waiting {
		select {
		case <-g.done:
		case <-end.Done():
		}
	}
	if end.Err() != nil && ctx.Err() == nil {
		if err := g.release(ctx); err != nil {
			d.logger.LogAttrs(ctx, slog.LevelWarn, g.failure, append(g.attrs, slog.Any(attrError, err))...)
		}
		g.cancel()
	}
	select {
	case <-g.done:
		d.post(ctx, g.ended)
	case <-ctx.Done():
	}
}

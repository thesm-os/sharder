// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver

import (
	"context"
	"fmt"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/protocol"
)

// handle performs e through the handler of the application, and returns the Handled of e.
// The run bounds ctx by the Timeout of e. A request whose Call is protocol.Add calls
// AddShard with the fencing token of e, and one whose Call is protocol.Drop calls
// DropShard. The Handled of a request with any other Call has an error that wraps
// protocol.ErrUnsupported.
func (d *Driver) handle(ctx context.Context, e *machine.Handle) machine.Event {
	if d.handler == nil {
		return machine.Handled{Err: fmt.Errorf("%w: a Handle without a handler", ErrConfig), Token: e.Token}
	}
	var err error
	switch e.Request.Call {
	case protocol.Add:
		err = d.handler.AddShard(ctx, e.Request, &e.Fence)
	case protocol.Drop:
		err = d.handler.DropShard(ctx, e.Request)
	default:
		err = fmt.Errorf("%w: the call %v", protocol.ErrUnsupported, e.Request.Call)
	}
	return machine.Handled{Err: err, Token: e.Token}
}

// acquire performs e through the fence guard, and returns the Acquired of e. The run
// bounds ctx by the Timeout of e.
func (d *Driver) acquire(ctx context.Context, e *machine.Acquire) machine.Event {
	if d.guard == nil {
		return machine.Acquired{Err: fmt.Errorf("%w: an Acquire without a fence guard", ErrConfig), Token: e.Token}
	}
	return machine.Acquired{Err: d.guard.Acquire(ctx, e.Fence), Token: e.Token}
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver

import (
	"context"
	"fmt"
	"slices"

	"go.thesmos.sh/sharder/internal/machine"
)

// callback performs e through the agent client, and returns the Called of e. The run
// bounds ctx by the Timeout of e.
func (d *Driver) callback(ctx context.Context, e *machine.Callback) machine.Event {
	if d.agents == nil {
		return machine.Called{Err: fmt.Errorf("%w: a Callback without an agent client", ErrConfig), Token: e.Token}
	}
	return machine.Called{Err: d.agents.Do(ctx, e.Node, e.Request), Token: e.Token}
}

// sendReport performs e through the controller client, and returns the Reported of e. The
// run bounds ctx by the Timeout of e. With a load reporter, sendReport sends a copy of the
// report of e in which each replica has the load that the load reporter reports for its
// shard. It does not modify the report of e.
func (d *Driver) sendReport(ctx context.Context, e *machine.SendReport) machine.Event {
	if d.controllers == nil {
		return machine.Reported{
			Err:   fmt.Errorf("%w: a SendReport without a controller client", ErrConfig),
			Token: e.Token,
		}
	}
	r := e.Report
	if d.loads != nil {
		loaded := *e.Report
		loaded.Replicas = slices.Clone(e.Report.Replicas)
		for i := range loaded.Replicas {
			loaded.Replicas[i].Load = d.loads.ShardLoad(loaded.Replicas[i].Shard)
		}
		r = &loaded
	}
	ack, err := d.controllers.Report(ctx, e.Cell, r)
	return machine.Reported{Ack: ack, Err: err, Token: e.Token}
}

// sendDisrupt performs e through the controller client, and returns the Decided of e. The
// run bounds ctx by the Timeout of e.
func (d *Driver) sendDisrupt(ctx context.Context, e *machine.SendDisrupt) machine.Event {
	if d.controllers == nil {
		return machine.Decided{
			Err:   fmt.Errorf("%w: a SendDisrupt without a controller client", ErrConfig),
			Token: e.Token,
		}
	}
	decision, err := d.controllers.Disrupt(ctx, e.Cell, e.Disruption)
	return machine.Decided{Decision: decision, Err: err, Token: e.Token}
}

// sendDone performs e through the controller client, and returns the Ended of e. The run
// bounds ctx by the Timeout of e.
func (d *Driver) sendDone(ctx context.Context, e *machine.SendDone) machine.Event {
	if d.controllers == nil {
		return machine.Ended{Err: fmt.Errorf("%w: a SendDone without a controller client", ErrConfig), Token: e.Token}
	}
	return machine.Ended{Err: d.controllers.Done(ctx, e.Cell, e.Args), Token: e.Token}
}

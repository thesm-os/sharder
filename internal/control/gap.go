// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control

import (
	"context"
	"slices"
	"time"

	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// gap is a writer gap in flight: the time in which a shard has no serving writer.
type gap struct {
	// start is the instant at which the gap started.
	start time.Time
	// reason is the histogram of the reason of the gap, which records the gap when it
	// ends.
	reason telemetry.Histogram
}

// open starts the writer gap of the shard sid with the histogram of its reason, unless the
// shard has a gap in flight. A gap keeps the start and the reason of its first cause.
func (c *Controller) open(sid shard.ID, reason telemetry.Histogram) {
	if _, ok := c.gaps.Get(sid); !ok {
		c.gaps.Set(sid, gap{start: c.clock.Time(), reason: reason})
	}
}

// stops starts a planned writer gap for each placement of the mutation mu whose writer is
// not on the node of the writer of the committed placement of its shard. A commit that
// removes the writer of a shard starts its gap. It runs before the controller applies mu.
func (c *Controller) stops(mu *store.Mutation) {
	for i := range mu.Put {
		p := &mu.Put[i]
		old := c.assignment.placements[p.Shard]
		was, ok := writer(&old, c.spec.writer)
		now, _ := writer(p, c.spec.writer)
		if ok && now.Node != was.Node {
			c.open(p.Shard, c.metrics.plannedGap)
		}
	}
}

// fails starts a failure writer gap for each shard whose committed writer is on one of the
// nodes ended, whose incarnations a view has ended. A placement without a writer has the
// zero Replica, whose empty node no view lists, because a membership refuses a node
// without an ID. fails costs O(n) for n placements.
func (c *Controller) fails(ended []shard.NodeID) {
	for sid, p := range c.assignment.placements {
		if w, _ := writer(&p, c.spec.writer); slices.Contains(ended, w.Node) {
			c.open(sid, c.metrics.failureGap)
		}
	}
}

// served ends the writer gap of each shard that the accepted report r lists as serving,
// when the node of r is the committed writer of the shard. It records the duration of the
// gap in the histogram of its reason.
func (c *Controller) served(r *protocol.Report) {
	for i := range r.Replicas {
		o := &r.Replicas[i]
		g, ok := c.gaps.Get(o.Shard)
		if !ok || o.Status != shard.StatusServing {
			continue
		}
		p := c.assignment.placements[o.Shard]
		if w, _ := writer(&p, c.spec.writer); w.Node != r.Node {
			continue
		}
		g.reason.Record(context.Background(), c.clock.Time().Sub(g.start).Seconds())
		c.gaps.Delete(o.Shard)
	}
}

// completed ends the writer gap of each shard whose transition the mutation mu ends, when the
// committed placement of the shard has a writer. It serves a scope whose nodes do not
// report. The last callback of such a transition returns once the writer serves, as the add
// of a cell returns once the controller of the cell has acquired its scope. It runs after the
// controller applies mu, and records the duration of each gap in the histogram of its reason.
func (c *Controller) completed(mu *store.Mutation) {
	for _, sid := range mu.Done {
		g, ok := c.gaps.Get(sid)
		p := c.assignment.placements[sid]
		if _, served := writer(&p, c.spec.writer); !ok || !served {
			continue
		}
		g.reason.Record(context.Background(), c.clock.Time().Sub(g.start).Seconds())
		c.gaps.Delete(sid)
	}
}

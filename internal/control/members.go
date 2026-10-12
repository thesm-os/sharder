// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control

import (
	"errors"
	"fmt"
	"time"

	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/resilience"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// The bounds of the backoff before the controller watches the views of its membership
// again after a transient error. resilience.Backoff returns each backoff as a draw of
// Config.Rand below its ceiling.
const (
	// ViewRetryBase is the ceiling of the backoff before the first retry. The ceiling
	// doubles with each later retry.
	ViewRetryBase = 100 * time.Millisecond
	// ViewRetryMax is the highest ceiling of a backoff.
	ViewRetryMax = 10 * time.Second
)

// member is one node of the last view of the scope, with the last report of its
// incarnation that the controller accepted.
type member struct {
	// node is the node in the view.
	node *shard.Node
	// replicas maps each shard of the last accepted report to what the node reported about
	// its replica, in the report. It is nil before the first report.
	replicas map[shard.ID]*shard.Observed
	// sequence is the Sequence of the last accepted report, and zero before the first. An
	// agent numbers the reports of an incarnation from one.
	sequence uint64
}

// faulted reports whether the last accepted report of mb has a permanent fault in its
// replica of the shard sid.
func (mb *member) faulted(sid shard.ID) bool {
	o := mb.replicas[sid]
	return o != nil && o.Fault == shard.FaultPermanent
}

// take makes the report r the last accepted report of mb, and reports whether r adds a
// permanent fault to the faults that the last report had.
func (mb *member) take(r *protocol.Report) bool {
	replicas := make(map[shard.ID]*shard.Observed, len(r.Replicas))
	faulted := false
	for i := range r.Replicas {
		o := &r.Replicas[i]
		replicas[o.Shard] = o
		if o.Fault == shard.FaultPermanent && !mb.faulted(o.Shard) {
			faulted = true
		}
	}
	mb.replicas, mb.sequence = replicas, r.Sequence
	return faulted
}

// members is the membership of the scope as the controller sees it: the nodes of the last
// view, each with the last report of its incarnation, and the watch of the views.
type members struct {
	// nodes maps each node of the last view to its state.
	nodes map[shard.NodeID]*member
	// view is the last view. The engine and nodes keep pointers to its nodes, so the
	// controller replaces the whole view and never modifies it.
	view membership.View
	// watch is the token of the WatchViews in flight, and zero without one.
	watch machine.Token
	// wait is the token of the last Timer before a WatchViews.
	wait machine.Token
	// attempt counts the watches of the views that ended with a transient error in a row.
	attempt int
	// viewed reports that the first view has arrived.
	viewed bool
}

// apply replaces the view of ms with v. A node of v at the incarnation that ms has keeps its
// last report. It returns the nodes of the old view whose incarnation has ended, and reports
// whether a node joined, left or restarted.
func (ms *members) apply(v *membership.View) ([]shard.NodeID, bool) {
	nodes := make(map[shard.NodeID]*member, len(v.Nodes))
	changed := len(v.Nodes) != len(ms.nodes)
	ms.view = *v
	for i := range ms.view.Nodes {
		n := &ms.view.Nodes[i]
		if old, ok := ms.nodes[n.ID]; ok && old.node.Incarnation == n.Incarnation {
			old.node = n
			nodes[n.ID] = old
			continue
		}
		changed = true
		nodes[n.ID] = &member{node: n}
	}
	var ended []shard.NodeID
	for nid, old := range ms.nodes {
		if nodes[nid] != old {
			ended = append(ended, nid)
		}
	}
	ms.nodes = nodes
	return ended, changed
}

// live returns the node nid of the last view, and reports whether the view has it.
func (ms *members) live(nid shard.NodeID) (*shard.Node, bool) {
	if mb, ok := ms.nodes[nid]; ok {
		return mb.node, true
	}
	return nil, false
}

// reported reports whether the first view has arrived, and every node of the last view has
// sent a report that ms accepted.
func (ms *members) reported() bool {
	if !ms.viewed {
		return false
	}
	for _, mb := range ms.nodes {
		if mb.sequence == 0 {
			return false
		}
	}
	return true
}

// faults returns each shard whose replica the live incarnation of its node reports with a
// permanent fault, with those nodes in the order of their IDs.
func (ms *members) faults() map[shard.ID][]shard.NodeID {
	faults := make(map[shard.ID][]shard.NodeID)
	for i := range ms.view.Nodes {
		mb := ms.nodes[ms.view.Nodes[i].ID]
		for sid, o := range mb.replicas {
			if o.Fault == shard.FaultPermanent {
				faults[sid] = append(faults[sid], mb.node.ID)
			}
		}
	}
	return faults
}

// loads returns the load of each shard of a: the load that the live incarnation of the node
// of its writer in the role wr reports, else load. A shard without either has no entry. A
// nil load leaves every shard without a report out.
func (ms *members) loads(a *assignment, wr shard.Role, load shard.Load) map[shard.ID]shard.Load {
	loads := make(map[shard.ID]shard.Load)
	if load != nil {
		for _, s := range a.layout.Shards {
			loads[s.ID] = load
		}
	}
	for sid, p := range a.placements {
		w, _ := writer(&p, wr)
		if mb, ok := ms.nodes[w.Node]; ok && mb.replicas[sid] != nil && mb.replicas[sid].Load != nil {
			loads[sid] = mb.replicas[sid].Load
		}
	}
	return loads
}

// unwatch appends the Unwatch of the watch of the views to out, and returns out unchanged
// when no watch runs.
func (ms *members) unwatch(out []machine.Effect) []machine.Effect {
	if ms.watch != 0 {
		out = append(out, machine.Unwatch{Token: ms.watch})
	}
	return out
}

// watchViews starts a watch of the views of the membership of the scope.
func (c *Controller) watchViews(out []machine.Effect) []machine.Effect {
	c.members.watch = c.next()
	return append(out, machine.WatchViews{Scope: c.scopeOf, Token: c.members.watch, Incarnation: c.incarnation})
}

// viewed takes one item of the watch of the views, and reports whether the item is of the
// watch in flight. A view replaces the membership of the scope, ends the incarnations that
// it no longer lists, and can start a pass. The writer gap of each shard whose writer is on
// a node that the view ends starts at the view. A watch that ends with a transient error
// runs again after a backoff.
//
// Error modes:
//   - an error that wraps the error of the watch for an error that is not transient.
//   - the error of engine.Engine.View, and the errors of the commits that the view starts.
func (c *Controller) viewed(ev *machine.Viewed, out []machine.Effect) ([]machine.Effect, bool, error) {
	if ev.Token == 0 || ev.Token != c.members.watch {
		return out, false, nil
	}
	if ev.Err != nil {
		c.members.watch = 0
		if !errs.Retryable(ev.Err) {
			return out, true, fmt.Errorf("control: the views of %v: %w", c.scopeOf, ev.Err)
		}
		c.members.attempt++
		c.members.wait = c.next()
		delay := resilience.Backoff(c.rand, c.members.attempt, ViewRetryBase, ViewRetryMax)
		return append(out, machine.Timer{Token: c.members.wait, Duration: delay}), true, nil
	}
	c.members.attempt = 0
	first := !c.members.viewed
	c.members.viewed = true
	ended, changed := c.members.apply(&ev.View)
	lost := c.repairs(ended)
	if lost {
		c.fails(ended)
	}
	out, err := c.engine.View(&c.members.view, out)
	out, err = c.batchAfter(out, err)
	out, serr := c.settle(out)
	if joined := errors.Join(err, serr); joined != nil {
		return out, true, joined
	}
	if first || changed && c.spec.schedule.OnMembership || lost {
		out, err = c.trigger(out)
	}
	return out, true, err
}

// repairs reports whether a replica of the scope is on one of the nodes nodes, whose
// incarnations have ended.
func (c *Controller) repairs(nodes []shard.NodeID) bool {
	for _, n := range nodes {
		if c.assignment.hosts(n, c.spec.roles) > 0 {
			return true
		}
	}
	return false
}

// report takes the report of an agent. The controller accepts a report of the live
// incarnation of a node of its view from the identity of the node, and discards a report
// that is not newer than the last one it accepted from the incarnation. An accepted report
// ends the writer gap of each shard whose writer the node serves. A permanent fault in the
// report starts a pass. The controller replies to every report at once.
func (c *Controller) report(ev *machine.Report, out []machine.Effect) ([]machine.Effect, error) {
	c.calls.Set(ev.Reply, callReport)
	r := ev.Report
	if err := c.serve(&ev.Metadata, r.Scope, r.Incarnation); err != nil {
		return reply(c, ev.Reply, protocol.Ack{}, err, out), nil
	}
	mb, ok := c.members.nodes[r.Node]
	if !ok || mb.node.Incarnation != r.NodeIncarnation {
		err := fmt.Errorf("%w: node %s at incarnation %d is not live", ErrStale, r.Node, r.NodeIncarnation)
		return reply(c, ev.Reply, protocol.Ack{}, err, out), nil
	}
	if !c.unauthenticated && ev.Metadata.Caller != mb.node.Identity {
		err := fmt.Errorf("%w: %q reports for node %s", ErrDenied, ev.Metadata.Caller, r.Node)
		return reply(c, ev.Reply, protocol.Ack{}, err, out), nil
	}
	for i := range r.Replicas {
		if o := &r.Replicas[i]; !o.Status.Valid() {
			err := fmt.Errorf("%w: node %s reports shard %v with the status %v", ErrCall, r.Node, o.Shard, o.Status)
			return reply(c, ev.Reply, protocol.Ack{}, err, out), nil
		}
	}
	faulted := false
	if r.Sequence > mb.sequence {
		c.reports++
		faulted = mb.take(r)
		c.served(r)
	}
	ack := protocol.Ack{Sequence: mb.sequence, Generation: c.assignment.generation, ControllerEpoch: c.term}
	out = reply(c, ev.Reply, ack, nil, out)
	if !faulted {
		return out, nil
	}
	return c.trigger(out)
}

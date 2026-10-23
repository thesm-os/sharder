// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"go.thesmos.sh/core/btree"
	"go.thesmos.sh/core/crypto"
	"go.thesmos.sh/core/crypto/sha256"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// RetryAfter is the RetryAfter of each deferral of a disruption. A caller asks again after
// it, so a longer wait makes each deploy longer.
const RetryAfter = time.Second

// The reasons of the deferrals that do not depend on the state of the scope.
const (
	// reasonReports is the reason of a deferral before every live node has reported.
	reasonReports = "the controller waits for a report of every live node"
	// reasonStart is the reason of the deferral of a disruption that is not negotiable.
	reasonStart = "the disruption is not negotiable, and the controller admits it at its start"
	// reasonEnding is the reason of the deferral of a disruption whose reservation ends.
	reasonEnding = "the reservation of the disruption ends"
)

// reserved is a reservation that has not ended, with the Timer and the commit that the
// controller waits for to admit or end it.
type reserved struct {
	// r is the reservation of the last commit that reserves it. The controller replaces the
	// reservation of an admission, and never modifies one in place.
	r *budget.Reservation
	// timer is the token of the Timer that admits or ends a pending reservation, and zero
	// before the controller arms it.
	timer machine.Token
	// ending reports that a commit in flight ends the reservation.
	ending bool
}

// waiting is a Disrupt call that waits for the admission or the end of the pending
// reservation of its disruption. A call whose caller has gone waits until its Timer or until
// the commit that admits or ends the reservation, and reply then drops its reply.
type waiting struct {
	// timer is the token of the Timer at machine.WaitMargin before the deadline of the call.
	timer machine.Token
	// to is the token of the call.
	to machine.ReplyToken
}

// budgets is the budget of a scope: its reservations and its drains. It takes the
// reservations and the drains of each commit when the controller sends the commit.
type budgets struct {
	// reservations maps the ID of each reservation that has not ended to the reservation,
	// in the order of the IDs. A reservation ends when the commit that releases it applies.
	reservations *btree.MapFunc[budget.ID, *reserved]
	// drains maps the key of each drain to the drain, in the order of the nodes and then of
	// the disruption IDs.
	drains *btree.MapFunc[store.DrainKey, store.Drain]
	// waits maps the ID of each pending reservation to the Disrupt calls that wait for it, in
	// the order of the IDs. The commit that admits or ends the reservation takes its calls,
	// so every reservation in waits is pending, and no commit ends it. A reservation keeps
	// its entry without a call once the Timers of its calls have elapsed.
	waits *btree.MapFunc[budget.ID, []waiting]
}

// newBudgets returns a budget without reservations, drains and waiting calls.
func newBudgets() budgets {
	byID := func(a, b budget.ID) int { return a.Compare(b.ID) }
	return budgets{
		reservations: btree.NewMapFunc[budget.ID, *reserved](byID),
		drains: btree.NewMapFunc[store.DrainKey, store.Drain](func(a, b store.DrainKey) int {
			return cmp.Or(cmp.Compare(a.Node, b.Node), a.Disruption.Compare(b.Disruption.ID))
		}),
		waits: btree.NewMapFunc[budget.ID, []waiting](byID),
	}
}

// restore takes the reservations and the drains of the state st of the scope.
func (b *budgets) restore(st *store.State) {
	for i := range st.Reservations {
		r := &st.Reservations[i]
		b.reservations.Set(r.Disruption.ID, &reserved{r: r})
	}
	for _, d := range st.Drains {
		b.drains.Set(store.DrainKey{Node: d.Node, Disruption: d.Disruption}, d)
	}
}

// commit takes the reservations and the drains of the mutation mu of a commit that the
// controller sends. It takes them in the order of a store: Reserve, then Drain, then
// Undrain. The commits apply in the order in which the controller sends them, so the
// commits of a later step apply after mu. A Done of a reservation whose commit is in flight
// ends the reservation. A move that starts after a drain commits its first record after the
// drain.
func (b *budgets) commit(mu *store.Mutation) {
	for i := range mu.Reserve {
		r := &mu.Reserve[i]
		b.reservations.Set(r.Disruption.ID, &reserved{r: r})
	}
	for _, d := range mu.Drain {
		b.drains.Set(store.DrainKey{Node: d.Node, Disruption: d.Disruption}, d)
	}
	for _, k := range mu.Undrain {
		b.drains.Delete(k)
	}
}

// apply ends each reservation that mu releases, once the commit of mu has applied. release
// marks the reservation as ending until then.
func (b *budgets) apply(mu *store.Mutation) {
	for _, id := range mu.Release {
		b.reservations.Delete(id)
	}
}

// followups ends the waits of the Disrupt calls of each reservation that the mutation mu of
// a commit admits or ends. It appends a followup of each call to fs: the admission, or the
// deferral of the end. The calls get their replies once the commit has applied.
func (b *budgets) followups(mu *store.Mutation, fs []followup) []followup {
	for i := range mu.Reserve {
		if r := &mu.Reserve[i]; !r.Admitted.IsZero() {
			ws, _ := b.waits.Delete(r.Disruption.ID)
			for _, w := range ws {
				fs = append(fs, followup{decision: budget.Decision{Allowed: true}, to: w.to})
			}
		}
	}
	for _, id := range mu.Release {
		ws, _ := b.waits.Delete(id)
		for _, w := range ws {
			fs = append(fs, followup{decision: budget.Decision{Reason: reasonEnding, RetryAfter: RetryAfter}, to: w.to})
		}
	}
	return fs
}

// draining reports whether the node n has a drain.
func (b *budgets) draining(n shard.NodeID) bool {
	k, _, ok := b.drains.Ceil(store.DrainKey{Node: n})
	return ok && k.Node == n
}

// covered reports whether an admitted reservation that a commit in flight does not end
// covers the node n.
func (b *budgets) covered(n shard.NodeID) bool {
	for _, e := range b.reservations.All() {
		if !e.r.Admitted.IsZero() && !e.ending && slices.Contains(e.r.Disruption.Nodes, n) {
			return true
		}
	}
	return false
}

// digest returns the SHA-256 digest of the encoding of d, after the checks of d that the
// controller runs before any effect.
//
// Error modes:
//   - an error that wraps ErrCall for a d without an ID, without nodes, or with an End
//     that is not after its Start.
//   - an error that wraps ErrCall and budget.ErrUnknownImpact for an Impact other than
//     zero that is not valid, and an error that wraps ErrCall for the zero Impact.
func digest(d *budget.Disruption) (crypto.Digest, error) {
	if d.ID.IsZero() {
		return crypto.Digest{}, fmt.Errorf("%w: the disruption has no ID", ErrCall)
	}
	if len(d.Nodes) == 0 {
		return crypto.Digest{}, fmt.Errorf("%w: disruption %v has no nodes", ErrCall, d.ID)
	}
	if !d.End.After(d.Start) {
		return crypto.Digest{}, fmt.Errorf("%w: disruption %v ends at %v, before its start at %v",
			ErrCall, d.ID, d.End, d.Start)
	}
	encoded, err := d.MarshalBinary()
	if err != nil {
		return crypto.Digest{}, fmt.Errorf("%w: disruption %v: %w", ErrCall, d.ID, err)
	}
	if !d.Impact.IsValid() {
		return crypto.Digest{}, fmt.Errorf("%w: disruption %v has no impact", ErrCall, d.ID)
	}
	return sha256.Hasher{}.Hash(encoded), nil
}

// release returns the mutation that ends the reservations es with their drains, and marks
// them as ending until the commit applies.
func release(es ...*reserved) *store.Mutation {
	mu := &store.Mutation{}
	for _, e := range es {
		e.ending = true
		id := e.r.Disruption.ID
		mu.Release = append(mu.Release, id)
		for _, n := range e.r.Disruption.Nodes {
			mu.Undrain = append(mu.Undrain, store.DrainKey{Node: n, Disruption: id})
		}
	}
	return mu
}

// disrupt takes a planned disruption. The controller commits a pending reservation of a new
// disruption with the drains of its nodes, or its admission, before it replies. It replies
// at once to a call that it refuses, and to a repeated disruption whose reservation it does
// not change. A deferral whose deadline is more than machine.WaitMargin away waits for the
// admission or the end of its reservation.
func (c *Controller) disrupt(ev *machine.Disrupt, out []machine.Effect) ([]machine.Effect, error) {
	c.calls.Set(ev.Reply, callDisrupt)
	d := ev.Disruption
	var sum crypto.Digest
	err := c.serve(&ev.Metadata, d.Scope, d.Incarnation)
	if err == nil {
		sum, err = digest(d)
	}
	if err == nil {
		err = c.authorize(ev.Metadata.Caller, d)
	}
	if err != nil {
		return reply(c, ev.Reply, budget.Decision{}, err, out), nil
	}
	if e, ok := c.budget.reservations.Get(d.ID); ok {
		return c.redisrupt(ev, e, sum, out)
	}
	now := c.clock.Time()
	r := budget.Reservation{Disruption: *d, Digest: sum, Owner: ev.Metadata.Caller, Incarnations: c.incarnations(d)}
	decision := c.decide(d)
	if decision.Allowed {
		r.Admitted = now
	} else {
		c.metrics.deferred.Add(context.Background(), 1)
	}
	mu := &store.Mutation{Reserve: []budget.Reservation{r}, Drain: make([]store.Drain, 0, len(d.Nodes))}
	for _, n := range d.Nodes {
		mu.Drain = append(mu.Drain, store.Drain{Since: now, Node: n, Caller: r.Owner, Disruption: d.ID})
	}
	f := followup{decision: decision, deadline: ev.Deadline, reservation: d.ID, to: ev.Reply}
	return c.commit(&pending{mutation: mu, followups: []followup{f}}, out)
}

// redisrupt takes a disruption whose ID the scope has reserved as e, and whose encoding has
// the digest sum. It allows an admitted disruption, and evaluates a pending one again. It
// commits nothing for a deferral.
func (c *Controller) redisrupt(ev *machine.Disrupt, e *reserved, sum crypto.Digest, out []machine.Effect) (
	[]machine.Effect, error,
) {
	r := e.r
	var decision budget.Decision
	switch {
	case r.Digest != sum:
		err := fmt.Errorf("%w: disruption %v", budget.ErrReusedID, r.Disruption.ID)
		return reply(c, ev.Reply, budget.Decision{}, err, out), nil
	case !r.Admitted.IsZero():
		decision = budget.Decision{Allowed: true}
	case e.ending:
		decision = budget.Decision{Reason: reasonEnding, RetryAfter: RetryAfter}
	default:
		decision = c.decide(&r.Disruption)
	}
	if !decision.Allowed {
		c.metrics.deferred.Add(context.Background(), 1)
		if c.waits(e, ev.Deadline) {
			return c.queue(r.Disruption.ID, ev.Reply, ev.Deadline, out), nil
		}
	}
	if !decision.Allowed || !r.Admitted.IsZero() {
		return reply(c, ev.Reply, decision, nil, out), nil
	}
	admitted := *r
	admitted.Admitted = c.clock.Time()
	mu := &store.Mutation{Reserve: []budget.Reservation{admitted}}
	return c.commit(&pending{mutation: mu, followups: []followup{{decision: decision, to: ev.Reply}}}, out)
}

// decide returns the decision of the budget for the pending disruption d at this step. The
// controller admits a disruption that is not negotiable at its Start, and evaluates every
// other one.
func (c *Controller) decide(d *budget.Disruption) budget.Decision {
	if !d.Negotiable {
		return budget.Decision{Reason: reasonStart, RetryAfter: RetryAfter}
	}
	return c.evaluate(d)
}

// waits reports whether a deferred Disrupt call waits for the reservation e until its
// deadline. The call waits when these conditions are true:
//   - e is pending.
//   - No commit ends e.
//   - deadline is more than machine.WaitMargin away.
//
// A call without a deadline gets its deferral at once.
func (c *Controller) waits(e *reserved, deadline time.Time) bool {
	return e.r.Admitted.IsZero() && !e.ending && deadline.Sub(c.clock.Time()) > machine.WaitMargin
}

// queue makes the deferred Disrupt call tok wait for the reservation id. It appends the Timer
// of the call to out, at machine.WaitMargin before deadline.
func (c *Controller) queue(id budget.ID, tok machine.ReplyToken, deadline time.Time, out []machine.Effect,
) []machine.Effect {
	w := waiting{timer: c.next(), to: tok}
	ws, _ := c.budget.waits.Get(id)
	c.budget.waits.Set(id, append(ws, w))
	return append(out, machine.Timer{Token: w.timer, Duration: deadline.Add(-machine.WaitMargin).Sub(c.clock.Time())})
}

// admit evaluates each reservation with a waiting call, in the order of the IDs, after a step
// that applies a commit, takes a report or takes a view. It admits each negotiable one that the
// budget allows, in one commit, and its calls get the admission once the commit has applied.
// An admission does not leave a replica of the drain roles on its nodes, and the drain roles
// are every role, so the admissions of one step do not change the count of
// maxConcurrentNodes.
// admit costs O(w × r) for w waiting reservations and r reservations.
//
// Error modes:
//   - the error of commit.
func (c *Controller) admit(out []machine.Effect) ([]machine.Effect, error) {
	mu := &store.Mutation{}
	now := c.clock.Time()
	for id, ws := range c.budget.waits.All() {
		if len(ws) == 0 {
			continue
		}
		e, _ := c.budget.reservations.Get(id)
		if d := &e.r.Disruption; d.Negotiable && c.evaluate(d).Allowed {
			admitted := *e.r
			admitted.Admitted = now
			mu.Reserve = append(mu.Reserve, admitted)
		}
	}
	if len(mu.Reserve) == 0 {
		return out, nil
	}
	return c.commit(&pending{mutation: mu}, out)
}

// timeout takes the Timer tok of a waiting call at machine.WaitMargin before the deadline of
// the call. It reports whether tok is such a Timer.
//
// The call gets the decision of its reservation with a RetryAfter of zero, and stops waiting.
// Its caller then asks again before its deadline. A reservation without a waiting call keeps
// its entry in waits until the commit that admits or ends it, and admit skips it.
//
// The budget can allow the reservation at the Timer, such as after a Done whose commit is in
// flight. timeout then admits the waiting reservations, and the call gets the admission once
// the commit has applied.
//
// Error modes:
//   - the error of admit.
func (c *Controller) timeout(tok machine.Token, out []machine.Effect) ([]machine.Effect, bool, error) {
	var id budget.ID
	var ws []waiting
	i := -1
	for k, v := range c.budget.waits.All() {
		if i = slices.IndexFunc(v, func(w waiting) bool { return w.timer == tok }); i >= 0 {
			id, ws = k, v
			break
		}
	}
	if i < 0 {
		return out, false, nil
	}
	e, _ := c.budget.reservations.Get(id)
	decision := c.decide(&e.r.Disruption)
	if decision.Allowed {
		next, err := c.admit(out)
		return next, true, err
	}
	to := ws[i].to
	c.budget.waits.Set(id, slices.Delete(ws, i, i+1))
	decision.RetryAfter = 0
	return reply(c, to, decision, nil, out), true, nil
}

// authorize returns nil when the controller authorises caller to request d: an operator,
// or the node of a disruption of itself.
//
// Error modes:
//   - an error that wraps ErrCall for a disruption of a node of itself with more than one
//     node.
//   - an error that wraps ErrStale for a disruption of a node of itself whose incarnation
//     is not the live incarnation of the node.
//   - an error that wraps ErrDenied for any other caller.
func (c *Controller) authorize(caller shard.Identity, d *budget.Disruption) error {
	var self *shard.Node
	if d.NodeIncarnation != 0 {
		if len(d.Nodes) != 1 {
			return fmt.Errorf("%w: disruption %v of a node of itself has %d nodes", ErrCall, d.ID, len(d.Nodes))
		}
		n, ok := c.members.live(d.Nodes[0])
		if !ok || n.Incarnation != d.NodeIncarnation {
			return fmt.Errorf("%w: node %s at incarnation %d is not live", ErrStale, d.Nodes[0], d.NodeIncarnation)
		}
		self = n
	}
	if c.unauthenticated || c.operators[caller] || self != nil && caller == self.Identity {
		return nil
	}
	return fmt.Errorf("%w: %q requests disruption %v", ErrDenied, caller, d.ID)
}

// incarnations returns the live incarnation of each node of d, and zero for a node that
// the view does not have.
func (c *Controller) incarnations(d *budget.Disruption) map[shard.NodeID]epoch.Epoch {
	incs := make(map[shard.NodeID]epoch.Epoch, len(d.Nodes))
	for _, n := range d.Nodes {
		if node, ok := c.members.live(n); ok {
			incs[n] = node.Incarnation
			continue
		}
		incs[n] = 0
	}
	return incs
}

// evaluate returns the decision of the budget for the disruption d at this step. The
// budget admits d only after every live node has reported, after the replicas of the
// drain roles have moved off the nodes of d, and while the admitted disruptions cover at
// most maxConcurrentNodes nodes with replicas. It also waits for each transition in flight
// that can end with a replica of the drain roles on a node of d. The drains of the nodes of
// d stop every later move to them. The drain roles of the support matrix are every role,
// so the nodes of d do not host a replica once the budget admits d. d then does not make a
// replica unavailable, and maxUnavailable does not bound it.
func (c *Controller) evaluate(d *budget.Disruption) budget.Decision {
	b := &c.spec.budget
	if !c.members.reported() {
		return budget.Decision{Reason: reasonReports, RetryAfter: RetryAfter}
	}
	remaining, inbound := 0, 0
	for _, n := range d.Nodes {
		remaining += c.assignment.hosts(n, b.DrainRoles)
		inbound += c.engine.Inbound(n, b.DrainRoles)
	}
	if remaining > 0 {
		reason := fmt.Sprintf("%d replicas of the drain roles remain on the nodes", remaining)
		return budget.Decision{Reason: reason, RetryAfter: RetryAfter}
	}
	if inbound > 0 {
		reason := fmt.Sprintf(
			"%d transitions in flight can end with a replica of the drain roles on the nodes",
			inbound,
		)
		return budget.Decision{Reason: reason, RetryAfter: RetryAfter}
	}
	if n := c.coveredNodes(); n > *b.MaxConcurrentNodes {
		reason := fmt.Sprintf("admitted disruptions cover %d nodes with replicas, above maxConcurrentNodes %d",
			n, *b.MaxConcurrentNodes)
		return budget.Decision{Reason: reason, RetryAfter: RetryAfter}
	}
	return budget.Decision{Allowed: true}
}

// coveredNodes counts the nodes that host a replica and that the admitted reservations
// cover, without the reservations that a commit in flight ends.
func (c *Controller) coveredNodes() int {
	nodes := make(map[shard.NodeID]bool)
	for _, e := range c.budget.reservations.All() {
		if e.r.Admitted.IsZero() || e.ending {
			continue
		}
		for _, n := range e.r.Disruption.Nodes {
			if c.assignment.hosts(n, c.spec.roles) > 0 {
				nodes[n] = true
			}
		}
	}
	return len(nodes)
}

// admits reports whether the budget admits a move of the writer from of the placement cur.
// The move makes the writer unavailable until the transition ends. The model of the support
// matrix has one replica per shard, so the move makes its shard unavailable, and
// maxUnavailable admits it above zero. A move of a writer that is unavailable already does
// not count: a writer that failed, or a writer on a node that an admitted reservation
// covers. A move that adds a replica has no source in the view, so it does not count
// either. The controller asks admits for the moves of its passes and for move proposals.
func (c *Controller) admits(cur *shard.Placement, from shard.Replica) bool {
	return *c.spec.budget.MaxUnavailable > 0 || c.failed(cur, from) || c.budget.covered(from.Node)
}

// failed reports whether the writer from of the placement cur has failed: the view does not
// have its node at the incarnation of cur, or the last report of that node incarnation lists
// its replica of the shard at the epoch of cur with a permanent fault. A report of another
// tenure of the shard does not count.
func (c *Controller) failed(cur *shard.Placement, from shard.Replica) bool {
	mb, live := c.members.at(from.Node, cur.Incarnation)
	return !live || mb.failed(cur.Shard, cur.Epoch)
}

// done ends a reservation for its owner or an operator. The controller replies once the
// commit that ends the reservation has applied, and replies at once for a reservation that
// has ended or that a commit in flight ends.
func (c *Controller) done(ev *machine.Done, out []machine.Effect) ([]machine.Effect, error) {
	c.calls.Set(ev.Reply, callDone)
	a := ev.Args
	if err := c.serve(&ev.Metadata, a.Scope, a.Incarnation); err != nil {
		return reply(c, ev.Reply, struct{}{}, err, out), nil
	}
	e, ok := c.budget.reservations.Get(a.ID)
	if !ok || e.ending {
		return reply(c, ev.Reply, struct{}{}, nil, out), nil
	}
	if caller := ev.Metadata.Caller; caller != e.r.Owner && !c.unauthenticated && !c.operators[caller] {
		err := fmt.Errorf("%w: %q ends the reservation of disruption %v", ErrDenied, ev.Metadata.Caller, a.ID)
		return reply(c, ev.Reply, struct{}{}, err, out), nil
	}
	return c.commit(&pending{mutation: release(e), followups: []followup{{to: ev.Reply}}}, out)
}

// arm appends a Timer for each pending reservation without one to out. The Timer of a
// disruption that is not negotiable is at its Start, when the controller admits the
// disruption. The Timer of any other disruption is at its End, when the controller ends the
// reservation. arm costs O(n) for n reservations.
func (c *Controller) arm(out []machine.Effect) []machine.Effect {
	now := c.clock.Time()
	for _, e := range c.budget.reservations.All() {
		if !e.r.Admitted.IsZero() || e.timer != 0 {
			continue
		}
		at := e.r.Disruption.End
		if !e.r.Disruption.Negotiable {
			at = e.r.Disruption.Start
		}
		e.timer = c.next()
		out = append(out, machine.Timer{Token: e.timer, Duration: at.Sub(now)})
	}
	return out
}

// expire takes the Timer tok of a pending reservation, and reports whether tok is the Timer
// of a reservation. It admits a disruption that is not negotiable at its Start, and ends
// the reservation of any other disruption at its End. It returns out unchanged for a Timer
// of no reservation, and for a reservation that a commit ends. An admission replaces the
// reservation without its Timer. It costs O(n) for n reservations.
func (c *Controller) expire(tok machine.Token, out []machine.Effect) ([]machine.Effect, bool, error) {
	var err error
	for _, e := range c.budget.reservations.All() {
		if e.timer != tok || e.ending {
			continue
		}
		if e.r.Disruption.Negotiable {
			out, err = c.commit(&pending{mutation: release(e)}, out)
			return out, true, err
		}
		admitted := *e.r
		admitted.Admitted = c.clock.Time()
		out, err = c.commit(&pending{mutation: &store.Mutation{Reserve: []budget.Reservation{admitted}}}, out)
		return out, true, err
	}
	return out, false, nil
}

// settle ends in one commit the reservations whose disruptions have ended without a Done:
// a pending reservation of a node of itself whose incarnation has ended, and an admitted
// reservation whose nodes have all left the view or restarted, and host no replica. It
// ends none before the first view, because the controller does not know the live nodes
// then.
func (c *Controller) settle(out []machine.Effect) ([]machine.Effect, error) {
	if !c.members.viewed {
		return out, nil
	}
	var ended []*reserved
	for _, e := range c.budget.reservations.All() {
		if !e.ending && c.over(e.r) {
			ended = append(ended, e)
		}
	}
	if len(ended) == 0 {
		return out, nil
	}
	return c.commit(&pending{mutation: release(ended...)}, out)
}

// over reports whether the disruption of r has ended without a Done.
func (c *Controller) over(r *budget.Reservation) bool {
	d := &r.Disruption
	if r.Admitted.IsZero() {
		if d.NodeIncarnation == 0 {
			return false
		}
		n, ok := c.members.live(d.Nodes[0])
		return !ok || n.Incarnation != d.NodeIncarnation
	}
	for _, nid := range d.Nodes {
		n, ok := c.members.live(nid)
		if ok && n.Incarnation == r.Incarnations[nid] || c.assignment.hosts(nid, c.spec.roles) > 0 {
			return false
		}
	}
	return true
}

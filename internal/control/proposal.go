// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// submit takes a proposal of an operator. The controller checks the proposal before any
// effect, and refuses a kind that Config.Kinds does not list. It applies a drain, an
// undrain or a spec with one commit, and runs a pass for a move or an explain. It replies
// once the proposal has applied, and at once to a proposal that it refuses.
func (c *Controller) submit(ev *machine.Submit, out []machine.Effect) ([]machine.Effect, error) {
	c.calls.Set(ev.Reply, callSubmit)
	p := ev.Proposal
	if err := c.serve(&ev.Metadata, p.Scope, p.Incarnation); err != nil {
		return reply(c, ev.Reply, protocol.Receipt{}, err, out), nil
	}
	var err error
	switch {
	case !c.unauthenticated && !c.operators[ev.Metadata.Caller]:
		err = fmt.Errorf("%w: %q submits proposal %v", ErrDenied, ev.Metadata.Caller, p.ID)
	case c.clock.Time().After(p.Expires):
		err = fmt.Errorf("%w: proposal %v expired at %v", ErrCall, p.ID, p.Expires)
	case c.applied.Has(p.ID) || c.proposing[p.ID]:
		err = fmt.Errorf("%w: proposal %v", ErrApplied, p.ID)
	case !slices.Contains(c.kinds, p.Kind):
		err = fmt.Errorf("%w: the controller of %v does not apply the kind %v", ErrCall, c.scopeOf, p.Kind)
	case p.Kind == proposal.Drain || p.Kind == proposal.Undrain:
		return c.drain(ev, out)
	case p.Kind == proposal.Move:
		return c.relocate(ev, out)
	case p.Kind == proposal.Explain:
		return c.explain(ev, out)
	default:
		// New refuses a kind that is not Valid, so the kind left is SpecChange.
		return c.respec(ev, out)
	}
	return reply(c, ev.Reply, protocol.Receipt{}, err, out), nil
}

// drain applies a drain or an undrain proposal with one commit, which records the proposal.
// The receipt of a drain contains the decision of the budget for a disruption of the node,
// so the operator learns whether the node may stop now.
func (c *Controller) drain(ev *machine.Submit, out []machine.Effect) ([]machine.Effect, error) {
	p := ev.Proposal
	var args proposal.NodeArgs
	err := decode(p.Body, &args)
	if err == nil {
		err = c.base(args.Node, p)
	}
	if err != nil {
		return reply(c, ev.Reply, protocol.Receipt{}, err, out), nil
	}
	mu := &store.Mutation{}
	var result []byte
	manual := store.DrainKey{Node: args.Node}
	if p.Kind == proposal.Drain {
		if !c.budget.drains.Has(manual) {
			mu.Drain = []store.Drain{{Since: c.clock.Time(), Node: args.Node, Caller: ev.Metadata.Caller}}
		}
		decision := c.evaluate(&budget.Disruption{Nodes: []shard.NodeID{args.Node}})
		result, err = json.Marshal(&decision)
	} else {
		mu.Undrain = []store.DrainKey{manual}
	}
	out, cerr := c.propose(ev, mu, result, out)
	return out, errors.Join(err, cerr)
}

// base returns nil when the node n is live at the incarnation p.Base.
//
// Error modes:
//   - an error that wraps ErrStale for a node that the view does not have, or that is at
//     another incarnation.
func (c *Controller) base(n shard.NodeID, p *proposal.Proposal) error {
	node, ok := c.members.live(n)
	if !ok || node.Incarnation != p.Base {
		return fmt.Errorf("%w: node %s at incarnation %d is not live", ErrStale, n, p.Base)
	}
	return nil
}

// relocate runs the pass of a move proposal. The pass checks the target with the writer of
// the shard on the node of the proposal against the constraints of the spec. The controller
// starts the move once placement.Validate has accepted it. The model of the support matrix
// has one replica per shard, so the target has the writer alone.
func (c *Controller) relocate(ev *machine.Submit, out []machine.Effect) ([]machine.Effect, error) {
	p := ev.Proposal
	var args proposal.MoveArgs
	err := decode(p.Body, &args)
	if err == nil {
		err = c.movable(&args, p)
	}
	if err != nil {
		return reply(c, ev.Reply, protocol.Receipt{}, err, out), nil
	}
	s := &submitted{proposal: p, caller: ev.Metadata.Caller, shard: args.Shard, to: ev.Reply}
	placer := &movePlacer{replicas: []shard.Replica{{Node: args.Node, Role: c.spec.writer}}, shard: args.Shard}
	return c.inquire(s, placer, out), nil
}

// movable returns nil when the proposal p can move the writer of the shard of args to the
// node of args.
//
// Error modes:
//   - an error that wraps ErrCall for a shard that the scope does not have, and for a
//     writer that is on the node already.
//   - an error that wraps ErrStale for a placement whose epoch is not p.Base, for a shard
//     with a transition in flight, and for a node that the view does not have.
func (c *Controller) movable(args *proposal.MoveArgs, p *proposal.Proposal) error {
	if !c.has(args.Shard) {
		return fmt.Errorf("%w: the scope has no shard %v", ErrCall, args.Shard)
	}
	cur := c.assignment.placement(args.Shard)
	if cur.Epoch != p.Base {
		return fmt.Errorf("%w: shard %v is at the epoch %d", ErrStale, args.Shard, cur.Epoch)
	}
	if _, ok := c.assignment.transitions.Get(args.Shard); ok {
		return fmt.Errorf("%w: shard %v has a transition in flight", ErrStale, args.Shard)
	}
	if w, _ := writer(&cur, c.spec.writer); w.Node == args.Node {
		return fmt.Errorf("%w: the writer of shard %v is on node %s", ErrCall, args.Shard, args.Node)
	}
	if _, ok := c.members.live(args.Node); !ok {
		return fmt.Errorf("%w: node %s is not live", ErrStale, args.Node)
	}
	return nil
}

// explain runs the pass of an explain proposal. The pass returns the cost of each tier of
// the goals, and placement.Validate returns the violations of the replicas of the shard.
func (c *Controller) explain(ev *machine.Submit, out []machine.Effect) ([]machine.Effect, error) {
	p := ev.Proposal
	var args proposal.ShardArgs
	err := decode(p.Body, &args)
	switch {
	case err != nil:
	case !c.has(args.Shard):
		err = fmt.Errorf("%w: the scope has no shard %v", ErrCall, args.Shard)
	case p.Base != c.assignment.generation:
		err = fmt.Errorf("%w: the scope is at the generation %d", ErrStale, c.assignment.generation)
	}
	if err != nil {
		return reply(c, ev.Reply, protocol.Receipt{}, err, out), nil
	}
	s := &submitted{proposal: p, caller: ev.Metadata.Caller, shard: args.Shard, to: ev.Reply}
	return c.inquire(s, &explainer{goals: c.spec.goals}, out), nil
}

// respec applies a proposal of the next spec of the namespace with one commit, which
// records the proposal and replaces the accepted spec with the expanded spec of the
// proposal. A later spec proposal checks its Base against that spec before the commit
// applies.
func (c *Controller) respec(ev *machine.Submit, out []machine.Effect) ([]machine.Effect, error) {
	p := ev.Proposal
	var next config.Spec
	err := decode(p.Body, &next)
	var to *config.Resolved
	if err == nil {
		to, err = c.successor(p, &next)
	}
	var raw []byte
	if err == nil {
		raw, err = json.Marshal(&to.Spec)
	}
	if err != nil {
		return reply(c, ev.Reply, protocol.Receipt{}, err, out), nil
	}
	c.accepted = raw
	return c.propose(ev, &store.Mutation{Spec: raw}, nil, out)
}

// successor returns the resolution of the spec next, which the proposal p proposes as the
// successor of the accepted spec of the namespace. The spec must have the version after
// p.Base, p.Base must be the version of the accepted spec, and config.Change must accept
// the change.
//
// Error modes:
//   - the error of Resolve for an accepted spec that does not resolve.
//   - an error that wraps ErrStale for a p.Base that is not the version of the accepted
//     spec.
//   - an error that wraps ErrCall for a next whose version is not the version after
//     p.Base.
//   - an error that wraps ErrSpec and the error of config.Resolve for a next that does not
//     resolve.
//   - the error of Build for a next whose strategies do not build.
//   - an error that wraps ErrSpec and the error of config.Change for a change that the
//     namespace cannot take.
func (c *Controller) successor(p *proposal.Proposal, next *config.Spec) (*config.Resolved, error) {
	from, err := Resolve(c.registry, c.provided, c.accepted)
	if err != nil {
		return nil, err
	}
	if p.Base != from.Spec.Version {
		return nil, fmt.Errorf("%w: the accepted spec is at the version %d", ErrStale, from.Spec.Version)
	}
	if want := p.Base + 1; next.Version != want {
		return nil, fmt.Errorf("%w: the spec has the version %d, and the next version is %d",
			ErrCall, next.Version, want)
	}
	to, err := config.Resolve(c.registry, next, c.provided)
	if err != nil {
		return nil, fmt.Errorf("%w: version %d: %w", ErrSpec, next.Version, err)
	}
	if _, err := Build(c.registry, to); err != nil {
		return nil, err
	}
	if err := config.Change(from, to); err != nil {
		return nil, fmt.Errorf("%w: version %d: %w", ErrSpec, next.Version, err)
	}
	return to, nil
}

// propose commits the mutation mu of the proposal of the Submit ev with the record of the
// proposal, and forgets the applied proposals whose Expires has passed. Once the commit has
// applied, the controller replies with a receipt whose Result is result.
func (c *Controller) propose(ev *machine.Submit, mu *store.Mutation, result []byte, out []machine.Effect) (
	[]machine.Effect, error,
) {
	p := ev.Proposal
	mu.Applied = []store.Applied{{Caller: ev.Metadata.Caller, Proposal: *p}}
	mu.Forget = c.expired()
	c.proposing[p.ID] = true
	f := followup{result: result, proposal: p.ID, to: ev.Reply}
	return c.commit(&pending{mutation: mu, followups: []followup{f}}, out)
}

// has reports whether the layout of the scope has the shard sid.
func (c *Controller) has(sid shard.ID) bool {
	_, ok := c.assignment.index.Get(sid)
	return ok
}

// inquire sends the pass of the proposal s with the placer placer, and keeps s until the
// result of the pass arrives.
func (c *Controller) inquire(s *submitted, placer placement.Placer, out []machine.Effect) []machine.Effect {
	c.proposing[s.proposal.ID] = true
	in := c.input()
	tok := c.next()
	c.passes.plans[tok] = &plan{started: c.clock.Time(), call: s, generation: in.Current.Generation}
	return append(out, machine.Plan{Placer: placer, Input: in, Constraints: c.spec.constraints, Token: tok})
}

// expired returns the IDs of the applied proposals whose Expires has passed, in the order
// of the IDs.
func (c *Controller) expired() []proposal.ID {
	now := c.clock.Time()
	var ids []proposal.ID
	for id, expires := range c.applied.All() {
		if expires.Before(now) {
			ids = append(ids, id)
		}
	}
	return ids
}

// decode decodes the JSON body into args. It refuses a key that args does not declare.
//
// Error modes:
//   - an error that wraps ErrCall and the error of encoding/json for a body that does not
//     decode into args.
func decode(body []byte, args any) error {
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(args); err != nil {
		return fmt.Errorf("%w: the body of the proposal: %w", ErrCall, err)
	}
	return nil
}

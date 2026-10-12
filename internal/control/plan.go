// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control

import (
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"time"

	"go.thesmos.sh/core/crypto/sha256"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/rand/pcg"

	"go.thesmos.sh/sharder/internal/engine"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The keys of the attributes of the logs of a pass, besides the namespace and the cell
// that the logger of a controller binds.
const (
	// attrError is an error that does not stop the controller.
	attrError = "error"
	// attrShard is the shard of a move or a violation.
	attrShard = "shard"
	// attrNode is the node of a violation.
	attrNode = "node"
	// attrConstraint is the constraint of a violation.
	attrConstraint = "constraint"
)

// passes is the state of the planning passes of a controller. The placement passes run one
// at a time. The pass of a move or an explain proposal runs while a placement pass runs.
type passes struct {
	// plans maps the token of each Plan in flight to its pass.
	plans map[machine.Token]*plan
	// moving maps the shard of each move proposal whose transition has started to the
	// record and the reply of the proposal, until the batch that commits the first record
	// of the transition.
	moving map[shard.ID]moving
	// running is the token of the placement pass in flight, and zero without one.
	running machine.Token
	// interval is the token of the Timer of the next periodic pass, and zero without one.
	interval machine.Token
	// cursor is the repair cursor of the next placement pass.
	cursor shard.ID
	// queued reports that a trigger arrived while a placement pass was in flight.
	queued bool
	// refused reports that the bounds of the engine refused a move of the last plan.
	refused bool
}

// plan is a pass in flight: a placement pass, or the pass of a move or an explain
// proposal.
type plan struct {
	// started is the instant at which the controller sent the Plan.
	started time.Time
	// call is the proposal of a move or an explain pass, and nil for a placement pass.
	call *submitted
	// generation is the generation of the assignment that the pass reads.
	generation epoch.Epoch
	// emergency reports that the pass restores missing and faulted replicas first.
	emergency bool
}

// submitted is a move or an explain proposal that a pass checks, with its call.
type submitted struct {
	// proposal is the proposal.
	proposal *proposal.Proposal
	// caller is the identity that submitted the proposal.
	caller shard.Identity
	// to is the token of the Submit.
	to machine.ReplyToken
	// shard is the shard of the proposal.
	shard shard.ID
}

// moving is a move proposal whose transition has started: the record that the commit of
// the first record of the transition adds, and the reply that follows that commit.
type moving struct {
	// followup is the reply to the Submit of the proposal.
	followup followup
	// applied is the record of the proposal.
	applied store.Applied
}

// movePlacer is the placer of the pass of a move proposal. It returns the target that the
// proposal asks for, so placement.Validate checks the move as it checks every plan.
type movePlacer struct {
	// replicas lists the replicas of the target.
	replicas []shard.Replica
	// shard is the shard of the target.
	shard shard.ID
}

var _ placement.Placer = (*movePlacer)(nil)

// Place returns the plan with the one target of p.
func (p *movePlacer) Place(_ context.Context, in *placement.Input) (placement.Plan, error) {
	targets := map[shard.ID]placement.Target{p.shard: {Replicas: p.replicas}}
	return placement.Plan{Targets: targets, RepairNext: in.RepairFrom}, nil
}

// Schedule returns the zero Schedule. The controller runs the pass of a move proposal
// once.
func (*movePlacer) Schedule() placement.Schedule {
	return placement.Schedule{}
}

// explainer is the placer of the pass of an explain proposal. It returns a plan without a
// target, with the cost of each tier of the goals for the current assignment, as the
// planner sums them.
type explainer struct {
	// goals are the goals of the placer of the controller.
	goals []goal
}

var _ placement.Placer = (*explainer)(nil)

// Place returns the cost of each tier of the goals of p for in.Current, in ascending tier
// order.
//
// Error modes:
//   - the error of placement.NewState.
//   - an error that wraps placement.ErrCost for a cost that is not finite.
func (p *explainer) Place(_ context.Context, in *placement.Input) (placement.Plan, error) {
	st, err := placement.NewState(in)
	if err != nil {
		return placement.Plan{}, fmt.Errorf("control: the state of an explanation: %w", err)
	}
	goals := slices.SortedStableFunc(slices.Values(p.goals), func(a, b goal) int { return cmp.Compare(a.tier, b.tier) })
	var costs []float64
	for i, g := range goals {
		c := g.weight * g.goal.Cost(st)
		if math.IsNaN(c) || math.IsInf(c, 0) {
			return placement.Plan{}, fmt.Errorf("%w: goal %s costs %v", placement.ErrCost, g.goal.Name(), c)
		}
		if i > 0 && goals[i-1].tier == g.tier {
			costs[len(costs)-1] += c
			continue
		}
		costs = append(costs, c)
	}
	return placement.Plan{Costs: costs, RepairNext: in.RepairFrom}, nil
}

// Schedule returns the zero Schedule. The controller runs the pass of an explain proposal
// once.
func (*explainer) Schedule() placement.Schedule {
	return placement.Schedule{}
}

// seedDomain is the domain of the hash that derives the seed of a pass from its snapshot.
const seedDomain = "sharder.pass.seed"

// seed returns the source of randomness of a pass that reads the state snap. The source is
// a PCG whose seed is the first 8 bytes of the SHA-256 digest of seedDomain and the Term,
// the SpecVersion, the Generation, the Membership and the Reports of snap, each as 8
// big-endian bytes. The same state gives the same source, so a recorded pass replays.
func seed(snap *placement.Snapshot) rand.Rand {
	b := make([]byte, 0, len(seedDomain)+5*8)
	b = append(b, seedDomain...)
	b = binary.BigEndian.AppendUint64(b, uint64(snap.Term))
	b = binary.BigEndian.AppendUint64(b, uint64(snap.SpecVersion))
	b = binary.BigEndian.AppendUint64(b, uint64(snap.Generation))
	b = binary.BigEndian.AppendUint64(b, uint64(snap.Membership))
	b = binary.BigEndian.AppendUint64(b, snap.Reports)
	d := sha256.Hasher{}.Hash(b)
	return pcg.New(rand.Seed(binary.BigEndian.Uint64(d.Bytes())))
}

// change returns the writer of cur in the role role, and the writer of the target t that
// takes its place. The model of the support matrix has one replica per shard: its writer.
// change returns the zero Replica for a cur without a writer, and reports false for a
// target without a writer, which has a deficit. A plan lists only the shards whose
// replicas it changes, so the writer of a target is on another node than the writer of
// cur.
func change(cur *shard.Placement, t *placement.Target, role shard.Role) (shard.Replica, shard.Replica, bool) {
	to, ok := writer(&shard.Placement{Replicas: t.Replicas}, role)
	from, _ := writer(cur, role)
	return from, to, ok
}

// trigger starts a placement pass, or queues one more pass when a pass is in flight. It
// does not start a pass before the first view. The first view starts the first pass.
func (c *Controller) trigger(out []machine.Effect) ([]machine.Effect, error) {
	if !c.members.viewed {
		return out, nil
	}
	if c.passes.running != 0 {
		c.passes.queued = true
		return out, nil
	}
	out, err := c.adopt(out)
	in := c.input()
	tok := c.next()
	c.passes.running = tok
	c.passes.plans[tok] = &plan{started: c.clock.Time(), generation: in.Current.Generation, emergency: in.Emergency}
	return append(out, machine.Plan{Placer: c.spec.placer, Input: in, Constraints: c.spec.constraints, Token: tok}), err
}

// adopt makes a newer accepted spec the spec of the controller, before the pass that
// applies it. The engine takes the settings of the spec, and the first commit of the pass
// records the version of the spec in the record of the controller.
func (c *Controller) adopt(out []machine.Effect) ([]machine.Effect, error) {
	s := c.newer
	if s == nil {
		return out, nil
	}
	c.newer, c.spec = nil, s
	out, err := c.engine.Configure(&s.settings, out)
	out, cerr := c.commit(&pending{mutation: &store.Mutation{Cell: c.record(s.version)}}, c.periodic(out))
	return out, errors.Join(err, cerr)
}

// periodic arms the Timer of the next periodic pass when no Timer is armed. The schedule
// of the planner always has an interval.
func (c *Controller) periodic(out []machine.Effect) []machine.Effect {
	if c.passes.interval == 0 {
		c.passes.interval = c.next()
		out = append(out, machine.Timer{Token: c.passes.interval, Duration: c.spec.schedule.Interval})
	}
	return out
}

// input returns the input of a pass over the committed state of the scope. The pass reads
// a copy of the assignment, because the controller changes the assignment during the pass.
func (c *Controller) input() *placement.Input {
	a := c.assignment
	in := &placement.Input{
		Clock:       c.clock,
		Transitions: make(map[shard.ID]placement.InFlight, len(a.transitions)),
		Faults:      c.members.faults(),
		Draining:    make(map[shard.NodeID]bool),
		ShardLoad:   c.members.loads(a, c.spec.writer, c.load),
		Model:       c.spec.model.Model,
		Layout:      a.layout,
		Current:     a.current(c.scopeOf),
		Nodes:       make([]shard.Node, 0, len(c.members.view.Nodes)),
		Metrics:     c.spec.metrics,
		Snapshot: placement.Snapshot{
			Term:        c.term,
			SpecVersion: c.spec.version,
			Generation:  a.generation,
			Membership:  c.members.view.Revision,
			Reports:     c.reports,
		},
		Parallelism: c.parallelism,
		RepairFrom:  c.passes.cursor,
	}
	in.Rand = seed(&in.Snapshot)
	for sid, t := range a.transitions {
		in.Transitions[sid] = placement.InFlight{To: t.To.Node, Fallback: t.Fallback.Node}
	}
	for _, n := range c.members.view.Nodes {
		if c.budget.draining(n.ID) {
			in.Draining[n.ID] = true
			continue
		}
		in.Nodes = append(in.Nodes, n)
	}
	in.Emergency = c.emergency(in)
	return in
}

// emergency reports whether a shard of in without a transition in flight has no writer on
// a live node, or a replica that in.Faults lists. The writer of a shard without one is the
// zero Replica, whose node the view does not have.
func (c *Controller) emergency(in *placement.Input) bool {
	if len(in.Faults) > 0 {
		return true
	}
	for _, s := range in.Layout.Shards {
		if _, ok := in.Transitions[s.ID]; ok {
			continue
		}
		p := in.Current.Placements[s.ID]
		w, _ := writer(&p, c.spec.writer)
		if _, live := c.members.live(w.Node); !live {
			return true
		}
	}
	return false
}

// planned takes the result of a pass: a placement pass, or the pass of a move or an
// explain proposal. It reports whether the result is of a pass in flight.
func (c *Controller) planned(ev *machine.Planned, out []machine.Effect) ([]machine.Effect, bool, error) {
	p, ok := c.passes.plans[ev.Token]
	if !ok {
		return out, false, nil
	}
	delete(c.passes.plans, ev.Token)
	var err error
	switch {
	case p.call == nil:
		out, err = c.placed(p, ev, out)
	case p.call.proposal.Kind == proposal.Move:
		out, err = c.moved(p.call, ev, out)
	default:
		out, err = c.explained(p, ev, out)
	}
	return out, true, err
}

// placed takes the plan of a placement pass. The controller logs the error of a pass that
// failed and every violation, and starts the moves of a plan. It then starts the pass that
// a trigger queued.
func (c *Controller) placed(p *plan, ev *machine.Planned, out []machine.Effect) ([]machine.Effect, error) {
	c.passes.running = 0
	h := c.metrics.periodic
	if p.emergency {
		h = c.metrics.emergency
	}
	h.Record(context.Background(), c.clock.Time().Sub(p.started).Seconds())
	for _, v := range ev.Violations {
		c.logger.LogAttrs(context.Background(), slog.LevelWarn, "control: a replica breaks a constraint",
			slog.String(attrShard, v.Shard.String()), slog.String(attrNode, string(v.Node)),
			slog.String(attrConstraint, string(v.Constraint)), slog.Any(attrError, v.Err))
	}
	var err error
	if ev.Err != nil {
		c.logger.LogAttrs(context.Background(), slog.LevelError, "control: a pass failed", slog.Any(attrError, ev.Err))
	} else {
		c.passes.cursor = ev.Plan.RepairNext
		out, err = c.moves(&ev.Plan, out)
	}
	if err != nil || !c.passes.queued {
		return out, err
	}
	c.passes.queued = false
	return c.trigger(out)
}

// moves starts the move of each shard that the plan pl changes, in the order of the IDs of
// the shards, and commits the next batch of the engine. It skips a target on a node that
// drains, because the drain started after the input of the pass. The commit of the drain
// starts the next pass. The budget defers a move that makes more replicas of its shard
// unavailable than maxUnavailable allows. The bounds of the engine refuse a move above
// them, and the end of a transition then starts a pass.
func (c *Controller) moves(pl *placement.Plan, out []machine.Effect) ([]machine.Effect, error) {
	var fault error
	refused := false
	for _, sid := range slices.SortedFunc(maps.Keys(pl.Targets), shard.ID.Compare) {
		t := pl.Targets[sid]
		cur := c.assignment.placement(sid)
		from, to, ok := change(&cur, &t, c.spec.writer)
		if !ok {
			continue
		}
		if c.budget.draining(to.Node) {
			continue
		}
		if !c.admits(from) {
			c.metrics.deferred.Add(context.Background(), 1)
			continue
		}
		mv := c.move(&cur, from, to)
		started, err := c.engine.Start(&mv)
		if errors.Is(err, engine.ErrMove) {
			c.logger.LogAttrs(context.Background(), slog.LevelWarn, "control: the engine does not start a move",
				slog.String(attrShard, sid.String()), slog.Any(attrError, err))
			continue
		}
		//dokimi:mutate-skip sbr-delete: Start returns another error only for an event that the lifecycle of a transition refuses, and a pass sends no such event
		fault = errors.Join(fault, err)
		if !started {
			refused = true
			c.metrics.deferred.Add(context.Background(), 1)
			continue
		}
		c.count(sid, from)
	}
	c.passes.refused = refused
	return c.batchAfter(out, fault)
}

// move returns the move of the replica from of the placement cur to the replica to. Each
// endpoint has the incarnation of its node in the view, and a source that the view does
// not have gets incarnation zero. The fallback is the source when the view has its node,
// the node does not drain, and its replica of the shard has no fault. The move has no
// fallback otherwise.
func (c *Controller) move(cur *shard.Placement, from, to shard.Replica) engine.Move {
	mv := engine.Move{
		From:        c.endpoint(from),
		To:          c.endpoint(to),
		Placement:   *cur,
		SpecVersion: c.spec.version,
	}
	mb, live := c.members.nodes[from.Node]
	if live && !c.budget.draining(from.Node) && !mb.faulted(cur.Shard) {
		mv.Fallback = mv.From
	}
	return mv
}

// endpoint returns the endpoint of the replica r at the incarnation of its node in the
// view, and at incarnation zero for a node that the view does not have. The zero Replica
// has the zero Endpoint.
func (c *Controller) endpoint(r shard.Replica) store.Endpoint {
	ep := store.Endpoint{Node: r.Node, Role: r.Role}
	if n, ok := c.members.live(r.Node); ok {
		ep.Incarnation = n.Incarnation
	}
	return ep
}

// count adds a started move of the replica from of the shard sid to the counter of its
// reason. The reason is a failure for a missing replica, a node that the view does not
// have and a faulted replica. It is a drain for a draining node, and a rebalance
// otherwise.
func (c *Controller) count(sid shard.ID, from shard.Replica) {
	ctx := context.Background()
	mb, live := c.members.nodes[from.Node]
	switch {
	case !live || mb.faulted(sid):
		c.metrics.failure.Add(ctx, 1)
	case c.budget.draining(from.Node):
		c.metrics.drain.Add(ctx, 1)
	default:
		c.metrics.rebalance.Add(ctx, 1)
	}
}

// moved takes the plan of the pass of the move proposal s. The controller starts the move
// of a plan that placement.Validate accepted, unless the node of the target has started to
// drain during the pass. It replies once the batch that commits the first record of the
// transition has applied.
func (c *Controller) moved(s *submitted, ev *machine.Planned, out []machine.Effect) ([]machine.Effect, error) {
	cur := c.assignment.placement(s.shard)
	t := ev.Plan.Targets[s.shard]
	from, to, _ := change(&cur, &t, c.spec.writer)
	var err, fault error
	switch {
	case ev.Err != nil:
		err = fmt.Errorf("%w: the move breaks a rule of placement: %w", ErrCall, ev.Err)
	case cur.Epoch != s.proposal.Base:
		err = fmt.Errorf("%w: shard %v moved during the pass of the proposal", ErrStale, s.shard)
	case c.budget.draining(to.Node):
		err = fmt.Errorf("%w: node %s started to drain during the pass of the proposal", ErrStale, to.Node)
	default:
		mv := c.move(&cur, from, to)
		var started bool
		started, fault = c.engine.Start(&mv)
		switch {
		case errors.Is(fault, engine.ErrMove):
			err = fmt.Errorf("%w: %w", ErrStale, fault)
		case !started:
			err = fmt.Errorf("%w: shard %v", ErrBusy, s.shard)
		}
	}
	if err != nil {
		delete(c.proposing, s.proposal.ID)
		return reply(c, s.to, protocol.Receipt{}, err, out), nil
	}
	c.metrics.rebalance.Add(context.Background(), 1)
	c.passes.moving[s.shard] = moving{
		applied:  store.Applied{Caller: s.caller, Proposal: *s.proposal},
		followup: followup{proposal: s.proposal.ID, to: s.to},
	}
	return c.batchAfter(out, fault)
}

// explained takes the plan of the pass of an explain proposal, and replies with the
// violations of the replicas of the shard and the cost of each tier of the goals.
func (c *Controller) explained(p *plan, ev *machine.Planned, out []machine.Effect) ([]machine.Effect, error) {
	s := p.call
	delete(c.proposing, s.proposal.ID)
	if err := ev.Err; err != nil {
		err = fmt.Errorf("%w: the explanation of shard %v: %w", ErrCall, s.shard, err)
		return reply(c, s.to, protocol.Receipt{}, err, out), nil
	}
	exp := proposal.Explanation{Violations: []proposal.Violation{}, Costs: ev.Plan.Costs}
	for _, v := range ev.Violations {
		if v.Shard == s.shard {
			exp.Violations = append(exp.Violations, proposal.Violation{
				Constraint: v.Constraint,
				Node:       v.Node,
				Reason:     v.Err.Error(),
				Role:       v.Role,
			})
		}
	}
	result, err := json.Marshal(&exp)
	receipt := protocol.Receipt{Result: result, Generation: p.generation, Incarnation: c.incarnation}
	return reply(c, s.to, receipt, nil, out), err
}

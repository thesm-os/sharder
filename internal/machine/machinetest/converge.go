// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machinetest

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/sim"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/internal/engine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/internal/machine/root"
	"go.thesmos.sh/sharder/internal/machine/routes"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The keys of a namespace that converged routes.
const (
	// keyPrefix is the prefix of each key, before its number.
	keyPrefix = "key-"
	// maxKeys is the most keys that converged tries for the range of a shard. Under a uniform
	// hash, 4,096 keys leave one of 16 slots of fixed-slots without a key with a probability
	// below 10^-100.
	maxKeys = 4096
)

// The nodes of the transfer whose script settled measures.
const (
	// moveFrom is the node that the transfer takes the replica from, and its fallback.
	moveFrom shard.NodeID = "from"
	// moveTo is the node that the transfer gives the replica to.
	moveTo shard.NodeID = "to"
)

// converged checks that the system of a world has settled once its faults have ended: each
// transition has ended, one controller serves each cell, and each shard has a writer on a node
// that may take it, which its agent and the routers serve, or no node that may take it. A drain
// has then completed, and repair has moved each replica that a changed constraint or a changed
// node refuses. A world in which an action has raised the schema of a scope has no such state,
// because each host that runs a machine of the scope exits at its error, and converged checks
// nothing in it.
//
//dokimi:converges within=settled
func converged(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	ctx := w.Context()
	if s.upgraded(ctx) {
		return
	}
	for i := range s.specs {
		ns := s.specs[i].Namespace
		inc := s.incarnationOf(ctx, ns)
		top, _ := s.stateOf(ctx, shard.Scope{Namespace: ns}, inc)
		expect.Empty(h, top.Transitions, h.Statement())
		scope, st, placements, placed := s.cellOf(ctx, ns)
		expect.True(h, placed, h.Statement())
		if !placed {
			continue
		}
		expect.Empty(h, st.Transitions, h.Statement())
		expect.Equal(h, s.controllers(w, scope), []string{st.Cell.Controller}, h.Statement())
		view := s.viewOf(scope, inc)
		admissible := s.admissible(&view, &st, s.version(ctx, ns))
		for j := range placements {
			s.settledShard(h, w, ns, &placements[j], admissible, &view)
		}
	}
}

// upgraded reports whether the root scope or the scope of the cell of a namespace of s has a
// schema above the schema that this release writes, as the action upgrade leaves it.
func (s *env) upgraded(ctx context.Context) bool {
	for i := range s.specs {
		scopes := []shard.Scope{{Namespace: s.specs[i].Namespace}}
		if cell, _, _, placed := s.cellOf(ctx, s.specs[i].Namespace); placed {
			scopes = append(scopes, cell)
		}
		for _, scope := range scopes {
			head, err := s.store.Head(ctx, scope)
			assert.NoError(s.tb, err, "the store of a world must read the header of a scope")
			if head.Schema > store.Schema {
				return true
			}
		}
	}
	return false
}

// controllers returns the address of each host of s whose cell machine of the scope serves: the
// machine that the agent of the root of the current epoch started, which has replied to Started
// without an error and has not stopped.
func (s *env) controllers(w *world, scope shard.Scope) []string {
	var out []string
	for _, hh := range s.hosts {
		name := address{host: hh.name, namespace: scope.Namespace, role: roleCell, cell: scope.Cell}.String()
		starter := w.Process(address{host: hh.name, namespace: scope.Namespace, role: roleRootAgent}.String())
		c := hh.cells[name]
		if c == nil || c.machine == nil || starter == nil || c.by != starter.Epoch() {
			continue
		}
		if c.ready && c.err == nil && !c.stopped && c.machine.Err() == nil {
			out = append(out, hh.node.Address)
		}
	}
	return out
}

// admissible returns the nodes of the view of a cell that may take a replica under the spec
// version version. It leaves out each node that a drain of st names, and each node in the zone
// that the constraint of the zones refuses. The first version of a spec of an unconstrained
// variant has no constraint.
func (s *env) admissible(view *membership.View, st *store.State, version epoch.Epoch) []shard.NodeID {
	constrained := !s.variant.unconstrained || version > firstVersion
	var out []shard.NodeID
	for _, n := range view.Nodes {
		drained := slices.ContainsFunc(st.Drains, func(d store.Drain) bool { return d.Node == n.ID })
		refused := constrained && slices.Contains(n.Labels, tag.Tag{Key: zoneLabel, Value: zoneWest})
		if !drained && !refused {
			out = append(out, n.ID)
		}
	}
	return out
}

// settledShard checks the shard of the placement p of the namespace ns against the view of its
// cell. A placement without a replica has no node in admissible. The writer of p is live while
// the view has its node at the incarnation of p. A writer is live and on a node of admissible
// unless admissible is empty. A writer that is not live remains only while admissible is empty.
// The agent of the node of the writer alone may have a tenure of the shard, at the epoch of p,
// and the agent of a live writer has it. A writer that is not live has it while its process runs
// on under ownership.ContinueFenced, and a node that restarts restores no writer. A route takes
// the address of each replica from the view, so the router of each host routes a key of the
// shard to the writer of p at its epoch while the view lists the node of the writer. It has no
// route of a shard without a replica or whose writer has left the view.
func (s *env) settledShard(h *sim.Check, w *world, ns shard.Namespace, p *shard.Placement,
	admissible []shard.NodeID, view *membership.View,
) {
	expect.True(h, len(p.Replicas) <= 1, h.Statement())
	var writer shard.NodeID
	live, listed := false, false
	if len(p.Replicas) == 0 {
		expect.Empty(h, admissible, h.Statement())
	} else {
		writer = p.Replicas[0].Node
		for _, n := range view.Nodes {
			if n.ID == writer {
				listed, live = true, n.Incarnation == p.Incarnation
			}
		}
		if len(admissible) > 0 {
			expect.True(h, live, h.Statement())
			expect.Contains(h, admissible, writer, h.Statement())
		}
	}
	key, found := s.keyIn(ns, p.Range)
	expect.True(h, found, "a key of the simulation falls into the range of each shard")
	for _, hh := range s.hosts {
		part := hh.parts[ns]
		if part.agent == nil || part.router == nil {
			expect.NotNil(h, part.agent, "each host runs the agent of each namespace")
			expect.NotNil(h, part.router, "each host runs the router of each namespace")
			continue
		}
		token, err := part.agent.Token(p.Shard)
		switch {
		case hh.node.ID != writer:
			expect.HasError(h, err, h.Statement())
		case live:
			expect.NoError(h, err, h.Statement())
			expect.Equal(h, token.Epoch, p.Epoch, h.Statement())
		case err == nil:
			expect.Equal(h, token.Epoch, p.Epoch, h.Statement())
		}
		if !found {
			continue
		}
		route, err := part.router.Route(w.Context(), key)
		if !listed {
			expect.ErrorIs(h, err, routes.ErrNoRoute, h.Statement())
			continue
		}
		expect.NoError(h, err, h.Statement())
		expect.Equal(h, route.Shard, p.Shard, h.Statement())
		expect.Equal(h, route.Epoch, p.Epoch, h.Statement())
		nodes := make([]shard.NodeID, 0, len(route.Replicas))
		for _, e := range route.Replicas {
			nodes = append(nodes, e.Node)
		}
		expect.Equal(h, nodes, []shard.NodeID{writer}, h.Statement())
	}
}

// keyIn returns the first key of the form key-<n>, for n below maxKeys, whose position in the
// namespace ns the range r contains. It reports false when none of them falls into r.
func (s *env) keyIn(ns shard.Namespace, r shard.Range) ([]byte, bool) {
	part := s.partitioners[ns]
	var key, pos []byte
	for n := range maxKeys {
		key = strconv.AppendInt(append(key[:0], keyPrefix...), int64(n), 10)
		if pos = part.Position(pos[:0], key); r.Contains(pos) {
			return slices.Clone(key), true
		}
	}
	return nil, false
}

// settled returns the bound of the convergence converged over the systems of the zero variant.
func settled() time.Duration { return (&variant{}).bound() }

// bound returns the bound of the convergence converged over the systems of v: twice the longest
// recovery of a namespace over the built-in presets, because a timer on the slowest clock of a
// world takes twice its duration in the time of the run. The recovery of a preset whose spec does
// not resolve does not count, because build fails the cases of that preset.
func (v *variant) bound() time.Duration {
	r := config.NewRegistry(builtin.Support())
	if err := builtin.Register(r); err != nil {
		return 0
	}
	presets := builtin.Presets()
	specs := make([]config.Spec, 0, len(presets))
	for i := range presets {
		if spec, err := v.spec(namespaces[0], presets[i].Name, maxSlots, ownership.ContinueFenced); err == nil {
			specs = append(specs, spec)
		}
	}
	var move time.Duration
	for i := range specs {
		if m, err := longestMove(r, &specs[i]); err == nil {
			move = max(move, m)
		}
	}
	var bound time.Duration
	for i := range specs {
		if d, err := recovery(r, &specs[i], move); err == nil {
			bound = max(bound, 2*d)
		}
	}
	return bound
}

// longestMove returns the longest move of the protocol of spec: the longest sum of the timeouts
// of the steps of its script up to a failed step and of the abort path of that step, after the
// longest backoff of a step, and the lock delay of a drop on a node that has left the view.
//
// Error modes: the errors of config.Resolve, config.Build and handoff.Protocol.Script for spec.
func longestMove(r *config.Registry, spec *config.Spec) (time.Duration, error) {
	res, err := config.Resolve(r, spec, provided)
	if err != nil {
		return 0, err
	}
	own, err := config.Build[ownership.Config](r, res, config.Ownership)
	if err != nil {
		return 0, err
	}
	protocol, err := config.Build[handoff.Protocol](r, res, config.Handoff)
	if err != nil {
		return 0, err
	}
	script, err := protocol.Script(handoff.Transfer{From: moveFrom, To: moveTo, Fallback: moveFrom})
	if err != nil {
		return 0, err
	}
	var forward, longest time.Duration
	for i, step := range script.Steps {
		forward += step.Timeout
		aborted := forward
		for _, a := range script.Abort[i] {
			aborted += a.Timeout
		}
		longest = max(longest, aborted)
	}
	return engine.RetryMax + longest + time.Duration(own.Lease.LockDelay), nil
}

// recovery returns the longest recovery of the namespace of spec once the faults have ended, as
// one chain of the waits of its machines: the TTL of its lease, the longest backoff of a
// campaign, of a watch of the views, of the moves of a shard, of an agent and of a router, the
// periodic passes of its placer, the move of its cell, and a move for each round of the moves
// of its shards. A pass examines at most maxSourceShards shards as the sources of its moves, so
// the recovery counts a pass for each maxSourceShards shards of the layout and one more. A round
// moves as many shards as the bounds of its budget on the transitions in flight admit, and move
// is the longest move of every preset.
//
// Error modes:
//   - the errors of config.Resolve and config.Build for spec.
//   - the errors of encoding/json for the resolved configuration of the planner of spec.
func recovery(r *config.Registry, spec *config.Spec, move time.Duration) (time.Duration, error) {
	res, err := config.Resolve(r, spec, provided)
	if err != nil {
		return 0, err
	}
	own, err := config.Build[ownership.Config](r, res, config.Ownership)
	if err != nil {
		return 0, err
	}
	placer, err := config.Build[placement.Placer](r, res, config.Placement)
	if err != nil {
		return 0, err
	}
	var conf planner.Config
	if err := json.Unmarshal(res.Spec.Placement.Config, &conf); err != nil {
		return 0, err
	}
	b := res.Spec.Budget
	perRound := min(*b.MaxTransitions, *b.MaxNodeTransitions)
	rounds := (maxSlots + perRound - 1) / perRound
	examined := max(conf.Search.MaxSourceShards, 1)
	backoffs := root.CampaignRetryMax + control.ViewRetryMax + control.MoveRetryMax + agent.RetryMax + routes.RetryMax
	passes := time.Duration(1+(maxSlots+examined-1)/examined) * placer.Schedule().Interval
	return time.Duration(own.Lease.TTL) + backoffs + passes + time.Duration(1+rounds)*move, nil
}

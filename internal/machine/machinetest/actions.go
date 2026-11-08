// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machinetest

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/sim"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The facts of the calls of the actions.
const (
	// intruder is an identity that is neither an operator nor the identity of a node.
	intruder shard.Identity = "intruder"
	// expiry is the time from the call of a proposal to its Expires. It outlasts every run on
	// the fastest clock of a world.
	expiry = 1000 * time.Hour
	// callFor is the time from a call to its deadline, for a call with a deadline.
	callFor = time.Minute
	// planned is the latest start of a disruption after its call.
	planned = 10 * time.Minute
	// writeKey is the key of each write of the action write.
	writeKey = "value"
	// maxPick is the most positions that an action chooses in a list of the world, such as a
	// shard, a callback or a disruption.
	maxPick = 16
)

// firstProcess is the name of the root of the first namespace on the first host. Every world
// has it, so an action reads the env of its world through it.
var firstProcess = address{host: hostPrefix + "1", namespace: namespaces[0], role: roleRoot}.String()

// operations lists the kinds of the proposals of the action submit.
var operations = []proposal.OperationKind{proposal.SpecChange, proposal.Drain, proposal.Undrain}

// target is the namespace and the host that an action chooses. The namespace is the position
// of a namespace of the system, and the host the position of a host, from 0. A world may have
// fewer of either, and the action then does nothing.
type target struct {
	// namespace is the position of the namespace in namespaces.
	namespace int
	// host is the position of the host in the hosts of the env.
	host int
}

// targets is the generator of the target of an action: a namespace and a host.
var targets = prop.Composite(func(c *prop.Case) target {
	return target{
		namespace: c.Draw(prop.Integer(0, maxNamespaces-1), "namespace"),
		host:      c.Draw(prop.Integer(0, maxHosts-1), "host"),
	}
})

// resolve returns the namespace and the host of t in the env of w, and reports false for a
// target that the world does not have.
func (t target) resolve(s *env) (shard.Namespace, *host, bool) {
	if t.namespace >= len(s.specs) || t.host >= len(s.hosts) {
		return "", nil, false
	}
	return s.specs[t.namespace].Namespace, s.hosts[t.host], true
}

// nextID returns an ID that no earlier call of the env has returned, for a proposal or a
// disruption.
func (s *env) nextID() id.ID {
	s.ids++
	var b [id.Size128]byte
	binary.BigEndian.PutUint64(b[8:], s.ids)
	return id.New128(b)
}

// incarnationOf returns the namespace incarnation that the header of the root scope of ns
// records, and the zero incarnation before the root creates the namespace.
func (s *env) incarnationOf(ctx context.Context, ns shard.Namespace) shard.NamespaceIncarnation {
	h, err := s.store.Head(ctx, shard.Scope{Namespace: ns})
	assert.NoError(s.tb, err, "the store of a world must read the header of a root scope")
	return h.Incarnation
}

// stateOf returns the state of the scope and its placements, for the namespace incarnation
// inc. It fails the case at an error of the store other than the incarnation of a scope that
// the action recreate has created again.
func (s *env) stateOf(ctx context.Context, scope shard.Scope, inc shard.NamespaceIncarnation) (
	store.State, []shard.Placement,
) {
	st, cur, err := s.store.Read(ctx, scope, inc, store.Query{}, page.Page{})
	if errors.Is(err, shard.ErrIncarnation) {
		return store.State{}, nil
	}
	assert.NoError(s.tb, err, "the store of a world must read a scope")
	var placements []shard.Placement
	for p, perr := range cur.Seq(ctx) {
		assert.NoError(s.tb, perr, "the cursor of the store of a world must yield its placements")
		placements = append(placements, p)
	}
	assert.NoError(s.tb, cur.Close(), "the cursor of the store of a world must close")
	return st, placements
}

// version returns the version of the spec that the root scope of the namespace ns records, and
// epoch.Zero before the root creates the namespace.
func (s *env) version(ctx context.Context, ns shard.Namespace) epoch.Epoch {
	top, _ := s.stateOf(ctx, shard.Scope{Namespace: ns}, s.incarnationOf(ctx, ns))
	if len(top.Spec) == 0 {
		return epoch.Zero
	}
	var spec config.Spec
	assert.NoError(s.tb, json.Unmarshal(top.Spec, &spec), "the spec of a root scope must decode")
	return spec.Version
}

// cellOf returns the scope and the state of the cell of the namespace ns, and its placements.
// It reports false before the root has placed the cell.
func (s *env) cellOf(ctx context.Context, ns shard.Namespace) (shard.Scope, store.State, []shard.Placement, bool) {
	inc := s.incarnationOf(ctx, ns)
	_, cells := s.stateOf(ctx, shard.Scope{Namespace: ns}, inc)
	if len(cells) == 0 {
		return shard.Scope{}, store.State{}, nil, false
	}
	scope := shard.Scope{Namespace: ns, Cell: shard.CellID(ulid.Format(cells[0].Shard.ID()))}
	st, placements := s.stateOf(ctx, scope, inc)
	return scope, st, placements, true
}

// viewOf returns the last view of the scope in the namespace incarnation inc, and the zero
// view before the first join.
func (s *env) viewOf(scope shard.Scope, inc shard.NamespaceIncarnation) membership.View {
	r := s.members.Views(scope, inc)
	defer r.Close()
	views, _, err := r.Next()
	if err != nil || len(views) == 0 {
		return membership.View{}
	}
	return views[len(views)-1]
}

// submit submits a proposal as a call of a client: a spec edit to the root of a host, or a
// drain or an undrain of the node of a host to the controller of the cell. The case chooses
// the namespace, the kind, the host and whether an operator or an intruder submits it.
//
//dokimi:action call
func submit(c *prop.Case) func(w *world, client int) {
	t := c.Draw(targets, "target")
	kind := c.Draw(prop.SampledFrom(operations...), "kind")
	caller := c.Draw(prop.SampledFrom(operator, intruder), "caller")
	return func(w *world, client int) {
		s := sim.Dependency[*env](w, w.Process(firstProcess))
		if ns, h, ok := t.resolve(s); ok {
			s.propose(w, client, ns, h, kind, caller)
		}
	}
}

// propose submits a proposal from caller as a call of client. A proposal of proposal.SpecChange
// is a spec edit of the namespace ns to the root on the host h, which raises the version of the
// spec and selects the placement of the edits of the env. A drain or an undrain of the node of h
// goes to the controller of the cell of ns. It submits nothing for a spec edit before the root
// creates the namespace, and for a drain or an undrain before the cell has a controller.
func (s *env) propose(w *world, client int, ns shard.Namespace, h *host, kind proposal.OperationKind,
	caller shard.Identity,
) {
	ctx := w.Context()
	inc := s.incarnationOf(ctx, ns)
	p := &proposal.Proposal{
		Scope:       shard.Scope{Namespace: ns},
		Expires:     w.Now().Add(expiry),
		Incarnation: inc,
		ID:          proposal.ID{ID: s.nextID()},
		Kind:        kind,
	}
	to := address{host: h.name, namespace: ns, role: roleRoot}.String()
	if kind == proposal.SpecChange {
		st, _ := s.stateOf(ctx, p.Scope, inc)
		if len(st.Spec) == 0 {
			return
		}
		var spec config.Spec
		assert.NoError(s.tb, json.Unmarshal(st.Spec, &spec), "the spec of a root scope must decode")
		p.Base, spec.Version = spec.Version, spec.Version+1
		spec.Placement.Config = s.edit
		body, err := json.Marshal(&spec)
		assert.NoError(s.tb, err, "a spec must encode")
		p.Body = body
	} else {
		scope, st, _, placed := s.cellOf(ctx, ns)
		if !placed {
			return
		}
		var err error
		if to, err = s.controllerOf(w, &st.Cell, scope); err != nil {
			return
		}
		p.Scope = scope
		for _, n := range s.viewOf(scope, inc).Nodes {
			if n.ID == h.node.ID {
				p.Base = n.Incarnation
			}
		}
		body, err := json.Marshal(&proposal.NodeArgs{Node: h.node.ID})
		assert.NoError(s.tb, err, "the arguments of a drain must encode")
		p.Body = body
	}
	md := protocol.Metadata{Caller: caller, Version: protocol.Current}
	w.Call(client, to, "submit", []any{kind, h.name, caller}, []any{ns}, func(reply uint64) machine.Event {
		return machine.Submit{Proposal: p, Metadata: md, Reply: machine.ReplyToken(reply)}
	})
}

// disrupt asks the controller of the cell for a planned disruption of the node of a host, as a
// call of a client. The case chooses the namespace, the host, whether the node or an operator
// asks, and whether the disruption is negotiable. Every second call has a deadline, so its
// deferral can wait for its admission.
//
//dokimi:action call
func disrupt(c *prop.Case) func(w *world, client int) {
	t := c.Draw(targets, "target")
	self := c.Draw(prop.Boolean(), "self")
	negotiable := c.Draw(prop.Boolean(), "negotiable")
	start := c.Draw(prop.Duration(0, planned), "start")
	return func(w *world, client int) {
		s := sim.Dependency[*env](w, w.Process(firstProcess))
		ns, h, ok := t.resolve(s)
		if !ok {
			return
		}
		ctx := w.Context()
		scope, st, _, placed := s.cellOf(ctx, ns)
		if !placed {
			return
		}
		to, err := s.controllerOf(w, &st.Cell, scope)
		if err != nil {
			return
		}
		inc := s.incarnationOf(ctx, ns)
		d := budget.Disruption{
			Start:       w.Now().Add(start),
			End:         w.Now().Add(start + planned),
			Scope:       scope,
			Nodes:       []shard.NodeID{h.node.ID},
			ID:          budget.ID{ID: s.nextID()},
			Incarnation: inc,
			Impact:      budget.ProcessRestart,
			Negotiable:  negotiable,
		}
		caller := operator
		if self {
			caller = h.node.Identity
			for _, n := range s.viewOf(scope, inc).Nodes {
				if n.ID == h.node.ID {
					d.NodeIncarnation = n.Incarnation
				}
			}
		}
		s.disruptions = append(s.disruptions, d)
		var deadline time.Time
		if len(s.disruptions)%2 == 0 {
			deadline = w.Now().Add(callFor)
		}
		md := protocol.Metadata{Caller: caller, Version: protocol.Current}
		w.Call(client, to, "disrupt", []any{h.name, self, negotiable}, []any{ns}, func(reply uint64) machine.Event {
			return machine.Disrupt{Disruption: &d, Metadata: md, Deadline: deadline, Reply: machine.ReplyToken(reply)}
		})
	}
}

// done ends the reservation of a disruption that the action disrupt asked for, as a call of a
// client to the controller of its cell. The case chooses the disruption, and whether an
// operator or an intruder ends it.
//
//dokimi:action call
func done(c *prop.Case) func(w *world, client int) {
	at := c.Draw(prop.Integer(0, maxPick-1), "disruption")
	caller := c.Draw(prop.SampledFrom(operator, intruder), "caller")
	return func(w *world, client int) {
		s := sim.Dependency[*env](w, w.Process(firstProcess))
		if at >= len(s.disruptions) {
			return
		}
		d := s.disruptions[at]
		_, st, _, placed := s.cellOf(w.Context(), d.Scope.Namespace)
		if !placed {
			return
		}
		to, err := s.controllerOf(w, &st.Cell, d.Scope)
		if err != nil {
			return
		}
		args := &protocol.DoneArgs{Scope: d.Scope, Incarnation: d.Incarnation, ID: d.ID}
		md := protocol.Metadata{Caller: caller, Version: protocol.Current}
		w.Call(client, to, "done", []any{at, caller}, []any{d.Scope.Namespace}, func(reply uint64) machine.Event {
			return machine.Done{Args: args, Metadata: md, Reply: machine.ReplyToken(reply)}
		})
	}
}

// closeAgent closes the agent of the node of a host, as a call of the application with a
// deadline. The case chooses the namespace, the host and the time to the deadline.
//
//dokimi:action call
func closeAgent(c *prop.Case) func(w *world, client int) {
	t := c.Draw(targets, "target")
	within := c.Draw(prop.Duration(time.Second, callFor), "within")
	return func(w *world, client int) {
		s := sim.Dependency[*env](w, w.Process(firstProcess))
		ns, h, ok := t.resolve(s)
		if !ok {
			return
		}
		deadline := w.Now().Add(within)
		to := address{host: h.name, namespace: ns, role: roleAgent}.String()
		w.Call(client, to, "close", []any{h.name}, []any{ns}, func(reply uint64) machine.Event {
			return machine.Close{Deadline: deadline, Reply: machine.ReplyToken(reply)}
		})
	}
}

// fault reports a fault of a replica of the node of a host, or its recovery, as a call of the
// application to the agent of the node. The case chooses the namespace, the host, the replica,
// and whether the call reports a transient fault, a permanent fault or a recovery.
//
//dokimi:action call
func fault(c *prop.Case) func(w *world, client int) {
	t := c.Draw(targets, "target")
	at := c.Draw(prop.Integer(0, maxPick-1), "replica")
	class := c.Draw(prop.SampledFrom(shard.FaultNone, shard.FaultTransient, shard.FaultPermanent), "fault")
	return func(w *world, client int) {
		s := sim.Dependency[*env](w, w.Process(firstProcess))
		ns, h, ok := t.resolve(s)
		if !ok {
			return
		}
		_, _, placements, placed := s.cellOf(w.Context(), ns)
		hosted := slices.DeleteFunc(placements, func(p shard.Placement) bool {
			return !slices.ContainsFunc(p.Replicas, func(r shard.Replica) bool { return r.Node == h.node.ID })
		})
		if !placed || at >= len(hosted) {
			return
		}
		sid := hosted[at].Shard
		to := address{host: h.name, namespace: ns, role: roleAgent}.String()
		w.Call(client, to, "fault", []any{h.name, sid, class}, []any{ns}, func(reply uint64) machine.Event {
			if class == shard.FaultNone {
				return machine.Recovered{Reply: machine.ReplyToken(reply), Shard: sid}
			}
			return machine.ReportFault{Cause: errReplica, Reply: machine.ReplyToken(reply), Shard: sid, Fault: class}
		})
	}
}

// refresh waits, as a call of a client of the router of a host, for the generation of the cell
// after the one that the store has. The case chooses the namespace and the host.
//
//dokimi:action call
func refresh(c *prop.Case) func(w *world, client int) {
	t := c.Draw(targets, "target")
	return func(w *world, client int) { refreshOn(w, client, t, 1) }
}

// readRoutes waits, as a call of a client of the router of a host, for the generation of the
// cell that the store has, once the faults of the run have ended. The case chooses the
// namespace and the host.
//
//dokimi:action call final
func readRoutes(c *prop.Case) func(w *world, client int) {
	t := c.Draw(targets, "target")
	return func(w *world, client int) { refreshOn(w, client, t, 0) }
}

// refreshOn calls Refresh of the router of the target t as a call of client, for the
// generation of the cell that the store has plus ahead.
func refreshOn(w *world, client int, t target, ahead int) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	ns, h, ok := t.resolve(s)
	if !ok {
		return
	}
	scope, st, _, placed := s.cellOf(w.Context(), ns)
	if !placed {
		return
	}
	wanted := st.Head.Generation + epoch.Epoch(ahead)
	inc := st.Head.Incarnation
	to := address{host: h.name, namespace: ns, role: roleRouter}.String()
	w.Call(client, to, "refresh", []any{h.name, wanted}, []any{ns}, func(reply uint64) machine.Event {
		return machine.Refresh{Cell: scope.Cell, Reply: machine.ReplyToken(reply), Min: wanted, Incarnation: inc}
	})
}

// forgery is a variant of a callback that the action forge sends.
type forgery uint8

// The variants of a callback that the action forge sends.
const (
	// forgeIncarnation is the callback for the incarnation of its node before the one that the
	// controller called.
	forgeIncarnation forgery = iota
	// forgeCaller is the callback from the identity intruder.
	forgeCaller
	// forgeReused is a callback with the key of the callback and another payload.
	forgeReused
)

// forge sends the agent of a node a variant of a callback that a controller sent: one for the
// earlier incarnation of the node, one from a caller that is not the controller, or one with
// the key of the callback and another payload. The case chooses the callback and the variant.
// The variant with the key of a callback forges a callback to which the controller had a reply
// without an error, and the other variants have the identity of the controller. The env records
// the request of each callback that forge sends.
//
//dokimi:action call
func forge(c *prop.Case) func(w *world, client int) {
	at := c.Draw(prop.Integer(0, maxPick-1), "callback")
	kind := c.Draw(prop.SampledFrom(forgeIncarnation, forgeCaller, forgeReused), "variant")
	return func(w *world, client int) {
		s := sim.Dependency[*env](w, w.Process(firstProcess))
		var picks []int
		for i, cb := range slices.Backward(s.callbacks) {
			if kind != forgeReused || cb.replied {
				picks = append(picks, i)
			}
		}
		if len(picks) == 0 {
			return
		}
		cb := &s.callbacks[picks[min(at, len(picks)-1)]]
		req := cb.request.CloneKanon()
		a := address{host: string(req.Node), namespace: req.Scope.Namespace, role: roleAgent}
		if req.Scope.Cell == "" {
			a.role = roleRootAgent
		}
		to := a.String()
		if w.Process(to) == nil {
			return
		}
		caller := cb.caller
		switch kind {
		case forgeIncarnation:
			req.NodeIncarnation--
		case forgeCaller:
			caller = intruder
		default:
			req.Range.End = append(slices.Clone(req.Range.End), 0)
		}
		s.forged[req] = true
		md := protocol.Metadata{Caller: caller, Version: protocol.Current}
		w.Call(client, to, "forge", []any{kind}, []any{req.Scope.Namespace}, func(reply uint64) machine.Event {
			return machine.Do{Request: req, Metadata: md, Reply: machine.ReplyToken(reply)}
		})
	}
}

// write writes to a shard in the fenced store as the writer of its tenure on a host. A write
// whose shard has an open transaction commits it. Any other write begins a transaction with
// the fencing token that the agent of the host publishes for the shard, so the case chooses
// the delay between the begin and the commit. The case chooses the namespace, the host and the
// shard.
//
//dokimi:action
func write(c *prop.Case) func(w *world) {
	t := c.Draw(targets, "target")
	at := c.Draw(prop.Integer(0, maxPick-1), "shard")
	return func(w *world) {
		s := sim.Dependency[*env](w, w.Process(firstProcess))
		ns, h, ok := t.resolve(s)
		if !ok {
			return
		}
		_, _, placements, placed := s.cellOf(w.Context(), ns)
		if !placed || at >= len(placements) {
			return
		}
		sid := placements[at].Shard
		if open := slices.IndexFunc(s.writes, func(tx *Txn) bool { return tx.token.Shard == sid }); open >= 0 {
			tx := s.writes[open]
			s.writes = slices.Delete(s.writes, open, open+1)
			// A commit that the fence refuses is the outcome that writer safety allows.
			_ = tx.Commit()
			return
		}
		token, err := h.parts[ns].agent.Token(sid)
		if err != nil {
			return
		}
		tx, err := s.app.Begin(&token)
		if err != nil {
			return
		}
		tx.Put(writeKey, strconv.FormatUint(uint64(token.Epoch), 10))
		s.writes = append(s.writes, tx)
	}
}

// join starts a new host, whose node joins each namespace, while the world has fewer than
// maxHosts hosts. The case chooses the seed and the zone of the host.
//
//dokimi:action
func join(c *prop.Case) func(w *world) {
	seed := c.Draw(prop.Integer(0, 1<<31-1), "seed")
	zone := c.Draw(prop.SampledFrom(zoneEast, zoneWest), "zone")
	return func(w *world) {
		s := sim.Dependency[*env](w, w.Process(firstProcess))
		if len(s.hosts) >= maxHosts {
			return
		}
		s.add(w, hostPrefix+strconv.Itoa(len(s.hosts)+1), rand.Seed(seed), zone)
	}
}

// change gives the node of a host another zone. The next restart of the host starts its
// machines with it. The case chooses the host and the zone.
//
//dokimi:action
func change(c *prop.Case) func(w *world) {
	at := c.Draw(prop.Integer(0, maxHosts-1), "host")
	zone := c.Draw(prop.SampledFrom(zoneEast, zoneWest), "zone")
	return func(w *world) {
		s := sim.Dependency[*env](w, w.Process(firstProcess))
		if at < len(s.hosts) {
			s.hosts[at].node.Labels = tag.Tags{{Key: zoneLabel, Value: zone}}
		}
	}
}

// recreate deletes the root scope and the scope of the cell of a namespace, as an operator does
// before the namespace is created again under the same name. The root that leads then creates
// the namespace again from its spec. The case chooses the namespace.
//
//dokimi:action
func recreate(c *prop.Case) func(w *world) {
	at := c.Draw(prop.Integer(0, maxNamespaces-1), "namespace")
	return func(w *world) {
		s := sim.Dependency[*env](w, w.Process(firstProcess))
		if at >= len(s.specs) {
			return
		}
		ns := s.specs[at].Namespace
		if scope, _, _, placed := s.cellOf(w.Context(), ns); placed {
			s.store.Delete(scope)
		}
		s.store.Delete(shard.Scope{Namespace: ns})
	}
}

// upgrade commits a newer schema to the root scope or the scope of the cell of a namespace, as
// a controller of a newer release does. The case chooses the namespace and the scope.
//
//dokimi:action
func upgrade(c *prop.Case) func(w *world) {
	at := c.Draw(prop.Integer(0, maxNamespaces-1), "namespace")
	inCell := c.Draw(prop.Boolean(), "cell")
	return func(w *world) {
		s := sim.Dependency[*env](w, w.Process(firstProcess))
		if at >= len(s.specs) {
			return
		}
		scope := shard.Scope{Namespace: s.specs[at].Namespace}
		if cellScope, _, _, placed := s.cellOf(w.Context(), scope.Namespace); placed && inCell {
			scope = cellScope
		}
		s.store.Upgrade(scope, store.Schema+1)
	}
}

// compact discards the history of each scope of the store, so a watch of an older generation
// ends with store.ErrCompacted.
//
//dokimi:action
func compact(*prop.Case) func(w *world) {
	return func(w *world) { sim.Dependency[*env](w, w.Process(firstProcess)).store.Compact() }
}

// loseRenewal arms a fault of the next renewal of a session, at a point that the case chooses.
// At PointRenewApply the renewal does not apply, and at PointRenewAck its reply is lost after it
// applied.
//
//dokimi:action
func loseRenewal(c *prop.Case) func(w *world) {
	point := c.Draw(prop.SampledFrom(memory.PointRenewApply, memory.PointRenewAck), "point")
	return func(w *world) { sim.Dependency[*env](w, w.Process(firstProcess)).store.FailAt(point) }
}

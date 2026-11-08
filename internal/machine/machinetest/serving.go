// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machinetest

import (
	"context"
	"errors"
	"slices"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/sim"
	"go.thesmos.sh/core/coretest/telemetrytest"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/routes"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// The instruments of a controller that the oracle of the counts reads, and the keys of their
// attributes, as the telemetry of a controller names them.
const (
	// transitionsInFlight is the gauge of the transitions in flight, per protocol and phase.
	transitionsInFlight telemetry.InstrumentName = "sharder.transitions.in_flight"
	// unplacedShards is the gauge of the shards of a scope without an assigned writer.
	unplacedShards telemetry.InstrumentName = "sharder.shards.unplaced"
	// attrNamespace is the key of the attribute of the namespace of a scope.
	attrNamespace = "namespace"
	// attrCell is the key of the attribute of the cell of a scope.
	attrCell = "cell"
)

// sightedRoute is the route of one probe key that a router returns, or its error.
type sightedRoute struct {
	// err is the error of Route, or nil.
	err error
	// route is the route, and the zero Route with an error.
	route routes.Route
}

// sightedToken is the fencing token of one shard that an agent returns, or its error.
type sightedToken struct {
	// err is the error of Token, or nil.
	err error
	// token is the fencing token, and the zero token with an error.
	token fence.Token
	// shard is the shard.
	shard shard.ID
}

// sighting is what one process of a router or of an agent of a cell serves after a step: the
// route of each probe key of its namespace, or the fencing token of each shard that a commit of
// a cell of its namespace has put.
type sighting struct {
	// routes lists the route of each probe key of a router, in the order of the keys.
	routes []sightedRoute
	// tokens lists the fencing token of each shard of an agent, in the order of the catalog.
	tokens []sightedToken
	// epoch is the epoch of the process.
	epoch uint64
}

// update is what one process of a router or of an agent of a cell served before and after the
// entries of the log that the process added since the last sight.
type update struct {
	// before is the sighting of the last sight, and nil for a process that no sight has read.
	before *sighting
	// after is the sighting of this sight.
	after *sighting
	// machine is the router or the agent of the process.
	machine machine.Machine
	// process is the name of the process.
	process string
	// namespace is the namespace of the process.
	namespace shard.Namespace
	// role is the role of the process: roleRouter or roleAgent.
	role role
	// entries lists the positions of the entries of the process in the log since the last sight.
	entries []int
}

// sightings is the state that the oracles of serving share: the last sighting of each process
// of a router or of an agent of a cell, the changes of the last sight, and the entries of the
// log that the last sight read.
type sightings struct {
	// last maps the name of each process that a sight has read to its last sighting.
	last map[string]*sighting
	// updates lists the update of each process with an entry in the log since the sight before
	// the last one.
	updates []update
	// log is the number of the entries of the log that the last sight read.
	log int
}

// catalog is the shards of each namespace that a commit of a cell has put, in the order of
// their first commit, and the number of the commits of the env that it has read.
type catalog struct {
	// shards maps each namespace to its shards.
	shards map[shard.Namespace][]shard.ID
	// commits is the number of the commits of the env that the catalog has read.
	commits int
}

// high is the highest generation and the highest epoch of the routes of one shard.
type high struct {
	// generation is the highest generation.
	generation epoch.Epoch
	// epoch is the highest epoch.
	epoch epoch.Epoch
}

// ackKey is an incarnation of a node in the view of one epoch of the process of a controller.
type ackKey struct {
	// node is the node.
	node shard.NodeID
	// proc is the epoch of the process of the controller.
	proc procKey
	// incarnation is the incarnation of the node.
	incarnation epoch.Epoch
}

// routeKey is a shard of one namespace incarnation in one epoch of the process of a router.
type routeKey struct {
	// proc is the epoch of the process of the router.
	proc procKey
	// incarnation is the namespace incarnation of the routes.
	incarnation shard.NamespaceIncarnation
	// shard is the shard.
	shard shard.ID
}

// ordering is the state of the oracle of order: the entries of the log that it has read, the
// last sequence that each controller acked to each node incarnation, the last generation that
// each controller acked, and the highest generation and epoch of the routes of each shard.
type ordering struct {
	// sequences maps each node incarnation of each controller to its last acked sequence.
	sequences map[ackKey]uint64
	// generations maps each epoch of the process of a controller to its last acked generation.
	generations map[procKey]epoch.Epoch
	// routes maps each shard of each router to the highest generation and epoch of its routes.
	routes map[routeKey]high
	// log is the number of the entries of the log that the oracle has read.
	log int
}

// outages is the state of the oracle of outages: the entries of the log that it has read, and
// each epoch of the process of an agent that has taken a Close.
type outages struct {
	// closing contains each epoch of the process of an agent that has taken a Close.
	closing map[procKey]bool
	// log is the number of the entries of the log that the oracle has read.
	log int
}

// deadlines is the state of the oracle of sessions: the entries of the log that it has read,
// and the token of the last Join of each epoch of the process of an agent.
type deadlines struct {
	// joins maps each epoch of the process of an agent to the token of its last Join.
	joins map[procKey]machine.Token
	// log is the number of the entries of the log that the oracle has read.
	log int
}

// counting is the state of the oracle of the counts: the entries of the log that it has read,
// and the last generation of its scope that each cell machine has committed.
type counting struct {
	// generations maps each epoch of the process of a cell machine to the generation of its
	// last commit that applied.
	generations map[procKey]epoch.Epoch
	// log is the number of the entries of the log that the oracle has read.
	log int
}

// viewsAdvance checks that the view of a controller and the routes of a router do not return to
// an older generation. The sequence that a controller acks to the reports of a node incarnation
// does not fall, and neither does the generation of its acks. The generation and the epoch of
// the routes of each shard of a namespace incarnation that a router serves do not fall.
//
//dokimi:invariant defect=replayedRoutes
func viewsAdvance(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	o := &s.ordering
	log := w.Log()
	for ; o.log < len(log); o.log++ {
		e := &log[o.log]
		r, isReport := e.Event.(machine.Report)
		if !isReport || e.Dropped {
			continue
		}
		proc := procKey{process: e.Process, epoch: e.Epoch}
		for _, f := range e.Effects {
			ack, ok := f.(machine.Reply[protocol.Ack])
			if !ok || ack.To != r.Reply || ack.Err != nil {
				continue
			}
			k := ackKey{proc: proc, node: r.Report.Node, incarnation: r.Report.NodeIncarnation}
			assert.True(h, ack.Value.Sequence >= o.sequences[k], h.Statement())
			assert.True(h, ack.Value.Generation >= o.generations[proc], h.Statement())
			o.sequences[k], o.generations[proc] = ack.Value.Sequence, ack.Value.Generation
		}
	}
	for _, u := range s.sight(w) {
		proc := procKey{process: u.process, epoch: u.after.epoch}
		for i := range u.after.routes {
			r := &u.after.routes[i].route
			if u.after.routes[i].err != nil {
				continue
			}
			k := routeKey{proc: proc, incarnation: r.Incarnation, shard: r.Shard}
			hi := o.routes[k]
			assert.True(h, r.Generation >= hi.generation, h.Statement())
			assert.True(h, r.Epoch >= hi.epoch, h.Statement())
			o.routes[k] = high{generation: r.Generation, epoch: r.Epoch}
		}
	}
}

// replayedRoutes is the planted defect of viewsAdvance. Its router takes an old generation of a
// cell again. Once a generation of a scope of a cell puts a shard at an epoch above the epoch
// at which the first generation with a writer of the shard put it, the router takes that first
// generation again at its next step, as the generation after the last one that it took.
func replayedRoutes(c *prop.Case) func(w *world) {
	return build(c, c.Draw(presets, "preset"), &variant{router: func(_ *env, m *routes.Machine) machine.Machine {
		cells := make(map[machine.Token]bool)
		first := make(map[shard.ID]machine.Watched)
		var replay *machine.Watched
		var latest epoch.Epoch
		step := func(ev machine.Event, out []machine.Effect) []machine.Effect {
			from := len(out)
			out = m.Step(ev, out)
			for _, f := range out[from:] {
				if wt, ok := f.(machine.Watch); ok && wt.Scope.Cell != "" {
					cells[wt.Token] = true
				}
			}
			wd, ok := ev.(machine.Watched)
			if ok && wd.Err == nil && cells[wd.Token] {
				latest = max(latest, wd.Event.Generation)
			}
			if replay != nil {
				again := *replay
				replay = nil
				latest++
				again.Event.Generation = latest
				out = m.Step(again, out)
			}
			if !ok || wd.Err != nil || !cells[wd.Token] {
				return out
			}
			for i := range wd.Event.Mutation.Put {
				p := &wd.Event.Mutation.Put[i]
				old, seen := first[p.Shard]
				switch {
				case len(p.Replicas) == 0:
				case !seen:
					first[p.Shard] = wd
				case p.Epoch > putEpoch(&old, p.Shard):
					old.Token = wd.Token
					replay = &old
				}
			}
			return out
		}
		return NewMachineStub(nil, MachineStubBenchMode(), MachineStubDelegateTo(m), WithMachineStep(step))
	}})
}

// putEpoch returns the epoch at which the generation of wd puts the shard sid, and the zero
// epoch when it does not put sid.
func putEpoch(wd *machine.Watched, sid shard.ID) epoch.Epoch {
	for i := range wd.Event.Mutation.Put {
		if p := &wd.Event.Mutation.Put[i]; p.Shard == sid {
			return p.Epoch
		}
	}
	return epoch.Zero
}

// outagesKeepServing checks that writers and routers serve during an outage as the outage table
// of the architecture states. An agent of a cell revokes or removes the fencing token of a tenure
// only at a step that drops the tenure, takes the result of its fence acquisition or its
// handler, stops the agent, belongs to a Close, or under ownership.Stop takes the end of the
// session of the node. A router changes its routes only at a step that takes a page, a
// generation or a view, or that stops it. An outage of the store, of the controller of a cell or
// of the root then leaves the agents and the routers with their tenures and their routes.
//
//dokimi:invariant defect=fragileRouters
func outagesKeepServing(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	o := &s.outages
	log := w.Log()
	for ; o.log < len(log); o.log++ {
		if e := &log[o.log]; !e.Dropped {
			if _, isClose := e.Event.(machine.Close); isClose {
				o.closing[procKey{process: e.Process, epoch: e.Epoch}] = true
			}
		}
	}
	for _, u := range s.sight(w) {
		if u.before == nil || u.before.epoch != u.after.epoch {
			continue
		}
		if u.role == roleRouter {
			if !sameRoutes(u.before.routes, u.after.routes) {
				assert.True(h, rerouted(w, &u), h.Statement())
			}
			continue
		}
		for i := range u.before.tokens {
			sid := u.before.tokens[i].shard
			if u.before.tokens[i].err != nil {
				continue
			}
			at := slices.IndexFunc(u.after.tokens, func(a sightedToken) bool { return a.shard == sid })
			if at >= 0 && u.after.tokens[at].err == nil {
				continue
			}
			assert.True(h, s.unserved(w, &u, sid), h.Statement())
		}
	}
}

// rerouted reports whether a step of the update u of a router may change its routes: a step
// that takes a page, a generation or a view without an error, or Stopped, or a step after which
// the router has failed.
func rerouted(w *world, u *update) bool {
	log := w.Log()
	for _, i := range u.entries {
		switch ev := log[i].Event.(type) {
		case machine.PageRead:
			if ev.Err == nil {
				return true
			}
		case machine.Watched:
			if ev.Err == nil {
				return true
			}
		case machine.Viewed:
			if ev.Err == nil {
				return true
			}
		case machine.Stopped:
			return true
		}
	}
	return u.machine.Err() != nil
}

// unserved reports whether a step of the update u of an agent may end its tenure of the shard
// sid: a step that drops or revokes the tenure, takes the result of a fence acquisition or of a
// call of the handler, or Stopped, a step of a Close, a step that under ownership.Stop takes
// the end of a session, or a step after which the agent has failed.
func (s *env) unserved(w *world, u *update, sid shard.ID) bool {
	log := w.Log()
	if s.outages.closing[procKey{process: u.process, epoch: u.after.epoch}] {
		return true
	}
	for _, i := range u.entries {
		e := &log[i]
		switch e.Event.(type) {
		case machine.Handled, machine.Acquired, machine.Stopped:
			return true
		case machine.SessionEnded:
			if s.modes[u.namespace] == ownership.Stop {
				return true
			}
		}
		for _, f := range e.Effects {
			switch f := f.(type) {
			case machine.Handle:
				if f.Request.Call == protocol.Drop && f.Request.Shard == sid {
					return true
				}
			case machine.Revoke:
				if f.Fence.Shard == sid {
					return true
				}
			}
		}
	}
	return u.machine.Err() != nil
}

// sameRoutes reports whether a and b have the same route or the same error for each probe key.
func sameRoutes(a, b []sightedRoute) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		ra, rb := &a[i].route, &b[i].route
		same := errors.Is(a[i].err, b[i].err) && ra.Shard == rb.Shard && ra.Epoch == rb.Epoch &&
			ra.Generation == rb.Generation && ra.Incarnation == rb.Incarnation && ra.Cell == rb.Cell &&
			slices.Equal(ra.Replicas, rb.Replicas)
		if !same {
			return false
		}
	}
	return true
}

// fragileRouters is the planted defect of outagesKeepServing. Its router gives up its routes at an
// error of the store or of the membership. A page, a generation or a view with an error stops
// the router, so it routes no key from then on.
func fragileRouters(c *prop.Case) func(w *world) {
	return build(c, c.Draw(presets, "preset"), &variant{router: func(_ *env, m *routes.Machine) machine.Machine {
		step := func(ev machine.Event, out []machine.Effect) []machine.Effect {
			out = m.Step(ev, out)
			var err error
			switch e := ev.(type) {
			case machine.PageRead:
				err = e.Err
			case machine.Watched:
				err = e.Err
			case machine.Viewed:
				err = e.Err
			}
			if err == nil {
				return out
			}
			return m.Step(machine.Stopped{}, out)
		}
		return NewMachineStub(nil, MachineStubBenchMode(), MachineStubDelegateTo(m), WithMachineStep(step))
	}})
}

// sessionsEndAtTheirDeadline checks that a session ends at the deadline of its last accepted
// renewal, and that an agent under ownership.Stop serves no tenure once its session has ended.
// The local deadline of each live session is the validity of its lease after the monotonic time
// of the clock of its host at its last renewal that the membership accepted. The step at which
// an agent under ownership.Stop takes the end of the session of its last Join revokes every
// fencing token of the agent.
//
//dokimi:invariant defect=movedDeadlines
func sessionsEndAtTheirDeadline(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	for _, j := range s.waits.joins {
		assert.Equal(h, j.valid, j.accepted+j.lease.Validity(), h.Statement())
	}
	for _, c := range s.waits.campaigns {
		assert.Equal(h, c.lease.valid, c.lease.accepted+c.lease.lease.Validity(), h.Statement())
	}
	d := &s.deadlines
	log := w.Log()
	for ; d.log < len(log); d.log++ {
		e := &log[d.log]
		for _, f := range e.Effects {
			if j, ok := f.(machine.Join); ok {
				d.joins[procKey{process: e.Process, epoch: e.Epoch}] = j.Token
			}
		}
	}
	for _, u := range s.sight(w) {
		if u.role != roleAgent || s.modes[u.namespace] != ownership.Stop {
			continue
		}
		join := d.joins[procKey{process: u.process, epoch: u.after.epoch}]
		ended := slices.ContainsFunc(u.entries, func(i int) bool {
			ev, ok := log[i].Event.(machine.SessionEnded)
			return ok && ev.Token == join && log[i].Epoch == u.after.epoch
		})
		if !ended {
			continue
		}
		for i := range u.after.tokens {
			assert.HasError(h, u.after.tokens[i].err, h.Statement())
		}
	}
}

// movedDeadlines is the planted defect of sessionsEndAtTheirDeadline: a membership client that
// moves the deadline of a session at each renewal, also at one that the membership refused or
// whose reply it lost.
func movedDeadlines(c *prop.Case) func(w *world) {
	return build(c, c.Draw(presets, "preset"), &variant{failedRenewals: true})
}

// countsAreSeparate checks that the instruments report the transitions in flight and the shards
// without a serving writer as separate counts. Once a cell machine that serves has committed the
// last generation of its scope, the live series of the gauge of the transitions in flight of its
// cell add up to the transitions of the scope. The gauge of the unplaced shards of its cell
// counts the shards of the layout without a writer.
//
//dokimi:invariant defect=mergedCounts
func countsAreSeparate(h *sim.Check, w *world) {
	s := sim.Dependency[*env](w, w.Process(firstProcess))
	c := &s.counting
	log := w.Log()
	for ; c.log < len(log); c.log++ {
		e := &log[c.log]
		if ev, ok := e.Event.(machine.Committed); ok && ev.Err == nil && !e.Dropped && s.hosting(e.Process) {
			c.generations[procKey{process: e.Process, epoch: e.Epoch}] = ev.Generation
		}
	}
	ctx := w.Context()
	for _, hh := range s.hosts {
		for name, hc := range hh.cells {
			p := w.Process(name)
			if p == nil || hc.machine == nil || !hc.ready || hc.err != nil || hc.stopped || hc.machine.Err() != nil {
				continue
			}
			g, committed := c.generations[procKey{process: name, epoch: p.Epoch()}]
			head, err := s.store.Head(ctx, hc.scope)
			assert.NoError(h, err, "the store of a world must read the header of a scope")
			attrs := []telemetry.Attr{
				telemetry.AttrString(attrNamespace, string(hc.scope.Namespace)),
				telemetry.AttrString(attrCell, string(hc.scope.Cell)),
			}
			if !committed || head.Generation != g || hh.reporter.Bound(unplacedShards, attrs...) == 0 {
				continue
			}
			st, placements := s.stateOf(ctx, hc.scope, head.Incarnation)
			writers := 0
			for i := range placements {
				if len(placements[i].Replicas) > 0 {
					writers++
				}
			}
			unplaced := hh.reporter.Values(unplacedShards, attrs...)
			if len(unplaced) == 0 {
				continue
			}
			inflight := liveTotal(hh.reporter, transitionsInFlight, attrs)
			assert.Equal(h, inflight, float64(len(st.Transitions)), h.Statement())
			assert.Equal(h, unplaced[len(unplaced)-1], float64(s.layouts[hc.scope.Namespace]-writers), h.Statement())
		}
	}
}

// liveTotal returns the sum of the last values of the series of the instrument name of r that
// have every attribute of attrs and an instrument that has not been released.
func liveTotal(r *telemetrytest.Recorder, name telemetry.InstrumentName, attrs []telemetry.Attr) float64 {
	sum := 0.0
	for _, set := range r.Series(name) {
		has := func(want telemetry.Attr) bool {
			return slices.ContainsFunc(set, func(a telemetry.Attr) bool {
				return a.Key == want.Key && a.Value.Str == want.Value.Str
			})
		}
		if !slices.ContainsFunc(attrs, func(a telemetry.Attr) bool { return !has(a) }) && r.Bound(name, set...) > 0 {
			if v := r.Values(name, set...); len(v) > 0 {
				sum += v[len(v)-1]
			}
		}
	}
	return sum
}

// mergedCounts is the planted defect of countsAreSeparate. Its cell machine reports each shard
// without a writer as a transition in flight too: the gauge of the unplaced shards of a cell also
// sets a series of the gauge of the transitions in flight of the cell.
func mergedCounts(c *prop.Case) func(w *world) {
	merging := func(s *env, r *telemetrytest.Recorder) telemetry.Reporter {
		gauge := func(spec telemetry.InstrumentSpec) telemetry.Gauge {
			g := r.Gauge(spec)
			if spec.Name != unplacedShards {
				return g
			}
			return mergedGauge{Gauge: g, inflight: r.Gauge(telemetry.InstrumentSpec{Name: transitionsInFlight})}
		}
		return telemetrytest.NewReporterStub(s.tb, telemetrytest.ReporterStubBenchMode(),
			telemetrytest.ReporterStubDelegateTo(r), telemetrytest.WithReporterGauge(gauge))
	}
	return build(c, c.Draw(presets, "preset"), &variant{cellReporter: merging})
}

// mergedGauge is a gauge of the unplaced shards that sets a series of the gauge of the
// transitions in flight with its attributes to its value too.
type mergedGauge struct {
	telemetry.Gauge
	// inflight is the series of the gauge of the transitions in flight.
	inflight telemetry.Gauge
}

// With returns the gauge of attrs, with the series of attrs of the gauge of the transitions in
// flight.
func (g mergedGauge) With(attrs []telemetry.Attr) telemetry.Gauge {
	return mergedGauge{Gauge: g.Gauge.With(attrs), inflight: g.inflight.With(attrs)}
}

// Set sets the gauge and its series of the transitions in flight to value.
func (g mergedGauge) Set(ctx context.Context, value float64) {
	g.Gauge.Set(ctx, value)
	g.inflight.Set(ctx, value)
}

// Release releases the gauge and its series of the transitions in flight.
func (g mergedGauge) Release() {
	g.Gauge.Release()
	g.inflight.Release()
}

// hosting reports whether a host of s has started the cell machine of the process name.
func (s *env) hosting(name string) bool {
	return slices.ContainsFunc(s.hosts, func(hh *host) bool {
		_, ok := hh.cells[name]
		return ok
	})
}

// index adds each shard that a commit of a cell put since the last call to the catalog of its
// namespace.
func (s *env) index() {
	for ; s.catalog.commits < len(s.commits); s.catalog.commits++ {
		c := &s.commits[s.catalog.commits]
		if c.scope.Cell == "" {
			continue
		}
		for i := range c.mutation.Put {
			if sid := c.mutation.Put[i].Shard; !slices.Contains(s.catalog.shards[c.scope.Namespace], sid) {
				s.catalog.shards[c.scope.Namespace] = append(s.catalog.shards[c.scope.Namespace], sid)
			}
		}
	}
}

// sight returns the update of each process of a router or of an agent of a cell with an entry in
// the log since the last sight, and reads what each of them serves now. A second sight after
// the same entries returns the updates of the first, so the oracles of one step share them.
func (s *env) sight(w *world) []update {
	log := w.Log()
	sg := &s.sightings
	if sg.log == len(log) {
		return sg.updates
	}
	entries := make(map[string][]int)
	for i := sg.log; i < len(log); i++ {
		entries[log[i].Process] = append(entries[log[i].Process], i)
	}
	sg.log, sg.updates = len(log), nil
	s.index()
	ctx := w.Context()
	for _, hh := range s.hosts {
		for i := range s.specs {
			ns := s.specs[i].Namespace
			for _, r := range [...]role{roleRouter, roleAgent} {
				name := address{host: hh.name, namespace: ns, role: r}.String()
				p := w.Process(name)
				if len(entries[name]) == 0 || p == nil {
					continue
				}
				after := &sighting{epoch: p.Epoch()}
				part := hh.parts[ns]
				var m machine.Machine = part.agent
				if r == roleRouter {
					m = part.router
					for _, key := range s.probes[ns] {
						route, err := part.router.Route(ctx, key)
						after.routes = append(after.routes, sightedRoute{err: err, route: route})
					}
				} else {
					for _, sid := range s.catalog.shards[ns] {
						t, err := part.agent.Token(sid)
						after.tokens = append(after.tokens, sightedToken{err: err, token: t, shard: sid})
					}
				}
				sg.updates = append(sg.updates, update{
					before: sg.last[name], after: after, machine: m, process: name, namespace: ns, role: r,
					entries: entries[name],
				})
				sg.last[name] = after
			}
		}
	}
	return sg.updates
}

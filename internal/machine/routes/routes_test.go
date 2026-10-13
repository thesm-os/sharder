// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes_test

import (
	"errors"
	"log/slog"
	"slices"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/fsm"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/rand/constant"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/routes"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The weights of the actions of the property of a router that a case takes more often than
// the others.
const (
	// deliveries is the weight of a read and of an item of a watch.
	deliveries = 4
	// commits is the weight of a commit of the controller of the cell.
	commits = 3
	// changes is the weight of a view and of a Refresh.
	changes = 2
)

// The labels of the cases of the property of a router. The coverage requirements of the
// property count them.
const (
	// labelRecreate counts a case in which the root creates the namespace again.
	labelRecreate = "the root creates the namespace again"
	// labelSameCell counts a case in which the root creates the namespace again with a cell
	// of the same key.
	labelSameCell = "the root creates the namespace again with the same cell"
	// labelInstalled counts a case in which the router installs a namespace that the root
	// created again.
	labelInstalled = "the router installs a namespace that the root created again"
	// labelCompaction counts a case in which the store compacts its scopes.
	labelCompaction = "the store compacts its scopes"
	// labelOutage counts a case in which the store has an outage.
	labelOutage = "the store has an outage"
	// labelViews counts a case in which a watch of views fails.
	labelViews = "a watch of views fails"
	// labelWaited counts a case in which a Refresh gets nil after the step that took it.
	labelWaited = "a Refresh waits for its generation"
	// labelCancelled counts a case in which the runtime cancels a call.
	labelCancelled = "the runtime cancels a call"
	// labelStopped counts a case in which the router stops.
	labelStopped = "the router stops"
	// labelLate counts a case in which the router takes a late item of a watch that it has
	// ended.
	labelLate = "a watch passes an item after its end"
)

// The least shares of the cases of the property of a router that the labels must count. Over
// 1,000 cases, each common label counts 18% to 39% of the cases. The Refresh calls that wait
// count 8%, and the late items count 10%.
const (
	// common is the least share of a common label.
	common = 0.1
	// rare is the least share of the Refresh calls that wait and of the late items.
	rare = 0.02
)

// lateGeneration is the generation of a late item of a watch. It is above every generation
// that the property of a router commits.
const lateGeneration epoch.Epoch = 1 << 20

// The parts of the traces of the property of a router in which the root creates the
// namespace again with the same cell. An old Refresh and a redirect to the new incarnation are
// outstanding at the time. Each part lists entries of prop.Draws without the brackets of the
// list.
const (
	// traceServing reads the namespace of incA and installs it with a view of nodes A and B.
	// The controller commits generations 2 and 3, and the router applies both. A Refresh of
	// incA for generation 4 waits. The root then creates the namespace again with the same
	// cell at generation 1.
	traceServing = `
		{"step": "read"}, {"label": "read", "value": {"type": "int", "value": 0}},
		{"step": "read"}, {"label": "read", "value": {"type": "int", "value": 0}},
		{"step": "read"}, {"label": "read", "value": {"type": "int", "value": 0}},
		{"step": "view"}, {"label": "watch of views", "value": {"type": "int", "value": 0}},
		{"label": "node A", "value": {"type": "bool", "value": true}},
		{"label": "node A moved", "value": {"type": "bool", "value": false}},
		{"label": "node B", "value": {"type": "bool", "value": true}},
		{"step": "commit"}, {"label": "slot", "value": {"type": "int", "value": 0}},
		{"label": "writer", "value": {"type": "string", "value": "node-b"}},
		{"step": "deliver"}, {"label": "watch", "value": {"type": "int", "value": 0}},
		{"step": "commit"}, {"label": "slot", "value": {"type": "int", "value": 1}},
		{"label": "writer", "value": {"type": "string", "value": "node-b"}},
		{"step": "deliver"}, {"label": "watch", "value": {"type": "int", "value": 0}},
		{"step": "refresh"}, {"label": "incarnation", "value": {"type": "int", "value": 0}},
		{"label": "unknown cell", "value": {"type": "bool", "value": false}},
		{"label": "generation", "value": {"type": "int", "value": 4}},
		{"step": "create the namespace again"},
		{"label": "the same cell", "value": {"type": "bool", "value": true}}`
	// traceLower passes the redirect to the new incarnation for its generation 1, which is
	// lower than the generation of the old one.
	traceLower = `
		{"step": "refresh"}, {"label": "incarnation", "value": {"type": "int", "value": 1}},
		{"label": "unknown cell", "value": {"type": "bool", "value": false}},
		{"label": "generation", "value": {"type": "int", "value": 1}}`
	// traceEqual commits generations 2 and 3 to the cell of the new incarnation. It then
	// passes the redirect to the new incarnation for its generation 3, which is the generation
	// of the old one.
	traceEqual = `
		{"step": "commit"}, {"label": "slot", "value": {"type": "int", "value": 2}},
		{"label": "writer", "value": {"type": "string", "value": "node-a"}},
		{"step": "commit"}, {"label": "slot", "value": {"type": "int", "value": 3}},
		{"label": "writer", "value": {"type": "string", "value": "node-a"}},
		{"step": "refresh"}, {"label": "incarnation", "value": {"type": "int", "value": 1}},
		{"label": "unknown cell", "value": {"type": "bool", "value": false}},
		{"label": "generation", "value": {"type": "int", "value": 3}}`
	// traceInstall reads the root scope and the cell of the new incarnation, and installs it
	// with a view of nodes A and B.
	traceInstall = `
		{"step": "read"}, {"label": "read", "value": {"type": "int", "value": 0}},
		{"step": "read"}, {"label": "read", "value": {"type": "int", "value": 0}},
		{"step": "read"}, {"label": "read", "value": {"type": "int", "value": 0}},
		{"step": "view"}, {"label": "watch of views", "value": {"type": "int", "value": 1}},
		{"label": "node A", "value": {"type": "bool", "value": true}},
		{"label": "node A moved", "value": {"type": "bool", "value": false}},
		{"label": "node B", "value": {"type": "bool", "value": true}}`
)

// incZ is a namespace incarnation that the root of the property of a router never creates.
var incZ = shard.NamespaceIncarnation{ID: id.New128([id.Size128]byte{0: 0x1a, 15: 0x0f})}

// incarnation is one namespace incarnation that the root has created, and the commits of the
// controller of its cell.
type incarnation struct {
	// commits lists the commits to the scope of the cell, in order.
	commits []committed
	// scope is the scope of the cell of the incarnation.
	scope shard.Scope
	// id is the namespace incarnation.
	id shard.NamespaceIncarnation
	// key is the key of the cell of the incarnation.
	key id.ID128
	// epochs lists the epoch of the last placement of each slot.
	epochs [slots]epoch.Epoch
}

// newIncarnation returns the record of the incarnation inc. The cell of the incarnation is
// the cell of h, and key is the key of that cell. The controller of the cell placed every
// slot on node A at the first epoch in the commit of the generation h.placed.
func newIncarnation(h *harness, inc shard.NamespaceIncarnation, key id.ID128) incarnation {
	rec := incarnation{
		commits: []committed{{puts: h.placements(nodeA.ID, firstEpoch), generation: h.placed}},
		scope:   h.cell,
		id:      inc,
		key:     key,
	}
	for i := range rec.epochs {
		rec.epochs[i] = firstEpoch
	}
	return rec
}

// placement returns the placement of slot i at the generation gen, and the generation of the
// commit that put it: the last commit at or below gen that puts the shard of the slot. It
// reports false when the commits up to gen do not put the shard.
func (inc *incarnation) placement(i int, gen epoch.Epoch) (shard.Placement, epoch.Epoch, bool) {
	var p shard.Placement
	var put epoch.Epoch
	for _, cm := range inc.commits {
		if cm.generation > gen {
			break
		}
		for _, q := range cm.puts {
			if q.Shard == shards[i] {
				p, put = q, cm.generation
			}
		}
	}
	return p, put, put != 0
}

// committed is one commit to the scope of a cell: the placements that it puts, and its
// generation.
type committed struct {
	// puts lists the placements that the commit puts.
	puts []shard.Placement
	// generation is the generation of the commit.
	generation epoch.Epoch
}

// shardOf is the shard of one slot in one incarnation.
type shardOf struct {
	// incarnation is the index of the incarnation in the incarnations of the model.
	incarnation int
	// slot is the slot.
	slot int
}

// model is the expected state of a router that the property of a router drives, and the
// record of what the router did. Its harness leaves every read pending until a step performs
// it, and passes each effect of the router to observe.
type model struct {
	// c is the case of the property.
	c *prop.Case
	// h is the harness of the router.
	h *harness
	// generations maps the shard of each slot of each incarnation to the generation of the
	// last route of the shard that Route returned.
	generations map[shardOf]epoch.Epoch
	// views maps the scope of each cell in each incarnation to the nodes of the last view
	// that a step passed to a watch of its views.
	views map[scopeKey][]shard.Node
	// seen contains the token of each watch of views to which a step has passed a view.
	seen map[machine.Token]bool
	// refreshes maps the token of each Refresh to the Refresh.
	refreshes map[machine.ReplyToken]machine.Refresh
	// replies counts the replies of the router to each call.
	replies map[machine.ReplyToken]int
	// cancelled contains the token of each call that the runtime cancelled.
	cancelled map[machine.ReplyToken]bool
	// incarnations lists the namespace incarnations that the root has created, the oldest
	// first.
	incarnations []incarnation
	// nodes lists the nodes of the last view that a step chose.
	nodes []shard.Node
	// started is the token of the Started of the run.
	started machine.ReplyToken
	// calling is the token of the Refresh that the running step passes, and zero during every
	// other step.
	calling machine.ReplyToken
	// last is the token of the last call before the router stopped, and zero while it runs.
	last machine.ReplyToken
	// installed is the index in incarnations of the incarnation of the last route that Route
	// returned.
	installed int
	// outage is true while the store has an outage.
	outage bool
	// halting is true during the step that stops the router.
	halting bool
	// broken is true once an error of the membership has stopped the router.
	broken bool
	// stopped is true once the router has stopped.
	stopped bool
}

// newModel returns the model of a router that has taken its Started. The read of the root
// scope waits in the harness until a step performs it.
func newModel(c *prop.Case) *model {
	m := &model{
		c:           c,
		h:           newHarness(c),
		generations: make(map[shardOf]epoch.Epoch),
		views:       make(map[scopeKey][]shard.Node),
		seen:        make(map[machine.Token]bool),
		refreshes:   make(map[machine.ReplyToken]machine.Refresh),
		replies:     make(map[machine.ReplyToken]int),
		cancelled:   make(map[machine.ReplyToken]bool),
		nodes:       []shard.Node{nodeA, nodeB},
	}
	m.incarnations = []incarnation{newIncarnation(m.h, incA, cellKey)}
	m.h.observe = m.observe
	m.h.pause = func(machine.Effect) bool { return true }
	m.started = m.h.call()
	m.h.step(c, machine.Started{Reply: m.started})
	return m
}

// observe checks the effect e that the router appends. A stopped router does not append an
// effect.
func (m *model) observe(e machine.Effect) {
	assert.False(m.c, m.stopped, "a stopped router must not append an effect")
	if r, ok := e.(machine.Reply[struct{}]); ok {
		m.replied(&r)
	}
}

// replied checks the reply r. The router replies to each call once, and never to a call that
// the runtime cancelled. Started gets nil once the router serves. At a stop by Stopped, each
// call that waits gets ErrClosed. At a stop by an error, Started gets the error and each
// Refresh that waits gets ErrClosed.
func (m *model) replied(r *machine.Reply[struct{}]) {
	c := m.c
	m.replies[r.To]++
	expect.Equal(c, m.replies[r.To], 1, "the router must reply to a call once")
	expect.False(c, m.cancelled[r.To], "the router must not reply to a call that the runtime cancelled")
	call, refresh := m.refreshes[r.To]
	switch {
	case m.halting && m.broken && !refresh:
		expect.ErrorIs(c, r.Err, errBroken, "Started must get the error that stopped the router")
	case m.halting:
		expect.ErrorIs(c, r.Err, routes.ErrClosed, "a call that waits must get ErrClosed when the router stops")
	case !refresh:
		expect.Nil(c, r.Err, "Started must get nil once the router serves")
	default:
		m.refreshed(&call, r.Err)
	}
}

// refreshed checks the reply err to the Refresh call. Route reveals the installed incarnation
// whenever it returns a route:
//   - nil comes for the cell of the installed incarnation once the harness has passed the
//     generation of the Refresh to the router.
//   - shard.ErrIncarnation comes for another incarnation than the installed one.
//   - ErrCell comes for a cell that the installed incarnation does not have.
func (m *model) refreshed(call *machine.Refresh, err error) {
	c := m.c
	k := slices.IndexFunc(m.incarnations, func(inc incarnation) bool { return inc.id == call.Incarnation })
	installed, ok := m.probe()
	if errors.Is(err, shard.ErrIncarnation) {
		if ok {
			expect.NotEqual(c, call.Incarnation, installed,
				"the router must return shard.ErrIncarnation only for another incarnation than the installed one")
		}
		return
	}
	assert.True(
		c,
		k >= 0,
		"the router must return shard.ErrIncarnation for an incarnation that the root has not created",
	)
	if ok {
		expect.Equal(c, call.Incarnation, installed,
			"the router must return shard.ErrIncarnation for another incarnation than the installed one")
	}
	inc := &m.incarnations[k]
	if err != nil {
		expect.ErrorIs(
			c,
			err,
			routes.ErrCell,
			"the router must return nil, shard.ErrIncarnation or ErrCell to a Refresh",
		)
		expect.NotEqual(
			c,
			call.Cell,
			inc.scope.Cell,
			"the router must return ErrCell only for a cell that it does not have",
		)
		return
	}
	expect.Equal(c, call.Cell, inc.scope.Cell, "the router must return ErrCell for a cell that it does not have")
	expect.True(c, m.h.delivered[scopeKey{scope: inc.scope, incarnation: inc.id}] >= call.Min,
		"the router must reply to a Refresh once it has applied the generation of the Refresh")
	for i, key := range m.h.keys {
		r, err := m.h.machine.Route(c.Context(), key)
		if _, put, ok := inc.placement(i, call.Min); err == nil && ok {
			expect.True(c, r.Generation >= put,
				"a route must have the commits up to the generation of a Refresh that got nil")
		}
	}
	if call.Reply != m.calling {
		c.Classify(labelWaited)
	}
}

// probe returns the incarnation of the routes that the router publishes: the incarnation of
// the first route that Route returns for the key of a slot. It reports false when Route
// returns an error for the key of every slot.
func (m *model) probe() (shard.NamespaceIncarnation, bool) {
	for _, key := range m.h.keys {
		if r, err := m.h.machine.Route(m.c.Context(), key); err == nil {
			return r.Incarnation, true
		}
	}
	return shard.NamespaceIncarnation{}, false
}

// commit commits a placement of a slot that c chooses to the cell of the newest incarnation.
// The placement has the next epoch of its shard. c chooses its writer from node A, node B and
// no node.
func (m *model) commit(c *prop.Case) {
	i := c.Draw(prop.Integer(0, slots-1), "slot")
	writer := c.Draw(prop.SampledFrom(nodeA.ID, nodeB.ID, noNode), "writer")
	inc := &m.incarnations[len(m.incarnations)-1]
	inc.epochs[i]++
	puts := []shard.Placement{m.h.placement(i, writer, inc.epochs[i])}
	gen := m.h.commit(c, inc.scope, &store.Mutation{Put: puts})
	inc.commits = append(inc.commits, committed{puts: puts, generation: gen})
}

// read performs a pending read that c chooses, and passes its result to the router.
func (m *model) read(c *prop.Case) {
	i := c.Draw(prop.Integer(0, len(m.h.pending)-1), "read")
	e := m.h.pending[i].(machine.Read)
	m.h.pending = slices.Delete(m.h.pending, i, i+1)
	m.h.step(c, m.h.read(c, &e))
}

// ready returns the index of each watch whose next item the store has. The indexes are in
// the order of the watches.
func (m *model) ready() []int {
	var ready []int
	for i := range m.h.watches {
		if _, ok := m.h.item(m.c, &m.h.watches[i]); ok {
			ready = append(ready, i)
		}
	}
	return ready
}

// deliver passes the next item of a watch to the router. c chooses the watch from the watches
// that have an item.
func (m *model) deliver(c *prop.Case) {
	ready := m.ready()
	i := ready[c.Draw(prop.Integer(0, len(ready)-1), "watch")]
	ev, ok := m.h.item(c, &m.h.watches[i])
	assert.True(c, ok, "the watch must have an item")
	m.h.pass(c, i, &ev)
}

// view passes a view to a watch of views that c chooses. c also chooses whether the view lists
// node A and node B, and whether node A has another address.
func (m *model) view(c *prop.Case) {
	i := c.Draw(prop.Integer(0, len(m.h.viewers)-1), "watch of views")
	var nodes []shard.Node
	if c.Draw(prop.Boolean(), "node A") {
		a := nodeA
		if c.Draw(prop.Boolean(), "node A moved") {
			a.Address = movedAddress
		}
		nodes = append(nodes, a)
	}
	if c.Draw(prop.Boolean(), "node B") {
		nodes = append(nodes, nodeB)
	}
	m.nodes = nodes
	m.send(c, i)
}

// send passes a view of the nodes of the last view that a step chose to the watch of views i.
func (m *model) send(c *prop.Case, i int) {
	w := m.h.viewers[i]
	m.views[scopeKey{scope: w.Scope, incarnation: w.Incarnation}] = m.nodes
	m.seen[w.Token] = true
	m.h.revision++
	v := membership.View{
		Scope:       w.Scope,
		Nodes:       slices.Clone(m.nodes),
		Revision:    m.h.revision,
		Incarnation: w.Incarnation,
	}
	m.h.step(c, machine.Viewed{View: v, Token: w.Token})
}

// fail ends a watch of views that c chooses with an error that c chooses. The error is one
// that a retry fixes, or shard.ErrIncarnation for a watch of an incarnation that the root has
// replaced.
func (m *model) fail(c *prop.Case) {
	c.Classify(labelViews)
	i := c.Draw(prop.Integer(0, len(m.h.viewers)-1), "watch of views")
	w := m.h.viewers[i]
	causes := []error{errBusy}
	if w.Incarnation != m.incarnations[len(m.incarnations)-1].id {
		causes = append(causes, shard.ErrIncarnation)
	}
	err := c.Draw(prop.SampledFrom(causes...), "error")
	m.h.viewers = slices.Delete(m.h.viewers, i, i+1)
	m.h.step(c, machine.Viewed{Err: err, Token: w.Token})
}

// expire passes the end of the Timer i of the harness to the router.
func (m *model) expire(c *prop.Case, i int) {
	tm := m.h.timers[i]
	m.h.timers = slices.Delete(m.h.timers, i, i+1)
	m.h.step(c, machine.Elapsed{Token: tm.Token})
}

// refresh passes a Refresh of an incarnation that c chooses, or of an incarnation that the root
// never creates. c also chooses the cell of the Refresh from the cell of the incarnation and a
// cell that no incarnation has. c chooses the generation of the Refresh from zero to two above
// the last commit of the cell.
func (m *model) refresh(c *prop.Case) {
	k := c.Draw(prop.Integer(0, len(m.incarnations)), "incarnation")
	target, which := &m.incarnations[len(m.incarnations)-1], incZ
	if k < len(m.incarnations) {
		target = &m.incarnations[k]
		which = target.id
	}
	cell := target.scope.Cell
	if c.Draw(prop.Boolean(), "unknown cell") {
		cell = unknownCell
	}
	head := target.commits[len(target.commits)-1].generation
	ev := machine.Refresh{
		Cell:        cell,
		Reply:       m.h.call(),
		Min:         c.Draw(prop.Integer(0, head+2), "generation"),
		Incarnation: which,
	}
	m.refreshes[ev.Reply] = ev
	m.calling = ev.Reply
	m.h.step(c, ev)
	m.calling = 0
}

// waiting returns the token of each call without a reply whose caller waits. The tokens are
// in the order of the calls.
func (m *model) waiting() []machine.ReplyToken {
	var calls []machine.ReplyToken
	for to := machine.ReplyToken(1); to <= m.h.calls; to++ {
		if m.replies[to] == 0 && !m.cancelled[to] {
			calls = append(calls, to)
		}
	}
	return calls
}

// cancel passes the Cancelled of a call that c chooses from the calls that wait for their
// reply.
func (m *model) cancel(c *prop.Case) {
	c.Classify(labelCancelled)
	calls := m.waiting()
	to := calls[c.Draw(prop.Integer(0, len(calls)-1), "call")]
	m.cancelled[to] = true
	m.h.step(c, machine.Cancelled{Reply: to})
}

// outages starts an outage of the store, or ends it.
func (m *model) outages(c *prop.Case) {
	if m.outage {
		m.h.st.EndOutage()
	} else {
		c.Classify(labelOutage)
		m.h.st.StartOutage()
	}
	m.outage = !m.outage
}

// recreate deletes the namespace, and the root creates it again in a new incarnation. c
// chooses whether the new cell has the key of the cell of the newest incarnation or a new key.
func (m *model) recreate(c *prop.Case) {
	c.Classify(labelRecreate)
	inc, key := recreation(byte(len(m.incarnations)))
	if c.Draw(prop.Boolean(), "the same cell") {
		c.Classify(labelSameCell)
		key = m.incarnations[len(m.incarnations)-1].key
	}
	m.h.recreate(c, inc, key)
	m.incarnations = append(m.incarnations, newIncarnation(m.h, inc, key))
}

// relist commits the placement of the cell of the newest incarnation to the root scope again.
// A root does so when it puts its cell map again.
func (m *model) relist(c *prop.Case) {
	newest := &m.incarnations[len(m.incarnations)-1]
	m.h.commit(c, rootScope, &store.Mutation{Put: []shard.Placement{{Shard: shard.ID{ID128: newest.key}}}})
}

// late passes an item of a watch that the router has ended, and the router must return out
// unchanged. c chooses the watch and the item. The item is a view, an error of the views, a
// generation or an error of the watch.
func (m *model) late(c *prop.Case) {
	c.Classify(labelLate)
	to := m.h.unwatched[c.Draw(prop.Integer(0, len(m.h.unwatched)-1), "watch")]
	items := []machine.Event{
		machine.Viewed{View: membership.View{Nodes: []shard.Node{nodeA, nodeB}}, Token: to},
		machine.Viewed{Err: errBroken, Token: to},
		machine.Watched{Event: store.Event{Generation: lateGeneration}, Token: to},
		machine.Watched{Err: errBroken, Token: to},
	}
	out := []machine.Effect{marker}
	got := m.h.machine.Step(c.Draw(prop.SampledFrom(items...), "item"), out)
	assert.Equal(c, got, out, "the router must ignore an item of a watch that it has ended")
}

// stop stops the router with Stopped or with an error of the membership that a retry does not
// fix. c chooses the cause. A router that an error stops ends its reads, and the runtime then
// ends every read of the router.
func (m *model) stop(c *prop.Case) {
	c.Classify(labelStopped)
	m.halting = true
	if len(m.h.viewers) > 0 && c.Draw(prop.Boolean(), "broken") {
		m.broken = true
		w := m.h.viewers[0]
		m.h.viewers = slices.Delete(m.h.viewers, 0, 1)
		m.h.step(c, machine.Viewed{Err: errBroken, Token: w.Token})
		expect.Empty(c, m.h.watches, "the router must end the watch of every scope when an error stops it")
		expect.Empty(c, m.h.viewers, "the router must end the watch of the views of every cell when an error stops it")
	} else {
		m.h.step(c, machine.Stopped{})
	}
	m.halting, m.stopped, m.last = false, true, m.h.calls
	m.h.pending, m.h.watches, m.h.viewers, m.h.timers = nil, nil, nil, nil
}

// complete ends the outage and passes what the router waits for. It performs each read, passes
// the item of each watch that has one, ends each Timer, and passes a view of the nodes of the
// last view that a step chose to each watch of views that has not had a view.
func (m *model) complete(c *prop.Case) {
	if m.outage {
		m.h.st.EndOutage()
		m.outage = false
	}
	pause := m.h.pause
	m.h.pause = nil
	m.h.run(c)
	m.h.follow(c)
	for range len(m.h.timers) {
		m.expire(c, 0)
		m.h.run(c)
	}
	for _, w := range slices.Clone(m.h.viewers) {
		i := slices.IndexFunc(m.h.viewers, func(x machine.WatchViews) bool { return x.Token == w.Token })
		if i >= 0 && !m.seen[w.Token] {
			m.send(c, i)
			m.h.run(c)
		}
	}
	m.h.pause = pause
}

// incomplete reports whether the drain has more to do. The drain has more to do while the
// store has an outage, a read or a Timer waits, a watch has an item, or a watch of views has
// not had a view.
func (m *model) incomplete() bool {
	return m.outage || len(m.h.pending) > 0 || len(m.h.timers) > 0 || len(m.ready()) > 0 ||
		slices.ContainsFunc(m.h.viewers, func(w machine.WatchViews) bool { return !m.seen[w.Token] })
}

// invariant checks the contract of the router after every step:
//   - Err returns nil or the error of the membership that stopped the router.
//   - Route returns ErrClosed for every key once the router has stopped.
//   - Every route is of one incarnation, and the router does not install an older one.
//   - Each route has the placement of its slot at its generation and the endpoints of the
//     nodes of the last view of its cell. The harness has passed that generation to the
//     router, and the generation of the route of a shard never falls.
func (m *model) invariant(c *prop.Case) {
	if m.broken {
		expect.ErrorIs(c, m.h.machine.Err(), errBroken, "Err must return the error that stopped the router")
	} else {
		expect.Nil(c, m.h.machine.Err(), "a step of the property must not stop the router with an error")
	}
	installed := -1
	for i, key := range m.h.keys {
		r, err := m.h.machine.Route(c.Context(), key)
		switch {
		case m.stopped:
			expect.ErrorIs(c, err, routes.ErrClosed, "a stopped router must refuse every key")
		case err != nil:
			expect.ErrorIs(c, err, routes.ErrNoRoute, "Route must return a route or ErrNoRoute")
		default:
			k := m.route(c, i, &r)
			if installed >= 0 {
				expect.Equal(c, k, installed, "every route must be of one incarnation")
			}
			installed = k
		}
	}
	if installed < 0 {
		return
	}
	expect.True(c, installed >= m.installed, "the router must not install an older incarnation")
	m.installed = installed
	if installed > 0 {
		c.Classify(labelInstalled)
	}
}

// route checks the route r that Route returned for the key of slot i, and returns the index
// of its incarnation.
func (m *model) route(c *prop.Case, i int, r *routes.Route) int {
	k := slices.IndexFunc(m.incarnations, func(inc incarnation) bool { return inc.id == r.Incarnation })
	assert.True(c, k >= 0, "a route must be of an incarnation that the root has created")
	inc := &m.incarnations[k]
	p, _, ok := inc.placement(i, r.Generation)
	assert.True(c, ok, "a route must be of a placement that a commit has put")
	expect.Equal(c, r.Cell, inc.scope.Cell, "a route must name the cell of its incarnation")
	expect.Equal(c, r.Shard, shards[i], "a route must name the shard of the slot of its key")
	expect.Equal(c, r.Range, p.Range, "a route must have the range of its placement")
	expect.Equal(c, r.Epoch, p.Epoch, "a route must have the epoch of its placement")
	key := scopeKey{scope: inc.scope, incarnation: inc.id}
	expect.Equal(c, r.Replicas, endpoints(&p, m.views[key]),
		"a route must list the replicas of its placement on the nodes of the last view of its cell")
	expect.True(
		c,
		r.Generation <= m.h.delivered[key],
		"a route must be of a generation that the harness has passed to the router",
	)
	s := shardOf{incarnation: k, slot: i}
	expect.True(c, r.Generation >= m.generations[s], "the generation of the route of a shard must never fall")
	m.generations[s] = r.Generation
	return k
}

// settle checks the router once the drain has ended the outage and passed everything that the
// router waits for. A stopped router has replied to each call before its stop that the runtime
// did not cancel. While the router runs, it serves the newest incarnation:
//   - It has replied to each call, except to a Refresh of the cell of the newest incarnation
//     whose generation the controller has not committed.
//   - It watches the root scope, the scope of the newest cell and the views of that cell.
//   - Route returns the last placement of each slot with the endpoints of the last view, or
//     ErrNoRoute for a slot without an endpoint.
func (m *model) settle(c *prop.Case) {
	newest := &m.incarnations[len(m.incarnations)-1]
	head := newest.commits[len(newest.commits)-1].generation
	last := m.h.calls
	if m.stopped {
		last = m.last
	}
	for to := machine.ReplyToken(1); to <= last; to++ {
		r := m.refreshes[to]
		if m.cancelled[to] || !m.stopped && r.Incarnation == newest.id && r.Cell == newest.scope.Cell && r.Min > head {
			continue
		}
		expect.Equal(c, m.replies[to], 1, "the router must reply to each call that the runtime did not cancel")
	}
	if m.stopped {
		return
	}
	scopes := make([]shard.Scope, 0, len(m.h.watches))
	for _, w := range m.h.watches {
		scopes = append(scopes, w.Scope)
		expect.Equal(c, w.Incarnation, newest.id, "every watch must be of the newest incarnation")
	}
	expect.Permutation(c, scopes, []shard.Scope{rootScope, newest.scope},
		"the router must watch the root scope and the scope of the newest cell")
	assert.Length(c, m.h.viewers, 1, "the router must watch the views of one cell")
	expect.Equal(c, m.h.viewers[0].Scope, newest.scope, "the router must watch the views of the newest cell")
	nodes := m.views[scopeKey{scope: newest.scope, incarnation: newest.id}]
	for i, key := range m.h.keys {
		p, put, ok := newest.placement(i, head)
		assert.True(c, ok, "every slot must have a placement")
		want := endpoints(&p, nodes)
		r, err := m.h.machine.Route(c.Context(), key)
		if want == nil {
			expect.ErrorIs(c, err, routes.ErrNoRoute, "a shard without an endpoint must not have a route")
			continue
		}
		assert.NoError(c, err, "a shard with an endpoint must have a route")
		expect.Equal(c, r.Incarnation, newest.id, "the route must be of the newest incarnation")
		expect.True(c, r.Generation >= put, "the route must have the commit of the last placement of its shard")
		expect.Equal(c, r.Epoch, p.Epoch, "the route must have the epoch of the last placement of its shard")
		expect.Equal(
			c,
			r.Replicas,
			want,
			"the route must list the replicas of the last placement on the nodes of the last view",
		)
	}
}

// TestMachine checks the construction, the lifecycle and the steps of a router.
func TestMachine(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give func(*routes.Config)
			name string
		}{
			{
				name: "returns ErrConfig for a config without a reporter",
				give: func(c *routes.Config) { c.Reporter = nil },
			},
			{name: "returns ErrConfig for a config without a logger", give: func(c *routes.Config) { c.Logger = nil }},
			{
				name: "returns ErrConfig for a config without a registry",
				give: func(c *routes.Config) { c.Registry = nil },
			},
			{
				name: "returns ErrConfig for a config without a source of randomness",
				give: func(c *routes.Config) { c.Rand = nil },
			},
			{
				name: "returns ErrConfig for a config without a namespace",
				give: func(c *routes.Config) { c.Namespace = "" },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				cfg := completeConfig(t)
				tt.give(&cfg)
				_, err := routes.New(&cfg)
				assert.ErrorIs(t, err, routes.ErrConfig, "New must refuse a config without a required field")
			})
		}

		t.Run("returns ErrConfig for a nil config", func(t *testing.T) {
			t.Parallel()
			_, err := routes.New(nil)
			assert.ErrorIs(t, err, routes.ErrConfig, "New must refuse a nil config")
		})
	})

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("replies to Started once the cell has its first view", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			to := h.call()
			h.step(t, machine.Started{Reply: to})
			h.run(t)
			h.follow(t)
			_, ok := h.replyTo(to)
			assert.False(t, ok, "the router must not serve before the first view of the cell")
			h.view(t, cellScope, nodeA)
			assert.Nil(t, h.replied(t, to).Err, "the router must serve once the cell has its first view")
		})

		t.Run("replies to Started once the cell has its first snapshot", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.pause = func(e machine.Effect) bool { return e.(machine.Read).Scope == cellScope }
			to := h.call()
			h.step(t, machine.Started{Reply: to})
			h.run(t)
			h.follow(t)
			h.view(t, cellScope, nodeA)
			_, ok := h.replyTo(to)
			assert.False(t, ok, "the router must not serve before the first snapshot of the cell")
			h.pause = nil
			h.run(t)
			assert.Nil(t, h.replied(t, to).Err, "the router must serve once the cell has its first snapshot")
		})

		t.Run("replies with ErrClosed to each call that waits at Stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			to := h.call()
			h.step(t, machine.Started{Reply: to})
			refresh := h.refresh(t, incA, cellID, h.placed)
			h.step(t, machine.Stopped{})
			expect.ErrorIs(t, h.replied(t, to).Err, routes.ErrClosed, "Started must get ErrClosed at Stopped")
			expect.ErrorIs(t, h.replied(t, refresh).Err, routes.ErrClosed, "a Refresh that waits must get ErrClosed")
		})

		t.Run("appends only the replies at Stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.refresh(t, incA, cellID, h.placed+1)
			got := h.machine.Step(machine.Stopped{}, []machine.Effect{marker})
			assert.Equal(t, got, []machine.Effect{marker, machine.Reply[struct{}]{Err: routes.ErrClosed, To: to}},
				"the router must append only the replies at Stopped")
		})

		t.Run("returns out unchanged for an event after it has stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Stopped{})
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Refresh{Cell: cellID, Reply: h.call(), Incarnation: incA}, out)
			assert.Equal(t, got, out, "a stopped router must return out unchanged")
		})

		t.Run("returns an error that wraps fsm.ErrRejected to a second Started", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.call()
			h.step(t, machine.Started{Reply: to})
			expect.ErrorIs(t, h.replied(t, to).Err, fsm.ErrRejected, "the lifecycle must refuse a second start")
			expect.ErrorIs(t, h.machine.Err(), fsm.ErrRejected, "Err must return the error that stopped the router")
		})

		t.Run("stops at an error of a read that is not transient", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.watchFails(t, cellScope, errBroken)
			expect.ErrorIs(t, h.machine.Err(), errBroken, "Err must return the error of the read")
			_, err := h.machine.Route(t.Context(), h.keys[1])
			expect.ErrorIs(t, err, routes.ErrClosed, "a stopped router must refuse every key")
		})

		t.Run("ends its reads when an error stops it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.watchFails(t, cellScope, errBroken)
			expect.Empty(t, h.watches, "the router must end the watch of every scope")
			expect.Empty(t, h.viewers, "the router must end the watch of the views of every cell")
		})

		t.Run("returns out unchanged for an event of another part of the machine", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Called{Token: 99}, out)
			assert.Equal(t, got, out, "the router must not take an event of another part")
		})

		t.Run("drops the reply to a Started that the runtime cancelled", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			to := h.call()
			h.step(t, machine.Started{Reply: to})
			h.step(t, machine.Cancelled{Reply: to})
			h.run(t)
			h.follow(t)
			h.view(t, cellScope, nodeA)
			_, ok := h.replyTo(to)
			assert.False(t, ok, "the router must not reply to a call that the runtime cancelled")
		})

		t.Run("keeps the contract of a router under every sequence of events", func(t *testing.T) {
			t.Parallel()
			coverage := []prop.Option{
				prop.Require(labelRecreate, common),
				prop.Require(labelSameCell, common),
				prop.Require(labelInstalled, common),
				prop.Require(labelCompaction, common),
				prop.Require(labelOutage, common),
				prop.Require(labelViews, common),
				prop.Require(labelCancelled, common),
				prop.Require(labelStopped, common),
				prop.Require(labelWaited, rare),
				prop.Require(labelLate, rare),
			}
			prop.ForAll(t, "a router keeps its contract under every sequence of events", exercise, coverage...)
		})

		t.Run("routes the incarnation that the root creates again at a lower generation", func(t *testing.T) {
			t.Parallel()
			trace := prop.Draws("[" + traceServing + "," + traceLower + "," + traceInstall + "]")
			prop.ForAll(t, "a router routes the namespace that the root creates again at a lower generation", exercise,
				trace, prop.Cases(1))
		})

		t.Run("routes the incarnation that the root creates again at an equal generation", func(t *testing.T) {
			t.Parallel()
			trace := prop.Draws("[" + traceServing + "," + traceEqual + "," + traceInstall + "]")
			prop.ForAll(t, "a router routes the namespace that the root creates again at an equal generation", exercise,
				trace, prop.Cases(1))
		})
	})

	t.Run("Err", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil while the router runs", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.Nil(t, h.machine.Err(), "Err must return nil while the router runs")
		})

		t.Run("returns nil after a run that ended with Stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Stopped{})
			assert.Nil(t, h.machine.Err(), "Err must return nil after a stop at Stopped")
		})
	})
}

// completeConfig returns a Config that New accepts.
func completeConfig(tb assert.TB) routes.Config {
	tb.Helper()
	r := config.NewRegistry(builtin.Support())
	assert.NoError(tb, builtin.Register(r), "the built-ins must register")
	return routes.Config{
		Rand:      constant.NewCounter(half),
		Reporter:  noop.New(),
		Logger:    slog.New(slog.DiscardHandler),
		Registry:  r,
		Provided:  provided,
		Namespace: namespace,
	}
}

// endpoints returns the endpoint of each replica of p whose node nodes lists, with the address
// of the node in nodes, in the order of the replicas. It returns nil when no replica has an
// endpoint.
func endpoints(p *shard.Placement, nodes []shard.Node) []routes.Endpoint {
	var eps []routes.Endpoint
	for _, r := range p.Replicas {
		if i := slices.IndexFunc(nodes, func(n shard.Node) bool { return n.ID == r.Node }); i >= 0 {
			eps = append(eps, routes.Endpoint{Node: r.Node, Address: nodes[i].Address, Role: r.Role})
		}
	}
	return eps
}

// exercise runs the steps of the case c over a router that has taken its Started. The steps
// drive the router as the store, the membership, the clients and the runtime do. They commit
// placements, perform the reads of the router, pass the items of its watches one at a time,
// pass views and end watches of views, end Timers, pass Refresh calls and cancel calls,
// compact the store, start and end outages, pass items of ended watches, put the cell map
// again, create the namespace again, and stop the router. The model checks the contract of
// the router after every step:
//   - No step stops the router with an error, except an error of the membership that the
//     step chooses. A stopped router does not append an effect, and Route returns ErrClosed.
//   - The router ignores the items of the watches that it has ended.
//   - The router replies to each call once, and never to a call that the runtime cancelled.
//     Each reply to a Refresh has the error that the installed incarnation decides.
//   - Each route has the placement of its slot at its generation and the endpoints of the
//     last view of its cell. The generation of the route of a shard never falls, and the
//     router never installs an older incarnation.
//
// The drain ends the outage and passes every read, item, Timer and first view that the router
// waits for. The router then serves the newest incarnation from the last commit and the last
// view, and has replied to every call that can have its reply.
func exercise(c *prop.Case) {
	m := newModel(c)
	running := func(struct{}) bool { return !m.stopped }
	viewing := func(struct{}) bool { return len(m.h.viewers) > 0 }
	stateful.Steps(c, stateful.Machine[struct{}]{
		Actions: []stateful.Action[struct{}]{
			{
				Name: "commit", Weight: commits, Enabled: func(struct{}) bool { return !m.stopped && !m.outage },
				Run: func(c *prop.Case, _ int, _ any) { m.commit(c) },
			},
			{
				Name: "read", Weight: deliveries, Enabled: func(struct{}) bool { return len(m.h.pending) > 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.read(c) },
			},
			{
				Name: "deliver", Weight: deliveries, Enabled: func(struct{}) bool { return len(m.ready()) > 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.deliver(c) },
			},
			{Name: "view", Weight: changes, Enabled: viewing, Run: func(c *prop.Case, _ int, _ any) { m.view(c) }},
			{Name: "refresh", Weight: changes, Run: func(c *prop.Case, _ int, _ any) { m.refresh(c) }},
			{
				Name: "elapse", Enabled: func(struct{}) bool { return len(m.h.timers) > 0 },
				Run: func(c *prop.Case, _ int, _ any) {
					m.expire(c, c.Draw(prop.Integer(0, len(m.h.timers)-1), "timer"))
				},
			},
			{
				Name: "cancel", Enabled: func(struct{}) bool { return len(m.waiting()) > 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.cancel(c) },
			},
			{
				Name: "compact", Enabled: running,
				Run: func(c *prop.Case, _ int, _ any) {
					c.Classify(labelCompaction)
					m.h.st.Compact()
				},
			},
			{Name: "start or end an outage", Enabled: running, Run: func(c *prop.Case, _ int, _ any) { m.outages(c) }},
			{Name: "fail a watch of views", Enabled: viewing, Run: func(c *prop.Case, _ int, _ any) { m.fail(c) }},
			{
				Name: "pass a late item", Enabled: func(struct{}) bool { return len(m.h.unwatched) > 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.late(c) },
			},
			{
				Name: "put the cell again", Enabled: func(struct{}) bool { return !m.stopped && !m.outage },
				Run: func(c *prop.Case, _ int, _ any) { m.relist(c) },
			},
			{
				Name: "create the namespace again", Enabled: func(struct{}) bool { return !m.stopped && !m.outage },
				Run: func(c *prop.Case, _ int, _ any) { m.recreate(c) },
			},
			{Name: "stop", Enabled: running, Run: func(c *prop.Case, _ int, _ any) { m.stop(c) }},
			{
				Name: "complete", Drain: true, Enabled: func(struct{}) bool { return m.incomplete() },
				Run: func(c *prop.Case, _ int, _ any) { m.complete(c) },
			},
		},
		Invariant: func(c *prop.Case, _ struct{}) { m.invariant(c) },
		Settle:    func(c *prop.Case, _ struct{}) { m.settle(c) },
	})
}

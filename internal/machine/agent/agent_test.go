// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/fsm"
	"go.thesmos.sh/core/rand/constant"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// noNode is the writer of a placement without replicas.
const noNode shard.NodeID = ""

// The weights of the actions of the property of an agent that a case takes more often than
// the others.
const (
	// placements is the weight of the commit of a placement.
	placements = 3
	// deliveries is the weight of the delivery of a generation.
	deliveries = 4
	// callbacks is the weight of a callback.
	callbacks = 4
	// answers is the weight of the result of a fence acquisition or a handler call.
	answers = 4
)

// The labels of the cases of the property of an agent, which its coverage requirements count.
const (
	// labelFenced counts a case in which a fence acquisition fails with epoch.ErrFenced.
	labelFenced = "a fence acquisition fails with ErrFenced"
	// labelCleanup counts a case in which the agent cleans up a tenure.
	labelCleanup = "the agent cleans up a tenure"
	// labelResend counts a case that sends a callback again.
	labelResend = "a callback arrives again"
	// labelSession counts a case in which the session of the node ends.
	labelSession = "the session of the node ends"
	// labelCompaction counts a case in which the watch of the scope of the cell ends with
	// store.ErrCompacted.
	labelCompaction = "the watch of the cell is compacted"
	// labelServe counts a case in which the handler of an add succeeds.
	labelServe = "a tenure serves"
	// labelDrop counts a case in which the handler of a drop runs.
	labelDrop = "the agent drops a tenure"
)

// The shares of the cases of the property of an agent that its labels must count, about half
// of the shares that 100 cases of one seed measured.
const (
	// common is the share of labelResend, labelSession and labelCompaction, whose steps do
	// not need a state. The measured shares were 22%, 26% and 28%.
	common = 0.1
	// occasional is the share of labelCleanup and labelServe, which need a tenure. The
	// measured shares were 15% and 18%.
	occasional = 0.05
	// rare is the share of labelFenced and labelDrop, which need a tenure and one of the four
	// results of a pending effect. The measured shares were 6% and 5%.
	rare = 0.02
)

// model is the expected state of an agent that the property of an agent drives, and the
// record of what the agent did. Its harness keeps every fence acquisition and handler call
// pending until a step passes its result, and passes each effect of the agent to observe.
type model struct {
	// c is the case of the property.
	c *prop.Case
	// h is the harness of the agent.
	h *harness
	// granted maps each shard to the fencing token that Token returns for it.
	granted map[shard.ID]fence.Token
	// read maps each shard to the placement of the last commit that the agent has read.
	read map[shard.ID]shard.Placement
	// retried maps the key of each callback and cleanup whose handler the agent has called
	// to whether the last call ended with a transient error.
	retried map[protocol.Key]bool
	// calls maps the token of each callback that the agent took to its request.
	calls map[machine.ReplyToken]*protocol.Request
	// cancelled contains the token of each call that the runtime cancelled.
	cancelled map[machine.ReplyToken]bool
	// replies counts the replies of the agent to each call.
	replies map[machine.ReplyToken]int
	// started maps the token of each fence acquisition to the number of ends of sessions
	// before the agent started it.
	started map[machine.Token]int
	// epochs maps each shard to the epoch of its last placement.
	epochs map[shard.ID]epoch.Epoch
	// operations maps each shard to the sequence of its newest operation.
	operations map[shard.ID]uint64
	// commits lists each commit to the scope of the cell after the start, in order.
	commits []commit
	// sent lists the request of each callback, in order.
	sent []*protocol.Request
	// applied is the number of commits that the agent has read.
	applied int
	// ends counts the ends of the sessions of the node.
	ends int
	// sequence is the Sequence of the last report.
	sequence uint64
	// incarnation is the NodeIncarnation of the last report.
	incarnation epoch.Epoch
	// term is the epoch of the last record of the controller of the cell.
	term epoch.Epoch
	// closing is the token of the Close, and zero before it.
	closing machine.ReplyToken
	// stops is true when the ownership of the case stops the writers at the end of the
	// lease.
	stops bool
	// live is true while the node has a session.
	live bool
	// stopping is true from the reply to the Close until the end of its step.
	stopping bool
	// stopped is true once the agent has stopped.
	stopped bool
	// failing is true while the controller refuses reports.
	failing bool
}

// commit is one commit to the scope of the cell: its generation and the placements that it
// puts.
type commit struct {
	// puts lists the placements that the commit puts.
	puts []shard.Placement
	// generation is the generation of the commit.
	generation epoch.Epoch
}

// newModel returns the model of an agent that serves. The ownership of the agent stops its
// writers at the end of the lease when stops is true.
func newModel(c *prop.Case, stops bool) *model {
	opts := make([]option, 0, 1)
	if stops {
		opts = append(opts, func(s *setup) {
			s.spec.Ownership = config.Selection{Config: json.RawMessage(stopOnLoss)}
		})
	}
	m := &model{
		c:          c,
		h:          newHarness(c, opts...),
		granted:    make(map[shard.ID]fence.Token),
		read:       make(map[shard.ID]shard.Placement),
		retried:    make(map[protocol.Key]bool),
		calls:      make(map[machine.ReplyToken]*protocol.Request),
		cancelled:  make(map[machine.ReplyToken]bool),
		replies:    make(map[machine.ReplyToken]int),
		started:    make(map[machine.Token]int),
		epochs:     make(map[shard.ID]epoch.Epoch),
		operations: make(map[shard.ID]uint64),
		term:       cellTerm,
		stops:      stops,
	}
	for _, sid := range allShards {
		m.operations[sid] = 1
	}
	m.h.observe = m.observe
	m.h.ack = m.ack
	m.h.start(c)
	m.h.pause = func(e machine.Effect) bool {
		switch e.(type) {
		case machine.Acquire, machine.Handle:
			return true
		}
		return false
	}
	return m
}

// observe checks the effect e that the agent appends, and takes what e changes in the
// expected state.
func (m *model) observe(e machine.Effect) {
	c := m.c
	c.Helper()
	assert.False(c, m.stopped, "a stopped agent must not append an effect")
	switch e := e.(type) {
	case machine.Join:
		m.live = true
	case machine.Acquire:
		m.acquire(e)
	case machine.Handle:
		m.handle(e.Request)
	case machine.SendReport:
		want := uint64(1)
		if e.Report.NodeIncarnation == m.incarnation {
			want = m.sequence + 1
		}
		expect.Equal(c, e.Report.Sequence, want, "the agent must number the reports of an incarnation from one")
		m.sequence, m.incarnation = e.Report.Sequence, e.Report.NodeIncarnation
	case machine.Reply[struct{}]:
		m.replies[e.To]++
		expect.Equal(c, m.replies[e.To], 1, "the agent must reply to a call once")
		expect.False(c, m.cancelled[e.To], "the agent must not reply to a call that the runtime cancelled")
		if e.To == m.closing {
			m.stopping = true
		}
	}
}

// acquire checks the fence acquisition e. The agent starts a tenure only while the node has a
// session, at the epoch of the placement on the node that it has read last.
func (m *model) acquire(e machine.Acquire) {
	c := m.c
	p := m.read[e.Fence.Shard]
	expect.True(c, m.live, "the agent must start a tenure only while the node has a session")
	expect.Equal(
		c,
		e.Fence.Epoch,
		p.Epoch,
		"the agent must acquire the fence at the epoch of the placement that it read",
	)
	expect.True(c, slices.Contains(p.Replicas, shard.Replica{Node: node.ID, Role: shard.RolePrimary}),
		"the agent must acquire the fence of a placement on the node")
	m.started[e.Token] = m.ends
}

// handle checks the handler call of the request r. The agent calls the handler for a key
// again only after a transient error, and a drop or a cleanup revokes the token of its
// tenure.
func (m *model) handle(r *protocol.Request) {
	key := r.Key()
	if transient, ok := m.retried[key]; ok {
		expect.True(m.c, transient, "the agent must call the handler for a key again only after a transient error")
	}
	m.retried[key] = false
	if r.Path == protocol.Cleanup {
		m.c.Classify(labelCleanup)
	}
	if g, ok := m.granted[r.Shard]; ok && r.Call == protocol.Drop && g.Epoch == r.Epoch {
		delete(m.granted, r.Shard)
	}
}

// ack returns the reply of the controller to the report r. The controller accepts r at the
// term of the last record, and returns protocol.ErrUnreachable while it refuses reports.
func (m *model) ack(r *protocol.Report) (protocol.Ack, error) {
	if m.failing {
		return protocol.Ack{}, protocol.ErrUnreachable
	}
	return protocol.Ack{Sequence: r.Sequence, ControllerEpoch: m.term}, nil
}

// place commits the placement of a shard that c chooses at the next epoch of the shard, with
// the node, another node or no node as its writer.
func (m *model) place(c *prop.Case) {
	sid := c.Draw(prop.SampledFrom(allShards...), "shard")
	writer := c.Draw(prop.SampledFrom(node.ID, otherNode, noNode), "writer")
	m.epochs[sid]++
	p := placement(sid, m.epochs[sid], writer)
	if writer == noNode {
		p.Replicas = nil
	}
	m.commit(c, &store.Mutation{Put: []shard.Placement{p}})
}

// assign commits the placement of a shard that c chooses on the node at the next epoch of the
// shard, and sends the add of the next operation of the shard at that epoch, as the
// controller of the cell moves a shard to the node.
func (m *model) assign(c *prop.Case) {
	sid := c.Draw(prop.SampledFrom(allShards...), "shard")
	m.epochs[sid]++
	m.operations[sid]++
	m.commit(c, &store.Mutation{Put: []shard.Placement{placement(sid, m.epochs[sid], node.ID)}})
	r := m.h.request(protocol.Add, sid, m.operations[sid], m.epochs[sid])
	r.ControllerEpoch = m.term
	m.send(c, r)
}

// release sends the drop of the next operation of a shard that c chooses at the epoch of its
// last placement, as the controller of the cell starts to move a shard away from the node.
func (m *model) release(c *prop.Case) {
	sid := c.Draw(prop.SampledFrom(allShards...), "shard")
	m.operations[sid]++
	r := m.h.request(protocol.Drop, sid, m.operations[sid], m.epochs[sid])
	r.ControllerEpoch = m.term
	m.send(c, r)
}

// transition commits a transition of the newest operation of a shard that c chooses, or of
// the operation after it. c also chooses the phase, and whether the node is the target or the
// source.
func (m *model) transition(c *prop.Case) {
	sid := c.Draw(prop.SampledFrom(allShards...), "shard")
	m.operations[sid] += c.Draw(prop.Integer[uint64](0, 1), "next operation")
	op := shard.OperationID{Incarnation: incA, Shard: sid, Sequence: m.operations[sid]}
	t := transition(op, c.Draw(prop.SampledFrom(shard.PhaseRunning, shard.PhaseAborting), "phase"))
	if c.Draw(prop.Boolean(), "from the node") {
		t.From.Node, t.To.Node = node.ID, otherNode
	}
	m.commit(c, &store.Mutation{Transitions: []store.Transition{t}})
}

// done commits the end of the transition of a shard that c chooses.
func (m *model) done(c *prop.Case) {
	sid := c.Draw(prop.SampledFrom(allShards...), "shard")
	m.commit(c, &store.Mutation{Done: []shard.ID{sid}})
}

// record commits the record of the next controller of the cell, with the identity of the
// controller of the tests.
func (m *model) record(c *prop.Case) {
	m.term++
	m.commit(c, &store.Mutation{Cell: record(m.term)})
}

// commit commits mu to the scope of the cell, and keeps the placements that mu puts with the
// generation of the commit.
func (m *model) commit(c *prop.Case, mu *store.Mutation) {
	gen := m.h.commitCell(c, mu)
	m.commits = append(m.commits, commit{puts: mu.Put, generation: gen})
}

// deliver passes the next generation of the scope of the cell to the agent. The placements of
// the commits up to that generation become the placements that the agent has read.
func (m *model) deliver(c *prop.Case) {
	i := slices.IndexFunc(m.h.watches, func(w machine.Watch) bool { return w.Scope == cellScope })
	w := m.h.watches[i]
	ctx, cancel := context.WithCancel(assert.Context(c))
	cancel()
	passed := false
	for ev, err := range m.h.st.Watch(ctx, w.Scope, w.Incarnation, w.From, w.Node) {
		assert.NoError(c, err, "the watch of the scope of the cell must yield its next generation")
		m.h.watches[i].From = ev.Generation
		m.advance(ev.Generation)
		m.h.step(c, machine.Watched{Event: ev, Token: w.Token})
		passed = true
		break
	}
	assert.True(c, passed, "the watch of the scope of the cell must yield a generation that the agent has not read")
	m.h.run(c)
	m.sync()
}

// advance takes the placements of the commits up to the generation gen as the placements that
// the agent has read.
func (m *model) advance(gen epoch.Epoch) {
	for m.applied < len(m.commits) && m.commits[m.applied].generation <= gen {
		for _, p := range m.commits[m.applied].puts {
			m.read[p.Shard] = p
		}
		m.applied++
	}
}

// callback sends an add or a drop for a shard that c chooses. The callback is of the newest
// operation of the shard or of the operation after it, at the epoch of the last placement of
// the shard or at the epoch before it. c chooses each of these, and the path and the step.
func (m *model) callback(c *prop.Case) {
	sid := c.Draw(prop.SampledFrom(allShards...), "shard")
	call := c.Draw(prop.SampledFrom(protocol.Add, protocol.Drop), "call")
	m.operations[sid] += c.Draw(prop.Integer[uint64](0, 1), "next operation")
	earlier := min(c.Draw(prop.Integer[epoch.Epoch](0, 1), "earlier epoch"), m.epochs[sid])
	r := m.h.request(call, sid, m.operations[sid], m.epochs[sid]-earlier)
	r.Path = c.Draw(prop.SampledFrom(protocol.Forward, protocol.Abort), "path")
	r.Step = c.Draw(prop.Integer(0, 1), "step")
	r.ControllerEpoch = m.term
	m.send(c, r)
}

// resend sends a callback again that a step sent before. c chooses the callback.
func (m *model) resend(c *prop.Case) {
	c.Classify(labelResend)
	m.send(c, m.sent[c.Draw(prop.Integer(0, len(m.sent)-1), "callback")])
}

// send passes the callback r to the agent, and records it. The model does not wait for the
// reply to a callback that it sends after the agent has stopped.
func (m *model) send(c *prop.Case, r *protocol.Request) {
	to := m.h.call()
	if !m.stopped {
		m.calls[to] = r
	}
	m.sent = append(m.sent, r)
	md := protocol.Metadata{Caller: controllerIdentity, Version: protocol.Current}
	m.h.step(c, machine.Do{Request: r, Metadata: md, Reply: to})
	m.h.run(c)
	m.sync()
}

// answer passes a result to a pending fence acquisition or handler call. c chooses the effect
// and the result.
func (m *model) answer(c *prop.Case) {
	i := c.Draw(prop.Integer(0, len(m.h.pending)-1), "effect")
	m.finish(c, i, c.Draw(prop.SampledFrom(outcomes...), "outcome"))
}

// finish passes the result err of the pending effect i to the agent, and takes what the result
// changes in the expected state. An acquisition replaces the token of its shard. A successful
// acquisition grants its token, unless the agent has stopped, or the session of the node has
// ended since its start under ownership.Stop. An add whose acquisition or handler fails
// loses its token.
func (m *model) finish(c *prop.Case, i int, err error) {
	e := m.h.pending[i]
	m.h.pending = slices.Delete(m.h.pending, i, i+1)
	switch e := e.(type) {
	case machine.Acquire:
		delete(m.granted, e.Fence.Shard)
		if err == nil && !m.stopped && (!m.stops || m.started[e.Token] == m.ends) {
			m.granted[e.Fence.Shard] = e.Fence
		}
		if errors.Is(err, epoch.ErrFenced) {
			c.Classify(labelFenced)
		}
		m.h.step(c, machine.Acquired{Err: err, Token: e.Token})
	case machine.Handle:
		r := e.Request
		m.retried[r.Key()] = errs.Retryable(err)
		if err != nil && r.Call == protocol.Add {
			delete(m.granted, r.Shard)
		}
		if err == nil && r.Call == protocol.Add {
			c.Classify(labelServe)
		}
		if r.Call == protocol.Drop && r.Path != protocol.Cleanup {
			c.Classify(labelDrop)
		}
		m.h.step(c, machine.Handled{Err: err, Token: e.Token})
	}
	m.h.run(c)
	m.sync()
}

// expire passes the end of the Timer i of the harness to the agent.
func (m *model) expire(c *prop.Case, i int) {
	tm := m.h.timers[i]
	m.h.timers = slices.Delete(m.h.timers, i, i+1)
	m.h.step(c, machine.Elapsed{Token: tm.Token})
	m.h.run(c)
	m.sync()
}

// end ends the session of the node. Under ownership.Stop, the end revokes every token.
func (m *model) end(c *prop.Case) {
	c.Classify(labelSession)
	m.live = false
	m.ends++
	if m.stops {
		clear(m.granted)
	}
	m.h.step(c, machine.SessionEnded{Token: m.h.join})
	m.h.run(c)
	m.sync()
}

// cancel ends the wait of the caller of a call without a reply. c chooses the call. An agent
// whose Close loses its caller stops.
func (m *model) cancel(c *prop.Case) {
	calls := m.waiting()
	to := calls[c.Draw(prop.Integer(0, len(calls)-1), "call")]
	m.cancelled[to] = true
	m.h.step(c, machine.Cancelled{Reply: to})
	m.h.run(c)
	if to == m.closing {
		m.stopping = true
	}
	m.sync()
}

// waiting returns the tokens of the callbacks and of the Close that have no reply and whose
// callers wait, in their order.
func (m *model) waiting() []machine.ReplyToken {
	var calls []machine.ReplyToken
	for _, to := range slices.Sorted(maps.Keys(m.calls)) {
		if m.replies[to] == 0 && !m.cancelled[to] {
			calls = append(calls, to)
		}
	}
	if m.closing != 0 && m.replies[m.closing] == 0 && !m.cancelled[m.closing] {
		calls = append(calls, m.closing)
	}
	return calls
}

// compact ends the watch of the scope of the cell with store.ErrCompacted, so the agent reads
// a snapshot of every commit.
func (m *model) compact(c *prop.Case) {
	c.Classify(labelCompaction)
	m.advance(m.h.state(c).Head.Generation)
	m.h.compact(c)
	m.sync()
}

// fault reports a fault in the replica of a shard that c chooses, or the recovery of the
// replica.
func (m *model) fault(c *prop.Case) {
	sid := c.Draw(prop.SampledFrom(allShards...), "shard")
	to := m.h.call()
	if c.Draw(prop.Boolean(), "recovery") {
		m.h.step(c, machine.Recovered{Reply: to, Shard: sid})
	} else {
		f := c.Draw(prop.SampledFrom(shard.FaultTransient, shard.FaultPermanent), "fault")
		m.h.step(c, machine.ReportFault{Cause: errDisk, Reply: to, Shard: sid, Fault: f})
	}
	m.h.run(c)
	m.sync()
}

// close passes a Close of the application with a deadline after now.
func (m *model) close(c *prop.Case) {
	m.closing = m.h.call()
	m.h.step(c, machine.Close{Deadline: origin.Add(grace), Reply: m.closing})
	m.h.run(c)
	m.sync()
}

// halt passes the Stopped of the run.
func (m *model) halt(c *prop.Case) {
	m.h.step(c, machine.Stopped{})
	m.stopped = true
	clear(m.granted)
}

// complete passes success to each pending fence acquisition and handler call. It then passes
// each generation of the scope that the agent has not read, and ends each Timer of a backoff.
// The controller accepts reports from then on.
func (m *model) complete(c *prop.Case) {
	m.failing = false
	for range len(m.h.pending) {
		m.finish(c, 0, nil)
	}
	for m.applied < len(m.commits) && !m.stopped &&
		slices.ContainsFunc(m.h.watches, func(w machine.Watch) bool { return w.Scope == cellScope }) {
		m.deliver(c)
	}
	for i, tm := range slices.Backward(m.h.timers) {
		if tm.Duration < loadWindow {
			m.expire(c, i)
		}
	}
}

// sync takes the end of a step. The agent has stopped once it has replied to its Close, and
// once its Close has lost its caller.
func (m *model) sync() {
	if m.stopping {
		m.stopping, m.stopped = false, true
		clear(m.granted)
	}
}

// invariant checks the agent after every step. No step stops the agent with an error, the
// agent runs one callback of a shard at a time, and Token returns the token that the model
// expects for each shard.
func (m *model) invariant(c *prop.Case) {
	assert.Nil(c, m.h.machine.Err(), "no step of the property may stop the agent with an error")
	running := make(map[shard.ID]int)
	for _, e := range m.h.pending {
		switch e := e.(type) {
		case machine.Acquire:
			running[e.Fence.Shard]++
		case machine.Handle:
			running[e.Request.Shard]++
		}
	}
	for _, sid := range allShards {
		expect.InRange(c, running[sid], 0, 1, "the agent must run one callback of a shard at a time")
		got, err := m.h.machine.Token(sid)
		if want, ok := m.granted[sid]; ok {
			expect.NoError(c, err, "Token must return the token of a tenure that has acquired its fence")
			expect.Equal(c, got, want, "Token must return the token of the last acquisition of the shard")
		} else {
			expect.HasError(c, err, "Token must return an error for a shard without a token")
		}
	}
}

// settle checks the agent once the drain has completed every call with success and passed
// every generation. Every callback has its reply, unless it is an add whose placement on the
// node the agent has not read at the epoch of the add. Token returns a token only for the
// placement on the node that the agent has read, at the epoch of that placement.
func (m *model) settle(c *prop.Case) {
	for _, to := range slices.Sorted(maps.Keys(m.calls)) {
		r := m.calls[to]
		if m.replies[to] > 0 || m.cancelled[to] {
			continue
		}
		p := m.read[r.Shard]
		expect.Equal(c, r.Call, protocol.Add, "every drop must have its reply")
		expect.False(
			c,
			p.Epoch == r.Epoch && slices.Contains(p.Replicas, shard.Replica{Node: node.ID, Role: shard.RolePrimary}),
			"an add whose placement the agent has read must have its reply",
		)
	}
	for _, sid := range allShards {
		got, err := m.h.machine.Token(sid)
		if err != nil {
			continue
		}
		p := m.read[sid]
		expect.Equal(c, got.Epoch, p.Epoch, "a token must have the epoch of the placement that the agent read")
		expect.True(c, slices.Contains(p.Replicas, shard.Replica{Node: node.ID, Role: shard.RolePrimary}),
			"a token must be of a placement on the node")
	}
}

// TestMachine checks the construction, the lifecycle and the replies of an agent.
func TestMachine(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give func(*agent.Config)
			name string
		}{
			{name: "returns ErrConfig for a config without a clock", give: func(c *agent.Config) { c.Clock = nil }},
			{
				name: "returns ErrConfig for a config without a reporter",
				give: func(c *agent.Config) { c.Reporter = nil },
			},
			{name: "returns ErrConfig for a config without a logger", give: func(c *agent.Config) { c.Logger = nil }},
			{
				name: "returns ErrConfig for a config without a registry",
				give: func(c *agent.Config) { c.Registry = nil },
			},
			{name: "returns ErrConfig for a node without an ID", give: func(c *agent.Config) { c.Node.ID = "" }},
			{
				name: "returns ErrConfig for a node without an address",
				give: func(c *agent.Config) { c.Node.Address = "" },
			},
			{
				name: "returns ErrConfig for a cell that is not the text of a shard ID",
				give: func(c *agent.Config) { c.Node.Cell = otherCell },
			},
			{
				name: "returns ErrConfig for a config without a source of randomness",
				give: func(c *agent.Config) { c.Rand = nil },
			},
			{
				name: "returns ErrConfig for a config without a namespace",
				give: func(c *agent.Config) { c.Namespace = "" },
			},
			{
				name: "returns ErrConfig for an agent of the root whose node has a cell",
				give: func(c *agent.Config) { c.Root, c.Node.Cell = true, cellID },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				cfg := completeConfig(t)
				tt.give(&cfg)
				_, err := agent.New(&cfg)
				assert.ErrorIs(t, err, agent.ErrConfig, "New must refuse a config without a required field")
			})
		}

		t.Run("returns ErrConfig for a nil config", func(t *testing.T) {
			t.Parallel()
			_, err := agent.New(nil)
			assert.ErrorIs(t, err, agent.ErrConfig, "New must refuse a nil config")
		})
	})

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("replies to Started once the node has a session and a snapshot of its cell", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			started := h.call()
			h.step(t, machine.Started{Reply: started})
			assert.Empty(t, h.replies, "the agent must not reply before it serves")
			h.run(t)
			assert.Equal(t, h.replies, []machine.Effect{machine.Reply[struct{}]{To: started}},
				"the agent must reply to Started once it serves")
		})

		t.Run("joins the cell of the node with the lease of the spec", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.Length(t, h.joins, 1, "the agent must join its cell once")
			got := h.joins[0]
			expect.Equal(t, got.Scope, cellScope, "the agent must join the scope of its cell")
			expect.Equal(t, got.Incarnation, incA, "the join must name the namespace incarnation")
			expect.Equal(t, got.Node.ID, node.ID, "the join must register the node")
			expect.Equal(t, got.Lease.TTL, shard.Duration(10*time.Second), "the join must have the lease of the spec")
		})

		t.Run("stops reading the root scope once it knows its cell", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.Length(t, h.watches, 1, "the agent must watch the scope of its cell alone")
			assert.Equal(t, h.watches[0].Scope, cellScope, "the watch must be of the scope of the cell")
		})

		t.Run("watches the scope of its cell for its node", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.Length(t, h.watches, 1, "the agent must watch the scope of its cell")
			expect.Equal(t, h.watches[0].Node, node.ID, "the watch must select the changes of the node")
			expect.Equal(t, h.watches[0].Incarnation, incA, "the watch must name the namespace incarnation")
		})

		t.Run("stops with fsm.ErrRejected for a second Started", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			first := h.call()
			h.step(t, machine.Started{Reply: first})
			second := h.call()
			h.step(t, machine.Started{Reply: second})
			expect.ErrorIs(t, h.machine.Err(), fsm.ErrRejected, "the lifecycle must refuse a second Started")
			expect.ErrorIs(t, replyTo[struct{}](t, h, first).Err, fsm.ErrRejected,
				"the first Started must get the error that stopped the agent")
			expect.ErrorIs(t, replyTo[struct{}](t, h, second).Err, fsm.ErrRejected,
				"the second Started must get the error that stopped the agent")
		})

		t.Run("replies with ErrNotServing to Started at Stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			started := h.call()
			h.step(t, machine.Started{Reply: started})
			h.step(t, machine.Stopped{})
			assert.ErrorIs(t, replyTo[struct{}](t, h, started).Err, agent.ErrNotServing,
				"Stopped must reply to Started with ErrNotServing")
			assert.Nil(t, h.machine.Err(), "an agent that Stopped ends must not report an error")
		})

		t.Run("ends the reads of its scopes when an error stops it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Started{Reply: h.call()})
			assert.NotNil(t, h.machine.Err(), "a second Started must stop the agent")
			expect.Empty(t, h.watches, "the agent must end the watch of its cell")
			expect.Contains(t, h.leaves, h.join, "the agent must end the session of its node")
		})

		t.Run("ends the read of the root scope when an error stops it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.create = false })
			h.step(t, machine.Started{Reply: h.call()})
			h.run(t)
			h.follow(t)
			assert.Length(t, h.watches, 1, "the agent must watch the root scope for the namespace")
			h.step(t, machine.Started{Reply: h.call()})
			assert.NotNil(t, h.machine.Err(), "a second Started must stop the agent")
			assert.Empty(t, h.watches, "the agent must end the watch of the root scope")
		})

		t.Run("appends no effect at Stopped when no call waits", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Stopped{}, out)
			assert.Equal(t, got, out, "Stopped must append only the replies to the calls that wait")
		})

		t.Run("returns out unchanged after it has stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Stopped{})
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Started{Reply: h.call()}, out)
			assert.Equal(t, got, out, "a stopped agent must not take an event")
		})

		t.Run("returns out unchanged after an error has stopped it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Started{Reply: h.call()})
			assert.NotNil(t, h.machine.Err(), "a second Started must stop the agent")
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Started{Reply: h.call()}, out)
			assert.Equal(t, got, out, "an agent that an error stopped must not take an event")
		})

		t.Run("drops the reply to a Started that the runtime cancelled", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			started := h.call()
			h.step(t, machine.Started{Reply: started})
			h.step(t, machine.Cancelled{Reply: started})
			h.run(t)
			assert.Empty(t, h.replies, "the agent must not reply to a call that the runtime cancelled")
		})

		t.Run("returns out unchanged for a Timer that it did not arm", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Elapsed{Token: 999}, out)
			assert.Equal(t, got, out, "the agent must ignore a Timer that it did not arm")
		})

		t.Run("returns out unchanged for a Timer before it knows its cell", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Elapsed{Token: 999}, out)
			assert.Equal(t, got, out, "the agent must ignore a Timer that it did not arm")
		})

		t.Run("returns out unchanged for the Cancelled of a call that does not wait", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Cancelled{Reply: 999}, out)
			assert.Equal(t, got, out, "the agent must ignore the Cancelled of a call that does not wait")
		})

		t.Run("returns out unchanged for an event of no part", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Planned{Token: 999}, out)
			assert.Equal(t, got, out, "the agent must ignore an event that no part waits for")
		})

		t.Run("releases its instruments at Stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Stopped{})
			expect.Equal(t, h.metrics.Bound(refusalsInstrument, series...), 0,
				"the agent must release the counter of refusals")
			expect.Equal(t, h.metrics.Bound(lagInstrument, series...), 0, "the agent must release the gauge of the lag")
		})

		t.Run("keeps its contract under every sequence of events", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "an agent keeps its contract under every sequence of events", exercise,
				prop.Require(labelFenced, rare),
				prop.Require(labelCleanup, occasional),
				prop.Require(labelResend, common),
				prop.Require(labelSession, common),
				prop.Require(labelCompaction, common),
				prop.Require(labelServe, occasional),
				prop.Require(labelDrop, rare),
			)
		})
	})

	t.Run("Err", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil while the agent runs", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.Nil(t, h.machine.Err(), "an agent that runs must not report an error")
		})
	})
}

// registry returns a registry with every built-in strategy of the support matrix.
func registry(tb assert.TB) *config.Registry {
	tb.Helper()
	r := config.NewRegistry(builtin.Support())
	assert.NoError(tb, builtin.Register(r), "the built-ins must register")
	return r
}

// completeConfig returns a Config that New accepts.
func completeConfig(tb assert.TB) agent.Config {
	tb.Helper()
	return agent.Config{
		Clock:     fake.New(origin),
		Rand:      constant.NewCounter(half),
		Reporter:  noop.New(),
		Logger:    slog.New(slog.DiscardHandler),
		Registry:  registry(tb),
		Namespace: namespace,
		Node:      node,
	}
}

// exercise runs the steps of the case c over an agent that serves, as the controller of its
// cell, the membership and the application drive it. Each step does one of these things:
//   - It commits a placement, a transition, the end of a transition or a record of the
//     controller.
//   - It passes the next generation to the agent.
//   - It sends a callback, or sends a callback again.
//   - It passes the result of a pending fence acquisition or handler call.
//   - It ends a Timer, or cancels a call.
//   - It ends the session of the node, or compacts the watch.
//   - It reports a fault, or closes or stops the agent.
//
// The model checks the contract of the agent after every step:
//   - No step stops the agent with an error. A stopped agent appends no effect.
//   - The agent replies to each call once. It never replies to a call that the runtime
//     cancelled.
//   - It runs one callback of a shard at a time. It calls the handler for a key again only
//     after a transient error.
//   - It starts a tenure only while the node has a session, at the epoch of the placement on
//     the node that it has read.
//   - Token returns the token of each tenure from its acquisition until a drop or a cleanup
//     of the tenure starts, its add fails, the session ends under ownership.Stop, or the
//     agent stops.
//   - It numbers the reports of each incarnation from one.
//
// The drain completes every call with success and passes every generation. Every callback then
// has its reply, unless it is an add whose placement the agent has not read at its epoch, and
// Token returns a token only for the placement on the node that the agent has read.
func exercise(c *prop.Case) {
	m := newModel(c, c.Draw(prop.Boolean(), "stop at the end of the lease"))
	watching := func(struct{}) bool {
		return !m.stopped &&
			slices.ContainsFunc(m.h.watches, func(w machine.Watch) bool { return w.Scope == cellScope })
	}
	behind := func(s struct{}) bool { return m.applied < len(m.commits) && watching(s) }
	holding := func(struct{}) bool { return len(m.h.pending) > 0 }
	timing := func(struct{}) bool { return len(m.h.timers) > 0 }
	incomplete := func(s struct{}) bool {
		return holding(s) || behind(s) ||
			slices.ContainsFunc(m.h.timers, func(tm machine.Timer) bool { return tm.Duration < loadWindow })
	}
	stateful.Steps(c, stateful.Machine[struct{}]{
		Actions: []stateful.Action[struct{}]{
			{Name: "place", Weight: placements, Run: func(c *prop.Case, _ int, _ any) { m.place(c) }},
			{Name: "assign", Weight: placements, Run: func(c *prop.Case, _ int, _ any) { m.assign(c) }},
			{Name: "release", Weight: placements, Run: func(c *prop.Case, _ int, _ any) { m.release(c) }},
			{Name: "start a transition", Run: func(c *prop.Case, _ int, _ any) { m.transition(c) }},
			{Name: "end a transition", Run: func(c *prop.Case, _ int, _ any) { m.done(c) }},
			{Name: "record a controller", Run: func(c *prop.Case, _ int, _ any) { m.record(c) }},
			{
				Name: "deliver", Weight: deliveries, Enabled: behind,
				Run: func(c *prop.Case, _ int, _ any) { m.deliver(c) },
			},
			{Name: "callback", Weight: callbacks, Run: func(c *prop.Case, _ int, _ any) { m.callback(c) }},
			{
				Name: "send a callback again", Enabled: func(struct{}) bool { return len(m.sent) > 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.resend(c) },
			},
			{
				Name: "answer", Weight: answers, Enabled: holding,
				Run: func(c *prop.Case, _ int, _ any) { m.answer(c) },
			},
			{
				Name: "cancel", Enabled: func(struct{}) bool { return len(m.waiting()) > 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.cancel(c) },
			},
			{
				Name: "elapse", Enabled: timing,
				Run: func(c *prop.Case, _ int, _ any) {
					m.expire(c, c.Draw(prop.Integer(0, len(m.h.timers)-1), "timer"))
				},
			},
			{
				Name: "end the session", Enabled: func(struct{}) bool { return m.live && !m.stopped },
				Run: func(c *prop.Case, _ int, _ any) { m.end(c) },
			},
			{Name: "compact", Enabled: watching, Run: func(c *prop.Case, _ int, _ any) { m.compact(c) }},
			{Name: "report a fault", Run: func(c *prop.Case, _ int, _ any) { m.fault(c) }},
			{Name: "refuse reports", Run: func(*prop.Case, int, any) { m.failing = !m.failing }},
			{
				Name: "close", Enabled: func(struct{}) bool { return m.closing == 0 && !m.stopped },
				Run: func(c *prop.Case, _ int, _ any) { m.close(c) },
			},
			{
				Name: "stop", Enabled: func(struct{}) bool { return !m.stopped },
				Run: func(c *prop.Case, _ int, _ any) { m.halt(c) },
			},
			{
				Name: "complete", Drain: true, Enabled: incomplete,
				Run: func(c *prop.Case, _ int, _ any) { m.complete(c) },
			},
		},
		Invariant: func(c *prop.Case, _ struct{}) { m.invariant(c) },
		Settle:    func(c *prop.Case, _ struct{}) { m.settle(c) },
	})
}

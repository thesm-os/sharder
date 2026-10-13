// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package cell_test

import (
	"encoding/binary"
	"log/slog"
	"slices"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/fsm"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/rand/constant"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/cell"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// otherCell is a cell other than the cell of the tests.
const otherCell shard.CellID = "other"

// The labels of the cases of the property of a cell, which its coverage requirements count.
const (
	// labelEarly counts a case in which a call arrives before the cell serves.
	labelEarly = "a call arrives before the cell serves"
	// labelSpec counts a case in which the root commits a newer spec.
	labelSpec = "the root commits a newer spec"
	// labelFenced counts a case in which a later term fences the scope of the cell.
	labelFenced = "a later term fences the cell"
	// labelStopped counts a case in which a step stops the cell.
	labelStopped = "a step stops the cell"
	// labelOutage counts a case in which the cell arms a Timer during an outage of the store.
	labelOutage = "the cell arms a Timer during an outage"
)

// The shares of the cases of the property of a cell that its labels must count, about half of
// the shares that 1,000 cases measured.
const (
	// often is the share of labelEarly and labelStopped. The measured shares were 49% and
	// 39%.
	often = 0.2
	// sometimes is the share of labelSpec and labelFenced. The measured shares were 27% and
	// 32%.
	sometimes = 0.13
	// rare is the share of labelOutage. The measured share was 7.5%.
	rare = 0.035
)

// nodeSets lists the sets of nodes that the views of the property of a cell contain.
var nodeSets = [][]shard.Node{{nodeA}, {nodeA, nodeB}, {nodeA, nodeB, nodeC}, {}}

// model is the expected state of a cell that the property of a cell drives, and the record
// of what the cell did. Its harness passes each effect of the cell to observe.
type model struct {
	// c is the case of the property.
	c *prop.Case
	// h is the harness of the cell.
	h *harness
	// want maps the token of each call whose reply the model knows to the error of the reply.
	want map[machine.ReplyToken]error
	// replies counts the replies of the cell to each call.
	replies map[machine.ReplyToken]int
	// cancelled contains the token of each call that the runtime cancelled.
	cancelled map[machine.ReplyToken]bool
	// late contains the token of each call that the cell took after it had stopped.
	late map[machine.ReplyToken]bool
	// retries contains the token of each Timer that the cell armed during an outage. The drain
	// ends each of them.
	retries map[machine.Token]bool
	// sent lists the token of each call, in order.
	sent []machine.ReplyToken
	// started is the token of Started, and zero before Started.
	started machine.ReplyToken
	// version is the version of the last spec that the root committed.
	version epoch.Epoch
	// sequence is the Sequence of the last report.
	sequence uint64
	// disruptions counts the disruptions, and gives each disruption its ID.
	disruptions byte
	// serving is true once the cell has started the watch of its views, which its controller
	// starts once it serves.
	serving bool
	// behind is true while the root scope has a spec that the cell has not followed.
	behind bool
	// fenced is true once a later term has committed to the scope of the cell.
	fenced bool
	// outage is true while the store refuses every call.
	outage bool
	// stopping is true while Stopped is the step.
	stopping bool
	// stopped is true once the cell has stopped.
	stopped bool
}

// newModel returns the model of a cell before Started.
func newModel(c *prop.Case) *model {
	m := &model{
		c:         c,
		h:         newHarness(c),
		want:      make(map[machine.ReplyToken]error),
		replies:   make(map[machine.ReplyToken]int),
		cancelled: make(map[machine.ReplyToken]bool),
		late:      make(map[machine.ReplyToken]bool),
		retries:   make(map[machine.Token]bool),
		version:   firstVersion,
	}
	m.h.observe = m.observe
	return m
}

// observe checks the effect e that the cell appends, and takes what e changes in the expected
// state.
func (m *model) observe(e machine.Effect) {
	c := m.c
	c.Helper()
	assert.False(c, m.stopped, "a stopped cell must not append an effect")
	switch e.(type) {
	case machine.Reply[struct{}], machine.Reply[protocol.Ack], machine.Reply[protocol.Receipt],
		machine.Reply[budget.Decision]:
	default:
		expect.False(c, m.stopping, "the cell must append only replies at Stopped")
	}
	var to machine.ReplyToken
	var err error
	switch e := e.(type) {
	case machine.Commit:
		expect.Equal(c, e.Scope, cellScope, "the cell must commit only to its own scope")
		expect.Equal(c, e.Term, cellTerm, "a commit must have the term that the root granted")
		return
	case machine.WatchViews:
		m.serving = true
		return
	case machine.Timer:
		if m.outage {
			c.Classify(labelOutage)
			m.retries[e.Token] = true
		}
		return
	case machine.Reply[struct{}]:
		to, err = e.To, e.Err
	case machine.Reply[protocol.Ack]:
		to, err = e.To, e.Err
	case machine.Reply[protocol.Receipt]:
		to, err = e.To, e.Err
	case machine.Reply[budget.Decision]:
		to, err = e.To, e.Err
	default:
		return
	}
	m.replies[to]++
	expect.Equal(c, m.replies[to], 1, "the cell must reply to a call once")
	expect.False(c, m.cancelled[to], "the cell must not reply to a call that the runtime cancelled")
	want, known := m.want[to]
	switch {
	case to == m.started && err == nil:
		expect.True(c, m.serving, "the cell must reply to Started without an error only once it serves")
	case to == m.started && m.stopping:
		expect.That(c, err).ErrorIs(control.ErrNotServing, "Stopped must reply to Started with ErrNotServing")
	case to == m.started:
		expect.Equal(c, err, m.h.machine.Err(), "the cell must reply to Started with the error that stopped it")
	case known:
		expect.That(c, err).ErrorIs(want, "the reply must have the error that the call expects")
	}
}

// call returns the token of the next inbound call. The cell replies with ErrNotServing to a
// call before it serves. The model expects a reply only to a call that the cell takes before
// it stops.
func (m *model) call(c *prop.Case) machine.ReplyToken {
	to := m.h.call()
	m.sent = append(m.sent, to)
	m.late[to] = m.stopped
	if !m.stopped && !m.serving {
		c.Classify(labelEarly)
		m.want[to] = control.ErrNotServing
	}
	return to
}

// step passes ev to the cell, and takes an error that stops the cell.
func (m *model) step(c *prop.Case, ev machine.Event) {
	m.h.step(c, ev)
	m.sync()
}

// run performs the effects that wait, and passes their results to the cell.
func (m *model) run(c *prop.Case) {
	m.h.run(c)
	m.sync()
}

// sync marks the cell as stopped once an error has stopped it.
func (m *model) sync() {
	if m.h.machine.Err() != nil {
		m.stopped = true
	}
}

// start passes the Started of the run.
func (m *model) start(c *prop.Case) {
	m.started = m.h.call()
	m.sent = append(m.sent, m.started)
	m.late[m.started] = false
	m.step(c, machine.Started{Reply: m.started})
}

// follow passes the generations of the root scope that the cell has not passed, and runs the
// cell. During an outage the watch of the root scope ends with the error of the outage, and
// the cell remains behind.
func (m *model) follow(c *prop.Case) {
	m.h.follow(c)
	m.behind = m.behind && m.outage
	m.sync()
}

// respec commits the next version of the spec to the root scope, as the root applies a
// proposal of an operator.
func (m *model) respec(c *prop.Case) {
	m.version++
	m.h.commitRoot(c, specMutation(c, m.version, nil))
	m.behind = true
}

// view passes a view of the nodes that c chooses, and runs the cell.
func (m *model) view(c *prop.Case) {
	m.h.view(c, c.Draw(prop.SampledFrom(nodeSets...), "nodes")...)
	m.sync()
}

// report passes the next report of the agent of nodeA.
func (m *model) report(c *prop.Case) {
	to := m.call(c)
	m.sequence++
	m.step(c, machine.Report{
		Report: &protocol.Report{
			Scope:           cellScope,
			Node:            nodeA.ID,
			NodeIncarnation: nodeA.Incarnation,
			Sequence:        m.sequence,
			Incarnation:     incA,
		},
		Metadata: protocol.Metadata{Caller: nodeA.Identity, Version: protocol.Current},
		Reply:    to,
	})
}

// disrupt passes a negotiable disruption of nodeA with an ID of its own, which waits for its
// commit once the cell serves.
func (m *model) disrupt(c *prop.Case) {
	to := m.call(c)
	m.disruptions++
	d := disruption(nodeA.ID)
	d.ID = budget.ID{ID: id.New128([16]byte{0: 0xd1, 15: m.disruptions})}
	m.step(c, machine.Disrupt{
		Disruption: &d,
		Metadata:   protocol.Metadata{Caller: operator, Version: protocol.Current},
		Reply:      to,
	})
}

// fence commits to the scope of the cell as a writer of the term after the term of the cell,
// so the next commit of the cell fails with epoch.ErrFenced.
func (m *model) fence(c *prop.Case) {
	ctx := assert.Context(c)
	head, err := m.h.st.Head(ctx, cellScope)
	assert.NoError(c, err, "the store must return the header of the scope of the cell")
	later := cellTerm + 1
	mu := &store.Mutation{ID: store.CommitID{Incarnation: incA, Term: later, Seq: 1}}
	_, err = m.h.st.Writer(cellScope, incA, later).Commit(ctx, head.Generation, mu)
	assert.NoError(c, err, "a writer of a later term must commit")
	m.fenced = true
}

// cancel ends the wait of the caller of a call without a reply. c chooses the call.
func (m *model) cancel(c *prop.Case) {
	calls := m.waiting()
	to := calls[c.Draw(prop.Integer(0, len(calls)-1), "call")]
	m.cancelled[to] = true
	m.step(c, machine.Cancelled{Reply: to})
}

// waiting returns the tokens of the calls whose callers wait for their replies, in their
// order.
func (m *model) waiting() []machine.ReplyToken {
	var calls []machine.ReplyToken
	for _, to := range m.sent {
		if m.replies[to] == 0 && !m.cancelled[to] && !m.late[to] {
			calls = append(calls, to)
		}
	}
	return calls
}

// elapse ends the Timer at the index i of the Timers that the cell has armed, and runs the
// cell.
func (m *model) elapse(c *prop.Case, i int) {
	tm := m.h.timers[i]
	m.h.timers = slices.Delete(m.h.timers, i, i+1)
	delete(m.retries, tm.Token)
	m.step(c, machine.Elapsed{Token: tm.Token})
	m.run(c)
}

// halt passes the Stopped of the run.
func (m *model) halt(c *prop.Case) {
	m.stopping = true
	m.step(c, machine.Stopped{})
	m.stopping, m.stopped = false, true
}

// toggle starts an outage of the store or ends the outage that runs.
func (m *model) toggle(*prop.Case) {
	if m.outage {
		m.h.st.EndOutage()
	} else {
		m.h.st.StartOutage()
	}
	m.outage = !m.outage
}

// drain returns the next step towards a cell that serves, and nil for a cell that serves or
// has stopped. The step is one of these:
//   - the end of an outage.
//   - the Started of the run.
//   - a run of the effects that wait.
//   - the end of a Timer that retries contains.
//   - the generations of the root scope that the cell has not passed.
func (m *model) drain() func(c *prop.Case) {
	switch {
	case m.stopped:
		return nil
	case m.outage:
		return m.toggle
	case m.started == 0:
		return m.start
	case len(m.h.pending) > 0:
		return m.run
	}
	if i := slices.IndexFunc(m.h.timers, func(tm machine.Timer) bool { return m.retries[tm.Token] }); i >= 0 {
		return func(c *prop.Case) { m.elapse(c, i) }
	}
	if m.behind {
		return m.follow
	}
	return nil
}

// invariant checks what is true after every step. Only a later term stops the cell with an
// error, and a cell that an error has stopped has ended its watches.
func (m *model) invariant(c *prop.Case) {
	err := m.h.machine.Err()
	if err == nil {
		return
	}
	expect.True(c, m.fenced, "only a later term may stop the cell with an error")
	expect.That(c, err).ErrorIs(epoch.ErrFenced, "the cell must stop with the error of the fence")
	expect.Empty(c, m.h.watches, "the cell must end its watch of the root scope when an error stops it")
	expect.Equal(c, m.h.views, machine.Token(0), "the cell must end the watch of its views when an error stops it")
}

// settle checks the cell once the drain has ended. A cell that has not stopped serves.
// settle then stops the cell, and checks that the cell has replied once to each call that it
// took before it stopped, unless the runtime cancelled the call.
func (m *model) settle(c *prop.Case) {
	if !m.stopped {
		assert.True(c, m.serving, "the cell must serve once it has read the root scope")
		m.halt(c)
	}
	for _, to := range m.sent {
		if !m.late[to] && !m.cancelled[to] {
			expect.Equal(c, m.replies[to], 1, "the cell must reply once to each call that it took before it stopped")
		}
	}
}

// TestMachine checks the construction, the lifecycle and the replies of a cell machine.
func TestMachine(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give func(*cell.Config)
			name string
		}{
			{name: "returns ErrConfig for a config without a cell", give: func(c *cell.Config) { c.Scope.Cell = "" }},
			{
				name: "returns ErrConfig for a config without a namespace incarnation",
				give: func(c *cell.Config) { c.Incarnation = shard.NamespaceIncarnation{} },
			},
			{
				name: "returns ErrConfig for a cell that is not the text of a shard ID",
				give: func(c *cell.Config) { c.Scope.Cell = otherCell },
			},
			{
				name: "returns ErrConfig for a config without a source of randomness",
				give: func(c *cell.Config) { c.Rand = nil },
			},
			{
				name: "returns ErrConfig for a config without a namespace",
				give: func(c *cell.Config) { c.Scope.Namespace = "" },
			},
			{name: "returns ErrConfig for a config without a clock", give: func(c *cell.Config) { c.Clock = nil }},
			{name: "returns ErrConfig for a config without a term", give: func(c *cell.Config) { c.Term = epoch.Zero }},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				cfg := completeConfig(t)
				tt.give(&cfg)
				_, err := cell.New(&cfg)
				assert.ErrorIs(t, err, cell.ErrConfig, "New must refuse a config without a required field")
			})
		}

		t.Run("returns ErrConfig for a nil config", func(t *testing.T) {
			t.Parallel()
			_, err := cell.New(nil)
			assert.ErrorIs(t, err, cell.ErrConfig, "New must refuse a nil config")
		})
	})

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("replies to Started once the cell serves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			started := h.call()
			h.step(t, machine.Started{Reply: started})
			assert.Empty(t, h.replies, "the cell must not reply before it serves")
			h.run(t)
			assert.Equal(t, h.replies, []machine.Effect{machine.Reply[struct{}]{To: started}},
				"the cell must reply to Started once its controller has acquired its scope")
		})

		t.Run("commits the record of the cell when it acquires its scope", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			got := h.state(t).Cell
			expect.Equal(t, got.ID, cellID, "the record must name the cell")
			expect.Equal(t, got.Controller, controllerAddress, "the record must contain the address of the controller")
			expect.Equal(t, got.ControllerIdentity, controllerIdentity,
				"the record must contain the identity of the controller")
			expect.Equal(t, got.Epoch, cellTerm, "the record must contain the term of the controller")
			expect.Equal(t, got.SpecVersion, firstVersion, "the record must contain the version of the spec")
		})

		t.Run("acquires its scope in the namespace incarnation of its config", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.Equal(t, h.state(t).Head.Incarnation, incA,
				"the acquisition must create the scope of the cell in the incarnation of the cell")
		})

		t.Run("watches the views of the cell once it serves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.NotEqual(t, h.views, machine.Token(0), "the cell must watch the views of its membership")
		})

		t.Run("places the shards of the cell at the first view", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA, nodeB)
			assert.Length(t, h.state(t).Transitions, 0, "the cell must finish the moves of the first pass")
			assert.Length(t, h.plans, 1, "the first view must start a pass")
		})

		t.Run("stops with ErrRoot for a root scope that does not place the cell", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.root.Put = nil })
			h.start(t)
			assert.ErrorIs(t, h.machine.Err(), cell.ErrRoot, "the cell must stop without a placement in the root")
			assert.Length(t, h.replies, 1, "the cell must reply to Started")
			expect.ErrorIs(t, h.replies[0].(machine.Reply[struct{}]).Err, cell.ErrRoot,
				"the reply to Started must contain the error that stopped the cell")
			expect.NotContains(t, h.unwatched, machine.Token(0),
				"the cell must not end a watch that it has not started")
		})

		t.Run("stops with ErrRoot for a read of the root scope that fails", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			assert.NoError(t, h.st.Close(), "the store must close")
			h.start(t)
			assert.ErrorIs(t, h.machine.Err(), cell.ErrRoot, "the cell must stop when it cannot read the root scope")
		})

		t.Run("takes the range of its cell from the last page of the root scope", func(t *testing.T) {
			t.Parallel()
			first, second := binary.BigEndian.AppendUint32(nil, 1), binary.BigEndian.AppendUint32(nil, 2)
			others := otherCells(pageLimit)
			others[0].Range = shard.Range{End: first}
			others[1].Range = shard.Range{Start: first, End: second}
			own := shard.Placement{Shard: shard.ID{ID128: cellKey}, Range: shard.Range{Start: second}}
			h := newHarness(t, func(s *setup) { s.root.Put = slices.Concat(others, []shard.Placement{own}) })
			h.start(t)
			assert.Nil(t, h.machine.Err(), "the cell must serve after the last page places it")
			assert.Equal(t, h.state(t).Cell.Range, shard.Range{Start: second},
				"the record must contain the range of the cell")
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
				"the first Started must get the error that stopped the cell")
			expect.ErrorIs(t, replyTo[struct{}](t, h, second).Err, fsm.ErrRejected,
				"the second Started must get the error that stopped the cell")
		})

		t.Run("replies with ErrNotServing to Started after Stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			started := h.call()
			h.step(t, machine.Started{Reply: started})
			h.step(t, machine.Stopped{})
			assert.Equal(t, h.replies,
				[]machine.Effect{machine.Reply[struct{}]{Err: control.ErrNotServing, To: started}},
				"Stopped must reply to Started with ErrNotServing")
			assert.Nil(t, h.machine.Err(), "a cell that Stopped ends must not report an error")
		})

		t.Run("replies with ErrNotServing to a call that waits at Stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA, nodeB)
			d := disruption(nodeA.ID)
			call := machine.Disrupt{
				Disruption: &d,
				Metadata:   protocol.Metadata{Caller: operator, Version: protocol.Current},
				Reply:      h.call(),
			}
			h.step(t, call)
			h.step(t, machine.Stopped{})
			assert.ErrorIs(t, replyTo[budget.Decision](t, h, call.Reply).Err, control.ErrNotServing,
				"a disruption that waits for its commit must get ErrNotServing at Stopped")
		})

		t.Run("replies to Started once", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Stopped{})
			assert.Equal(t, h.replies, []machine.Effect{machine.Reply[struct{}]{To: 1}},
				"Stopped must not reply again to a Started that has a reply")
		})

		t.Run("returns out unchanged after it has stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Stopped{})
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Started{Reply: h.call()}, out)
			assert.Equal(t, got, out, "a stopped cell must not take an event")
		})

		t.Run("drops the reply to a Started that the runtime cancelled", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			started := h.call()
			h.step(t, machine.Started{Reply: started})
			h.step(t, machine.Cancelled{Reply: started})
			h.run(t)
			assert.Empty(t, h.replies, "the cell must not reply to a call that the runtime cancelled")
		})

		t.Run("passes the Cancelled of another call to its controller", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA, nodeB)
			d := disruption(nodeA.ID)
			call := machine.Disrupt{
				Disruption: &d,
				Metadata:   protocol.Metadata{Caller: operator, Version: protocol.Current},
				Reply:      h.call(),
			}
			h.step(t, call)
			before := len(h.replies)
			h.step(t, machine.Cancelled{Reply: call.Reply})
			h.run(t)
			assert.Length(t, h.replies, before, "the controller must not reply to a call that the runtime cancelled")
		})

		t.Run("replies with ErrNotServing to a call before its controller serves", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			p := proposal.Proposal{Scope: cellScope, Incarnation: incA, Kind: proposal.Drain}
			call := machine.Submit{Proposal: &p, Metadata: protocol.Metadata{Caller: operator}, Reply: h.call()}
			h.step(t, call)
			assert.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrNotServing,
				"the cell must refuse a proposal before its controller serves")
		})

		t.Run("returns out unchanged for a Timer that no part armed", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Elapsed{Token: 99}, out)
			assert.Equal(t, got, out, "the cell must ignore a Timer that no part armed")
		})

		t.Run("stops at an error of its controller", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			ctx := assert.Context(t)
			head, err := h.st.Head(ctx, cellScope)
			assert.NoError(t, err, "the store must return the header of the scope of the cell")
			later := cellTerm + 1
			fence := &store.Mutation{ID: store.CommitID{Incarnation: incA, Term: later, Seq: 1}}
			_, err = h.st.Writer(cellScope, incA, later).Commit(ctx, head.Generation, fence)
			assert.NoError(t, err, "a controller of a later term must commit")
			h.start(t)
			assert.ErrorIs(t, h.machine.Err(), epoch.ErrFenced, "the cell must stop when a later term fences its scope")
			assert.Length(t, h.replies, 1, "the cell must reply to Started")
			expect.ErrorIs(t, h.replies[0].(machine.Reply[struct{}]).Err, epoch.ErrFenced,
				"the reply to Started must contain the error that stopped the cell")
		})

		t.Run("ends its watches when an error stops it", func(t *testing.T) {
			t.Parallel()
			full := false
			h := newHarness(t, func(s *setup) {
				s.fits = func(*store.Mutation) error {
					if full {
						return store.ErrTooLarge
					}
					return nil
				}
			})
			h.start(t)
			h.view(t, nodeA, nodeB)
			full = true
			d := disruption(nodeA.ID)
			h.step(t, machine.Disrupt{
				Disruption: &d,
				Metadata:   protocol.Metadata{Caller: operator, Version: protocol.Current},
				Reply:      h.call(),
			})
			assert.ErrorIs(t, h.machine.Err(), store.ErrTooLarge, "the cell must stop when a commit does not fit")
			expect.Equal(t, h.views, machine.Token(0), "the cell must end the watch of its views")
			expect.Empty(t, h.watches, "the cell must end the watch of the root scope")
		})

		t.Run("drops the effects of an event that stops it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.root.Spec = []byte(malformed) })
			h.start(t)
			assert.ErrorIs(t, h.machine.Err(), control.ErrSpec,
				"the cell must stop at a first spec that does not apply")
			assert.Empty(t, h.opened, "the cell must drop the watch of the root scope that the last page started")
		})

		t.Run("returns out unchanged after an error has stopped it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.root.Put = nil })
			h.start(t)
			assert.NotNil(t, h.machine.Err(), "the cell must stop without a placement in the root")
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Started{Reply: h.call()}, out)
			assert.Equal(t, got, out, "a cell that an error stopped must not take an event")
		})

		t.Run("refuses a proposal of a caller that is not an operator", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			p := proposal.Proposal{
				Scope: cellScope, Incarnation: incA, Kind: proposal.Drain, Expires: origin.Add(time.Hour),
			}
			call := machine.Submit{
				Proposal: &p,
				Metadata: protocol.Metadata{Caller: controllerIdentity, Version: protocol.Current},
				Reply:    h.call(),
			}
			h.step(t, call)
			assert.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrDenied,
				"the controller of the cell must authenticate the callers of its config")
		})

		t.Run("keeps its contract under every sequence of events", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "a cell keeps its contract under every sequence of events", exercise,
				prop.Require(labelEarly, often),
				prop.Require(labelSpec, sometimes),
				prop.Require(labelFenced, sometimes),
				prop.Require(labelStopped, often),
				prop.Require(labelOutage, rare),
			)
		})
	})

	t.Run("Err", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil while the cell runs", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.Nil(t, h.machine.Err(), "a cell that runs must not report an error")
		})
	})
}

// completeConfig returns a Config that New accepts.
func completeConfig(tb assert.TB) cell.Config {
	tb.Helper()
	return cell.Config{
		Clock:       fake.New(origin),
		Rand:        constant.NewCounter(half),
		Reporter:    noop.New(),
		Registry:    registry(tb),
		Logger:      slog.New(slog.DiscardHandler),
		Fits:        func(*store.Mutation) error { return nil },
		Scope:       cellScope,
		Node:        shard.Node{Address: controllerAddress},
		Term:        cellTerm,
		Parallelism: 1,
		Incarnation: incA,
	}
}

// disruption returns a negotiable disruption of the nodes nodes. It restarts the processes
// of the nodes from an hour after the start of the clock for an hour.
func disruption(nodes ...shard.NodeID) budget.Disruption {
	return budget.Disruption{
		Start:       origin.Add(time.Hour),
		End:         origin.Add(2 * time.Hour),
		Scope:       cellScope,
		Nodes:       nodes,
		ID:          budget.ID{ID: id.New128([16]byte{0: 0xd1, 15: 1})},
		Incarnation: incA,
		Impact:      budget.ProcessRestart,
		Negotiable:  true,
	}
}

// replyTo returns the reply of the machine to the call to, which must have the type
// machine.Reply[T].
func replyTo[T machine.Result](tb assert.TB, h *harness, to machine.ReplyToken) machine.Reply[T] {
	tb.Helper()
	for _, e := range h.replies {
		if r, ok := e.(machine.Reply[T]); ok && r.To == to {
			return r
		}
	}
	tb.Fatalf("the machine has not replied to the call %d", to)
	return machine.Reply[T]{}
}

// exercise runs the steps of the case c over a cell machine, as the runtime, the root, the
// membership, the agents and the operators drive it. The steps of a case:
//   - pass Started.
//   - run the effects that wait.
//   - end Timers.
//   - commit newer specs to the root scope and pass its generations.
//   - pass views, reports and disruptions.
//   - cancel calls.
//   - fence the scope of the cell with a later term.
//   - start and end outages of the store.
//   - stop the cell.
//
// During an outage the root and the later term do not commit. The model checks the contract
// of the cell after every step:
//   - Only a later term stops the cell with an error, and an outage does not stop it. A stop
//     by an error ends the watches of the cell.
//   - A stopped cell does not append an effect, and Stopped appends only replies.
//   - The cell replies to each call once, and never to a call that the runtime cancelled.
//   - It replies to a call before it serves with control.ErrNotServing.
//   - It replies to Started without an error once it serves, with the error that stopped it,
//     or with control.ErrNotServing at Stopped.
//   - It commits only to its own scope, with the term that the root granted.
//
// The drain takes the steps that drain returns until the cell serves. Once Stopped, the cell
// has replied once to each call that it took before it stopped, unless the runtime cancelled
// the call.
func exercise(c *prop.Case) {
	m := newModel(c)
	running := func(struct{}) bool { return !m.stopped }
	stateful.Steps(c, stateful.Machine[struct{}]{
		Actions: []stateful.Action[struct{}]{
			{
				Name: "start", Enabled: func(struct{}) bool { return !m.stopped && m.started == 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.start(c) },
			},
			{
				Name: "run", Enabled: func(struct{}) bool { return !m.stopped && len(m.h.pending) > 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.run(c) },
			},
			{
				Name: "commit a spec", Enabled: func(struct{}) bool { return !m.stopped && !m.outage },
				Run: func(c *prop.Case, _ int, _ any) {
					c.Classify(labelSpec)
					m.respec(c)
				},
			},
			{
				Name: "follow", Enabled: func(struct{}) bool { return !m.stopped && len(m.h.watches) > 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.follow(c) },
			},
			{
				Name: "view", Enabled: func(struct{}) bool { return !m.stopped && m.h.views != 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.view(c) },
			},
			{Name: "report", Run: func(c *prop.Case, _ int, _ any) { m.report(c) }},
			{Name: "disrupt", Run: func(c *prop.Case, _ int, _ any) { m.disrupt(c) }},
			{
				Name: "cancel", Enabled: func(struct{}) bool { return !m.stopped && len(m.waiting()) > 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.cancel(c) },
			},
			{
				Name: "elapse", Enabled: func(struct{}) bool { return !m.stopped && len(m.h.timers) > 0 },
				Run: func(c *prop.Case, _ int, _ any) {
					m.elapse(c, c.Draw(prop.Integer(0, len(m.h.timers)-1), "timer"))
				},
			},
			{
				Name: "fence", Enabled: func(struct{}) bool { return !m.stopped && !m.fenced && !m.outage },
				Run: func(c *prop.Case, _ int, _ any) {
					c.Classify(labelFenced)
					m.fence(c)
				},
			},
			{Name: "toggle an outage", Enabled: running, Run: func(c *prop.Case, _ int, _ any) { m.toggle(c) }},
			{
				Name: "stop", Enabled: running,
				Run: func(c *prop.Case, _ int, _ any) {
					c.Classify(labelStopped)
					m.halt(c)
				},
			},
			{
				Name: "complete", Drain: true, Enabled: func(struct{}) bool { return m.drain() != nil },
				Run: func(c *prop.Case, _ int, _ any) { m.drain()(c) },
			},
		},
		Invariant: func(c *prop.Case, _ struct{}) { m.invariant(c) },
		Settle:    func(c *prop.Case, _ struct{}) { m.settle(c) },
	})
}

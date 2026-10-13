// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package root_test

import (
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
	"go.thesmos.sh/core/coretest/telemetrytest"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/fsm"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/root"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The log message of an error of a term.
const logTerm = "root: the term failed"

// attrNamespaceKey is the key of the attribute that contains the namespace in the logs of
// the root.
const attrNamespaceKey = "namespace"

// firstCampaignBackoff is the backoff before the first retry of a campaign with the source
// of randomness of the tests: half of root.CampaignRetryBase.
const firstCampaignBackoff = root.CampaignRetryBase / 2

// grants is the weight of the grant of a campaign in the property of a root, which a case takes
// more often than the other actions.
const grants = 3

// The labels of the cases of the property of a root, which its coverage requirements count.
const (
	// labelGranted counts a case in which a step grants a term to the root.
	labelGranted = "a step grants a term"
	// labelEnded counts a case in which a step ends a term of the root in the store.
	labelEnded = "a step ends a term"
	// labelFault counts a case in which an error of the controller of a term ends the term.
	labelFault = "an error of a controller ends its term"
	// labelStopped counts a case in which a step stops the root.
	labelStopped = "a step stops the root"
	// labelOutage counts a case in which the root arms a Timer during an outage of the store.
	labelOutage = "the root arms a Timer during an outage"
)

// The shares of the cases of the property of a root that its labels must count, about half of
// the shares that 1,000 cases measured.
const (
	// often is the share of labelGranted and labelStopped. The measured shares were 30% and
	// 37%.
	often = 0.15
	// sometimes is the share of labelEnded. The measured share was 16%.
	sometimes = 0.08
	// rare is the share of labelFault, which needs a fault and a commit after it, and of
	// labelOutage. The measured shares were 8.7% and 8.8%.
	rare = 0.04
)

// The errors of the campaigns of the tests.
var (
	// errOutage is a transient error of a campaign.
	errOutage = errs.WithClass(errors.New("test: the store is unreachable"), errs.Transient)
	// errBroken is an error of a campaign that a retry does not fix.
	errBroken = errs.WithClass(errors.New("test: the store is broken"), errs.Invalid)
)

// processes lists the sets of controller processes that the views of the property of a root
// contain.
var processes = [][]shard.Node{{procA}, {procB}, {procA, procB}, {}}

// cellCalls build the calls of a cell controller that the root refuses, each for the token of
// its reply.
var cellCalls = []func(to machine.ReplyToken) machine.Event{
	func(to machine.ReplyToken) machine.Event {
		return machine.Report{Report: &protocol.Report{Scope: rootScope}, Reply: to}
	},
	func(to machine.ReplyToken) machine.Event {
		return machine.Disrupt{Disruption: &budget.Disruption{Scope: rootScope}, Reply: to}
	},
	func(to machine.ReplyToken) machine.Event {
		return machine.Done{Args: &protocol.DoneArgs{Scope: rootScope}, Reply: to}
	},
}

// model is the expected state of a root that the property of a root drives, and the record of
// what the root did. Its harness performs a Campaign only when a step grants it, and passes
// each effect of the root to observe.
type model struct {
	// c is the case of the property.
	c *prop.Case
	// h is the harness of the root.
	h *harness
	// leader is the term that the store has granted to the root and not ended, and nil
	// without one.
	leader store.Leadership
	// want maps the token of each call whose reply the model knows to the error of the reply.
	want map[machine.ReplyToken]error
	// replies counts the replies of the root to each call.
	replies map[machine.ReplyToken]int
	// replied maps the token of each call to the error of its last reply.
	replied map[machine.ReplyToken]error
	// cancelled contains the token of each call that the runtime cancelled.
	cancelled map[machine.ReplyToken]bool
	// late contains the token of each call that the root took after it had stopped.
	late map[machine.ReplyToken]bool
	// watches maps the token of each WatchViews that has not ended to the term of its
	// controller.
	watches map[machine.Token]epoch.Epoch
	// backoffs contains the token of each Timer that the drain ends. Such a Timer is the
	// backoff before a campaign, or a Timer that the root armed during an outage.
	backoffs map[machine.Token]bool
	// sent lists the token of each call, in order.
	sent []machine.ReplyToken
	// granted is the token of the last campaign that the store granted.
	granted machine.Token
	// term is the term of the controller that runs, and zero outside a term.
	term epoch.Epoch
	// fenced is the term of the last commit of another writer, and zero without one.
	fenced epoch.Epoch
	// proposals counts the proposals, and gives each proposal its ID.
	proposals byte
	// full is true while the bound of a commit refuses every mutation.
	full bool
	// outage is true while the store refuses every call.
	outage bool
	// broken is true once a campaign has failed with an error that a retry does not fix.
	broken bool
	// stopping is true while Stopped is the step.
	stopping bool
	// stopped is true once the root has stopped.
	stopped bool
}

// newModel returns the model of a root that has taken Started and campaigns.
func newModel(c *prop.Case) *model {
	m := &model{
		c:         c,
		want:      make(map[machine.ReplyToken]error),
		replies:   make(map[machine.ReplyToken]int),
		replied:   make(map[machine.ReplyToken]error),
		cancelled: make(map[machine.ReplyToken]bool),
		late:      make(map[machine.ReplyToken]bool),
		watches:   make(map[machine.Token]epoch.Epoch),
		backoffs:  make(map[machine.Token]bool),
	}
	m.h = newHarness(c, func(s *setup) {
		s.fits = func(mu *store.Mutation) error {
			if m.full {
				return store.ErrTooLarge
			}
			return m.h.st.Fits(rootScope, mu)
		}
	})
	m.h.paused = true
	m.h.observe = m.observe
	started := m.call()
	m.want[started] = nil
	m.step(c, machine.Started{Reply: started})
	return m
}

// observe checks the effect e that the root appends, and takes what e changes in the expected
// state.
func (m *model) observe(e machine.Effect) {
	c := m.c
	c.Helper()
	assert.False(c, m.stopped, "a stopped root must not append an effect")
	switch e.(type) {
	case machine.Reply[struct{}], machine.Reply[protocol.Ack], machine.Reply[protocol.Receipt],
		machine.Reply[budget.Decision]:
	default:
		expect.False(c, m.stopping, "the root must append only replies at Stopped")
	}
	var to machine.ReplyToken
	var err error
	switch e := e.(type) {
	case machine.Campaign:
		expect.Equal(c, m.term, epoch.Zero, "the root must campaign only outside a term")
		return
	case machine.Commit:
		expect.NotEqual(c, m.term, epoch.Zero, "the root must commit only within a term")
		expect.Equal(c, e.Term, m.term, "a commit must have the term of the controller that runs")
		return
	case machine.WatchViews:
		expect.NotEqual(c, m.term, epoch.Zero, "the root must watch the views only within a term")
		m.watches[e.Token] = m.term
		return
	case machine.Unwatch:
		delete(m.watches, e.Token)
		return
	case machine.Timer:
		if m.outage {
			c.Classify(labelOutage)
			m.backoffs[e.Token] = true
		}
		return
	case machine.Resign:
		expect.Equal(c, e.Token, m.granted, "a Resign must have the token of the campaign of the term")
		expect.True(c, m.full || m.fenced > m.term,
			"the root must resign a term only after a commit that does not fit or a fence")
		c.Classify(labelFault)
		m.term = epoch.Zero
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
	m.replied[to] = err
	expect.Equal(c, m.replies[to], 1, "the root must reply to a call once")
	expect.False(c, m.cancelled[to], "the root must not reply to a call that the runtime cancelled")
	want, known := m.want[to]
	switch {
	case !known:
	case want == nil:
		expect.Nil(c, err, "the root must reply to Started without an error")
	default:
		expect.That(c, err).ErrorIs(want, "the reply must have the error that the call expects")
	}
}

// call returns the token of the next inbound call. The model expects a reply only to a call
// that the root takes before it stops.
func (m *model) call() machine.ReplyToken {
	to := m.h.call()
	m.sent = append(m.sent, to)
	m.late[to] = m.stopped
	return to
}

// step passes ev to the root, and takes an error that stops the root.
func (m *model) step(c *prop.Case, ev machine.Event) {
	m.h.step(c, ev)
	m.sync()
}

// run performs the effects that wait, except a Campaign, and passes their results to the
// root.
func (m *model) run(c *prop.Case) {
	m.h.run(c)
	m.sync()
}

// sync marks the root as stopped once an error has stopped it.
func (m *model) sync() {
	if m.h.machine.Err() != nil {
		m.stopped = true
	}
}

// campaign removes the Campaign that waits for its result from the effects that wait, and
// returns it.
func (m *model) campaign(c *prop.Case) machine.Campaign {
	i := slices.IndexFunc(m.h.pending, campaigning)
	assert.True(c, i >= 0, "a Campaign of the root must wait for its result")
	camp := m.h.pending[i].(machine.Campaign)
	m.h.pending = slices.Delete(m.h.pending, i, i+1)
	return camp
}

// grant has the store grant the term of the Campaign that waits, and runs the root until the
// controller of the term serves.
func (m *model) grant(c *prop.Case) {
	camp := m.campaign(c)
	assert.Nil(c, m.leader, "the store must end each term of the root before the root campaigns")
	l, err := m.h.st.Campaign(assert.Context(c), rootScope)
	assert.NoError(c, err, "the store must grant a term to the only candidate")
	m.leader, m.granted, m.term = l, camp.Token, l.Term()
	m.step(c, machine.Campaigned{Token: camp.Token, Term: l.Term()})
	m.run(c)
}

// lose ends the Campaign that waits with err, and passes the result to the root. A transient
// err starts a backoff, whose Timer the model keeps.
func (m *model) lose(c *prop.Case, err error) {
	camp := m.campaign(c)
	before := len(m.h.timers)
	m.step(c, machine.Campaigned{Err: err, Token: camp.Token})
	for _, tm := range m.h.timers[before:] {
		m.backoffs[tm.Token] = true
	}
}

// end has the store end the term that it granted to the root, and passes the end of the term
// to the root, which campaigns again.
func (m *model) end(c *prop.Case) {
	assert.NoError(c, m.leader.Resign(assert.Context(c)), "the store must end the term")
	m.leader, m.term = nil, epoch.Zero
	m.step(c, machine.TermEnded{Token: m.granted})
	m.run(c)
}

// elapse ends the Timer at the index i of the Timers that the root has armed, and runs the
// root.
func (m *model) elapse(c *prop.Case, i int) {
	tm := m.h.timers[i]
	m.h.timers = slices.Delete(m.h.timers, i, i+1)
	delete(m.backoffs, tm.Token)
	m.step(c, machine.Elapsed{Token: tm.Token})
	m.run(c)
}

// view passes a view of the controller processes that c chooses, and runs the root.
func (m *model) view(c *prop.Case) {
	m.h.view(c, c.Draw(prop.SampledFrom(processes...), "processes")...)
	m.sync()
}

// submit passes a proposal of the next version of the spec of the root scope. The root
// replies with control.ErrNotServing outside a term.
func (m *model) submit(c *prop.Case) {
	to := m.call()
	if !m.stopped && m.term == epoch.Zero {
		m.want[to] = control.ErrNotServing
	}
	base := m.version(c)
	m.proposals++
	m.step(c, machine.Submit{
		Proposal: m.h.specProposal(c, m.proposals, base, base+1),
		Metadata: protocol.Metadata{Caller: operator, Version: protocol.Current},
		Reply:    to,
	})
}

// version returns the version of the spec of the root scope, and zero before the namespace
// exists.
func (m *model) version(c *prop.Case) epoch.Epoch {
	st := m.h.state(c)
	if len(st.Spec) == 0 {
		return epoch.Zero
	}
	var s config.Spec
	assert.NoError(c, json.Unmarshal(st.Spec, &s), "the spec of the root scope must decode")
	return s.Version
}

// misdirect passes the call of a cell controller that c chooses. The root refuses the call
// with control.ErrCall.
func (m *model) misdirect(c *prop.Case) {
	to := m.call()
	if !m.stopped {
		m.want[to] = control.ErrCall
	}
	m.step(c, c.Draw(prop.SampledFrom(cellCalls...), "call")(to))
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

// fence commits to the root scope as a writer of a term above every term of the root, so the
// next commit of the controller that runs fails.
func (m *model) fence(c *prop.Case) {
	ctx := assert.Context(c)
	head, err := m.h.st.Head(ctx, rootScope)
	assert.NoError(c, err, "the store must return the header of the root scope")
	m.fenced = max(head.Watermark, m.term) + 1
	mu := &store.Mutation{ID: store.CommitID{Incarnation: head.Incarnation, Term: m.fenced, Seq: 1}}
	_, err = m.h.st.Writer(rootScope, head.Incarnation, m.fenced).Commit(ctx, head.Generation, mu)
	assert.NoError(c, err, "a writer of a later term must commit")
}

// stale passes the result or the end of an older campaign that c chooses, and checks that the
// root ignores it.
func (m *model) stale(c *prop.Case) {
	old := m.h.campaigns[c.Draw(prop.Integer(0, len(m.h.campaigns)-2), "campaign")]
	ev := machine.Event(machine.TermEnded{Token: old})
	if c.Draw(prop.Boolean(), "granted") {
		ev = machine.Campaigned{Token: old, Term: max(m.term, m.fenced) + 1}
	}
	out := []machine.Effect{marker}
	assert.Equal(c, m.h.machine.Step(ev, out), out, "the root must ignore the result of an older campaign")
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

// drain returns the next step towards a root that serves, and nil for a root that serves or
// has stopped. The step is one of these:
//   - the end of an outage.
//   - the grant of a Campaign that waits.
//   - the end of a term that the root has resigned or that a later term has fenced.
//   - the end of a Timer that backoffs contains.
func (m *model) drain() func(c *prop.Case) {
	if m.stopped {
		return nil
	}
	if m.outage {
		return m.toggle
	}
	if slices.ContainsFunc(m.h.pending, campaigning) {
		return m.grant
	}
	if m.leader != nil && (m.term == epoch.Zero || m.fenced > m.term) {
		return m.end
	}
	if i := slices.IndexFunc(m.h.timers, func(tm machine.Timer) bool { return m.backoffs[tm.Token] }); i >= 0 {
		return func(c *prop.Case) { m.elapse(c, i) }
	}
	return nil
}

// invariant checks what is true after every step. Only a campaign error that a retry does not
// fix stops the root with an error, and every watch of a root that runs belongs to the
// controller that runs.
func (m *model) invariant(c *prop.Case) {
	if err := m.h.machine.Err(); err != nil {
		expect.True(c, m.broken, "only a campaign error that a retry does not fix may stop the root")
		expect.That(c, err).ErrorIs(errBroken, "the root must stop with the error of the campaign")
		return
	}
	if m.stopped {
		return
	}
	for _, term := range m.watches {
		expect.Equal(c, term, m.term, "every watch of the root must belong to the controller that runs")
	}
}

// settle checks the root once the drain has ended. The controller of the term records the
// term in the root scope and applies a proposal. settle then stops the root, and checks that
// the root has replied once to each call that it took before it stopped, unless the runtime
// cancelled the call.
func (m *model) settle(c *prop.Case) {
	if !m.stopped {
		m.run(c)
		assert.NotEqual(c, m.term, epoch.Zero, "the root must lead a term once the drain ends")
		assert.Equal(c, m.h.state(c).Cell.Epoch, m.term,
			"the controller of the term must record the term in the root scope")
		m.submit(c)
		m.run(c)
		to := m.sent[len(m.sent)-1]
		assert.Equal(c, m.replies[to], 1, "the controller of the term must reply to a proposal")
		assert.NoError(c, m.replied[to], "the controller of the term must apply a proposal of the next version")
		m.halt(c)
	}
	for _, to := range m.sent {
		if !m.late[to] && !m.cancelled[to] {
			expect.Equal(c, m.replies[to], 1, "the root must reply once to each call that it took before it stopped")
		}
	}
}

// TestMachine checks the construction, the lifecycle and the replies of a root machine.
func TestMachine(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give func(*root.Config)
			name string
		}{
			{name: "returns ErrConfig for a config without a clock", give: func(c *root.Config) { c.Clock = nil }},
			{
				name: "returns ErrConfig for a config without a source of randomness",
				give: func(c *root.Config) { c.Rand = nil },
			},
			{
				name: "returns ErrConfig for a config without a reporter",
				give: func(c *root.Config) { c.Reporter = nil },
			},
			{name: "returns ErrConfig for a config without a logger", give: func(c *root.Config) { c.Logger = nil }},
			{
				name: "returns ErrConfig for a config without a registry",
				give: func(c *root.Config) { c.Registry = nil },
			},
			{
				name: "returns ErrConfig for a config without a bound of a commit",
				give: func(c *root.Config) { c.Fits = nil },
			},
			{
				name: "returns ErrConfig for a config without the address of the controller process",
				give: func(c *root.Config) { c.Node.Address = "" },
			},
			{
				name: "returns ErrConfig for a parallelism below one",
				give: func(c *root.Config) { c.Parallelism = 0 },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				cfg := completeConfig(t)
				tt.give(&cfg)
				_, err := root.New(&cfg)
				assert.ErrorIs(t, err, root.ErrConfig, "New must refuse a config without a required field")
			})
		}

		t.Run("returns ErrConfig for a nil config", func(t *testing.T) {
			t.Parallel()
			_, err := root.New(nil)
			assert.ErrorIs(t, err, root.ErrConfig, "New must refuse a nil config")
		})
	})

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("campaigns at Started", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.paused = true
			started := h.call()
			h.step(t, machine.Started{Reply: started})
			expect.Length(t, h.campaigns, 1, "the root must campaign at Started")
			expect.Equal(t, h.replies, []machine.Effect{machine.Reply[struct{}]{To: started}},
				"the root must reply to Started once it campaigns")
		})

		t.Run("serves the namespace in the term that the election grants", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			st := h.state(t)
			expect.Equal(t, st.Cell.Controller, rootAddress, "the record of the root must contain its address")
			expect.Equal(t, st.Cell.ControllerIdentity, rootIdentity,
				"the record of the root must contain its identity")
			expect.Equal(t, st.Cell.ID, shard.CellID(""), "the record of the root must not name a cell")
			expect.Equal(t, st.Cell.Epoch, h.term(t), "the record of the root must contain its term")
			expect.NotEqual(t, h.views, machine.Token(0), "the root must watch the views of the root scope")
		})

		t.Run("ignores the result of an older campaign", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.paused = true
			h.step(t, machine.Started{Reply: h.call()})
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Campaigned{Token: h.campaigns[0] + 99}, out)
			expect.Equal(t, got, out, "the root must ignore the result of another campaign")
			got = h.machine.Step(machine.TermEnded{Token: h.campaigns[0] + 99}, out)
			expect.Equal(t, got, out, "the root must ignore the end of another campaign")
		})

		t.Run("ignores a Timer that it did not arm", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Elapsed{Token: 999}, out)
			assert.Equal(t, got, out, "the root must ignore a Timer that no part armed")
		})

		t.Run("campaigns again after a backoff for a transient error of a campaign", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.paused = true
			h.step(t, machine.Started{Reply: h.call()})
			h.campaigned(t, errOutage, epoch.Zero)
			assert.Length(t, h.campaigns, 1, "the root must not campaign before the backoff")
			h.elapse(t, firstCampaignBackoff)
			assert.Length(t, h.campaigns, 2, "the root must campaign again after the backoff")
		})

		t.Run("doubles the backoff of each failed campaign in a row", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.paused = true
			h.step(t, machine.Started{Reply: h.call()})
			h.campaigned(t, errOutage, epoch.Zero)
			h.elapse(t, firstCampaignBackoff)
			h.campaigned(t, errOutage, epoch.Zero)
			assert.NotEmpty(t, h.timers, "the second failure must arm a backoff")
			assert.Equal(t, h.timers[len(h.timers)-1].Duration, 2*firstCampaignBackoff,
				"the second backoff in a row must have twice the ceiling")
		})

		t.Run("starts the backoff again after a granted term", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.paused = true
			h.step(t, machine.Started{Reply: h.call()})
			h.campaigned(t, errOutage, epoch.Zero)
			h.elapse(t, firstCampaignBackoff)
			h.grant(t)
			h.end(t)
			h.campaigned(t, errOutage, epoch.Zero)
			assert.Equal(t, h.timers[len(h.timers)-1].Duration, firstCampaignBackoff,
				"a granted term must start the backoff again at the first ceiling")
		})

		t.Run("stops at an error of a campaign that is not transient", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.paused = true
			h.step(t, machine.Started{Reply: h.call()})
			h.campaigned(t, errBroken, epoch.Zero)
			expect.ErrorIs(t, h.machine.Err(), errBroken, "the root must stop at a broken store")
			expect.Empty(t, h.resigned, "the root must not resign a campaign that has ended")
		})

		t.Run("stops at a term that its controller refuses", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.paused = true
			h.step(t, machine.Started{Reply: h.call()})
			h.campaigned(t, nil, epoch.Zero)
			expect.ErrorIs(t, h.machine.Err(), control.ErrConfig, "the root must stop at the zero term")
			expect.Equal(t, h.resigned, h.campaigns, "the root must resign the campaign of the term")
		})

		t.Run("campaigns again when the store ends the term", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			watch := h.views
			h.end(t)
			expect.Contains(t, h.unwatched, watch, "the controller of the term must end the watch of its views")
			expect.Length(t, h.campaigns, 2, "the root must campaign again")
		})

		t.Run("replies with control.ErrNotServing to a proposal that waits when the term ends", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, procA)
			call := h.submitCall(t, h.specProposal(t, 1, 1, 2), operator)
			h.step(t, call)
			h.end(t)
			assert.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrNotServing,
				"a proposal that waits for its commit must get ErrNotServing")
		})

		t.Run("resigns the term at an error of its controller", func(t *testing.T) {
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
			full = true
			h.view(t, procA)
			expect.Equal(t, h.resigned, h.campaigns, "the root must resign the term")
			expect.Nil(t, h.machine.Err(), "an error of the controller of a term must not stop the root")
			got := slices.DeleteFunc(h.logs.All(), func(r slog.Record) bool { return r.Message != logTerm })
			assert.Length(t, got, 1, "the root must log the error of the term")
			var named string
			got[0].Attrs(func(a slog.Attr) bool {
				if a.Key == attrNamespaceKey {
					named = a.Value.String()
				}
				return true
			})
			expect.Equal(t, named, string(namespace), "the log must name the namespace")
			full = false
			h.end(t)
			expect.Length(t, h.campaigns, 2, "the root must campaign again once the term has ended")
		})

		t.Run("stops with fsm.ErrRejected for a second Started", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.paused = true
			h.step(t, machine.Started{Reply: h.call()})
			second := h.call()
			h.step(t, machine.Started{Reply: second})
			expect.ErrorIs(t, h.machine.Err(), fsm.ErrRejected, "the lifecycle must refuse a second Started")
			expect.ErrorIs(t, replyTo[struct{}](t, h, second).Err, fsm.ErrRejected,
				"the second Started must get the error that stopped the root")
			expect.Equal(t, h.resigned, h.campaigns, "the root must end the campaign that it waits for")
		})

		t.Run("releases the instruments of every controller when a second grant stops it", func(t *testing.T) {
			t.Parallel()
			rec := telemetrytest.NewRecorder()
			h := newHarness(t, func(s *setup) { s.cfg.Reporter = rec })
			h.start(t)
			h.step(t, machine.Campaigned{Token: h.campaigns[len(h.campaigns)-1], Term: h.term(t) + 1})
			expect.ErrorIs(
				t,
				h.machine.Err(),
				fsm.ErrRejected,
				"the lifecycle must refuse a second grant of a campaign",
			)
			assert.NotEmpty(t, rec.Specs(), "the controllers of the root must report instruments")
			for _, spec := range rec.Specs() {
				for _, attrs := range rec.Series(spec.Name) {
					expect.Equal(t, rec.Bound(spec.Name, attrs...), 0,
						"the root must release every instrument that its controllers bound")
				}
			}
		})

		t.Run("replies to the first Started once", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.paused = true
			h.step(t, machine.Started{Reply: h.call()})
			h.step(t, machine.Started{Reply: h.call()})
			assert.Length(t, h.replies, 2, "the root must reply to each Started once")
		})

		t.Run("stops the controller of its term when an error stops it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			watch := h.views
			h.step(t, machine.Started{Reply: h.call()})
			expect.ErrorIs(t, h.machine.Err(), fsm.ErrRejected, "the lifecycle must refuse a second Started")
			expect.Contains(t, h.unwatched, watch, "the controller of the term must end the watch of its views")
			expect.Equal(t, h.resigned, h.campaigns, "the root must resign its term")
		})

		t.Run("returns out unchanged after it has stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.step(t, machine.Stopped{})
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Started{Reply: h.call()}, out)
			expect.Equal(t, got, out, "a stopped root must not take an event")
			expect.Nil(t, h.machine.Err(), "a root that Stopped ends must not report an error")
		})

		t.Run("replies with control.ErrNotServing to a proposal that waits at Stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, procA)
			call := h.submitCall(t, h.specProposal(t, 1, 1, 2), operator)
			h.step(t, call)
			h.step(t, machine.Stopped{})
			expect.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrNotServing,
				"a proposal that waits for its commit must get ErrNotServing")
			expect.Empty(t, h.resigned, "Stopped must not resign the term")
		})

		t.Run("returns out unchanged after an error has stopped it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.paused = true
			h.step(t, machine.Started{Reply: h.call()})
			h.campaigned(t, errBroken, epoch.Zero)
			assert.NotNil(t, h.machine.Err(), "the root must stop at a broken store")
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Started{Reply: h.call()}, out)
			assert.Equal(t, got, out, "a root that an error stopped must not take an event")
		})

		t.Run("takes Stopped after a term has ended", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.end(t)
			h.step(t, machine.Stopped{})
			assert.Nil(t, h.machine.Err(), "the root must not stop the controller of an ended term again")
		})

		t.Run("gives each effect a token of its own", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.paused = true
			h.step(t, machine.Started{Reply: h.call()})
			h.campaigned(t, errOutage, epoch.Zero)
			h.elapse(t, firstCampaignBackoff)
			h.campaigned(t, errOutage, epoch.Zero)
			h.elapse(t, 2*firstCampaignBackoff)
			h.grant(t)
			h.view(t, procA)
			assert.NoDuplicates(t, func() ([]machine.Token, error) { return h.tokens, nil },
				"each effect must have a token that no other effect has")
		})

		t.Run("returns out unchanged at Stopped outside a term", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.paused = true
			h.step(t, machine.Started{Reply: h.call()})
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Stopped{}, out)
			assert.Equal(t, got, out, "Stopped outside a term must append nothing")
		})

		t.Run("replies with control.ErrNotServing to a proposal outside a term", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.paused = true
			h.step(t, machine.Started{Reply: h.call()})
			call := h.submitCall(t, h.specProposal(t, 1, 1, 2), operator)
			h.step(t, call)
			assert.ErrorIs(t, replyTo[protocol.Receipt](t, h, call.Reply).Err, control.ErrNotServing,
				"the root must refuse a proposal outside a term")
		})

		t.Run("replies with control.ErrCall to a report", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			call := machine.Report{Report: &protocol.Report{Scope: rootScope}, Reply: h.call()}
			h.step(t, call)
			assert.ErrorIs(t, replyTo[protocol.Ack](t, h, call.Reply).Err, control.ErrCall,
				"the root must refuse a report")
		})

		t.Run("replies with control.ErrCall to a disruption", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			call := machine.Disrupt{Disruption: &budget.Disruption{Scope: rootScope}, Reply: h.call()}
			h.step(t, call)
			assert.ErrorIs(t, replyTo[budget.Decision](t, h, call.Reply).Err, control.ErrCall,
				"the root must refuse a disruption")
		})

		t.Run("replies with control.ErrCall to the end of a disruption", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			call := machine.Done{Args: &protocol.DoneArgs{Scope: rootScope}, Reply: h.call()}
			h.step(t, call)
			assert.ErrorIs(t, replyTo[struct{}](t, h, call.Reply).Err, control.ErrCall,
				"the root must refuse the end of a disruption")
		})

		t.Run("keeps its contract under every sequence of events", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "a root keeps its contract under every sequence of events", exercise,
				prop.Require(labelGranted, often),
				prop.Require(labelEnded, sometimes),
				prop.Require(labelFault, rare),
				prop.Require(labelStopped, often),
				prop.Require(labelOutage, rare),
			)
		})
	})

	t.Run("Err", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil while the root runs", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			assert.Nil(t, h.machine.Err(), "a root that runs must not report an error")
		})
	})
}

// campaigning reports whether e is a Campaign.
func campaigning(e machine.Effect) bool {
	_, ok := e.(machine.Campaign)
	return ok
}

// exercise runs the steps of the case c over a root that has taken Started, as the store, the
// controller processes, the operators and the runtime drive it. The steps of a case:
//   - grant, lose and fail campaigns.
//   - end terms in the store.
//   - end Timers and run the effects that wait.
//   - pass views, proposals and the calls of a cell controller.
//   - cancel calls.
//   - fence the root scope with a later term.
//   - make the bound of a commit refuse every mutation.
//   - start and end outages of the store.
//   - pass the results of older campaigns.
//   - stop the root.
//
// During an outage the store does not grant or end a term, another writer does not commit,
// and the operators do not send a proposal. The model checks the contract of the root after
// every step:
//   - Only a campaign error that a retry does not fix stops the root with an error. A stopped
//     root does not append an effect, and Stopped appends only replies.
//   - The root replies to each call once, and never to a call that the runtime cancelled.
//   - It replies to Started without an error, to a proposal outside a term with
//     control.ErrNotServing, and to each call of a cell controller with control.ErrCall.
//   - It campaigns only outside a term. It commits and watches the views only within a term,
//     with the term of the controller that runs.
//   - Every watch of a root that runs belongs to the controller that runs, so one controller
//     runs at a time.
//   - Each Resign has the token of the campaign of the term.
//   - Only a commit that does not fit or a fence leads to a Resign, so an outage does not end
//     a term.
//   - The root ignores the results of older campaigns.
//
// The drain takes the steps that drain returns until the root serves again. The controller
// of the term then applies a proposal. Once Stopped, the root has replied once to each call
// that it took before it stopped, unless the runtime cancelled the call.
func exercise(c *prop.Case) {
	m := newModel(c)
	campaigns := func(struct{}) bool { return !m.stopped && slices.ContainsFunc(m.h.pending, campaigning) }
	stateful.Steps(c, stateful.Machine[struct{}]{
		Actions: []stateful.Action[struct{}]{
			{
				Name: "grant", Weight: grants,
				Enabled: func(s struct{}) bool { return campaigns(s) && !m.outage },
				Run: func(c *prop.Case, _ int, _ any) {
					c.Classify(labelGranted)
					m.grant(c)
				},
			},
			{
				Name:    "lose a campaign",
				Enabled: campaigns,
				Run:     func(c *prop.Case, _ int, _ any) { m.lose(c, errOutage) },
			},
			{
				Name: "fail a campaign", Enabled: campaigns,
				Run: func(c *prop.Case, _ int, _ any) {
					m.broken = true
					m.lose(c, errBroken)
				},
			},
			{
				Name:    "end the term",
				Enabled: func(struct{}) bool { return !m.stopped && m.leader != nil && !m.outage },
				Run: func(c *prop.Case, _ int, _ any) {
					c.Classify(labelEnded)
					m.end(c)
				},
			},
			{
				Name: "elapse", Enabled: func(struct{}) bool { return !m.stopped && len(m.h.timers) > 0 },
				Run: func(c *prop.Case, _ int, _ any) {
					m.elapse(c, c.Draw(prop.Integer(0, len(m.h.timers)-1), "timer"))
				},
			},
			{
				Name: "view", Enabled: func(struct{}) bool { return !m.stopped && m.h.views != 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.view(c) },
			},
			{
				Name: "submit", Enabled: func(struct{}) bool { return !m.outage },
				Run: func(c *prop.Case, _ int, _ any) { m.submit(c) },
			},
			{Name: "call as a cell controller", Run: func(c *prop.Case, _ int, _ any) { m.misdirect(c) }},
			{
				Name: "cancel", Enabled: func(struct{}) bool { return !m.stopped && len(m.waiting()) > 0 },
				Run: func(c *prop.Case, _ int, _ any) { m.cancel(c) },
			},
			{
				Name: "run",
				Enabled: func(struct{}) bool {
					return !m.stopped &&
						slices.ContainsFunc(m.h.pending, func(e machine.Effect) bool { return !campaigning(e) })
				},
				Run: func(c *prop.Case, _ int, _ any) { m.run(c) },
			},
			{
				Name:    "fence",
				Enabled: func(struct{}) bool { return !m.stopped && m.term != epoch.Zero && !m.outage },
				Run:     func(c *prop.Case, _ int, _ any) { m.fence(c) },
			},
			{Name: "fill the store", Run: func(*prop.Case, int, any) { m.full = !m.full }},
			{
				Name: "toggle an outage", Enabled: func(struct{}) bool { return !m.stopped },
				Run: func(c *prop.Case, _ int, _ any) { m.toggle(c) },
			},
			{
				Name: "pass an older result", Enabled: func(struct{}) bool { return len(m.h.campaigns) > 1 },
				Run: func(c *prop.Case, _ int, _ any) { m.stale(c) },
			},
			{
				Name: "stop", Enabled: func(struct{}) bool { return !m.stopped },
				Run: func(c *prop.Case, _ int, _ any) {
					c.Classify(labelStopped)
					m.halt(c)
				},
			},
			{
				Name: "complete", Drain: true,
				Enabled: func(struct{}) bool { return m.drain() != nil || m.full && !m.stopped },
				Run: func(c *prop.Case, _ int, _ any) {
					m.full = false
					if step := m.drain(); step != nil {
						step(c)
					}
				},
			},
		},
		Invariant: func(c *prop.Case, _ struct{}) { m.invariant(c) },
		Settle:    func(c *prop.Case, _ struct{}) { m.settle(c) },
	})
}

// grant performs the Campaign in flight, and runs the machine until the controller of the
// term serves.
func (h *harness) grant(tb assert.TB) {
	tb.Helper()
	h.paused = false
	h.run(tb)
}

// end resigns the term in the store and passes its end to the machine. It then runs the
// machine and keeps the next Campaign of the machine pending.
func (h *harness) end(tb assert.TB) {
	tb.Helper()
	tok := h.campaigns[len(h.campaigns)-1]
	l, ok := h.terms[tok]
	assert.True(tb, ok, "the store must have granted the last campaign")
	assert.NoError(tb, l.Resign(assert.Context(tb)), "the term must end")
	h.paused = true
	h.step(tb, machine.TermEnded{Token: tok})
	h.run(tb)
}

// term returns the term of the last campaign that the store granted.
func (h *harness) term(tb assert.TB) epoch.Epoch {
	tb.Helper()
	l, ok := h.terms[h.campaigns[len(h.campaigns)-1]]
	assert.True(tb, ok, "the store must have granted the last campaign")
	return l.Term()
}

// campaigned drops the pending Campaign of the last campaign of the machine, and passes a
// result of the campaign with the error err and the fence epoch term to the machine. The
// store does not grant a term to a campaign that this method ends.
func (h *harness) campaigned(tb assert.TB, err error, term epoch.Epoch) {
	tb.Helper()
	tok := h.campaigns[len(h.campaigns)-1]
	h.pending = slices.DeleteFunc(h.pending, func(e machine.Effect) bool {
		c, ok := e.(machine.Campaign)
		return ok && c.Token == tok
	})
	h.step(tb, machine.Campaigned{Err: err, Token: tok, Term: term})
}

// elapse performs the oldest Timer whose Duration is d, and runs the machine afterwards.
func (h *harness) elapse(tb assert.TB, d time.Duration) {
	tb.Helper()
	i := slices.IndexFunc(h.timers, func(tm machine.Timer) bool { return tm.Duration == d })
	assert.True(tb, i >= 0, "a Timer of the duration must be armed")
	tm := h.timers[i]
	h.timers = slices.Delete(h.timers, i, i+1)
	h.step(tb, machine.Elapsed{Token: tm.Token})
	h.run(tb)
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package engine_test

import (
	"context"
	"encoding/binary"
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
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand/constant"

	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/handoff/breakbeforemake"
	"go.thesmos.sh/sharder/internal/engine"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The names of the records of the tests.
const (
	namespace shard.Namespace = "orders"
	cell      shard.CellID    = "cell-1"
)

// The first bytes of the identifiers of the tests, one per kind of identifier.
const (
	tagIncarnation byte = 0x1a
	tagShard       byte = 0x5d
)

// The epochs of the tests.
const (
	// term is the term of the controller of the tests.
	term epoch.Epoch = 2
	// placed is the epoch, the reserved epoch and the sequence of each placement before
	// its move.
	placed epoch.Epoch = 1
	// newEpoch is the epoch that a move reserves for its destination.
	newEpoch = placed + 1
	// abortEpoch is the epoch that a move reserves for its abort path.
	abortEpoch = placed + 2
	// specVersion is the version of the spec of each move.
	specVersion epoch.Epoch = 7
)

// The timeouts of the steps of break-before-make in the tests.
const (
	addTimeout  = 30 * time.Second
	dropTimeout = 10 * time.Second
)

// The bounds of the Engine of the tests, unless a test sets others.
const (
	maxTransitions     = 64
	maxNodeTransitions = 16
	nodeCallbacks      = 16
)

// taken is the last token that the other parts of the machine of each test took before
// the Engine took its first.
const taken machine.Token = 7

// half is the value of each draw of the source of randomness of the tests, so each backoff
// is half of its ceiling.
const half = 0.5

// maxDeliveries bounds the deliveries of a drain, so an Engine that never settles fails its
// test instead of the timeout of go test.
const maxDeliveries = 10000

// done is the step that steps reports for a batch that ends a transition.
const done = -1

// addStep is the step of break-before-make that adds the destination. It is the only step
// of a move with a source whose abort path has steps.
const addStep = 2

// lockDelay is the lock delay of the tests that set one.
const lockDelay = 5 * time.Second

// refused is the message of the refusal of the tests.
const refused = "agent: the node refuses the callback"

// otherProtocol is the name of a protocol that the Engine of the tests does not run.
const otherProtocol shard.StrategyName = "make-before-break"

// The labels of the faults of the property of the Engine, and the least share of its cases
// that contain each fault.
const (
	failedCall  = "a callback fails with a transient error"
	refusedCall = "an agent refuses a callback"
	restart     = "a node restarts"
	leave       = "a node leaves the view"
	faultShare  = 0.1
)

// The bounds of the Engine of the property. The bounds are low, so Start often returns
// false in a case of the property.
const (
	// fewTransitions is the most transitions in flight.
	fewTransitions = 3
	// fewNodeTransitions is the most transitions in flight that count against one node.
	fewNodeTransitions = 2
	// fewCallbacks is the most callbacks in flight to one node.
	fewCallbacks = 1
	// shards is the number of shards of the property.
	shards = 6
)

// The weights of the actions of the property that move the transitions forward. With these
// weights, a case commits batches and replies to callbacks more often than it restarts
// nodes or takes them out of the view.
const (
	// commits is the weight of a commit of a batch.
	commits = 2
	// answers is the weight of an answer to a callback.
	answers = 3
)

// The moves of the test of the retry budget. Each warm move sends a drop and an add. The
// retries of the failing moves exceed MinRetries, and RetryBudget of all their callbacks
// exceeds the retries.
const (
	// warmMoves is the number of moves whose callbacks succeed.
	warmMoves = 60
	// failingMoves is the number of moves whose drops fail until their last attempt.
	failingMoves = 3
)

// The nodes of the tests, each at its first incarnation.
var (
	nodeA = shard.Node{ID: "node-a", Address: "node-a:7000", Incarnation: 1}
	nodeB = shard.Node{ID: "node-b", Address: "node-b:7000", Incarnation: 1}
	nodeC = shard.Node{ID: "node-c", Address: "node-c:7000", Incarnation: 1}
)

// The primary replicas on the nodes of the tests.
var (
	primaryA = shard.Replica{Node: nodeA.ID, Role: shard.RolePrimary}
	primaryB = shard.Replica{Node: nodeB.ID, Role: shard.RolePrimary}
	primaryC = shard.Replica{Node: nodeC.ID, Role: shard.RolePrimary}
)

// The scope, the incarnation and the clock of the tests.
var (
	// cellScope is the scope of the cell of the tests.
	cellScope = shard.Scope{Namespace: namespace, Cell: cell}
	// incA is the namespace incarnation of the tests.
	incA = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0a})}
	// origin is the start of the clock of each test.
	origin = time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC)
)

// foreign is an effect of another part of the machine. A test passes it to the Engine in
// out, and expects it back unchanged.
var foreign machine.Effect = machine.Timer{Token: taken}

// refusal is the answer of an agent that refuses a callback, as the transport of the
// controller returns it.
var refusal error = &protocol.CallError{Message: refused, ErrorClass: errs.Conflict}

// The scripts of the protocols of the tests other than break-before-make. Each has one add
// on To.
var (
	// unabortable has no abort path, which Validate refuses.
	unabortable = handoff.Script{Steps: []handoff.Step{{Action: handoff.Add, Side: handoff.To, Timeout: addTimeout}}}
	// abortable has an abort path that commits the fallback as the writer.
	abortable = handoff.Script{
		Steps: []handoff.Step{{Action: handoff.Add, Side: handoff.To, Timeout: addTimeout}},
		Abort: [][]handoff.Step{{{Action: handoff.Commit, Side: handoff.Fallback}}},
	}
)

// refusalOf is an add that failed definitively, as Config.Refused receives it.
type refusalOf struct {
	// at is the endpoint of the add.
	at store.Endpoint
	// sid is the shard of the add.
	sid shard.ID
}

// testCase is the test, or the case of a property, that a harness runs in.
type testCase interface {
	assert.TB
	// Cleanup registers f to run when the test or the case ends.
	Cleanup(f func())
}

// setup is the configuration of a harness: the Config of its Engine and the bound of a
// batch.
type setup struct {
	// fits is the bound of a batch, and nil for the bound of the store.
	fits func(m *store.Mutation) error
	// cfg is the Config of the Engine, which newHarness completes with the counter of
	// tokens and the bound of a batch.
	cfg engine.Config
}

// harness is the machine and the runtime of the tests. It commits the batches of one
// Engine to a memory store, and performs the Timers of the Engine. It replies to the
// callbacks of the Engine as the agents of the nodes do.
type harness struct {
	// answer returns the answer of the agent to the request r, and nil for success. With a
	// nil answer, the harness replies to every request with success.
	answer func(r *protocol.Request) error
	// st is the store of the Engine.
	st *memory.Store
	// engine is the Engine under test.
	engine *engine.Engine
	// calls lists the callbacks in flight, the oldest first.
	calls []machine.Callback
	// timers lists the Timers in flight, the oldest first.
	timers []machine.Timer
	// sent lists the request of every callback that the Engine sent, in order.
	sent []*protocol.Request
	// waits lists the duration of each Timer that the harness performed.
	waits []time.Duration
	// batches lists every batch that applied, in order.
	batches []*store.Mutation
	// refusals lists every add that the Engine reported as failed definitively, in order.
	refusals []refusalOf
	// tokens is the counter of the tokens of the machine of the Engine.
	tokens machine.Token
	// seq is the Seq of the last commit to the store.
	seq uint64
}

// newHarness returns a harness of an Engine of the scope of the tests. Each option changes
// the setup before New. The placements ps are the placements of the scope before the
// first move, and the first commit of the harness creates the scope with them.
func newHarness(tb testCase, ps []shard.Placement, opts ...func(*setup)) *harness {
	tb.Helper()
	s := setup{
		cfg: engine.Config{
			Settings:    settings(tb),
			Clock:       fake.New(origin),
			Rand:        constant.FromFloat64(half),
			Scope:       cellScope,
			Term:        term,
			Incarnation: incA,
		},
	}
	for _, opt := range opts {
		opt(&s)
	}
	h := &harness{st: memory.New(fake.New(origin)), tokens: taken}
	tb.Cleanup(func() { assert.NoError(tb, h.st.Close(), "the store must close") })
	s.cfg.Tokens = &h.tokens
	s.cfg.Refused = func(sid shard.ID, at store.Endpoint) { h.refusals = append(h.refusals, refusalOf{at: at, sid: sid}) }
	s.cfg.Fits = s.fits
	if s.fits == nil {
		s.cfg.Fits = func(m *store.Mutation) error { return h.st.Fits(cellScope, m) }
	}
	var err error
	h.engine, err = engine.New(&s.cfg)
	assert.NoError(tb, err, "New must return an Engine of a complete config")
	h.commit(tb, &store.Mutation{Put: ps})
	return h
}

// model is the state of the property of the Engine. It contains the harness of the Engine
// and the transitions that the property started. It also lists the nodes A, B and C, and
// whether each node is in the view.
type model struct {
	// h is the harness of the Engine.
	h *harness
	// flight contains the shards whose transitions are in flight.
	flight map[shard.ID]bool
	// phases maps the shard of each transition in flight to its committed phase.
	phases map[shard.ID]shard.Phase
	// started counts the transitions that the property started, by shard.
	started map[shard.ID]uint64
	// nodes lists the nodes A, B and C, each at its latest incarnation.
	nodes []shard.Node
	// present reports for each node of nodes whether it is in the view.
	present []bool
	// shards lists the shards of the property.
	shards []shard.ID
}

// call is the kind of a callback and the node that it addresses.
type call struct {
	// call is the callback.
	call protocol.Call
	// node is the node of the callback.
	node shard.NodeID
}

// scripted is a handoff.Protocol with the name and the version of break-before-make. Its
// script is the same for every transfer.
type scripted struct {
	// script is the script of every transfer.
	script handoff.Script
}

// Name returns the name of break-before-make, so the records of scripted look like its own.
func (scripted) Name() shard.StrategyName { return breakbeforemake.Name }

// Version returns 1.
func (scripted) Version() uint32 { return 1 }

// Script returns the script of s.
func (s scripted) Script(handoff.Transfer) (handoff.Script, error) { return s.script, nil }

// TestEngine checks the construction, the transitions, the batches and the steps of an
// Engine against a memory store.
func TestEngine(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give func(*engine.Config)
			name string
		}{
			{
				name: "returns ErrConfig for a config without a clock",
				give: func(c *engine.Config) { c.Clock = nil },
			},
			{
				name: "returns ErrConfig for a config without a source of randomness",
				give: func(c *engine.Config) { c.Rand = nil },
			},
			{
				name: "returns ErrConfig for a config without a counter of tokens",
				give: func(c *engine.Config) { c.Tokens = nil },
			},
			{
				name: "returns ErrConfig for a config without a protocol",
				give: func(c *engine.Config) { c.Protocol = nil },
			},
			{
				name: "returns ErrConfig for a config without a bound of a commit",
				give: func(c *engine.Config) { c.Fits = nil },
			},
			{
				name: "returns ErrConfig for a config without a receiver of the refused adds",
				give: func(c *engine.Config) { c.Refused = nil },
			},
			{
				name: "returns ErrConfig for a config without a namespace",
				give: func(c *engine.Config) { c.Scope.Namespace = "" },
			},
			{
				name: "returns ErrConfig for a config without a namespace incarnation",
				give: func(c *engine.Config) { c.Incarnation = shard.NamespaceIncarnation{} },
			},
			{
				name: "returns ErrConfig for a config without a term",
				give: func(c *engine.Config) { c.Term = epoch.Zero },
			},
			{
				name: "returns ErrConfig for a lock delay below zero",
				give: func(c *engine.Config) { c.LockDelay = -time.Second },
			},
			{
				name: "returns ErrConfig for a bound of the transitions below one",
				give: func(c *engine.Config) { c.MaxTransitions = 0 },
			},
			{
				name: "returns ErrConfig for a bound of the transitions of a node below one",
				give: func(c *engine.Config) { c.MaxNodeTransitions = 0 },
			},
			{
				name: "returns ErrConfig for a bound of the callbacks of a node below one",
				give: func(c *engine.Config) { c.NodeCallbacks = 0 },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				var tokens machine.Token
				cfg := engine.Config{
					Settings:    settings(t),
					Clock:       fake.New(origin),
					Rand:        constant.FromFloat64(half),
					Tokens:      &tokens,
					Fits:        func(*store.Mutation) error { return nil },
					Refused:     func(shard.ID, store.Endpoint) {},
					Scope:       cellScope,
					Term:        term,
					Incarnation: incA,
				}
				tt.give(&cfg)
				_, err := engine.New(&cfg)
				assert.ErrorIs(t, err, engine.ErrConfig, "New must refuse a config without a required field")
			})
		}

		t.Run("returns ErrConfig for a nil config", func(t *testing.T) {
			t.Parallel()
			_, err := engine.New(nil)
			assert.ErrorIs(t, err, engine.ErrConfig, "New must refuse a nil config")
		})
	})

	t.Run("Start", func(t *testing.T) {
		t.Parallel()

		t.Run("commits the record of the transition in the next batch", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			assert.True(t, h.start(t, moveOf(p, nodeA, nodeB, nodeA)), "Start must admit a move within the bounds")
			assert.True(t, h.flush(t), "the Engine must return a batch")
			want := p
			want.Reserved, want.Sequence = abortEpoch, uint64(placed)+1
			expect.Equal(t, h.batches[0].Put, []shard.Placement{want},
				"the batch must reserve the epochs and the sequence of the move in the placement")
			expect.Equal(t, h.batches[0].Transitions, []store.Transition{{
				Started:         origin,
				Protocol:        breakbeforemake.Name,
				Fallback:        endpoint(nodeA),
				From:            endpoint(nodeA),
				To:              endpoint(nodeB),
				Operation:       shard.OperationID{Incarnation: incA, Shard: p.Shard, Sequence: uint64(placed) + 1},
				SpecVersion:     specVersion,
				OldEpoch:        placed,
				NewEpoch:        newEpoch,
				AbortEpoch:      abortEpoch,
				ProtocolVersion: 1,
				Phase:           shard.PhaseRunning,
			}}, "the batch must commit the record of the transition at its first step")
		})

		t.Run("returns false while MaxTransitions transitions are in flight", func(t *testing.T) {
			t.Parallel()
			p, q := placement(1, nodeA), placement(2, nodeA)
			h := newHarness(t, []shard.Placement{p, q}, func(s *setup) { s.cfg.MaxTransitions = 1 })
			assert.True(t, h.start(t, moveOf(p, nodeA, nodeB, nodeA)), "Start must admit the first move")
			assert.False(t, h.start(t, moveOf(q, nodeA, nodeC, nodeA)), "Start must refuse a move above the bound")
		})

		tests := []struct {
			first  [3]shard.Node
			second [3]shard.Node
			name   string
		}{
			{
				name:   "returns false for a move to the destination of a transition at the bound",
				first:  [3]shard.Node{nodeA, nodeB, nodeA},
				second: [3]shard.Node{nodeC, nodeB, nodeC},
			},
			{
				name:   "returns false for a move whose fallback is the destination of a transition at the bound",
				first:  [3]shard.Node{nodeA, nodeB, nodeA},
				second: [3]shard.Node{nodeC, nodeC, nodeB},
			},
			{
				name:   "returns false for a move from the destination of a transition at the bound",
				first:  [3]shard.Node{nodeA, nodeB, nodeA},
				second: [3]shard.Node{nodeB, nodeC, nodeC},
			},
			{
				name:   "returns false for a move to the fallback of a transition at the bound",
				first:  [3]shard.Node{nodeA, nodeB, nodeC},
				second: [3]shard.Node{{}, nodeC, {}},
			},
			{
				name:   "returns false for a move to the source of a transition at the bound",
				first:  [3]shard.Node{nodeA, nodeB, nodeB},
				second: [3]shard.Node{{}, nodeA, {}},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				p, q := placement(1, nodeA), placement(2, nodeC)
				h := newHarness(t, []shard.Placement{p, q}, func(s *setup) { s.cfg.MaxNodeTransitions = 1 })
				h.view(t, nodeA, nodeB, nodeC)
				assert.True(t, h.start(t, moveOf(p, tt.first[0], tt.first[1], tt.first[2])),
					"Start must admit the first move")
				assert.False(t, h.start(t, moveOf(q, tt.second[0], tt.second[1], tt.second[2])),
					"Start must refuse a move above the bound of a node")
			})
		}

		t.Run("returns true for a move from a node whose earlier incarnation is a source", func(t *testing.T) {
			t.Parallel()
			p, q := placement(1, nodeB), placement(2, nodeB)
			h := newHarness(t, []shard.Placement{p, q}, func(s *setup) { s.cfg.MaxNodeTransitions = 1 })
			h.view(t, nodeA, nodeB, nodeC)
			assert.True(t, h.start(t, moveOf(p, nodeB, nodeA, nodeA)), "Start must admit the first move")
			h.view(t, nodeA, restarted(nodeB), nodeC)
			assert.True(t, h.start(t, moveOf(q, restarted(nodeB), nodeC, nodeC)),
				"a move must not count against the node of a source whose incarnation has ended")
		})

		t.Run("returns true for a move from a node that has left the view", func(t *testing.T) {
			t.Parallel()
			p, q := placement(1, nodeB), placement(2, nodeB)
			h := newHarness(t, []shard.Placement{p, q}, func(s *setup) { s.cfg.MaxNodeTransitions = 1 })
			h.view(t, nodeA, nodeC)
			assert.True(t, h.start(t, moveOf(p, nodeB, nodeA, nodeA)), "Start must admit the first move")
			assert.True(t, h.start(t, moveOf(q, nodeB, nodeC, nodeC)),
				"a move must not count against a source that has left the view")
		})

		t.Run("returns false for a shard with a transition in flight", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			assert.True(t, h.start(t, moveOf(p, nodeA, nodeB, nodeA)), "Start must admit the first move")
			assert.False(t, h.start(t, moveOf(p, nodeA, nodeC, nodeA)),
				"Start must refuse a second transition of a shard as a bound")
		})

		t.Run("returns ErrMove for a move that the protocol cannot script", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			m := moveOf(p, nodeA, nodeB, nodeA)
			m.To = store.Endpoint{}
			_, err := h.engine.Start(&m)
			expect.That(t, err).ErrorIs(engine.ErrMove, "Start must refuse a move without a destination").
				ErrorIs(handoff.ErrTransfer, "the error must wrap the error of the protocol")
		})

		t.Run("returns ErrMove for a script that Validate refuses", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p}, func(s *setup) { s.cfg.Protocol = scripted{script: unabortable} })
			m := moveOf(p, nodeA, nodeB, nodeA)
			_, err := h.engine.Start(&m)
			expect.That(t, err).ErrorIs(engine.ErrMove, "Start must refuse a move with an invalid script").
				ErrorIs(handoff.ErrScript, "the error must wrap the error of Validate")
		})
	})

	t.Run("Resume", func(t *testing.T) {
		t.Parallel()

		t.Run("continues a transition at its committed step", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeB)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			rec := record(p, nodeA, nodeB, nodeA)
			rec.Step = 2
			h.resume(t, &rec, &p)
			assert.Length(t, h.calls, 1, "Resume must send the callback of the committed step")
			expect.Equal(t, h.calls[0].Request.Call, protocol.Add, "the committed step must be the add")
			expect.Equal(t, h.calls[0].Request.Node, nodeB.ID, "the add must address the destination")
		})

		t.Run("commits the commit step of a resumed transition", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			rec := record(p, nodeA, nodeB, nodeA)
			rec.Step = 1
			out := []machine.Effect{foreign}
			got, err := h.engine.Resume(&rec, &p, out)
			assert.NoError(t, err, "Resume must take the record")
			expect.Equal(t, got, out, "Resume must return out unchanged for a commit step")
			assert.True(t, h.flush(t), "the Engine must return a batch")
			assert.Length(t, h.batches[0].Put, 1, "the batch must put the placement")
			expect.Equal(t, h.batches[0].Put[0].Replicas, []shard.Replica{primaryB},
				"the commit step must commit the destination")
			expect.Equal(t, h.batches[0].Put[0].Epoch, newEpoch, "the destination must write at the new epoch")
		})

		t.Run("runs the abort path of the failed step of a record", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p}, func(s *setup) { s.cfg.Protocol = scripted{script: abortable} })
			rec := record(p, nodeA, nodeB, nodeC)
			rec.Phase = shard.PhaseAborting
			h.resume(t, &rec, &p)
			assert.True(t, h.flush(t), "the Engine must return a batch")
			assert.Length(t, h.batches[0].Put, 1, "the abort path must put the placement")
			expect.Equal(t, h.batches[0].Put[0].Replicas, []shard.Replica{primaryC},
				"the abort path of the first step must commit the fallback")
		})

		failed := []struct {
			name string
			give int
		}{
			{name: "ends a record whose failed step is below the first step", give: -1},
			{name: "ends a record whose failed step is past the last step", give: 3},
		}
		for _, tt := range failed {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				p := placement(1, nodeA)
				h := newHarness(t, []shard.Placement{p})
				rec := record(p, nodeA, nodeB, nodeA)
				rec.Phase, rec.Failed = shard.PhaseAborting, tt.give
				h.resume(t, &rec, &p)
				assert.True(t, h.flush(t), "the Engine must return a batch")
				expect.Equal(t, h.batches[0].Done, []shard.ID{p.Shard}, "the batch must end the transition")
				expect.Empty(t, h.batches[0].Put,
					"an abort path that the script does not have must not change the placement")
			})
		}

		t.Run("counts a resumed transition against the bounds", func(t *testing.T) {
			t.Parallel()
			p, q := placement(1, nodeA), placement(2, nodeA)
			h := newHarness(t, []shard.Placement{p, q}, func(s *setup) { s.cfg.MaxTransitions = 1 })
			rec := record(p, nodeA, nodeB, nodeA)
			h.resume(t, &rec, &p)
			assert.False(t, h.start(t, moveOf(q, nodeA, nodeC, nodeA)),
				"Start must refuse a move above the bound of the resumed transitions")
		})

		tests := []struct {
			give func(*store.Transition)
			want error
			name string
		}{
			{
				name: "returns ErrProtocol for a record of another protocol",
				give: func(r *store.Transition) { r.Protocol = otherProtocol },
				want: engine.ErrProtocol,
			},
			{
				name: "returns ErrProtocol for a record of another version of the protocol",
				give: func(r *store.Transition) { r.ProtocolVersion = 2 },
				want: engine.ErrProtocol,
			},
			{
				name: "returns ErrRecord for a record with a negative step",
				give: func(r *store.Transition) { r.Step = -1 },
				want: engine.ErrRecord,
			},
			{
				name: "returns ErrProtocol for a record without a phase",
				give: func(r *store.Transition) { r.Phase = 0 },
				want: engine.ErrProtocol,
			},
			{
				name: "returns ErrProtocol for a record with a phase that the lifecycle does not have",
				give: func(r *store.Transition) { r.Phase = shard.PhaseAborted + 1 },
				want: engine.ErrProtocol,
			},
			{
				name: "returns ErrRecord for a record that the protocol cannot script",
				give: func(r *store.Transition) { r.To = store.Endpoint{} },
				want: engine.ErrRecord,
			},
			{
				name: "returns fsm.ErrRejected for a record in a terminal phase",
				give: func(r *store.Transition) { r.Phase, r.Failed = shard.PhaseAborted, addStep },
				want: fsm.ErrRejected,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				p := placement(1, nodeA)
				h := newHarness(t, []shard.Placement{p})
				rec := record(p, nodeA, nodeB, nodeA)
				tt.give(&rec)
				out := []machine.Effect{foreign}
				got, err := h.engine.Resume(&rec, &p, out)
				expect.That(t, err).ErrorIs(tt.want, "Resume must refuse a record that the Engine cannot continue")
				expect.Equal(t, got, out, "a refused Resume must return out unchanged")
			})
		}

		t.Run("includes the context of a refused event in the error", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			rec := record(p, nodeA, nodeB, nodeA)
			rec.Phase = shard.PhaseAborted
			_, err := h.engine.Resume(&rec, &p, nil)
			assert.ErrorIs(t, err, fsm.ErrRejected, "the phase of an ended transition must refuse Finish")
			expect.That(t, err.Error()).
				Contains(handoff.Finish.String(), "the error must contain the refused event").
				Contains(p.Shard.String(), "the error must contain the shard of the transition").
				Contains(shard.PhaseAborted.String(), "the error must contain the phase of the transition")
		})

		t.Run("returns ErrRecord for a second record of one shard", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			rec := record(p, nodeA, nodeB, nodeA)
			h.resume(t, &rec, &p)
			_, err := h.engine.Resume(&rec, &p, nil)
			assert.ErrorIs(t, err, engine.ErrRecord, "Resume must refuse a second record of a shard")
		})
	})

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("moves the writer of a shard to the destination", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.drain(t)
			got := h.placement(t, p.Shard)
			expect.Equal(t, got.Replicas, []shard.Replica{primaryB}, "the destination must be the only replica")
			expect.Equal(t, got.Epoch, newEpoch, "the destination must write at the new epoch")
			expect.Empty(t, h.records(t), "the end of the transition must remove its record")
		})

		t.Run("sends the steps of a move in the order of the script", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.drain(t)
			assert.Equal(t, calls(h.sent), []call{{protocol.Drop, nodeA.ID}, {protocol.Add, nodeB.ID}},
				"the Engine must drop the source before it adds the destination")
		})

		t.Run("commits each step in its own batch", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.drain(t)
			assert.Equal(t, steps(h.batches), []int{0, 2, done},
				"the batches must commit the first step, the add after the commit step, and the end")
		})

		t.Run("retries a callback that fails with a transient error", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.answer = failures(1, protocol.ErrUnreachable)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.drain(t)
			assert.Length(t, h.sent, 3, "the Engine must send the drop again after its failure")
			expect.Equal(t, h.sent[1], h.sent[0], "the retry must send the same request", expect.ByIdentity())
		})

		t.Run("retries a callback that runs past its timeout", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.answer = failures(1, context.DeadlineExceeded)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.drain(t)
			assert.Length(t, h.sent, 3, "the Engine must send the drop again after its timeout")
		})

		t.Run("waits a backoff that doubles with each retry", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.answer = failures(engine.Attempts-1, protocol.ErrUnreachable)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.drain(t)
			assert.Equal(t, h.waits, []time.Duration{
				50 * time.Millisecond,
				100 * time.Millisecond,
				200 * time.Millisecond,
				400 * time.Millisecond,
			}, "each backoff must be half of a ceiling that doubles from RetryBase")
		})

		t.Run("ends the transition when the drop of the source fails after its attempts", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.answer = failures(engine.Attempts, protocol.ErrUnreachable)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.drain(t)
			expect.Length(t, h.sent, engine.Attempts, "the Engine must stop after its attempts")
			expect.Equal(t, h.placement(t, p.Shard).Replicas, []shard.Replica{primaryA},
				"the empty abort path of the drop must leave the source in place")
			expect.Empty(t, h.records(t), "the end of the transition must remove its record")
		})

		t.Run("fails a step at once for an error that is not transient", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.answer = failures(1, refusal)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.drain(t)
			assert.Length(t, h.sent, 1, "the Engine must not retry a refusal")
		})

		t.Run("starts the abort path of an add that fails", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB, nodeC)
			h.answer = refuse(protocol.Add, nodeB.ID)
			h.start(t, moveOf(p, nodeA, nodeB, nodeC))
			h.drain(t)
			got := h.placement(t, p.Shard)
			expect.Equal(t, got.Replicas, []shard.Replica{primaryC}, "the fallback must be the only replica")
			expect.Equal(t, got.Epoch, abortEpoch, "the fallback must write at the abort epoch")
			expect.Equal(t, calls(h.sent), []call{
				{protocol.Drop, nodeA.ID},
				{protocol.Add, nodeB.ID},
				{protocol.Add, nodeC.ID},
				{protocol.Drop, nodeB.ID},
			}, "the abort path must add the fallback before it drops the destination")
		})

		t.Run("commits the phase of the abort path with its first commit", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB, nodeC)
			h.answer = refuse(protocol.Add, nodeB.ID)
			h.start(t, moveOf(p, nodeA, nodeB, nodeC))
			h.drain(t)
			assert.Length(t, h.batches, 5, "the transition must commit five batches")
			assert.Length(t, h.batches[2].Transitions, 1, "the third batch must commit the record")
			rec := h.batches[2].Transitions[0]
			expect.Equal(t, rec.Phase, shard.PhaseAborting, "the record must be in the phase of the abort path")
			expect.Equal(t, rec.Failed, addStep, "the record must name the add as the failed step")
			expect.Equal(t, rec.Step, 1, "the record must be at the add of the abort path")
		})

		t.Run("ends a transition without a writer when its abort path has no fallback", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.answer = refuse(protocol.Add, nodeB.ID)
			h.start(t, moveOf(p, nodeA, nodeB, shard.Node{}))
			h.drain(t)
			got := h.placement(t, p.Shard)
			expect.Empty(t, got.Replicas, "the placement must not have a replica on a node of the move")
			expect.Equal(t, got.Epoch, epoch.Zero, "the placement must not have a writer")
			expect.Empty(t, h.records(t), "the end of the transition must remove its record")
		})

		t.Run("abandons an abort path whose step fails definitively", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB, nodeC)
			h.answer = refuse(protocol.Add, "")
			h.start(t, moveOf(p, nodeA, nodeB, nodeC))
			h.drain(t)
			got := h.placement(t, p.Shard)
			expect.Empty(t, got.Replicas, "the placement must not have a replica on a node of the move")
			expect.Equal(t, got.Epoch, epoch.Zero, "the placement must not have a writer")
			expect.Equal(t, got.Reserved, abortEpoch, "the end must keep the reserved epoch")
			expect.Empty(t, h.records(t), "the end of the transition must remove its record")
		})

		t.Run("reports the destination of an add that fails definitively", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB, nodeC)
			h.answer = refuse(protocol.Add, nodeB.ID)
			h.start(t, moveOf(p, nodeA, nodeB, nodeC))
			h.drain(t)
			assert.Equal(t, h.refusals, []refusalOf{{at: endpoint(nodeB), sid: p.Shard}},
				"the Engine must report the destination of the refused add")
		})

		t.Run("reports the fallback of an abort path whose add fails definitively", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB, nodeC)
			h.answer = refuse(protocol.Add, "")
			h.start(t, moveOf(p, nodeA, nodeB, nodeC))
			h.drain(t)
			assert.Equal(
				t,
				h.refusals,
				[]refusalOf{{at: endpoint(nodeB), sid: p.Shard}, {at: endpoint(nodeC), sid: p.Shard}},
				"the Engine must report the destination and then the fallback",
			)
		})

		t.Run("does not report a drop that fails definitively", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.answer = refuse(protocol.Drop, nodeA.ID)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.drain(t)
			assert.Empty(t, h.refusals, "the Engine must report only a refused add")
		})

		t.Run("does not report an add that fails after its attempts", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB, nodeC)
			h.answer = func(r *protocol.Request) error {
				if r.Call == protocol.Add && r.Node == nodeB.ID {
					return protocol.ErrUnreachable
				}
				return nil
			}
			h.start(t, moveOf(p, nodeA, nodeB, nodeC))
			h.drain(t)
			assert.Empty(t, h.refusals, "an add that a retry could fix must not be reported")
		})

		t.Run("runs a step of an abort path again after RetryMax", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB, nodeC)
			unreachable := failures(engine.Attempts, protocol.ErrUnreachable)
			h.answer = func(r *protocol.Request) error {
				if r.Node == nodeB.ID && r.Call == protocol.Add {
					return refusal
				}
				if r.Node == nodeC.ID {
					return unreachable(r)
				}
				return nil
			}
			h.start(t, moveOf(p, nodeA, nodeB, nodeC))
			h.drain(t)
			expect.Contains(t, h.waits, engine.RetryMax, "the Engine must wait RetryMax before the next round")
			expect.Equal(t, h.placement(t, p.Shard).Replicas, []shard.Replica{primaryC},
				"the next round must add the fallback")
			expect.Empty(t, h.records(t), "the transition must end after the next round")
		})

		t.Run("grants retries above MinRetries within RetryBudget of the callbacks", func(t *testing.T) {
			t.Parallel()
			ps := placements(warmMoves+failingMoves, nodeA)
			h := newHarness(t, ps, func(s *setup) { s.cfg.MaxNodeTransitions = maxTransitions })
			h.view(t, nodeA, nodeB)
			for _, p := range ps[:warmMoves] {
				h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			}
			h.drain(t)
			retries := failingMoves * (engine.Attempts - 1)
			h.answer = failures(retries, protocol.ErrUnreachable)
			for _, p := range ps[warmMoves:] {
				h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			}
			h.drain(t)
			assert.Length(t, h.waits, retries, "the budget must grant a retry for each failure")
		})

		tests := []struct {
			give func(machine.Token) machine.Event
			name string
		}{
			{
				name: "reports false for a Called of another token",
				give: func(tok machine.Token) machine.Event { return machine.Called{Token: tok} },
			},
			{
				name: "reports false for an Elapsed of another token",
				give: func(tok machine.Token) machine.Event { return machine.Elapsed{Token: tok} },
			},
			{
				name: "reports false for an event that an Engine does not take",
				give: func(machine.Token) machine.Event { return machine.Started{} },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				p := placement(1, nodeA)
				h := newHarness(t, []shard.Placement{p})
				h.view(t, nodeA, nodeB)
				h.start(t, moveOf(p, nodeA, nodeB, nodeA))
				h.flush(t)
				out := []machine.Effect{foreign}
				got, ok := h.offer(t, tt.give(h.tokens+1), out)
				expect.False(t, ok, "Step must not take an event that the Engine does not wait for")
				expect.Equal(t, got, out, "Step must return out unchanged for an event that it does not take")
			})
		}

		t.Run("runs every move to its end within its bounds", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "an Engine runs every move to its end within its bounds", exercise,
				prop.Require(failedCall, faultShare),
				prop.Require(refusedCall, faultShare),
				prop.Require(restart, faultShare),
				prop.Require(leave, faultShare),
			)
		})

		t.Run("reports false for the result of a callback that it has taken", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			ev := machine.Called{Token: h.calls[0].Token}
			h.answerOne(t)
			_, ok := h.offer(t, ev, nil)
			assert.False(t, ok, "Step must take the result of a callback once")
		})

		t.Run("reports false for a Timer that it has taken", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.answer = failures(1, protocol.ErrUnreachable)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			h.answerOne(t)
			assert.Length(t, h.timers, 1, "the failure must arm a backoff")
			ev := machine.Elapsed{Token: h.timers[0].Token}
			h.elapseOne(t)
			_, ok := h.offer(t, ev, nil)
			assert.False(t, ok, "Step must take the end of a Timer once")
		})

		t.Run("returns out unchanged when the lock delay of a drop elapses", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p}, func(s *setup) { s.cfg.LockDelay = lockDelay })
			h.view(t, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			assert.Length(t, h.timers, 1, "the drop must wait for the lock delay")
			out := []machine.Effect{foreign}
			got, ok := h.offer(t, machine.Elapsed{Token: h.timers[0].Token}, out)
			assert.True(t, ok, "Step must take the end of the lock delay")
			expect.Equal(t, got, out, "the end of the lock delay must return out unchanged")
		})
	})

	t.Run("View", func(t *testing.T) {
		t.Parallel()

		t.Run("sends the callbacks that wait for the first view", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			assert.Empty(t, h.calls, "the Engine must not send a callback before the first view")
			h.view(t, nodeA, nodeB)
			assert.Length(t, h.calls, 1, "the first view must send the drop")
			expect.Equal(t, *h.calls[0].Node, nodeA, "the callback must address the node of the view")
		})

		t.Run("completes a drop when the node has a later incarnation", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			h.view(t, restarted(nodeA), nodeB)
			h.calls = nil
			h.drain(t)
			expect.Equal(t, calls(h.sent), []call{{protocol.Drop, nodeA.ID}, {protocol.Add, nodeB.ID}},
				"the drop must complete without its answer")
			expect.Equal(t, h.placement(t, p.Shard).Replicas, []shard.Replica{primaryB},
				"the move must complete")
		})

		t.Run("completes a drop without the lock delay when the node has a later incarnation", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p}, func(s *setup) { s.cfg.LockDelay = lockDelay })
			h.view(t, nodeA, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			h.view(t, restarted(nodeA), nodeB)
			assert.Empty(t, h.timers, "the drop must not wait for the lock delay")
		})

		t.Run("keeps the lock delay of a drop across views", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p}, func(s *setup) { s.cfg.LockDelay = lockDelay })
			h.view(t, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			h.view(t, nodeB, nodeC)
			assert.Length(t, h.timers, 1, "a later view must not arm a second lock delay")
			_, ok := h.offer(t, machine.Elapsed{Token: h.timers[0].Token}, nil)
			assert.True(t, ok, "the first lock delay must complete the drop")
		})

		t.Run("returns out unchanged when it ends a queued drop", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			out := []machine.Effect{foreign}
			v := &membership.View{Scope: cellScope, Nodes: []shard.Node{nodeB}, Incarnation: incA}
			got, err := h.engine.View(v, out)
			assert.NoError(t, err, "View must take a view of the tests")
			assert.Equal(t, got, out, "View must return out unchanged for a drop that completes")
		})

		t.Run("completes a drop after the lock delay when the node has left the view", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p}, func(s *setup) { s.cfg.LockDelay = lockDelay })
			h.view(t, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.drain(t)
			expect.Equal(t, calls(h.sent), []call{{protocol.Add, nodeB.ID}},
				"the drop must complete without a callback")
			expect.Equal(t, h.waits, []time.Duration{lockDelay}, "the drop must wait for the lock delay")
		})

		t.Run("completes a drop at once when the node has left the view without a lock delay", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.drain(t)
			expect.Equal(t, calls(h.sent), []call{{protocol.Add, nodeB.ID}},
				"the drop must complete without a callback")
			expect.Empty(t, h.waits, "the drop must not wait")
		})

		t.Run("fails an add whose node has ended the incarnation", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB, nodeC)
			h.start(t, moveOf(p, nodeA, nodeB, nodeC))
			h.flush(t)
			h.answerOne(t)
			h.flush(t)
			assert.Length(t, h.calls, 1, "the add must be in flight")
			assert.Equal(t, h.calls[0].Request.Call, protocol.Add, "the callback in flight must be the add")
			h.view(t, nodeA, restarted(nodeB), nodeC)
			h.calls = nil
			h.drain(t)
			expect.Equal(t, h.placement(t, p.Shard).Replicas, []shard.Replica{primaryC},
				"the abort path must make the fallback the writer")
		})

		t.Run("reports an add whose node has ended the incarnation", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB, nodeC)
			h.start(t, moveOf(p, nodeA, nodeB, nodeC))
			h.flush(t)
			h.answerOne(t)
			h.flush(t)
			h.view(t, nodeA, restarted(nodeB), nodeC)
			assert.Equal(t, h.refusals, []refusalOf{{at: endpoint(nodeB), sid: p.Shard}},
				"the Engine must report the add on the ended incarnation")
		})

		t.Run("cancels the retry of a callback whose node has ended the incarnation", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.answer = failures(1, protocol.ErrUnreachable)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			h.answerOne(t)
			assert.Length(t, h.timers, 1, "the failure must arm a backoff")
			ev := machine.Elapsed{Token: h.timers[0].Token}
			h.view(t, restarted(nodeA), nodeB)
			_, ok := h.offer(t, ev, nil)
			assert.False(t, ok, "the end of the incarnation must cancel the backoff")
		})

		t.Run("cancels the round of a step whose node has ended the incarnation", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB, nodeC)
			h.answer = func(r *protocol.Request) error {
				if r.Call == protocol.Add {
					return protocol.ErrUnreachable
				}
				return nil
			}
			h.start(t, moveOf(p, nodeA, nodeB, nodeC))
			for range maxDeliveries {
				if len(h.timers) > 0 && h.timers[0].Duration == engine.RetryMax || !h.next(t) {
					break
				}
			}
			assert.Length(t, h.timers, 1, "the failed round must arm the wait of the next round")
			assert.Equal(t, h.timers[0].Duration, engine.RetryMax, "the next round must wait RetryMax")
			ev := machine.Elapsed{Token: h.timers[0].Token}
			h.view(t, nodeA, nodeB)
			_, ok := h.offer(t, ev, nil)
			assert.False(t, ok, "the end of the incarnation must cancel the next round")
		})
	})

	t.Run("Batch", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil without a change", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, nil)
			assert.Nil(t, h.engine.Batch(), "Batch must return nil without a change")
		})

		t.Run("returns nil while a batch is in flight", func(t *testing.T) {
			t.Parallel()
			p, q := placement(1, nodeA), placement(2, nodeA)
			h := newHarness(t, []shard.Placement{p, q})
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			assert.NotNil(t, h.engine.Batch(), "Batch must return the first batch")
			h.start(t, moveOf(q, nodeA, nodeB, nodeA))
			assert.Nil(t, h.engine.Batch(), "Batch must return nil while a batch is in flight")
		})

		t.Run("commits the changes of the transitions in one batch", func(t *testing.T) {
			t.Parallel()
			ps := placements(3, nodeA)
			h := newHarness(t, ps)
			for _, p := range ps {
				h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			}
			assert.True(t, h.flush(t), "the Engine must return a batch")
			assert.Length(t, h.batches[0].Transitions, 3, "one batch must commit the records of every move")
		})

		t.Run("splits the changes where Fits refuses a larger batch", func(t *testing.T) {
			t.Parallel()
			ps := placements(3, nodeA)
			h := newHarness(t, ps, func(s *setup) {
				s.fits = func(m *store.Mutation) error {
					if len(m.Put) > 2 {
						return store.ErrTooLarge
					}
					return nil
				}
			})
			for _, p := range ps {
				h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			}
			for h.flush(t) {
			}
			assert.Length(t, h.batches, 2, "the changes must take two batches")
			expect.Length(t, h.batches[0].Transitions, 2, "the first batch must take the changes that fit")
			expect.Length(t, h.batches[0].Put, 2, "the first batch must not put the placement of a later change")
			expect.Length(t, h.batches[1].Transitions, 1, "the second batch must take the rest")
		})

		t.Run("puts the first change in a batch that Fits refuses", func(t *testing.T) {
			t.Parallel()
			ps := placements(2, nodeA)
			h := newHarness(t, ps, func(s *setup) { s.fits = func(*store.Mutation) error { return store.ErrTooLarge } })
			for _, p := range ps {
				h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			}
			m := h.engine.Batch()
			assert.NotNil(t, m, "Batch must return a batch")
			assert.Length(t, m.Transitions, 1, "a batch must take its first change")
		})

		t.Run("returns the end of a move without its placement", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.drain(t)
			assert.Length(t, h.batches, 3, "the move must commit three batches")
			expect.Empty(t, h.batches[2].Put, "the batch that ends the move must not put the placement again")
		})
	})

	t.Run("Configure", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for nil settings", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, nil)
			out := []machine.Effect{foreign}
			got, err := h.engine.Configure(nil, out)
			expect.That(t, err).ErrorIs(engine.ErrConfig, "Configure must refuse nil settings")
			expect.Equal(t, got, out, "a refused Configure must return out unchanged")
		})

		tests := []struct {
			give func(*engine.Settings)
			name string
		}{
			{
				name: "returns ErrConfig for settings without a protocol",
				give: func(s *engine.Settings) { s.Protocol = nil },
			},
			{
				name: "returns ErrConfig for a lock delay below zero",
				give: func(s *engine.Settings) { s.LockDelay = -time.Second },
			},
			{
				name: "returns ErrConfig for a bound of the transitions below one",
				give: func(s *engine.Settings) { s.MaxTransitions = 0 },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, nil)
				s := settings(t)
				tt.give(&s)
				out := []machine.Effect{foreign}
				got, err := h.engine.Configure(&s, out)
				expect.That(t, err).ErrorIs(engine.ErrConfig, "Configure must refuse settings that New refuses")
				expect.Equal(t, got, out, "a refused Configure must return out unchanged")
			})
		}

		t.Run("sends the queued callbacks that a higher NodeCallbacks allows", func(t *testing.T) {
			t.Parallel()
			ps := placements(2, nodeA)
			h := newHarness(t, ps, func(s *setup) { s.cfg.NodeCallbacks = 1 })
			h.view(t, nodeA, nodeB)
			for _, p := range ps {
				h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			}
			h.flush(t)
			assert.Length(t, h.calls, 1, "the second drop must wait for a permit")
			s := settings(t)
			out, err := h.engine.Configure(&s, []machine.Effect{foreign})
			assert.NoError(t, err, "Configure must take valid settings")
			h.route(out)
			expect.Equal(t, out[0], foreign, "Configure must keep the effects in out")
			expect.Length(t, h.calls, 2, "the new permits must send the queued drop")
		})

		t.Run("returns out unchanged before the first view", func(t *testing.T) {
			t.Parallel()
			ps := placements(2, nodeA)
			h := newHarness(t, ps, func(s *setup) { s.cfg.NodeCallbacks = 1 })
			for _, p := range ps {
				h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			}
			h.flush(t)
			s := settings(t)
			out := []machine.Effect{foreign}
			got, err := h.engine.Configure(&s, out)
			assert.NoError(t, err, "Configure must take valid settings")
			assert.Equal(t, got, out, "Configure must not send a callback before the first view")
		})

		t.Run("starts a move with the protocol of the new settings", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			s := settings(t)
			s.Protocol = scripted{script: abortable}
			_, err := h.engine.Configure(&s, nil)
			assert.NoError(t, err, "Configure must take valid settings")
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			assert.Length(t, h.calls, 1, "the first batch must send the first step of the new script")
			expect.Equal(t, h.calls[0].Request.Call, protocol.Add, "the first step of the new script must be the add")
		})

		t.Run("applies a lower MaxTransitions to the next move", func(t *testing.T) {
			t.Parallel()
			p, q := placement(1, nodeA), placement(2, nodeA)
			h := newHarness(t, []shard.Placement{p, q})
			s := settings(t)
			s.MaxTransitions = 1
			_, err := h.engine.Configure(&s, nil)
			assert.NoError(t, err, "Configure must take valid settings")
			assert.True(t, h.start(t, moveOf(p, nodeA, nodeB, nodeA)), "Start must admit the first move")
			assert.False(t, h.start(t, moveOf(q, nodeA, nodeC, nodeA)), "Start must refuse a move above the new bound")
		})

		t.Run("applies a lower MaxNodeTransitions to the next move", func(t *testing.T) {
			t.Parallel()
			p, q := placement(1, nodeA), placement(2, nodeC)
			h := newHarness(t, []shard.Placement{p, q})
			s := settings(t)
			s.MaxNodeTransitions = 1
			_, err := h.engine.Configure(&s, nil)
			assert.NoError(t, err, "Configure must take valid settings")
			assert.True(t, h.start(t, moveOf(p, nodeA, nodeB, nodeA)), "Start must admit the first move")
			assert.False(t, h.start(t, moveOf(q, nodeC, nodeB, nodeC)),
				"Start must refuse a move above the new bound of a node")
		})

		t.Run("applies the lock delay to the next drop on a node that has left the view", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			s := settings(t)
			s.LockDelay = lockDelay
			_, err := h.engine.Configure(&s, nil)
			assert.NoError(t, err, "Configure must take valid settings")
			h.view(t, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			assert.Length(t, h.timers, 1, "the drop must wait for the new lock delay")
			expect.Equal(t, h.timers[0].Duration, lockDelay, "the wait must be the new lock delay")
		})
	})

	t.Run("Applied", func(t *testing.T) {
		t.Parallel()

		t.Run("returns out unchanged without a batch in flight", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, nil)
			out := []machine.Effect{foreign}
			got, err := h.engine.Applied(out)
			assert.NoError(t, err, "Applied must take the end of a batch")
			assert.Equal(t, got, out, "Applied must return out unchanged without a batch")
		})

		t.Run("takes the token of a callback from the counter of the machine", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.flush(t)
			assert.Length(t, h.calls, 1, "the first batch must send the drop")
			expect.Equal(t, h.calls[0].Token, taken+1,
				"the callback must take the token after the last one of the machine")
			expect.Equal(t, h.tokens, taken+1, "the Engine must advance the counter of the machine")
		})

		t.Run("removes a transition that the batch ended", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p}, func(s *setup) { s.cfg.MaxTransitions = 1 })
			h.view(t, nodeA, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.drain(t)
			assert.True(t, h.start(t, moveOf(h.placement(t, p.Shard), nodeB, nodeA, nodeB)),
				"Start must admit a move of a shard whose transition has ended")
		})
	})

	t.Run("Inbound", func(t *testing.T) {
		t.Parallel()

		primary := []shard.Role{shard.RolePrimary}
		tests := []struct {
			name      string
			giveNode  shard.NodeID
			giveRoles []shard.Role
			want      int
		}{
			{
				name:      "counts a transition whose destination is on the node",
				giveNode:  nodeB.ID,
				giveRoles: primary,
				want:      1,
			},
			{
				name:      "counts a transition whose fallback is on the node",
				giveNode:  nodeA.ID,
				giveRoles: primary,
				want:      1,
			},
			{name: "returns zero for a node without a transition", giveNode: nodeC.ID, giveRoles: primary},
			{name: "returns zero for a destination in a role outside the roles", giveNode: nodeB.ID},
			{name: "returns zero for a fallback in a role outside the roles", giveNode: nodeA.ID},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				p := placement(1, nodeA)
				h := newHarness(t, []shard.Placement{p})
				assert.True(t, h.start(t, moveOf(p, nodeA, nodeB, nodeA)), "Start must admit a move within the bounds")
				assert.Equal(t, h.engine.Inbound(tt.giveNode, tt.giveRoles), tt.want,
					"Inbound must count the transitions that can end with a replica on the node")
			})
		}

		t.Run("returns zero once the transition has ended", func(t *testing.T) {
			t.Parallel()
			p := placement(1, nodeA)
			h := newHarness(t, []shard.Placement{p})
			h.view(t, nodeA, nodeB)
			h.start(t, moveOf(p, nodeA, nodeB, nodeA))
			h.drain(t)
			assert.Equal(t, h.engine.Inbound(nodeB.ID, []shard.Role{shard.RolePrimary}), 0,
				"Inbound must not count a transition whose last batch has applied")
		})
	})
}

// exercise runs the steps of the case c over an Engine of the shards of the property. The
// shards start on the nodes A, B and C in round-robin order. The steps start moves, commit
// batches, reply to callbacks with success or a failure, and perform Timers. Other steps
// restart nodes, and move nodes out of the view and back into it. At each step, a node and
// a transition each have at most one callback in flight, and at most fewTransitions
// transitions are in flight. Once the nodes are back and the agents reply with success,
// every transition must end. Each placement must then have at most one writer. The writer
// must have an epoch that a transition of its shard reserved.
func exercise(c *prop.Case) {
	m := &model{
		flight:  make(map[shard.ID]bool),
		phases:  make(map[shard.ID]shard.Phase),
		started: make(map[shard.ID]uint64),
		nodes:   []shard.Node{nodeA, nodeB, nodeC},
		present: []bool{true, true, true},
	}
	ps := make([]shard.Placement, 0, shards)
	for i := range shards {
		p := placement(i+1, m.nodes[i%len(m.nodes)])
		ps = append(ps, p)
		m.shards = append(m.shards, p.Shard)
	}
	m.h = newHarness(c, ps, func(s *setup) {
		s.cfg.MaxTransitions, s.cfg.MaxNodeTransitions = fewTransitions, fewNodeTransitions
		s.cfg.NodeCallbacks, s.cfg.LockDelay = fewCallbacks, lockDelay
	})
	m.view(c)
	calling := func(struct{}) bool { return len(m.h.calls) > 0 }
	timing := func(struct{}) bool { return len(m.h.timers) > 0 }
	stateful.Steps(c, stateful.Machine[struct{}]{
		Actions: []stateful.Action[struct{}]{
			{Name: "start", Run: func(c *prop.Case, _ int, _ any) { m.start(c) }},
			{Name: "commit", Weight: commits, Run: func(c *prop.Case, _ int, _ any) { m.commit(c) }},
			{Name: "answer", Weight: answers, Enabled: calling, Run: func(c *prop.Case, _ int, _ any) { m.answer(c) }},
			{Name: "elapse", Enabled: timing, Run: func(c *prop.Case, _ int, _ any) { m.elapse(c) }},
			{Name: "restart", Run: func(c *prop.Case, _ int, _ any) { m.restart(c) }},
			{Name: "leave", Run: func(c *prop.Case, _ int, _ any) { m.leave(c) }},
			{Name: "join", Run: func(c *prop.Case, _ int, _ any) { m.join(c) }},
		},
		Invariant: func(c *prop.Case, _ struct{}) { m.invariant(c) },
		Settle:    func(c *prop.Case, _ struct{}) { m.settle(c) },
	}, stateful.Swarm(false))
}

// view passes the nodes that are present to View.
func (m *model) view(c *prop.Case) {
	nodes := make([]shard.Node, 0, len(m.nodes))
	for i, n := range m.nodes {
		if m.present[i] {
			nodes = append(nodes, n)
		}
	}
	m.h.view(c, nodes...)
}

// endpoint returns the endpoint of the primary replica on the node i at its latest
// incarnation, and the zero Endpoint for the index -1.
func (m *model) endpoint(i int) store.Endpoint {
	if i < 0 {
		return store.Endpoint{}
	}
	return endpoint(m.nodes[i])
}

// writer returns the endpoint of the writer of p at the latest incarnation of its node, and
// the zero Endpoint for a placement without a writer.
func (m *model) writer(p shard.Placement) store.Endpoint {
	for _, r := range p.Replicas {
		if r.Role == shard.RolePrimary {
			return m.endpoint(slices.IndexFunc(m.nodes, func(n shard.Node) bool { return n.ID == r.Node }))
		}
	}
	return store.Endpoint{}
}

// start starts a move of a shard that c draws to a node that c draws, with a fallback that
// c draws. It does nothing for a shard with a transition in flight.
func (m *model) start(c *prop.Case) {
	s := m.shards[c.Draw(prop.Integer(0, shards-1), "shard")]
	to := c.Draw(prop.Integer(0, len(m.nodes)-1), "to")
	fallback := c.Draw(prop.Integer(-1, len(m.nodes)-1), "fallback")
	if m.flight[s] {
		return
	}
	p := m.h.placement(c, s)
	move := engine.Move{
		From:        m.writer(p),
		To:          m.endpoint(to),
		Fallback:    m.endpoint(fallback),
		Placement:   p,
		SpecVersion: specVersion,
	}
	ok, err := m.h.engine.Start(&move)
	assert.NoError(c, err, "Start must take a move of the property")
	if ok {
		m.flight[s] = true
		m.started[s]++
	}
}

// commit commits the next batch, and checks that each change of phase is an edge of the
// lifecycle. It reports false when the Engine has no batch.
func (m *model) commit(c *prop.Case) bool {
	if !m.h.flush(c) {
		return false
	}
	batch := m.h.batches[len(m.h.batches)-1]
	for _, t := range batch.Transitions {
		s := t.Operation.Shard
		if prev, ok := m.phases[s]; ok {
			assert.True(c, handoff.Lifecycle.Allows(prev, t.Phase),
				"a batch must change the phase of a transition along an edge of the lifecycle")
		}
		m.phases[s] = t.Phase
	}
	for _, s := range batch.Done {
		delete(m.phases, s)
		delete(m.flight, s)
	}
	return true
}

// answer replies to the oldest callback in flight with an outcome that c draws. The outcome
// is success, a transient error, the end of the timeout of the callback or a refusal.
func (m *model) answer(c *prop.Case) {
	var err error
	switch c.Draw(prop.Integer(0, 3), "answer") {
	case 1:
		c.Classify(failedCall)
		err = protocol.ErrUnreachable
	case 2:
		c.Classify(failedCall)
		err = context.DeadlineExceeded
	case 3:
		c.Classify(refusedCall)
		err = refusal
	}
	m.reply(c, err)
}

// reply passes err to the Engine as the result of the oldest callback in flight. It reports
// false without a callback in flight. A runtime passes the result of a callback that the
// Engine has cancelled, so reply passes it too.
func (m *model) reply(c *prop.Case, err error) bool {
	if len(m.h.calls) == 0 {
		return false
	}
	call := m.h.calls[0]
	m.h.calls = slices.Delete(m.h.calls, 0, 1)
	out, _ := m.h.offer(c, machine.Called{Err: err, Token: call.Token}, nil)
	m.h.route(out)
	return true
}

// elapse performs the oldest Timer in flight, and reports false without one. A runtime
// passes the end of a Timer that the Engine has cancelled, so elapse passes it too.
func (m *model) elapse(c *prop.Case) bool {
	if len(m.h.timers) == 0 {
		return false
	}
	tm := m.h.timers[0]
	m.h.timers = slices.Delete(m.h.timers, 0, 1)
	out, _ := m.h.offer(c, machine.Elapsed{Token: tm.Token}, nil)
	m.h.route(out)
	return true
}

// restart starts the next incarnation of a node that c draws.
func (m *model) restart(c *prop.Case) {
	i := c.Draw(prop.Integer(0, len(m.nodes)-1), "node")
	c.Classify(restart)
	m.nodes[i].Incarnation++
	m.view(c)
}

// leave takes a node that c draws out of the view.
func (m *model) leave(c *prop.Case) {
	i := c.Draw(prop.Integer(0, len(m.nodes)-1), "node")
	c.Classify(leave)
	m.present[i] = false
	m.view(c)
}

// join brings a node that c draws back into the view, at its next incarnation.
func (m *model) join(c *prop.Case) {
	i := c.Draw(prop.Integer(0, len(m.nodes)-1), "node")
	if !m.present[i] {
		m.present[i] = true
		m.nodes[i].Incarnation++
	}
	m.view(c)
}

// invariant checks the bounds of the callbacks and of the transitions in flight. A callback
// to an incarnation that has ended is no longer in flight, because the Engine cancelled it.
func (m *model) invariant(c *prop.Case) {
	live := func(r *protocol.Request) bool {
		i := slices.IndexFunc(m.nodes, func(n shard.Node) bool { return n.ID == r.Node })
		return m.present[i] && m.nodes[i].Incarnation == r.NodeIncarnation
	}
	for _, n := range m.nodes {
		count := 0
		for _, call := range m.h.calls {
			if call.Request.Node == n.ID && live(call.Request) {
				count++
			}
		}
		assert.InRange(c, count, 0, fewCallbacks, "a node must have at most NodeCallbacks callbacks in flight")
	}
	for _, s := range m.shards {
		count := 0
		for _, call := range m.h.calls {
			if call.Request.Shard == s && live(call.Request) {
				count++
			}
		}
		assert.InRange(c, count, 0, 1, "a transition must have at most one callback in flight")
	}
	assert.InRange(c, len(m.flight), 0, fewTransitions, "at most MaxTransitions transitions may be in flight")
}

// settle brings every node back into the view. It then replies to every callback with
// success until the Engine has nothing left to do. Every transition must then have ended.
func (m *model) settle(c *prop.Case) {
	for i := range m.nodes {
		if !m.present[i] {
			m.present[i] = true
			m.nodes[i].Incarnation++
		}
	}
	m.view(c)
	for range maxDeliveries {
		if m.commit(c) || m.reply(c, nil) || m.elapse(c) {
			continue
		}
		assert.Empty(c, m.flight, "every transition must end")
		assert.Empty(c, m.h.records(c), "the store must not keep a record of a transition")
		for _, s := range m.shards {
			p := m.h.placement(c, s)
			writers := 0
			for _, r := range p.Replicas {
				if r.Role == shard.RolePrimary {
					writers++
				}
			}
			assert.InRange(c, writers, 0, 1, "a placement must have at most one writer")
			assert.InRange(c, float64(p.Epoch), 0, float64(p.Reserved),
				"the writer must have an epoch that a transition of the shard reserved")
			assert.Equal(c, p.Sequence, uint64(placed)+m.started[s], "each transition must take the next sequence")
		}
		return
	}
	c.Fatalf("the Engine still runs after %d deliveries", maxDeliveries)
}

// settings returns the settings of the Engine of the tests: break-before-make with the
// timeouts of the tests, no lock delay, and the bounds of the tests.
func settings(tb assert.TB) engine.Settings {
	tb.Helper()
	protocol, err := breakbeforemake.New(breakbeforemake.Config{
		AddTimeout:  shard.Duration(addTimeout),
		DropTimeout: shard.Duration(dropTimeout),
	})
	assert.NoError(tb, err, "the protocol of the tests must build")
	return engine.Settings{
		Protocol:           protocol,
		MaxTransitions:     maxTransitions,
		MaxNodeTransitions: maxNodeTransitions,
		NodeCallbacks:      nodeCallbacks,
	}
}

// placement returns the placement of the shard n of the tests with its writer on the
// incarnation of the node on, at the epoch, the reserved epoch and the sequence placed.
func placement(n int, on shard.Node) shard.Placement {
	pos := binary.BigEndian.AppendUint32(nil, uint32(n))
	return shard.Placement{
		Shard:       shard.ID{ID128: id.ID128{0: tagShard, 12: pos[0], 13: pos[1], 14: pos[2], 15: pos[3]}},
		Range:       shard.Range{Start: pos},
		Replicas:    []shard.Replica{{Node: on.ID, Role: shard.RolePrimary}},
		Epoch:       placed,
		Incarnation: on.Incarnation,
		Reserved:    placed,
		Sequence:    uint64(placed),
	}
}

// placements returns the placements of the shards 1 to n of the tests on the node on.
func placements(n int, on shard.Node) []shard.Placement {
	ps := make([]shard.Placement, 0, n)
	for i := range n {
		ps = append(ps, placement(i+1, on))
	}
	return ps
}

// endpoint returns the endpoint of the primary replica on n, at its incarnation. It
// returns the zero Endpoint for the zero Node.
func endpoint(n shard.Node) store.Endpoint {
	if n.ID == "" {
		return store.Endpoint{}
	}
	return store.Endpoint{Node: n.ID, Incarnation: n.Incarnation, Role: shard.RolePrimary}
}

// moveOf returns the move of the replica of p from the node from to the node to, with the
// fallback fallback.
func moveOf(p shard.Placement, from, to, fallback shard.Node) engine.Move {
	return engine.Move{
		Placement:   p,
		From:        endpoint(from),
		To:          endpoint(to),
		Fallback:    endpoint(fallback),
		SpecVersion: specVersion,
	}
}

// record returns the record of the move of p from the node from to the node to, with the
// fallback fallback, at its first step.
func record(p shard.Placement, from, to, fallback shard.Node) store.Transition {
	return store.Transition{
		Started:         origin,
		Protocol:        breakbeforemake.Name,
		Fallback:        endpoint(fallback),
		From:            endpoint(from),
		To:              endpoint(to),
		Operation:       shard.OperationID{Incarnation: incA, Shard: p.Shard, Sequence: p.Sequence + 1},
		SpecVersion:     specVersion,
		OldEpoch:        p.Epoch,
		NewEpoch:        p.Reserved + 1,
		AbortEpoch:      p.Reserved + 2,
		ProtocolVersion: 1,
		Phase:           shard.PhaseRunning,
	}
}

// restarted returns n at its next incarnation.
func restarted(n shard.Node) shard.Node {
	n.Incarnation++
	return n
}

// failures returns an answer that fails the first n requests with err. The answer replies
// to every later request with success.
func failures(n int, err error) func(*protocol.Request) error {
	failed := 0
	return func(*protocol.Request) error {
		if failed < n {
			failed++
			return err
		}
		return nil
	}
}

// refuse returns an answer that refuses each callback c to the node on, and to any node
// for the empty on. It replies to every other request with success.
func refuse(c protocol.Call, on shard.NodeID) func(*protocol.Request) error {
	return func(r *protocol.Request) error {
		if r.Call == c && (on == "" || r.Node == on) {
			return refusal
		}
		return nil
	}
}

// calls returns the kind and the node of each request of rs.
func calls(rs []*protocol.Request) []call {
	cs := make([]call, 0, len(rs))
	for _, r := range rs {
		cs = append(cs, call{call: r.Call, node: r.Node})
	}
	return cs
}

// steps returns the step of the record that each batch of ms commits, and done for a batch
// that ends its transition. Each batch must have one change.
func steps(ms []*store.Mutation) []int {
	ss := make([]int, 0, len(ms))
	for _, m := range ms {
		if len(m.Done) > 0 {
			ss = append(ss, done)
			continue
		}
		for _, t := range m.Transitions {
			ss = append(ss, t.Step)
		}
	}
	return ss
}

// start passes m to Start, and reports whether Start admitted it.
func (h *harness) start(tb assert.TB, m engine.Move) bool {
	tb.Helper()
	ok, err := h.engine.Start(&m)
	assert.NoError(tb, err, "Start must take a move of the tests")
	return ok
}

// resume passes rec and p to Resume, and keeps the effects that it appends.
func (h *harness) resume(tb assert.TB, rec *store.Transition, p *shard.Placement) {
	tb.Helper()
	out, err := h.engine.Resume(rec, p, nil)
	assert.NoError(tb, err, "Resume must take a record of the tests")
	h.route(out)
}

// view passes the view of nodes to View, and keeps the effects that it appends.
func (h *harness) view(tb assert.TB, nodes ...shard.Node) {
	tb.Helper()
	v := &membership.View{Scope: cellScope, Nodes: slices.Clone(nodes), Incarnation: incA}
	out, err := h.engine.View(v, nil)
	assert.NoError(tb, err, "View must take a view of the tests")
	h.route(out)
}

// route keeps the callbacks and the Timers of effects to perform later.
func (h *harness) route(effects []machine.Effect) {
	for _, e := range effects {
		switch e := e.(type) {
		case machine.Callback:
			h.calls = append(h.calls, e)
			h.sent = append(h.sent, e.Request)
		case machine.Timer:
			h.timers = append(h.timers, e)
		}
	}
}

// flush commits the next batch of the Engine, and reports it as applied. It reports false
// when the Engine has no batch.
func (h *harness) flush(tb assert.TB) bool {
	tb.Helper()
	m := h.engine.Batch()
	if m == nil {
		return false
	}
	h.commit(tb, m)
	h.batches = append(h.batches, m)
	out, err := h.engine.Applied(nil)
	assert.NoError(tb, err, "Applied must take the end of a batch")
	h.route(out)
	return true
}

// answerOne replies to the oldest callback in flight with the answer of h, and passes the
// reply to the Engine. It reports false without a callback in flight.
func (h *harness) answerOne(tb assert.TB) bool {
	tb.Helper()
	if len(h.calls) == 0 {
		return false
	}
	c := h.calls[0]
	h.calls = slices.Delete(h.calls, 0, 1)
	var err error
	if h.answer != nil {
		err = h.answer(c.Request)
	}
	h.step(tb, machine.Called{Err: err, Token: c.Token})
	return true
}

// elapseOne performs the oldest Timer in flight, and passes its end to the Engine. It
// reports false without a Timer in flight.
func (h *harness) elapseOne(tb assert.TB) bool {
	tb.Helper()
	if len(h.timers) == 0 {
		return false
	}
	tm := h.timers[0]
	h.timers = slices.Delete(h.timers, 0, 1)
	h.waits = append(h.waits, tm.Duration)
	h.step(tb, machine.Elapsed{Token: tm.Token})
	return true
}

// next commits a batch, replies to a callback or performs a Timer. It prefers them in that
// order, and reports false when it has nothing to do.
func (h *harness) next(tb assert.TB) bool {
	tb.Helper()
	return h.flush(tb) || h.answerOne(tb) || h.elapseOne(tb)
}

// drain runs next until it has nothing to do.
func (h *harness) drain(tb assert.TB) {
	tb.Helper()
	for range maxDeliveries {
		if !h.next(tb) {
			return
		}
	}
	tb.Fatalf("the Engine still runs after %d deliveries", maxDeliveries)
}

// step passes ev to the Engine, and keeps the effects that it appends.
func (h *harness) step(tb assert.TB, ev machine.Event) {
	tb.Helper()
	out, ok := h.offer(tb, ev, nil)
	assert.True(tb, ok, "the Engine must take the result of its own effect")
	h.route(out)
}

// offer passes ev and out to Step, and returns the effects and whether the Engine took ev.
// Step must not return an error.
func (h *harness) offer(tb assert.TB, ev machine.Event, out []machine.Effect) ([]machine.Effect, bool) {
	tb.Helper()
	got, ok, err := h.engine.Step(ev, out)
	assert.NoError(tb, err, "Step must take an event of the tests")
	return got, ok
}

// commit commits m to the scope of the tests at its current generation, through the writer
// of the controller of the tests.
func (h *harness) commit(tb assert.TB, m *store.Mutation) {
	tb.Helper()
	ctx := assert.Context(tb)
	head, err := h.st.Head(ctx, cellScope)
	assert.NoError(tb, err, "the store must return the header of the scope")
	h.seq++
	m.ID = store.CommitID{Incarnation: incA, Term: term, Seq: h.seq}
	_, err = h.st.Writer(cellScope, incA, term).Commit(ctx, head.Generation, m)
	assert.NoError(tb, err, "the store must commit the batch")
}

// placement returns the placement of the shard s in the store.
func (h *harness) placement(tb assert.TB, s shard.ID) shard.Placement {
	tb.Helper()
	ctx := assert.Context(tb)
	_, cursor, err := h.st.Read(ctx, cellScope, incA, store.Query{}, page.Page{})
	assert.NoError(tb, err, "the store must read the placements")
	defer func() { assert.NoError(tb, cursor.Close(), "the cursor must close") }()
	for p, err := range cursor.Seq(ctx) {
		assert.NoError(tb, err, "the cursor of the memory store must yield its placements")
		if p.Shard == s {
			return p
		}
	}
	tb.Fatalf("the store has no placement of the shard %v", s)
	return shard.Placement{}
}

// records returns the records of the transitions in the store.
func (h *harness) records(tb assert.TB) []store.Transition {
	tb.Helper()
	ctx := assert.Context(tb)
	state, cursor, err := h.st.Read(ctx, cellScope, incA, store.Query{}, page.Page{})
	assert.NoError(tb, err, "the store must read the state")
	assert.NoError(tb, cursor.Close(), "the cursor must close")
	return state.Transitions
}

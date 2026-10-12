// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package cell_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/fsm"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/cell"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The names of the records and the callers of the tests.
const (
	// namespace is the namespace of the tests.
	namespace shard.Namespace = "orders"
	// operator is the identity of the operator of the tests.
	operator shard.Identity = "operator"
	// controllerAddress is the address of the controller of the tests.
	controllerAddress = "controller-1:7000"
	// controllerIdentity is the identity of the controller of the tests.
	controllerIdentity shard.Identity = "controller-1"
	// otherCell is a cell other than the cell of the tests.
	otherCell shard.CellID = "other"
)

// The epochs of the tests.
const (
	// rootTerm is the term of the root that writes the root scope.
	rootTerm epoch.Epoch = 1
	// cellTerm is the term of the machine of each test.
	cellTerm epoch.Epoch = 3
	// firstVersion is the version of the spec of the tests.
	firstVersion epoch.Epoch = 1
)

// The tests divide each cell into four slots, so each cell has four shards.
const (
	// slotsConfig is the configuration of fixed-slots in the spec of the tests.
	slotsConfig = `{"slots":4}`
	// shards is the number of shards of the cell.
	shards = 4
)

// maxDeliveries bounds the effects that run performs, so a machine that never settles
// fails its test instead of the timeout of go test.
const maxDeliveries = 10000

// marker is the effect that the harness passes in out with each event: an Unwatch without a
// watch, which the machine never returns itself.
var marker machine.Effect = machine.Unwatch{}

// pageLimit is the number of placements on each page that the harness reads, so a snapshot
// of the four shards of a cell takes two pages.
const pageLimit = 2

// The nodes of the tests, each at its first incarnation.
var (
	nodeA = shard.Node{ID: "node-a", Address: "node-a:7000", Identity: "agent-a", Incarnation: 1}
	nodeB = shard.Node{ID: "node-b", Address: "node-b:7000", Identity: "agent-b", Incarnation: 1}
	nodeC = shard.Node{ID: "node-c", Address: "node-c:7000", Identity: "agent-c", Incarnation: 1}
)

// The scopes, the incarnation, the clock and the capabilities of the tests.
var (
	// cellKey is the 16 bytes of the ULID of the cell, which is the root shard of the cell.
	cellKey = id.ID128{0: 0x01, 15: 0xce}
	// cellID is the cell of the tests: the text of the ULID of cellKey.
	cellID = shard.CellID(ulid.Format(cellKey.ID()))
	// cellScope is the scope of the cell of the tests.
	cellScope = shard.Scope{Namespace: namespace, Cell: cellID}
	// rootScope is the root scope of the namespace of the tests.
	rootScope = shard.Scope{Namespace: namespace}
	// incA is the namespace incarnation of the tests.
	incA = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: 0x1a, 15: 0x0a})}
	// origin is the start of the clock of each test.
	origin = time.Date(2026, time.October, 12, 0, 0, 0, 0, time.UTC)
	// provided lists the capabilities of the store and the membership of the tests.
	provided = map[config.Axis][]config.Capability{
		config.Store:      {config.Durable},
		config.Membership: {config.Leases},
	}
)

// testCase is the test, or the case of a property, that a harness runs in.
type testCase interface {
	assert.TB
	// Cleanup registers f to run when the test or the case ends.
	Cleanup(f func())
}

// halves is a source of randomness whose every draw of rand.Float64 is one half, so each
// backoff is half of its ceiling. Its values differ in their low bits, so the IDs of the
// shards that the controller generates differ.
type halves struct {
	// n counts the draws.
	n uint64
}

var _ rand.Rand = (*halves)(nil)

// Uint64 returns the next value: the top bit and the count of the draws.
func (h *halves) Uint64() uint64 {
	h.n++
	return 1<<63 | h.n
}

// Read fills p with the bytes of successive values of Uint64.
func (h *halves) Read(p []byte) (int, error) {
	for i := 0; i < len(p); i += 8 {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], h.Uint64())
		copy(p[i:], b[:])
	}
	return len(p), nil
}

// journal is a slog.Handler that keeps the message and the attributes of every record. The
// handlers that WithAttrs returns share the records of their journal.
type journal struct {
	// log contains the records.
	log *entries
	// attrs lists the attributes that WithAttrs bound, in their order.
	attrs []slog.Attr
}

// entries is the list of the records of a journal.
type entries struct {
	// list lists the records in order.
	list []entry
	// mu guards list.
	mu sync.Mutex
}

// entry is one record of a journal.
type entry struct {
	// attrs maps the key of each attribute to its value as text.
	attrs map[string]string
	// message is the message of the record.
	message string
}

var _ slog.Handler = (*journal)(nil)

// newJournal returns a journal without records.
func newJournal() *journal {
	return &journal{log: &entries{}}
}

// Enabled reports true for every level.
func (*journal) Enabled(context.Context, slog.Level) bool { return true }

// Handle keeps the record r with the attributes that WithAttrs bound.
func (j *journal) Handle(_ context.Context, r slog.Record) error {
	e := entry{attrs: make(map[string]string), message: r.Message}
	for _, a := range j.attrs {
		e.attrs[a.Key] = a.Value.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		e.attrs[a.Key] = a.Value.String()
		return true
	})
	j.log.mu.Lock()
	defer j.log.mu.Unlock()
	j.log.list = append(j.log.list, e)
	return nil
}

// WithAttrs returns a journal that shares the records of j and binds attrs after the
// attributes of j.
func (j *journal) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &journal{log: j.log, attrs: slices.Concat(j.attrs, attrs)}
}

// WithGroup returns j. The machine does not group its attributes.
func (j *journal) WithGroup(string) slog.Handler { return j }

// logged returns the entries with the message message.
func (j *journal) logged(message string) []entry {
	j.log.mu.Lock()
	defer j.log.mu.Unlock()
	var got []entry
	for _, e := range j.log.list {
		if e.message == message {
			got = append(got, e)
		}
	}
	return got
}

// setup is the configuration of a harness before New.
type setup struct {
	// spec is the spec that the root records.
	spec config.Spec
	// cfg is the Config of the machine, which newHarness completes.
	cfg cell.Config
	// root is the mutation that creates the root scope, which newHarness completes with
	// the spec and the mapping.
	root store.Mutation
	// fits is the bound of a commit, and nil for the bound of the store.
	fits func(m *store.Mutation) error
	// extend registers more strategies in the registry of the machine, or is nil.
	extend func(r *config.Registry) error
	// remap changes the mapping that the root records after the spec resolves, or is nil.
	remap func(m *shard.Mapping)
	// support is the support matrix of the registry of the machine.
	support config.Support
}

// option changes the setup of a harness.
type option func(*setup)

// harness is the runtime of the tests. It performs the effects of one cell machine against a
// memory store, replies to the callbacks of the machine as the agents of the nodes do, runs
// its passes and keeps its replies.
type harness struct {
	// st is the store of the machine.
	st *memory.Store
	// clock is the clock of the machine.
	clock *fake.Clock
	// machine is the machine under test.
	machine *cell.Machine
	// logs are the logs of the machine.
	logs *journal
	// pending lists the effects that run performs, the oldest first: the reads and the
	// commits of the store, the callbacks and the passes.
	pending []machine.Effect
	// watches lists the watches of the root scope that run.
	watches []machine.Watch
	// opened lists the token of every watch of the root scope that the machine started, in
	// order.
	opened []machine.Token
	// timers lists the Timers that the machine armed, the oldest first.
	timers []machine.Timer
	// replies lists every reply of the machine, in order.
	replies []machine.Effect
	// plans lists every Plan that the machine sent, in order.
	plans []machine.Plan
	// unwatched lists the tokens of every Unwatch of the machine, in order.
	unwatched []machine.Token
	// views is the token of the WatchViews that runs, and zero without one.
	views machine.Token
	// revision is the revision of the last view that the harness passed.
	revision epoch.Epoch
	// calls is the token of the last inbound call of the harness.
	calls machine.ReplyToken
	// rootSeq is the Seq of the last commit to the root scope.
	rootSeq uint64
}

// newHarness returns a harness of a machine of the cell of the tests. The root scope places
// the cell and records the spec of the setup. Each option changes the setup before New.
func newHarness(tb testCase, opts ...option) *harness {
	tb.Helper()
	s := setup{
		spec: config.Spec{
			Namespace:    namespace,
			Preset:       builtin.ExclusiveWorker,
			Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
			Version:      firstVersion,
		},
		cfg: cell.Config{
			Rand:        &halves{},
			Reporter:    noop.New(),
			Provided:    provided,
			Operators:   []shard.Identity{operator},
			Scope:       cellScope,
			Node:        shard.Node{ID: "controller-1", Address: controllerAddress, Identity: controllerIdentity},
			Term:        cellTerm,
			Parallelism: 1,
			Incarnation: incA,
		},
		root:    store.Mutation{Put: []shard.Placement{{Shard: shard.ID{ID128: cellKey}}}},
		support: builtin.Support(),
	}
	for _, opt := range opts {
		opt(&s)
	}
	h := &harness{st: memory.New(fake.New(origin)), clock: fake.New(origin), logs: newJournal()}
	tb.Cleanup(func() { assert.NoError(tb, h.st.Close(), "the store must close") })
	r := config.NewRegistry(s.support)
	assert.NoError(tb, builtin.Register(r), "the built-ins must register")
	if s.extend != nil {
		assert.NoError(tb, s.extend(r), "the strategies of the test must register")
	}
	res, err := config.Resolve(r, &s.spec, provided)
	assert.NoError(tb, err, "the spec of the tests must resolve")
	raw, err := json.Marshal(&res.Spec)
	assert.NoError(tb, err, "the resolved spec must encode")
	if s.root.Spec == nil {
		s.root.Spec = raw
	}
	if s.root.Mapping == nil {
		s.root.Mapping = &res.Mapping
		if s.remap != nil {
			s.remap(s.root.Mapping)
		}
	}
	h.commitRoot(tb, &s.root)
	s.cfg.Clock = h.clock
	s.cfg.Registry = r
	s.cfg.Logger = slog.New(h.logs)
	s.cfg.Fits = s.fits
	if s.fits == nil {
		s.cfg.Fits = func(m *store.Mutation) error { return h.st.Fits(cellScope, m) }
	}
	h.machine, err = cell.New(&s.cfg)
	assert.NoError(tb, err, "New must return a machine of a complete config")
	return h
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

// registry returns a registry with every built-in strategy of the support matrix.
func registry(tb assert.TB) *config.Registry {
	tb.Helper()
	r := config.NewRegistry(builtin.Support())
	assert.NoError(tb, builtin.Register(r), "the built-ins must register")
	return r
}

// completeConfig returns a Config that New accepts.
func completeConfig(tb assert.TB) cell.Config {
	tb.Helper()
	return cell.Config{
		Clock:       fake.New(origin),
		Rand:        &halves{},
		Reporter:    noop.New(),
		Registry:    registry(tb),
		Logger:      slog.New(newJournal()),
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

// commitRoot commits m to the root scope through the root of the tests.
func (h *harness) commitRoot(tb assert.TB, m *store.Mutation) {
	tb.Helper()
	ctx := assert.Context(tb)
	head, err := h.st.Head(ctx, rootScope)
	assert.NoError(tb, err, "the store must return the header of the root scope")
	h.rootSeq++
	m.ID = store.CommitID{Incarnation: incA, Term: rootTerm, Seq: h.rootSeq}
	_, err = h.st.Writer(rootScope, incA, rootTerm).Commit(ctx, head.Generation, m)
	assert.NoError(tb, err, "the root must commit")
}

// call returns the token of the next inbound call.
func (h *harness) call() machine.ReplyToken {
	h.calls++
	return h.calls
}

// start passes Started to the machine and runs it until it serves.
func (h *harness) start(tb assert.TB) {
	tb.Helper()
	h.step(tb, machine.Started{Reply: h.call()})
	h.run(tb)
}

// step passes ev to the machine with the marker in out, checks that the machine keeps out,
// and keeps the effects that the machine appends.
func (h *harness) step(tb assert.TB, ev machine.Event) {
	tb.Helper()
	got := h.machine.Step(ev, []machine.Effect{marker})
	assert.NotEmpty(tb, got, "the machine must keep the effects that out has")
	assert.Equal(tb, got[0], marker, "the machine must keep the effects that out has")
	h.route(got[1:])
}

// route keeps each effect of effects in the list of its kind.
func (h *harness) route(effects []machine.Effect) {
	for _, e := range effects {
		switch e := e.(type) {
		case machine.Watch:
			h.watches = append(h.watches, e)
			h.opened = append(h.opened, e.Token)
		case machine.Unwatch:
			h.unwatched = append(h.unwatched, e.Token)
			h.watches = slices.DeleteFunc(h.watches, func(w machine.Watch) bool { return w.Token == e.Token })
			if h.views == e.Token {
				h.views = 0
			}
		case machine.WatchViews:
			h.views = e.Token
		case machine.Timer:
			h.timers = append(h.timers, e)
		case machine.Reply[struct{}], machine.Reply[protocol.Ack], machine.Reply[protocol.Receipt],
			machine.Reply[budget.Decision]:
			h.replies = append(h.replies, e)
		case machine.Plan:
			h.plans = append(h.plans, e)
			h.pending = append(h.pending, e)
		default:
			h.pending = append(h.pending, e)
		}
	}
}

// run performs the pending effects, the oldest first, and passes their results to the
// machine until no effect is pending.
func (h *harness) run(tb assert.TB) {
	tb.Helper()
	for range maxDeliveries {
		if len(h.pending) == 0 {
			return
		}
		e := h.pending[0]
		h.pending = slices.Delete(h.pending, 0, 1)
		h.step(tb, h.perform(tb, e))
	}
	tb.Fatalf("the machine still runs after %d deliveries", maxDeliveries)
}

// perform performs e and returns the event of its result. Every agent replies to its
// callbacks with success.
func (h *harness) perform(tb assert.TB, e machine.Effect) machine.Event {
	tb.Helper()
	ctx := assert.Context(tb)
	switch e := e.(type) {
	case machine.Head:
		head, err := h.st.Head(ctx, e.Scope)
		return machine.HeadRead{Err: err, Token: e.Token, Head: head}
	case machine.Read:
		return h.read(tb, &e)
	case machine.Commit:
		gen, err := h.st.Writer(e.Scope, e.Incarnation, e.Term).Commit(ctx, e.Expect, e.Mutation)
		return machine.Committed{Err: err, Token: e.Token, Generation: gen}
	case machine.Outcome:
		receipt, found, err := h.st.Outcome(ctx, e.Scope, e.ID)
		return machine.OutcomeRead{Err: err, Token: e.Token, Receipt: receipt, Found: found}
	case machine.Callback:
		return machine.Called{Token: e.Token}
	case machine.Plan:
		plan, err := e.Placer.Place(ctx, e.Input)
		var violations []placement.Violation
		if err == nil {
			violations, err = placement.Validate(e.Input, plan, e.Constraints)
		}
		return machine.Planned{Err: err, Violations: violations, Plan: plan, Token: e.Token}
	default:
		tb.Fatalf("the machine returned a %T, which the harness does not perform", e)
		return nil
	}
}

// read reads the page of e from the store and returns the event of its result.
func (h *harness) read(tb assert.TB, e *machine.Read) machine.Event {
	tb.Helper()
	ctx := assert.Context(tb)
	p := e.Page
	p.Limit = pageLimit
	state, cursor, err := h.st.Read(ctx, e.Scope, e.Incarnation, e.Query, p)
	if err != nil {
		return machine.PageRead{Err: err, Token: e.Token}
	}
	var ps []shard.Placement
	for p, err := range cursor.Seq(ctx) {
		assert.NoError(tb, err, "the cursor of the memory store must yield its placements")
		ps = append(ps, p)
	}
	next := cursor.NextPage()
	assert.NoError(tb, cursor.Close(), "the cursor must close")
	return machine.PageRead{Placements: ps, Next: next, State: state, Token: e.Token}
}

// follow passes the generations of the root scope that the watches of the machine have
// not passed, and runs the machine afterwards.
func (h *harness) follow(tb assert.TB) {
	tb.Helper()
	for len(h.watches) > 0 {
		w := h.watches[0]
		ctx, cancel := context.WithCancel(assert.Context(tb))
		cancel()
		passed := false
		for ev, err := range h.st.Watch(ctx, w.Scope, w.Incarnation, w.From, w.Node) {
			if err != nil {
				h.watches = slices.Delete(h.watches, 0, 1)
				h.step(tb, machine.Watched{Err: err, Token: w.Token})
				passed = true
				break
			}
			h.watches[0].From = ev.Generation
			h.step(tb, machine.Watched{Event: ev, Token: w.Token})
			passed = true
			break
		}
		h.run(tb)
		if !passed {
			return
		}
	}
}

// view passes the view of nodes to the machine, and runs the machine afterwards.
func (h *harness) view(tb assert.TB, nodes ...shard.Node) {
	tb.Helper()
	assert.NotEqual(tb, h.views, machine.Token(0), "the machine must watch the views")
	h.revision++
	v := membership.View{Scope: cellScope, Nodes: slices.Clone(nodes), Revision: h.revision, Incarnation: incA}
	h.step(tb, machine.Viewed{View: v, Token: h.views})
	h.run(tb)
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

// state returns the state of the scope of the cell.
func (h *harness) state(tb assert.TB) store.State {
	tb.Helper()
	state, cursor, err := h.st.Read(assert.Context(tb), cellScope, incA, store.Query{}, page.Page{})
	assert.NoError(tb, err, "the store must read the scope of the cell")
	assert.NoError(tb, cursor.Close(), "the cursor must close")
	return state
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

// otherCells returns the root placements of n other cells. Each starts at its own
// position, after the start of the cell of the tests.
func otherCells(n int) []shard.Placement {
	ps := make([]shard.Placement, 0, n)
	for i := range n {
		k := cellKey
		k[14] = byte(i) + 1
		ps = append(ps, shard.Placement{Shard: shard.ID{ID128: k}, Range: shard.Range{Start: []byte{0xf0, byte(i)}}})
	}
	return ps
}

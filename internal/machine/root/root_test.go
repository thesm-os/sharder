// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package root_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/fsm"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/telemetry"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/root"
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
	// rootAddress is the address of the controller process of the root of the tests.
	rootAddress = "controller-1:7000"
	// rootIdentity is the identity of the controller process of the root of the tests.
	rootIdentity shard.Identity = "controller-1"
	// slotsConfig is the configuration of fixed-slots in the spec of the tests.
	slotsConfig = `{"slots":4}`
	// otherTerm is the term of another root of the namespace.
	otherTerm epoch.Epoch = 1
)

// maxDeliveries bounds the effects that run performs, so a machine that never settles
// fails its test instead of the timeout of go test.
const maxDeliveries = 10000

// marker is the effect that the harness passes in out with each event: an Unwatch without a
// watch, which the machine never returns itself.
var marker machine.Effect = machine.Unwatch{}

// The log message of an error of a term.
const logTerm = "root: the term failed"

// gapInstrument is the name of the histogram of the writer gaps.
const gapInstrument telemetry.InstrumentName = "sharder.writer.gap"

// The controller processes of the tests, each at its first incarnation and with the
// capacity of one cell.
var (
	procA = shard.Node{
		ID: "proc-a", Address: "proc-a:7000", Identity: "proc-a", Incarnation: 1,
		Capacity: map[string]float64{root.Capacity: 1},
	}
	procB = shard.Node{
		ID: "proc-b", Address: "proc-b:7000", Identity: "proc-b", Incarnation: 1,
		Capacity: map[string]float64{root.Capacity: 1},
	}
)

// The scope, the incarnations, the clock and the capabilities of the tests.
var (
	// rootScope is the root scope of the namespace of the tests.
	rootScope = shard.Scope{Namespace: namespace}
	// incB is the incarnation of a namespace that another root creates.
	incB = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: 0x1a, 15: 0x0b})}
	// origin is the start of the clock of each test.
	origin = time.Date(2026, time.October, 12, 0, 0, 0, 0, time.UTC)
	// provided lists the capabilities of the store and the membership of the tests.
	provided = map[config.Axis][]config.Capability{
		config.Store:      {config.Durable},
		config.Membership: {config.Leases},
	}
)

// The errors of the campaigns of the tests.
var (
	// errOutage is a transient error of a campaign.
	errOutage = errs.WithClass(errors.New("test: the store is unreachable"), errs.Transient)
	// errBroken is an error of a campaign that a retry does not fix.
	errBroken = errs.WithClass(errors.New("test: the store is broken"), errs.Invalid)
)

// halves is a source of randomness whose every draw of rand.Float64 is one half, so each
// backoff is half of its ceiling. Its values differ in their low bits, so the IDs that the
// root generates differ.
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

// journal is a slog.Handler that keeps the message and the attributes of every record.
type journal struct {
	// entries lists the records in order.
	entries []entry
	// mu guards entries.
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

// Enabled reports true for every level.
func (*journal) Enabled(context.Context, slog.Level) bool { return true }

// Handle keeps the record r.
func (j *journal) Handle(_ context.Context, r slog.Record) error {
	e := entry{attrs: make(map[string]string), message: r.Message}
	r.Attrs(func(a slog.Attr) bool {
		e.attrs[a.Key] = a.Value.String()
		return true
	})
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, e)
	return nil
}

// WithAttrs returns j. The tests read the attributes of the records of the root alone.
func (j *journal) WithAttrs([]slog.Attr) slog.Handler { return j }

// WithGroup returns j. The root does not group its attributes.
func (j *journal) WithGroup(string) slog.Handler { return j }

// logged returns the entries with the message message.
func (j *journal) logged(message string) []entry {
	j.mu.Lock()
	defer j.mu.Unlock()
	var got []entry
	for _, e := range j.entries {
		if e.message == message {
			got = append(got, e)
		}
	}
	return got
}

// histograms is a telemetry.Reporter that counts the records of each histogram. It returns
// the instruments of its Reporter for every other kind.
type histograms struct {
	telemetry.Reporter
	// records maps the name of each histogram to the number of its records.
	records map[telemetry.InstrumentName]int
}

var _ telemetry.Reporter = (*histograms)(nil)

// Histogram returns a histogram that counts its records in r.
func (r *histograms) Histogram(spec telemetry.InstrumentSpec) telemetry.Histogram {
	return &histogram{of: r, name: spec.Name}
}

// histogram is one histogram of a histograms reporter.
type histogram struct {
	// of is the reporter that counts the records.
	of *histograms
	// name is the name of the histogram.
	name telemetry.InstrumentName
}

var _ telemetry.Histogram = (*histogram)(nil)

// Record counts one record of h.
func (h *histogram) Record(context.Context, float64) { h.of.records[h.name]++ }

// With returns h. The tests count the records of a histogram over every attribute.
func (h *histogram) With([]telemetry.Attr) telemetry.Histogram { return h }

// Release does nothing, because h keeps no state of its attributes.
func (*histogram) Release() {}

// setup is the configuration of a harness before New.
type setup struct {
	// cfg is the Config of the machine, which newHarness completes.
	cfg root.Config
	// fits is the bound of a commit, and nil for the bound of the store.
	fits func(m *store.Mutation) error
}

// option changes the setup of a harness.
type option func(*setup)

// harness is the runtime of the tests. It performs the effects of one root machine against
// a memory store, replies to the callbacks of the machine as the controller processes do,
// runs its passes and keeps its replies.
type harness struct {
	// answer returns the answer of a controller process to the request r, and nil for
	// success. With a nil answer, every process replies with success.
	answer func(r *protocol.Request) error
	// st is the store of the machine.
	st *memory.Store
	// machine is the machine under test.
	machine *root.Machine
	// logs are the logs of the machine.
	logs *journal
	// terms maps the token of each Campaign that the store granted to its term.
	terms map[machine.Token]store.Leadership
	// pending lists the effects that run performs, the oldest first.
	pending []machine.Effect
	// timers lists the Timers that the machine armed, the oldest first.
	timers []machine.Timer
	// replies lists every reply of the machine, in order.
	replies []machine.Effect
	// sent lists the request of every callback of the machine, in order.
	sent []*protocol.Request
	// campaigns lists the token of every Campaign of the machine, in order.
	campaigns []machine.Token
	// tokens lists the token of every effect of the machine that has a token of its own, in
	// order.
	tokens []machine.Token
	// resigned lists the token of every Resign of the machine, in order.
	resigned []machine.Token
	// unwatched lists the token of every Unwatch of the machine, in order.
	unwatched []machine.Token
	// views is the token of the WatchViews that runs, and zero without one.
	views machine.Token
	// revision is the revision of the last view that the harness passed.
	revision epoch.Epoch
	// calls is the token of the last inbound call of the harness.
	calls machine.ReplyToken
	// others is the Seq of the last commit of another root.
	others uint64
	// paused reports that run does not perform a Campaign, so a campaign remains in flight.
	paused bool
}

// newHarness returns a harness of a root machine of the namespace of the tests. Each
// option changes the setup before New.
func newHarness(tb testing.TB, opts ...option) *harness {
	tb.Helper()
	s := setup{cfg: root.Config{
		Rand:        &halves{},
		Reporter:    noop.New(),
		Provided:    provided,
		Spec:        namespaceSpec(),
		Operators:   []shard.Identity{operator},
		Node:        shard.Node{ID: "controller-1", Address: rootAddress, Identity: rootIdentity},
		Parallelism: 1,
	}}
	for _, opt := range opts {
		opt(&s)
	}
	h := &harness{st: memory.New(fake.New(origin)), logs: &journal{}, terms: make(map[machine.Token]store.Leadership)}
	tb.Cleanup(func() { assert.NoError(tb, h.st.Close(), "the store must close") })
	s.cfg.Clock = fake.New(origin)
	s.cfg.Registry = registry(tb)
	s.cfg.Logger = slog.New(h.logs)
	s.cfg.Fits = s.fits
	if s.fits == nil {
		s.cfg.Fits = func(m *store.Mutation) error { return h.st.Fits(rootScope, m) }
	}
	var err error
	h.machine, err = root.New(&s.cfg)
	assert.NoError(tb, err, "New must return a machine of a complete config")
	return h
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
			got := h.logs.logged(logTerm)
			assert.Length(t, got, 1, "the root must log the error of the term")
			expect.Equal(t, got[0].attrs[attrNamespaceKey], string(namespace), "the log must name the namespace")
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

// firstCampaignBackoff is the backoff before the first retry of a campaign with the source
// of randomness of the tests: half of root.CampaignRetryBase.
const firstCampaignBackoff = root.CampaignRetryBase / 2

// attrNamespaceKey is the key of the attribute of the logs of the root that names the
// namespace.
const attrNamespaceKey = "namespace"

// namespaceSpec returns the spec of the namespace of the tests.
func namespaceSpec() config.Spec {
	return config.Spec{
		Namespace:    namespace,
		Preset:       builtin.ExclusiveWorker,
		Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
	}
}

// registry returns a registry with every built-in strategy of the support matrix.
func registry(tb assert.TB) *config.Registry {
	tb.Helper()
	r := config.NewRegistry(builtin.Support())
	assert.NoError(tb, builtin.Register(r), "the built-ins must register")
	return r
}

// completeConfig returns a Config that New accepts.
func completeConfig(tb assert.TB) root.Config {
	tb.Helper()
	return root.Config{
		Clock:       fake.New(origin),
		Rand:        &halves{},
		Reporter:    noop.New(),
		Provided:    provided,
		Logger:      slog.New(&journal{}),
		Fits:        func(*store.Mutation) error { return nil },
		Registry:    registry(tb),
		Spec:        namespaceSpec(),
		Node:        shard.Node{Address: rootAddress},
		Parallelism: 1,
	}
}

// call returns the token of the next inbound call.
func (h *harness) call() machine.ReplyToken {
	h.calls++
	return h.calls
}

// start passes Started to the machine and runs it until the controller of its first term
// serves.
func (h *harness) start(tb assert.TB) {
	tb.Helper()
	h.step(tb, machine.Started{Reply: h.call()})
	h.run(tb)
}

// grant performs the Campaign in flight, and runs the machine until the controller of the
// term serves.
func (h *harness) grant(tb assert.TB) {
	tb.Helper()
	h.paused = false
	h.run(tb)
}

// end resigns the term in the store, passes its end to the machine, and runs the machine.
// The machine campaigns again, and the harness keeps the new campaign pending.
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

// step passes ev to the machine with the marker in out, checks that the machine keeps out,
// and keeps the effects that the machine appends.
func (h *harness) step(tb assert.TB, ev machine.Event) {
	tb.Helper()
	got := h.machine.Step(ev, []machine.Effect{marker})
	assert.NotEmpty(tb, got, "the machine must keep the effects that out has")
	assert.Equal(tb, got[0], marker, "the machine must keep the effects that out has")
	h.route(got[1:])
}

// route keeps each effect of effects in the list of its kind. It also keeps the token of
// each effect that has a token of its own.
func (h *harness) route(effects []machine.Effect) {
	for _, e := range effects {
		switch e := e.(type) {
		case machine.Campaign:
			h.tokens = append(h.tokens, e.Token)
			h.campaigns = append(h.campaigns, e.Token)
			h.pending = append(h.pending, e)
		case machine.Resign:
			h.resigned = append(h.resigned, e.Token)
		case machine.Unwatch:
			h.unwatched = append(h.unwatched, e.Token)
			if h.views == e.Token {
				h.views = 0
			}
		case machine.WatchViews:
			h.tokens = append(h.tokens, e.Token)
			h.views = e.Token
		case machine.Timer:
			h.tokens = append(h.tokens, e.Token)
			h.timers = append(h.timers, e)
		case machine.Reply[struct{}], machine.Reply[protocol.Ack], machine.Reply[protocol.Receipt],
			machine.Reply[budget.Decision]:
			h.replies = append(h.replies, e)
		case machine.Callback:
			h.tokens = append(h.tokens, e.Token)
			h.sent = append(h.sent, e.Request)
			h.pending = append(h.pending, e)
		case machine.Head:
			h.tokens = append(h.tokens, e.Token)
			h.pending = append(h.pending, e)
		case machine.Commit:
			h.tokens = append(h.tokens, e.Token)
			h.pending = append(h.pending, e)
		default:
			h.pending = append(h.pending, e)
		}
	}
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

// deliver performs the oldest pending effect, and passes the event of its result to the
// machine.
func (h *harness) deliver(tb assert.TB) {
	tb.Helper()
	assert.NotEmpty(tb, h.pending, "an effect of the machine must be pending")
	e := h.pending[0]
	h.pending = slices.Delete(h.pending, 0, 1)
	h.step(tb, h.perform(tb, e))
}

// run performs the pending effects, the oldest first, and passes their results to the
// machine until no effect is pending. It leaves a Campaign pending while paused is set.
func (h *harness) run(tb assert.TB) {
	tb.Helper()
	for range maxDeliveries {
		i := slices.IndexFunc(h.pending, func(e machine.Effect) bool {
			_, isCampaign := e.(machine.Campaign)
			return !h.paused || !isCampaign
		})
		if i < 0 {
			return
		}
		e := h.pending[i]
		h.pending = slices.Delete(h.pending, i, i+1)
		h.step(tb, h.perform(tb, e))
	}
	tb.Fatalf("the machine still runs after %d deliveries", maxDeliveries)
}

// perform performs e and returns the event of its result.
func (h *harness) perform(tb assert.TB, e machine.Effect) machine.Event {
	tb.Helper()
	ctx := assert.Context(tb)
	switch e := e.(type) {
	case machine.Campaign:
		l, err := h.st.Campaign(ctx, e.Scope)
		assert.NoError(tb, err, "the store must grant a term")
		h.terms[e.Token] = l
		return machine.Campaigned{Token: e.Token, Term: l.Term()}
	case machine.Head:
		head, err := h.st.Head(ctx, e.Scope)
		return machine.HeadRead{Err: err, Token: e.Token, Head: head}
	case machine.Read:
		state, cursor, err := h.st.Read(ctx, e.Scope, e.Incarnation, e.Query, e.Page)
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
	case machine.Commit:
		gen, err := h.st.Writer(e.Scope, e.Incarnation, e.Term).Commit(ctx, e.Expect, e.Mutation)
		return machine.Committed{Err: err, Token: e.Token, Generation: gen}
	case machine.Outcome:
		receipt, found, err := h.st.Outcome(ctx, e.Scope, e.ID)
		return machine.OutcomeRead{Err: err, Token: e.Token, Receipt: receipt, Found: found}
	case machine.Callback:
		var err error
		if h.answer != nil {
			err = h.answer(e.Request)
		}
		return machine.Called{Err: err, Token: e.Token}
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

// view passes the view of the controller processes procs to the machine, and runs the
// machine afterwards.
func (h *harness) view(tb assert.TB, procs ...shard.Node) {
	tb.Helper()
	assert.NotEqual(tb, h.views, machine.Token(0), "the machine must watch the views")
	h.revision++
	v := membership.View{
		Scope:       rootScope,
		Nodes:       slices.Clone(procs),
		Revision:    h.revision,
		Incarnation: h.state(tb).Head.Incarnation,
	}
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

// state returns the state of the root scope.
func (h *harness) state(tb assert.TB) store.State {
	tb.Helper()
	state, cursor, err := h.st.Read(assert.Context(tb), rootScope, shard.NamespaceIncarnation{}, store.Query{},
		page.Page{})
	assert.NoError(tb, err, "the store must read the root scope")
	assert.NoError(tb, cursor.Close(), "the cursor must close")
	return state
}

// placements returns the placements of the root scope.
func (h *harness) placements(tb assert.TB) []shard.Placement {
	tb.Helper()
	ctx := assert.Context(tb)
	_, cursor, err := h.st.Read(ctx, rootScope, shard.NamespaceIncarnation{}, store.Query{}, page.Page{})
	assert.NoError(tb, err, "the store must read the root scope")
	defer func() { assert.NoError(tb, cursor.Close(), "the cursor must close") }()
	var ps []shard.Placement
	for p, err := range cursor.Seq(ctx) {
		assert.NoError(tb, err, "the cursor of the memory store must yield its placements")
		ps = append(ps, p)
	}
	return ps
}

// create commits the creation of the namespace of the tests by another root, in the
// incarnation incB with the spec of the version v.
func (h *harness) create(tb assert.TB, v epoch.Epoch) {
	tb.Helper()
	ctx := assert.Context(tb)
	spec := namespaceSpec()
	spec.Version = v
	res, err := config.Resolve(registry(tb), &spec, provided)
	assert.NoError(tb, err, "the spec of the other root must resolve")
	raw, err := json.Marshal(&res.Spec)
	assert.NoError(tb, err, "the spec of the other root must encode")
	head, err := h.st.Head(ctx, rootScope)
	assert.NoError(tb, err, "the store must return the header of the root scope")
	h.others++
	m := &store.Mutation{
		ID:      store.CommitID{Incarnation: incB, Term: otherTerm, Seq: h.others},
		Mapping: &res.Mapping,
		Spec:    raw,
	}
	_, err = h.st.Writer(rootScope, incB, otherTerm).Commit(ctx, head.Generation, m)
	assert.NoError(tb, err, "the other root must commit")
}

// specProposal returns a spec proposal with the ID n and the base base, whose body is the
// spec of the namespace at the version v, in the incarnation of the root scope.
func (h *harness) specProposal(tb assert.TB, n byte, base, v epoch.Epoch) *proposal.Proposal {
	tb.Helper()
	spec := namespaceSpec()
	spec.Version = v
	body, err := json.Marshal(&spec)
	assert.NoError(tb, err, "the spec must encode")
	return &proposal.Proposal{
		Scope:       rootScope,
		Expires:     origin.Add(time.Hour),
		Body:        body,
		Base:        base,
		Incarnation: h.state(tb).Head.Incarnation,
		ID:          proposal.ID{ID: id.New128([16]byte{0: 0x9e, 15: n})},
		Kind:        proposal.SpecChange,
	}
}

// submitCall returns the Submit of p from caller as the next inbound call.
func (h *harness) submitCall(tb assert.TB, p *proposal.Proposal, caller shard.Identity) machine.Submit {
	tb.Helper()
	return machine.Submit{
		Proposal: p,
		Metadata: protocol.Metadata{Caller: caller, Version: protocol.Current},
		Reply:    h.call(),
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

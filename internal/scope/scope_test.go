// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package scope_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"slices"
	"testing"
	"time"
	"weak"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand/constant"
	"go.thesmos.sh/core/telemetry"
	"go.thesmos.sh/core/version"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/scope"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The names of the records of the tests.
const (
	namespace shard.Namespace = "orders"
	cell      shard.CellID    = "cell-1"
	node      shard.NodeID    = "node-a"
	// address is the address of the controller of the tests.
	address = "controller-a:7000"
	// identity is the identity of the controller of the tests.
	identity shard.Identity = "spiffe://scope_test/controller"
	// spec is the spec that the root of the tests records when it creates the namespace.
	spec = "{}"
	// nextPage is the token of the second page of a read whose results a test builds itself.
	nextPage = "page-2"
)

// The first bytes of the identifiers of the tests, one per kind of identifier.
const (
	tagIncarnation byte = 0x1a
	tagShard       byte = 0x5d
)

// The terms of the controllers of the tests.
const (
	// oldTerm is the term of the controller that the Scope of a test replaces.
	oldTerm epoch.Epoch = 1
	// term is the term of the Scope of each test.
	term epoch.Epoch = 2
	// newTerm is the term of a controller that replaces the Scope of a test.
	newTerm epoch.Epoch = 3
)

// taken is the last token that the other parts of the machine of each test took before
// the Scope took its first.
const taken machine.Token = 7

// half is the value of each draw of the source of randomness of the tests, so each backoff
// is half of its ceiling.
const half = 0.5

// crowd is the number of placements of a scope whose snapshot takes three pages of the
// memory store. The memory store puts 1,000 placements on a page.
const crowd = 2001

// maxDeliveries bounds the deliveries of a drain, so a Scope that never stops waiting fails
// its test instead of the timeout of go test.
const maxDeliveries = 10000

// The labels of the faults of the property of the commits, and the least share of its cases
// that contain each fault.
const (
	lostCommit = "a fault loses a commit"
	lostReply  = "a fault loses the reply of a commit"
	outage     = "an outage fails an effect"
	faultShare = 0.1
)

// The limits of the stores of the tests of mutations that are too large.
var (
	// onePlacement allows one placement per commit. Every acquisition of the tests fits it.
	onePlacement = store.Limits{MaxPlacements: 1, MaxBytes: 4096}
	// oneByte allows one byte per commit, and every acquisition of the tests is larger.
	oneByte = store.Limits{MaxPlacements: 1, MaxBytes: 1}
)

// foreign is an effect of another part of the machine. A test passes it to the Scope in
// out, and expects it back unchanged.
var foreign machine.Effect = machine.Timer{Token: taken}

// The scopes, the incarnations, the record, the mapping and the clock of the tests.
var (
	// cellScope is the scope of the cell of the tests.
	cellScope = shard.Scope{Namespace: namespace, Cell: cell}
	// rootScope is the root scope of the namespace of the tests.
	rootScope = shard.Scope{Namespace: namespace}
	// incA is the namespace incarnation of the Scope of each test.
	incA = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0a})}
	// incB is the incarnation of another namespace of the same name.
	incB = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0b})}
	// record is the controller record that each acquisition of the tests commits.
	record = shard.Cell{ID: cell, Controller: address, ControllerIdentity: identity, Epoch: term}
	// mapping is the mapping that the root of the tests records when it creates the
	// namespace.
	mapping = shard.Mapping{Partitioner: singleton.Name}
	// acquisitionID is the commit ID of the first attempt of each acquisition.
	acquisitionID = store.CommitID{Incarnation: incA, Term: term, Seq: 1}
	// origin is the start of the clock of each store of the tests.
	origin = time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC)
)

// testCase is the test, or the case of a property, that a harness runs in.
type testCase interface {
	assert.TB
	// Cleanup registers f to run when the test or the case ends.
	Cleanup(f func())
}

// counter is a telemetry.Counter that sums the values of its adds. A harness runs on one
// goroutine, so the sum needs no lock.
type counter struct {
	// sum is the sum of the values of the adds.
	sum int64
}

var _ telemetry.Counter = (*counter)(nil)

// Add adds value to the sum of c.
func (c *counter) Add(_ context.Context, value int64) {
	c.sum += value
}

// With returns c, because the Scope does not bind attributes.
func (c *counter) With([]telemetry.Attr) telemetry.Counter {
	return c
}

// Release does nothing, because the Scope does not release its counter.
func (*counter) Release() {}

// harness is the runtime of the tests. It performs the effects of one Scope against a
// memory store, one at a time in the order of the steps that appended them, and keeps what
// the Scope reports.
type harness struct {
	// st is the store of the Scope.
	st *memory.Store
	// scope is the Scope under test.
	scope *scope.Scope
	// unknowns counts the attempts of commits with an unknown outcome.
	unknowns counter
	// pending lists the effects that the harness has not performed, the oldest first.
	pending []machine.Effect
	// results lists every Result that reports something, in the order of the steps.
	results []scope.Result
	// waits lists the duration of each Timer that the harness performed.
	waits []time.Duration
	// tokens is the counter of the tokens of the machine of the Scope.
	tokens machine.Token
	// others is the Seq of the last commit of the other controllers of a test.
	others uint64
	// reads counts the pages that the harness read from the store.
	reads int
}

// newHarness returns a harness of a Scope of the scope id at the term of the tests. The
// acquisition of the Scope commits what acquisition returns, and the store of the harness
// is a new memory store with opts.
func newHarness(tb testCase, id shard.Scope, acquisition scope.Acquisition, opts ...memory.Option) *harness {
	tb.Helper()
	h := &harness{st: memory.New(fake.New(origin), opts...), tokens: taken}
	tb.Cleanup(func() { assert.NoError(tb, h.st.Close(), "the store must close") })
	s, err := scope.New(&scope.Config{
		Rand:        constant.FromFloat64(half),
		Unknown:     &h.unknowns,
		Acquisition: acquisition,
		Tokens:      &h.tokens,
		Scope:       id,
		Term:        term,
	})
	assert.NoError(tb, err, "New must return a Scope of a complete config")
	h.scope = s
	return h
}

// TestScope checks the construction, the acquisition, the commits and the steps of a Scope
// against a memory store.
func TestScope(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give func(*scope.Config)
			name string
		}{
			{
				name: "returns ErrConfig for a config without a source of randomness",
				give: func(c *scope.Config) { c.Rand = nil },
			},
			{
				name: "returns ErrConfig for a config without a counter of unknown outcomes",
				give: func(c *scope.Config) { c.Unknown = nil },
			},
			{
				name: "returns ErrConfig for a config without an acquisition",
				give: func(c *scope.Config) { c.Acquisition = nil },
			},
			{
				name: "returns ErrConfig for a config without a counter of tokens",
				give: func(c *scope.Config) { c.Tokens = nil },
			},
			{
				name: "returns ErrConfig for a config without a namespace",
				give: func(c *scope.Config) { c.Scope.Namespace = "" },
			},
			{
				name: "returns ErrConfig for a config without a term",
				give: func(c *scope.Config) { c.Term = epoch.Zero },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				var tokens machine.Token
				cfg := scope.Config{
					Rand:        constant.FromFloat64(half),
					Unknown:     &counter{},
					Acquisition: cellAcquisition,
					Tokens:      &tokens,
					Scope:       cellScope,
					Term:        term,
				}
				tt.give(&cfg)
				_, err := scope.New(&cfg)
				assert.ErrorIs(t, err, scope.ErrConfig, "New must refuse a config without a required field")
			})
		}

		t.Run("returns ErrConfig for a nil config", func(t *testing.T) {
			t.Parallel()
			_, err := scope.New(nil)
			assert.ErrorIs(t, err, scope.ErrConfig, "New must refuse a nil config")
		})
	})

	t.Run("Acquire", func(t *testing.T) {
		t.Parallel()

		t.Run("creates a scope before its first commit", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			snap := h.acquire(t)
			expect.Equal(t, snap.State.Head,
				store.Head{Generation: 1, Watermark: term, Schema: store.Schema, Incarnation: incA},
				"the acquisition must create the first generation of the scope at the term")
			expect.Equal(t, snap.State.Cell, record, "the acquisition must commit the record of the controller")
			expect.Empty(t, snap.Placements, "a new scope must have no placements")
		})

		t.Run("creates the namespace with the mutation of its acquisition", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, rootScope, rootAcquisition)
			snap := h.acquire(t)
			expect.Equal(t, snap.State.Head.Incarnation, incA, "the root must create the scope with its incarnation")
			expect.Equal(t, snap.State.Mapping, mapping, "the root must record the mapping of the namespace")
			expect.Equal(t, snap.State.Spec, []byte(spec), "the root must record the spec of the namespace")
		})

		t.Run("reads every placement over the pages of the store", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.write(t, cellScope, incA, oldTerm, placements(crowd)...)
			snap := h.acquire(t)
			expect.Equal(t, snap.Placements, placements(crowd),
				"the snapshot must list every placement in position order")
			expect.Equal(t, h.reads, 3, "the Scope must read the placements in three pages")
		})

		t.Run("returns out unchanged after the first call", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.pending = h.scope.Acquire(h.pending)
			out := []machine.Effect{foreign}
			assert.Equal(t, h.scope.Acquire(out), out, "a later Acquire must return out unchanged")
		})

		t.Run("takes the token of the header read from the counter of the machine", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.pending = h.scope.Acquire(h.pending)
			head := take[machine.Head](t, h)
			expect.Equal(t, head.Token, taken+1, "the read of the header must take the next token of the counter")
			expect.Equal(t, h.tokens, head.Token, "the counter must contain the last token that the Scope took")
		})

		t.Run("commits again after a commit of the replaced controller", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.write(t, cellScope, incA, oldTerm)
			h.pending = h.scope.Acquire(h.pending)
			h.deliver(t)
			h.write(t, cellScope, incA, oldTerm)
			snap := h.acquired(t)
			expect.Equal(t, snap.State.Head.Generation, epoch.Epoch(3),
				"the second attempt must create the generation after the commit of the replaced controller")
			expect.False(t, h.receipt(t, acquisitionID), "the first attempt must not apply")
			expect.True(t, h.receipt(t, store.CommitID{Incarnation: incA, Term: term, Seq: 2}),
				"the second attempt must apply")
		})

		t.Run("acquires a scope that another root created first", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, rootScope, rootAcquisition)
			h.pending = h.scope.Acquire(h.pending)
			h.deliver(t)
			h.write(t, rootScope, incB, oldTerm)
			snap := h.acquired(t)
			expect.Equal(t, snap.State.Head.Incarnation, incB, "the Scope must take the incarnation of the other root")
			expect.Equal(t, snap.State.Cell, record, "the acquisition must commit the record of the controller")
		})

		t.Run("returns ErrSchema for a scope of a newer schema", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.pending = h.scope.Acquire(h.pending)
			head := take[machine.Head](t, h)
			h.step(t, machine.HeadRead{Token: head.Token, Head: store.Head{Schema: store.Schema + 1}})
			assert.ErrorIs(t, h.failure(t), scope.ErrSchema, "the Scope must refuse a scope of a newer schema")
		})

		t.Run("returns epoch.ErrFenced for a scope that a newer controller acquired", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.write(t, cellScope, incA, newTerm)
			h.pending = h.scope.Acquire(h.pending)
			assert.ErrorIs(t, h.failure(t), epoch.ErrFenced, "the Scope must stop at the fence of a newer controller")
		})

		t.Run("returns shard.ErrIncarnation for a scope of another incarnation", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.write(t, cellScope, incB, oldTerm)
			h.pending = h.scope.Acquire(h.pending)
			assert.ErrorIs(t, h.failure(t), shard.ErrIncarnation,
				"the Scope must stop at a scope of another incarnation")
		})

		t.Run("returns store.ErrTooLarge for an acquisition that the store refuses", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition, memory.WithLimits(oneByte))
			h.pending = h.scope.Acquire(h.pending)
			assert.ErrorIs(t, h.failure(t), store.ErrTooLarge,
				"the Scope must stop at an acquisition that cannot commit")
		})

		t.Run("commits a lost acquisition again with its commit ID", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.st.FailAt(memory.PointCommitApply)
			snap := h.acquire(t)
			expect.Equal(t, snap.State.Head.Generation, epoch.Epoch(1), "the acquisition must apply once")
			expect.True(t, h.receipt(t, acquisitionID), "the retry must apply the commit ID of the first attempt")
			expect.Equal(t, h.waits, []time.Duration{50 * time.Millisecond},
				"the Scope must wait one backoff before the retry")
		})

		t.Run("settles an acquisition whose reply is lost", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.st.FailAt(memory.PointCommitAck)
			snap := h.acquire(t)
			expect.Equal(t, snap.State.Head.Generation, epoch.Epoch(1), "the acquisition must apply once")
			expect.True(t, h.receipt(t, acquisitionID), "the store must keep the receipt of the acquisition")
		})

		t.Run("takes the receipt of a lost acquisition that applied", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.pending = h.scope.Acquire(h.pending)
			h.deliver(t)
			h.st.FailAt(memory.PointCommitAck)
			h.deliver(t)
			h.deliver(t)
			retry := take[machine.Commit](t, h)
			h.step(t, machine.Committed{Err: version.ErrMismatch, Token: retry.Token})
			snap := h.acquired(t)
			expect.Equal(t, snap.State.Head.Generation, epoch.Epoch(1),
				"the Scope must take the generation of the receipt")
		})

		t.Run("reads the header again when a lost acquisition has no receipt", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.write(t, cellScope, incA, oldTerm)
			h.pending = h.scope.Acquire(h.pending)
			h.deliver(t)
			h.st.FailAt(memory.PointCommitApply)
			h.deliver(t)
			h.write(t, cellScope, incA, oldTerm)
			snap := h.acquired(t)
			expect.Equal(t, snap.State.Head.Generation, epoch.Epoch(3),
				"the next attempt must create the generation after the commit of the replaced controller")
			expect.False(t, h.receipt(t, acquisitionID), "the lost attempt must not apply")
			expect.True(t, h.receipt(t, store.CommitID{Incarnation: incA, Term: term, Seq: 2}),
				"the next attempt must apply")
		})

		t.Run("starts the backoff again for the header of the next attempt", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.pending = h.scope.Acquire(h.pending)
			h.deliver(t)
			first := take[machine.Commit](t, h)
			h.step(t, machine.Committed{Err: version.ErrOutcomeUnknown, Token: first.Token})
			h.deliver(t)
			retry := take[machine.Commit](t, h)
			h.step(t, machine.Committed{Err: shard.ErrIncarnation, Token: retry.Token})
			h.deliverDuringOutage(t)
			timer := take[machine.Timer](t, h)
			assert.Equal(t, timer.Duration, 50*time.Millisecond,
				"the backoff of the header of the next attempt must start at the first ceiling")
		})

		t.Run("reads the header again after an outage", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.pending = h.scope.Acquire(h.pending)
			h.deliverDuringOutage(t)
			snap := h.acquired(t)
			expect.Equal(t, snap.State.Head.Generation, epoch.Epoch(1), "the acquisition must apply after the outage")
			expect.Equal(t, h.waits, []time.Duration{50 * time.Millisecond},
				"the Scope must wait one backoff before it reads the header again")
		})

		t.Run("returns the error of a header that is not transient", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			assert.NoError(t, h.st.Close(), "the store must close")
			h.pending = h.scope.Acquire(h.pending)
			assert.ErrorIs(t, h.failure(t), memory.ErrClosed, "the Scope must stop at an error that a retry cannot fix")
		})

		t.Run("reads a page again after an outage", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.write(t, cellScope, incA, oldTerm, placements(crowd)...)
			h.pending = h.scope.Acquire(h.pending)
			h.deliver(t)
			h.deliver(t)
			h.deliverDuringOutage(t)
			snap := h.acquired(t)
			expect.Equal(t, snap.Placements, placements(crowd), "the snapshot must list every placement once")
		})

		t.Run("returns the error of a page that is not transient", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.pending = h.scope.Acquire(h.pending)
			h.deliver(t)
			h.deliver(t)
			assert.NoError(t, h.st.Close(), "the store must close")
			assert.ErrorIs(t, h.failure(t), memory.ErrClosed, "the Scope must stop at an error that a retry cannot fix")
		})

		t.Run("starts the read again after store.ErrCompacted", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.write(t, cellScope, incA, oldTerm, placements(crowd)...)
			h.pending = h.scope.Acquire(h.pending)
			h.deliver(t)
			h.deliver(t)
			h.deliver(t)
			read := take[machine.Read](t, h)
			h.step(t, machine.PageRead{Err: store.ErrCompacted, Token: read.Token})
			h.deliver(t)
			restart := take[machine.Read](t, h)
			expect.Equal(t, restart.Page, page.Page{}, "the read must start again at the first page")
			h.step(t, h.perform(t, restart))
			snap := h.acquired(t)
			expect.Equal(t, snap.Placements, placements(crowd), "the snapshot must list every placement once")
		})

		t.Run("keeps the state of the first page", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.pending = h.scope.Acquire(h.pending)
			h.deliver(t)
			h.deliver(t)
			first := take[machine.Read](t, h)
			state := store.State{Spec: []byte(spec), Head: store.Head{Generation: 1}}
			h.step(t, machine.PageRead{
				Placements: []shard.Placement{placement(1)},
				Next:       nextPage,
				State:      state,
				Token:      first.Token,
			})
			second := take[machine.Read](t, h)
			expect.Equal(t, second.Page, page.Page{Token: nextPage}, "the second read must ask for the next page")
			h.step(t, machine.PageRead{
				Placements: []shard.Placement{placement(2)},
				State:      store.State{Head: store.Head{Generation: 1}},
				Token:      second.Token,
			})
			snap := h.snapshot(t)
			expect.Equal(t, snap.State, state, "the snapshot must keep the state of the first page")
			expect.Equal(t, snap.Placements, placements(2), "the snapshot must list the placements of both pages")
		})

		t.Run("returns ErrChanged for a scope that changed after its acquisition", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.pending = h.scope.Acquire(h.pending)
			h.deliver(t)
			h.deliver(t)
			h.st.Delete(cellScope)
			assert.ErrorIs(t, h.failure(t), scope.ErrChanged,
				"the Scope must stop at a generation that it did not create")
		})

		t.Run(
			"returns shard.ErrIncarnation for a scope that another incarnation created during the read",
			func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, cellScope, cellAcquisition)
				h.pending = h.scope.Acquire(h.pending)
				h.deliver(t)
				h.deliver(t)
				h.st.Delete(cellScope)
				h.write(t, cellScope, incB, oldTerm)
				assert.ErrorIs(t, h.failure(t), shard.ErrIncarnation,
					"the Scope must read the scope of its own incarnation only")
			},
		)

		t.Run("releases the mutation of an attempt that did not apply", func(t *testing.T) {
			t.Parallel()
			var made []weak.Pointer[store.Mutation]
			acquisition := func(head store.Head) (shard.NamespaceIncarnation, *store.Mutation) {
				inc, m := cellAcquisition(head)
				made = append(made, weak.Make(m))
				return inc, m
			}
			h := newHarness(t, cellScope, acquisition)
			h.write(t, cellScope, incA, oldTerm)
			h.pending = h.scope.Acquire(h.pending)
			h.deliver(t)
			h.write(t, cellScope, incA, oldTerm)
			h.deliver(t)
			runtime.GC()
			assert.Length(t, made, 1, "the Scope must call the acquisition once before the next header arrives")
			assert.Nil(t, made[0].Value(), "the Scope must release the mutation of an attempt that did not apply")
			runtime.KeepAlive(h)
		})

		t.Run("releases the snapshot once it has reported it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.write(t, cellScope, incA, oldTerm, placements(2)...)
			gone := func() weak.Pointer[shard.Placement] {
				snap := h.acquire(t)
				h.results = nil
				return weak.Make(&snap.Placements[0])
			}()
			runtime.GC()
			assert.Nil(t, gone.Value(), "the Scope must release the snapshot once it has reported it")
			runtime.KeepAlive(h)
		})

		t.Run("releases the pages of a read that stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.pending = h.scope.Acquire(h.pending)
			h.deliver(t)
			h.deliver(t)
			first := take[machine.Read](t, h)
			gone := func() weak.Pointer[store.Drain] {
				state := store.State{Drains: []store.Drain{{Node: node}}, Head: store.Head{Generation: 1}}
				h.step(t, machine.PageRead{Next: nextPage, State: state, Token: first.Token})
				return weak.Make(&state.Drains[0])
			}()
			assert.NoError(t, h.st.Close(), "the store must close")
			assert.ErrorIs(t, h.failure(t), memory.ErrClosed, "the closed store must stop the Scope")
			runtime.GC()
			assert.Nil(t, gone.Value(), "a stopped Scope must release the pages that it read")
			runtime.KeepAlive(h)
		})
	})

	t.Run("Commit", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrNotAcquired before the acquisition", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			_, out, err := h.scope.Commit(&store.Mutation{}, nil)
			expect.That(t, err).ErrorIs(scope.ErrNotAcquired, "Commit must refuse a mutation before the acquisition")
			expect.Empty(t, out, "a refused commit must append nothing")
		})

		t.Run("returns ErrNotAcquired after the scope has stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			h.write(t, cellScope, incA, newTerm)
			h.commit(t, &store.Mutation{Put: []shard.Placement{placement(1)}})
			assert.ErrorIs(t, h.failure(t), epoch.ErrFenced, "the fence of a newer controller must stop the Scope")
			_, _, err := h.scope.Commit(&store.Mutation{}, nil)
			assert.ErrorIs(t, err, scope.ErrNotAcquired, "Commit must refuse a mutation after a stop")
		})

		t.Run("returns the next commit ID of the term", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			got := h.commit(t, &store.Mutation{Put: placements(1)})
			assert.Equal(t, got, store.CommitID{Incarnation: incA, Term: term, Seq: 2},
				"the first commit after the acquisition must have the second commit ID of the term")
		})

		t.Run("commits one mutation at a time", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			for n := range 3 {
				h.commit(t, &store.Mutation{Put: []shard.Placement{placement(n + 1)}})
			}
			assert.Length(t, h.pending, 1, "one commit must be in flight")
		})

		t.Run("applies the mutations in the order of the calls", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			ids := make([]store.CommitID, 0, 3)
			for n := range 3 {
				ids = append(ids, h.commit(t, &store.Mutation{Put: []shard.Placement{placement(n + 1)}}))
			}
			h.drain(t)
			expect.Equal(t, h.applied(), ids, "the Scope must report each commit in the order of the calls")
			expect.Equal(t, h.placements(t), placements(3), "the store must contain the placement of each commit")
		})

		t.Run("confirms the receipt of the commit before it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			first := &store.Mutation{Put: []shard.Placement{placement(1)}}
			h.commit(t, first)
			h.drain(t)
			second := &store.Mutation{Put: []shard.Placement{placement(2)}}
			h.commit(t, second)
			expect.Equal(t, first.Confirm, []store.CommitID{acquisitionID},
				"the first commit must confirm the receipt of the acquisition")
			expect.Equal(t, second.Confirm, []store.CommitID{first.ID},
				"the second commit must confirm the receipt of the first")
		})

		t.Run("leaves the receipt of the last commit alone", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			first := h.commit(t, &store.Mutation{Put: []shard.Placement{placement(1)}})
			second := h.commit(t, &store.Mutation{Put: []shard.Placement{placement(2)}})
			h.drain(t)
			expect.False(t, h.receipt(t, acquisitionID), "a later commit must confirm the receipt of the acquisition")
			expect.False(t, h.receipt(t, first), "a later commit must confirm the receipt of the first commit")
			expect.True(t, h.receipt(t, second), "the store must keep the receipt of the last commit")
		})

		t.Run("commits a lost commit again with the same mutation", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			m := &store.Mutation{Put: []shard.Placement{placement(1)}}
			cid := h.commit(t, m)
			h.st.FailAt(memory.PointCommitApply)
			h.deliver(t)
			h.deliver(t)
			retry := take[machine.Commit](t, h)
			expect.Equal(t, retry.Mutation, m, "the retry must commit the same mutation", expect.ByIdentity())
			expect.Equal(t, retry.Expect, epoch.Epoch(1), "the retry must expect the same generation")
			h.step(t, h.perform(t, retry))
			expect.Equal(t, h.applied(), []store.CommitID{cid}, "the retry must apply the commit")
		})

		t.Run("counts each attempt that fails with an unknown outcome", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			h.commit(t, &store.Mutation{Put: []shard.Placement{placement(1)}})
			h.st.FailAt(memory.PointCommitApply)
			h.deliver(t)
			h.deliver(t)
			h.st.FailAt(memory.PointCommitAck)
			h.drain(t)
			assert.Equal(t, h.unknowns.sum, int64(2), "the Scope must count both attempts with an unknown outcome")
		})

		t.Run("applies a commit whose reply is lost once", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			h.commit(t, &store.Mutation{Put: []shard.Placement{placement(1)}})
			h.st.FailAt(memory.PointCommitAck)
			h.drain(t)
			assert.Length(t, h.results, 2, "the Scope must report the acquisition and the commit")
			expect.Equal(t, h.results[1].Generation, epoch.Epoch(2), "the commit must create one generation")
			expect.Equal(t, h.head(t).Generation, epoch.Epoch(2), "the retry must not apply the mutation again")
		})

		t.Run("reads the receipt again after an outage", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			h.commit(t, &store.Mutation{Put: []shard.Placement{placement(1)}})
			h.st.FailAt(memory.PointCommitApply)
			h.deliver(t)
			h.st.Delete(cellScope)
			h.deliver(t)
			h.deliver(t)
			h.deliverDuringOutage(t)
			assert.ErrorIs(t, h.failure(t), scope.ErrChanged, "the Scope must stop at a commit without a receipt")
			expect.Equal(t, h.waits, []time.Duration{50 * time.Millisecond, 50 * time.Millisecond},
				"the Scope must wait one backoff before each retry")
		})

		t.Run("returns the error of a receipt read that is not transient", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			h.commit(t, &store.Mutation{Put: []shard.Placement{placement(1)}})
			h.st.FailAt(memory.PointCommitApply)
			h.deliver(t)
			h.st.Delete(cellScope)
			h.deliver(t)
			h.deliver(t)
			assert.NoError(t, h.st.Close(), "the store must close")
			assert.ErrorIs(t, h.failure(t), memory.ErrClosed, "the Scope must stop at an error that a retry cannot fix")
		})

		t.Run("returns ErrChanged for a scope that an operator deleted", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			h.st.Delete(cellScope)
			h.commit(t, &store.Mutation{Put: []shard.Placement{placement(1)}})
			err := h.failure(t)
			expect.That(t, err).ErrorIs(scope.ErrChanged, "the Scope must stop at a generation that it did not create")
			expect.That(t, err).ErrorIs(version.ErrMismatch, "the error must wrap the refusal of the store")
		})

		t.Run("returns ErrChanged for a lost commit without a receipt", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			h.commit(t, &store.Mutation{Put: []shard.Placement{placement(1)}})
			h.st.FailAt(memory.PointCommitApply)
			h.deliver(t)
			h.st.Delete(cellScope)
			assert.ErrorIs(t, h.failure(t), scope.ErrChanged, "the Scope must stop at a commit without a receipt")
		})

		t.Run("returns version.ErrMismatch for a deleted scope after an earlier commit was lost", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			h.commit(t, &store.Mutation{Put: []shard.Placement{placement(1)}})
			h.st.FailAt(memory.PointCommitApply)
			h.drain(t)
			h.st.Delete(cellScope)
			h.commit(t, &store.Mutation{Put: []shard.Placement{placement(2)}})
			assert.ErrorIs(t, h.failure(t), version.ErrMismatch,
				"the Scope must stop at the refusal of the store without a read of the receipt")
		})

		t.Run("returns shard.ErrIncarnation for a scope that another incarnation created again", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			h.st.Delete(cellScope)
			h.write(t, cellScope, incB, oldTerm)
			h.commit(t, &store.Mutation{Put: []shard.Placement{placement(1)}})
			assert.ErrorIs(t, h.failure(t), shard.ErrIncarnation,
				"the Scope must stop at a scope of another incarnation")
		})

		t.Run("returns epoch.ErrFenced after a newer controller acquired the scope", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			h.write(t, cellScope, incA, newTerm)
			h.commit(t, &store.Mutation{Put: []shard.Placement{placement(1)}})
			assert.ErrorIs(t, h.failure(t), epoch.ErrFenced, "the Scope must stop at the fence of a newer controller")
		})

		t.Run(
			"returns epoch.ErrFenced for a lost commit after a newer controller acquired the scope",
			func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, cellScope, cellAcquisition)
				h.acquire(t)
				h.commit(t, &store.Mutation{Put: []shard.Placement{placement(1)}})
				h.st.FailAt(memory.PointCommitApply)
				h.deliver(t)
				h.write(t, cellScope, incA, newTerm)
				assert.ErrorIs(t, h.failure(t), epoch.ErrFenced,
					"the Scope must stop at the fence of a newer controller")
			},
		)

		t.Run("returns store.ErrTooLarge for a mutation that the store refuses", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition, memory.WithLimits(onePlacement))
			h.acquire(t)
			h.commit(t, &store.Mutation{Put: placements(2)})
			assert.ErrorIs(t, h.failure(t), store.ErrTooLarge, "the Scope must stop at a mutation that cannot commit")
		})

		t.Run("releases a mutation once its commit has applied", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			gone := func() weak.Pointer[store.Mutation] {
				m := &store.Mutation{Put: []shard.Placement{placement(1)}}
				h.commit(t, m)
				return weak.Make(m)
			}()
			h.drain(t)
			runtime.GC()
			assert.Nil(t, gone.Value(), "the Scope must release a mutation once its commit has applied")
			runtime.KeepAlive(h)
		})

		t.Run("releases its mutations once the Scope has stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			h.write(t, cellScope, incA, newTerm)
			gone := func() []weak.Pointer[store.Mutation] {
				first := &store.Mutation{Put: []shard.Placement{placement(1)}}
				second := &store.Mutation{Put: []shard.Placement{placement(2)}}
				h.commit(t, first)
				h.commit(t, second)
				return []weak.Pointer[store.Mutation]{weak.Make(first), weak.Make(second)}
			}()
			assert.ErrorIs(t, h.failure(t), epoch.ErrFenced, "the fence of a newer controller must stop the Scope")
			runtime.GC()
			expect.Nil(t, gone[0].Value(), "a stopped Scope must release the mutation of its commit in flight")
			expect.Nil(t, gone[1].Value(), "a stopped Scope must release the mutations that wait")
			runtime.KeepAlive(h)
		})

		t.Run("applies each mutation once in the order of the calls", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "a Scope applies each mutation once, in the order of the calls", func(c *prop.Case) {
				h := newHarness(c, cellScope, cellAcquisition)
				base := h.acquire(c).State.Head.Generation
				var ids []store.CommitID
				busy := func(struct{}) bool { return len(h.pending) > 0 }
				committing := func(struct{}) bool {
					if len(h.pending) == 0 {
						return false
					}
					_, ok := h.pending[0].(machine.Commit)
					return ok
				}
				stateful.Steps(c, stateful.Machine[struct{}]{
					Actions: []stateful.Action[struct{}]{
						{Name: "commit", Run: func(c *prop.Case, _ int, _ any) {
							m := &store.Mutation{Put: []shard.Placement{placement(len(ids) + 1)}}
							ids = append(ids, h.commit(c, m))
						}},
						{Name: "deliver", Enabled: busy, Run: func(c *prop.Case, _ int, _ any) { h.deliver(c) }},
						{Name: "lose the commit", Enabled: committing, Run: func(c *prop.Case, _ int, _ any) {
							c.Classify(lostCommit)
							h.st.FailAt(memory.PointCommitApply)
							h.deliver(c)
						}},
						{Name: "lose the reply", Enabled: committing, Run: func(c *prop.Case, _ int, _ any) {
							c.Classify(lostReply)
							h.st.FailAt(memory.PointCommitAck)
							h.deliver(c)
						}},
						{Name: "fail during an outage", Enabled: busy, Run: func(c *prop.Case, _ int, _ any) {
							c.Classify(outage)
							h.deliverDuringOutage(c)
						}},
					},
					Invariant: func(c *prop.Case, _ struct{}) {
						assert.InRange(c, len(h.pending), 0, 1, "at most one effect of the Scope may be in flight")
						applied := h.applied()
						assert.InRange(c, len(applied), 0, float64(len(ids)),
							"the Scope may report only the commits of the calls")
						assert.Equal(c, applied, ids[:len(applied)],
							"the Scope must report the commits in the order of the calls", assert.EquateEmpty())
					},
					Settle: func(c *prop.Case, _ struct{}) {
						h.drain(c)
						assert.Total(c, func(res scope.Result) error { return res.Err }, h.results,
							"no fault may stop the Scope")
						assert.Equal(c, h.applied(), ids, "the Scope must report every commit", assert.EquateEmpty())
						want := make([]epoch.Epoch, 0, len(h.results))
						for i := range h.results {
							want = append(want, base+epoch.Epoch(i))
						}
						assert.Equal(c, h.generations(), want, "each commit must create the next generation once",
							assert.EquateEmpty())
						assert.Equal(c, h.placements(c), placements(len(ids)),
							"the store must contain the placement of each commit", assert.EquateEmpty())
						confirmed := append([]store.CommitID{acquisitionID}, ids...)
						last := confirmed[len(confirmed)-1]
						assert.True(c, h.receipt(c, last), "the store must keep the receipt of the last commit")
						assert.Total(c, func(cid store.CommitID) error {
							if h.receipt(c, cid) {
								return fmt.Errorf("the store keeps the receipt of %v", cid)
							}
							return nil
						}, confirmed[:len(confirmed)-1], "a later commit must confirm each earlier receipt")
					},
				})
			}, prop.Require(lostCommit, faultShare), prop.Require(lostReply, faultShare), prop.Require(outage, faultShare))
		})
	})

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give func(machine.Token) machine.Event
			name string
		}{
			{
				name: "reports false for an Elapsed of another token",
				give: func(tok machine.Token) machine.Event { return machine.Elapsed{Token: tok} },
			},
			{
				name: "reports false for a HeadRead of another token",
				give: func(tok machine.Token) machine.Event { return machine.HeadRead{Token: tok} },
			},
			{
				name: "reports false for a Committed of another token",
				give: func(tok machine.Token) machine.Event { return machine.Committed{Token: tok} },
			},
			{
				name: "reports false for an OutcomeRead of another token",
				give: func(tok machine.Token) machine.Event { return machine.OutcomeRead{Token: tok} },
			},
			{
				name: "reports false for a PageRead of another token",
				give: func(tok machine.Token) machine.Event { return machine.PageRead{Token: tok} },
			},
			{
				name: "reports false for an event that a Scope does not take",
				give: func(machine.Token) machine.Event { return machine.Started{} },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, cellScope, cellAcquisition)
				h.pending = h.scope.Acquire(h.pending)
				out := []machine.Effect{foreign}
				_, got, ok := h.scope.Step(tt.give(h.tokens+1), out)
				expect.False(t, ok, "Step must not take an event that the Scope does not wait for")
				expect.Equal(t, got, out, "Step must return out unchanged for an event that it does not take")
			})
		}

		t.Run("reports false for the result of a commit that has applied", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			h.commit(t, &store.Mutation{Put: []shard.Placement{placement(1)}})
			ev := h.perform(t, take[machine.Commit](t, h))
			h.step(t, ev)
			_, _, ok := h.scope.Step(ev, nil)
			assert.False(t, ok, "Step must take the result of an effect once")
		})

		t.Run("reports false for the last page of a snapshot that it has taken", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.pending = h.scope.Acquire(h.pending)
			h.deliver(t)
			h.deliver(t)
			ev := h.perform(t, take[machine.Read](t, h))
			h.step(t, ev)
			_, _, ok := h.scope.Step(ev, nil)
			assert.False(t, ok, "Step must take the last page of a snapshot once")
		})

		t.Run("reports false after the Scope has stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.write(t, cellScope, incA, newTerm)
			h.pending = h.scope.Acquire(h.pending)
			h.deliver(t)
			ev := h.perform(t, take[machine.Commit](t, h))
			h.step(t, ev)
			_, _, ok := h.scope.Step(ev, nil)
			assert.False(t, ok, "a stopped Scope must not take an event")
		})

		t.Run("returns out unchanged when a commit applies and no mutation waits", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.acquire(t)
			h.commit(t, &store.Mutation{Put: []shard.Placement{placement(1)}})
			ev := h.perform(t, take[machine.Commit](t, h))
			out := []machine.Effect{foreign}
			_, got, ok := h.scope.Step(ev, out)
			assert.True(t, ok, "the Scope must take the result of its own commit")
			assert.Equal(t, got, out, "Step must return out unchanged when no mutation waits")
		})

		t.Run("takes the token of a Timer from the counter of the machine", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.pending = h.scope.Acquire(h.pending)
			h.deliverDuringOutage(t)
			timer := take[machine.Timer](t, h)
			expect.Equal(t, timer.Token, taken+2,
				"the Timer must take the next token of the counter after the read of the header")
			expect.Equal(t, h.tokens, timer.Token, "the counter must contain the last token that the Scope took")
		})

		t.Run("waits a backoff that doubles with each retry up to RetryMax", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.st.StartOutage()
			h.pending = h.scope.Acquire(h.pending)
			for range 9 {
				h.deliver(t)
				h.deliver(t)
			}
			h.st.EndOutage()
			h.acquired(t)
			assert.Equal(t, h.waits, []time.Duration{
				50 * time.Millisecond,
				100 * time.Millisecond,
				200 * time.Millisecond,
				400 * time.Millisecond,
				800 * time.Millisecond,
				1600 * time.Millisecond,
				3200 * time.Millisecond,
				5 * time.Second,
				5 * time.Second,
			}, "each backoff must be half of a ceiling that doubles from RetryBase up to RetryMax")
		})

		t.Run("starts the backoff again after each attempt that succeeds", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, cellScope, cellAcquisition)
			h.write(t, cellScope, incA, oldTerm, placements(crowd)...)
			h.pending = h.scope.Acquire(h.pending)
			for range 3 {
				h.deliverDuringOutage(t)
				h.deliver(t)
				h.deliver(t)
			}
			h.deliverDuringOutage(t)
			h.acquired(t)
			assert.Equal(t, h.waits, []time.Duration{
				50 * time.Millisecond,
				50 * time.Millisecond,
				50 * time.Millisecond,
				50 * time.Millisecond,
			}, "each operation must start its backoff at the first ceiling")
		})
	})
}

// cellAcquisition is the acquisition of the controller of the cell of the tests, in the
// incarnation incA.
func cellAcquisition(store.Head) (shard.NamespaceIncarnation, *store.Mutation) {
	rec := record
	return incA, &store.Mutation{Cell: &rec}
}

// rootAcquisition is the acquisition of the root of the tests. It creates the namespace with
// the incarnation incA, the mapping and the spec when the scope has no incarnation, and
// acquires the incarnation of the scope otherwise.
func rootAcquisition(head store.Head) (shard.NamespaceIncarnation, *store.Mutation) {
	rec := record
	if head.Incarnation.IsZero() {
		m := mapping
		return incA, &store.Mutation{Cell: &rec, Mapping: &m, Spec: []byte(spec)}
	}
	return head.Incarnation, &store.Mutation{Cell: &rec}
}

// placement returns the placement of the shard n of the tests. Its range starts at the
// position n, so the placements of the tests are in position order by n.
func placement(n int) shard.Placement {
	pos := binary.BigEndian.AppendUint32(nil, uint32(n))
	return shard.Placement{
		Shard:    shard.ID{ID128: id.ID128{0: tagShard, 12: pos[0], 13: pos[1], 14: pos[2], 15: pos[3]}},
		Range:    shard.Range{Start: pos},
		Replicas: []shard.Replica{{Node: node, Role: shard.RolePrimary}},
	}
}

// placements returns the placements of the shards 1 to n of the tests, in position order.
func placements(n int) []shard.Placement {
	ps := make([]shard.Placement, 0, n)
	for i := range n {
		ps = append(ps, placement(i+1))
	}
	return ps
}

// take removes the oldest pending effect and returns it. The effect must be an E.
func take[E machine.Effect](tb assert.TB, h *harness) E {
	tb.Helper()
	assert.NotEmpty(tb, h.pending, "an effect of the Scope must be pending")
	e, ok := h.pending[0].(E)
	assert.True(tb, ok, "the oldest pending effect must have the type that the test expects")
	h.pending = slices.Delete(h.pending, 0, 1)
	return e
}

// acquire starts the acquisition of the Scope and runs it to its end. It returns the
// snapshot of the acquisition.
func (h *harness) acquire(tb assert.TB) *scope.Snapshot {
	tb.Helper()
	h.pending = h.scope.Acquire(h.pending)
	return h.acquired(tb)
}

// acquired runs the pending effects to their end, and returns the snapshot of the
// acquisition.
func (h *harness) acquired(tb assert.TB) *scope.Snapshot {
	tb.Helper()
	h.drain(tb)
	return h.snapshot(tb)
}

// snapshot returns the snapshot that the last Result of the Scope reported.
func (h *harness) snapshot(tb assert.TB) *scope.Snapshot {
	tb.Helper()
	assert.NotEmpty(tb, h.results, "the Scope must report a Result")
	last := h.results[len(h.results)-1]
	assert.NoError(tb, last.Err, "the acquisition must not stop the Scope")
	assert.NotNil(tb, last.Snapshot, "the last Result must report the snapshot")
	return last.Snapshot
}

// failure runs the pending effects to their end, and returns the error that stopped the
// Scope. The last Result must report that error and nothing else.
func (h *harness) failure(tb assert.TB) error {
	tb.Helper()
	h.drain(tb)
	assert.NotEmpty(tb, h.results, "the Scope must report a Result")
	last := h.results[len(h.results)-1]
	assert.Nil(tb, last.Snapshot, "the Result of a stop must not report a snapshot")
	assert.Equal(tb, last.Commit, store.CommitID{}, "the Result of a stop must not report a commit")
	return last.Err
}

// commit passes m to Commit and keeps the effects that Commit appends. It returns the
// commit ID that Commit gave m.
func (h *harness) commit(tb assert.TB, m *store.Mutation) store.CommitID {
	tb.Helper()
	cid, out, err := h.scope.Commit(m, h.pending)
	assert.NoError(tb, err, "Commit must take a mutation of an acquired scope")
	h.pending = out
	return cid
}

// drain delivers the pending effects and the effects that follow them, until no effect is
// pending.
func (h *harness) drain(tb assert.TB) {
	tb.Helper()
	for range maxDeliveries {
		if len(h.pending) == 0 {
			return
		}
		h.deliver(tb)
	}
	tb.Fatalf("the Scope has %d pending effects after %d deliveries", len(h.pending), maxDeliveries)
}

// deliver performs the oldest pending effect, and passes the event of its result to the
// Scope.
func (h *harness) deliver(tb assert.TB) {
	tb.Helper()
	h.step(tb, h.perform(tb, take[machine.Effect](tb, h)))
}

// deliverDuringOutage performs the oldest pending effect during an outage of the store,
// and passes the event of its failure to the Scope.
func (h *harness) deliverDuringOutage(tb assert.TB) {
	tb.Helper()
	h.st.StartOutage()
	h.deliver(tb)
	h.st.EndOutage()
}

// step passes ev to the Scope and keeps the effects that the Scope appends. It also keeps
// the Result when the Result reports something.
func (h *harness) step(tb assert.TB, ev machine.Event) {
	tb.Helper()
	res, out, ok := h.scope.Step(ev, h.pending)
	assert.True(tb, ok, "the Scope must take the result of its own effect")
	h.pending = out
	if res.Err != nil || res.Snapshot != nil || res.Commit != (store.CommitID{}) {
		h.results = append(h.results, res)
	}
}

// perform performs e against the store, and returns the event of its result.
func (h *harness) perform(tb assert.TB, e machine.Effect) machine.Event {
	tb.Helper()
	ctx := assert.Context(tb)
	switch e := e.(type) {
	case machine.Head:
		head, err := h.st.Head(ctx, e.Scope)
		return machine.HeadRead{Err: err, Token: e.Token, Head: head}
	case machine.Commit:
		gen, err := h.st.Writer(e.Scope, e.Incarnation, e.Term).Commit(ctx, e.Expect, e.Mutation)
		return machine.Committed{Err: err, Token: e.Token, Generation: gen}
	case machine.Outcome:
		receipt, found, err := h.st.Outcome(ctx, e.Scope, e.ID)
		return machine.OutcomeRead{Err: err, Token: e.Token, Receipt: receipt, Found: found}
	case machine.Read:
		return h.read(tb, &e)
	case machine.Timer:
		h.waits = append(h.waits, e.Duration)
		return machine.Elapsed{Token: e.Token}
	default:
		tb.Fatalf("the Scope appended a %T, which it does not use", e)
		return nil
	}
}

// read reads the page of e from the store, and returns the event of its result.
func (h *harness) read(tb assert.TB, e *machine.Read) machine.Event {
	tb.Helper()
	ctx := assert.Context(tb)
	h.reads++
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
}

// write commits puts to the scope id at its current generation, as another controller
// does. The writer of the commit has the incarnation inc and the term at.
func (h *harness) write(
	tb assert.TB, id shard.Scope, inc shard.NamespaceIncarnation, at epoch.Epoch, puts ...shard.Placement,
) {
	tb.Helper()
	ctx := assert.Context(tb)
	head, err := h.st.Head(ctx, id)
	assert.NoError(tb, err, "the store must return the header of the scope")
	h.others++
	m := &store.Mutation{ID: store.CommitID{Incarnation: inc, Term: at, Seq: h.others}, Put: puts}
	_, err = h.st.Writer(id, inc, at).Commit(ctx, head.Generation, m)
	assert.NoError(tb, err, "the other controller must commit")
}

// head returns the header of the scope of the cell.
func (h *harness) head(tb assert.TB) store.Head {
	tb.Helper()
	head, err := h.st.Head(assert.Context(tb), cellScope)
	assert.NoError(tb, err, "the store must return the header of the scope")
	return head
}

// receipt reports whether the scope of the cell keeps the receipt of the commit cid.
func (h *harness) receipt(tb assert.TB, cid store.CommitID) bool {
	tb.Helper()
	_, found, err := h.st.Outcome(assert.Context(tb), cellScope, cid)
	assert.NoError(tb, err, "the store must read the receipt")
	return found
}

// placements returns every placement of the scope of the cell, in position order.
func (h *harness) placements(tb assert.TB) []shard.Placement {
	tb.Helper()
	ctx := assert.Context(tb)
	var all []shard.Placement
	p := page.Page{}
	for {
		_, cursor, err := h.st.Read(ctx, cellScope, incA, store.Query{}, p)
		assert.NoError(tb, err, "the store must read the placements")
		for pl, err := range cursor.Seq(ctx) {
			assert.NoError(tb, err, "the cursor of the memory store must yield its placements")
			all = append(all, pl)
		}
		next := cursor.NextPage()
		assert.NoError(tb, cursor.Close(), "the cursor must close")
		if next == "" {
			return all
		}
		p = page.Page{Token: next}
	}
}

// applied returns the IDs of the commits that the Scope reported, in order.
func (h *harness) applied() []store.CommitID {
	var ids []store.CommitID
	for _, res := range h.results {
		if res.Commit != (store.CommitID{}) {
			ids = append(ids, res.Commit)
		}
	}
	return ids
}

// generations returns the generation of each Result of the Scope, in order. The first is
// the generation of the acquisition, and each later one is the generation of a commit.
func (h *harness) generations() []epoch.Epoch {
	gens := make([]epoch.Epoch, 0, len(h.results))
	for _, res := range h.results {
		if res.Snapshot != nil {
			gens = append(gens, res.Snapshot.State.Head.Generation)
		} else {
			gens = append(gens, res.Generation)
		}
	}
	return gens
}

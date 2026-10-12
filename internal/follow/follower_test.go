// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package follow_test

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand/constant"

	"go.thesmos.sh/sharder/internal/follow"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The names of the records of the tests.
const (
	namespace shard.Namespace = "orders"
	cell      shard.CellID    = "cell-1"
	nodeA     shard.NodeID    = "node-a"
	nodeB     shard.NodeID    = "node-b"
)

// The first bytes of the identifiers of the tests, one per kind of identifier.
const (
	tagIncarnation byte = 0x1a
	tagShard       byte = 0x5d
)

// term is the term of the controller that writes the scope of the tests.
const term epoch.Epoch = 2

// taken is the last token that the other parts of the machine of each test took before
// the Follower took its first.
const taken machine.Token = 7

// half is the value of each draw of the source of randomness of the tests, so each backoff
// is half of its ceiling.
const half = 0.5

// pageLimit is the number of placements on each page that a harness reads, so a snapshot
// of five placements takes three pages.
const pageLimit = 2

// shards is the number of shards that the controller of the properties moves.
const shards = 8

// deliveries is the weight of a delivery among the actions of the properties, so the
// Follower passes events between the faults.
const deliveries = 3

// maxDeliveries bounds the deliveries of a drain, so a Follower that never stops reading
// fails its test instead of the timeout of go test.
const maxDeliveries = 10000

// The labels of the faults of the properties of the Follower, and the least share of the
// cases of a property that each fault takes part in.
const (
	outage     = "an outage fails a read or a watch"
	compaction = "a compaction discards the generation of a read or a watch"
	repeated   = "the watch passes an item again"
	faultShare = 0.1
)

// foreign is an effect of another part of the machine. A test passes it to the Follower in
// out, and expects it back unchanged.
var foreign machine.Effect = machine.Timer{Token: taken}

// The scope, the incarnations and the clock of the tests.
var (
	// cellScope is the scope of the cell of the tests.
	cellScope = shard.Scope{Namespace: namespace, Cell: cell}
	// incA is the namespace incarnation of the scope of each test.
	incA = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0a})}
	// incB is the incarnation of another namespace of the same name.
	incB = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0b})}
	// origin is the start of the clock of each store of the tests.
	origin = time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC)
)

// testCase is the test, or the case of a property, that a harness runs in.
type testCase interface {
	assert.TB
	// Cleanup registers f to run when the test or the case ends.
	Cleanup(f func())
}

// harness is the runtime and the machine of the tests. It performs the effects of one
// Follower against a memory store, and keeps the state that a machine builds from the
// results of the Follower.
type harness struct {
	// state is the state that the machine keeps. It contains the placements by shard.
	state map[shard.ID]shard.Placement
	// partial contains the pages of the snapshot whose last page has not arrived, and is
	// nil without such a snapshot.
	partial map[shard.ID]shard.Placement
	// st is the store of the Follower.
	st *memory.Store
	// follower is the Follower under test.
	follower *follow.Follower
	// pending lists the reads and the Timers that the harness has not performed, the
	// oldest first.
	pending []machine.Effect
	// watches lists the watches that run. The From of each watch is the generation of the
	// last item that the harness took from it.
	watches []machine.Watch
	// results lists every Result that passes something, in the order of the steps.
	results []follow.Result
	// waits lists the duration of each Timer that the harness performed.
	waits []time.Duration
	// last is the last item with an event that the harness passed to the Follower.
	last machine.Watched
	// node is the node that the Follower selects, and empty for every node.
	node shard.NodeID
	// generation is the generation of the state that the machine keeps.
	generation epoch.Epoch
	// tokens is the counter of the tokens of the machine of the Follower.
	tokens machine.Token
	// seq is the Seq of the last commit of the controller of the tests.
	seq uint64
	// compacted reports that a read or a watch of the Follower met a compaction.
	compacted bool
}

// newHarness returns a harness of a Follower of the scope of the tests, which selects node
// and expects the incarnation inc. The store of the harness is a new memory store.
func newHarness(tb testCase, node shard.NodeID, inc shard.NamespaceIncarnation) *harness {
	tb.Helper()
	h := &harness{st: memory.New(fake.New(origin)), node: node, tokens: taken}
	tb.Cleanup(func() { assert.NoError(tb, h.st.Close(), "the store must close") })
	f, err := follow.New(&follow.Config{
		Rand:        constant.FromFloat64(half),
		Tokens:      &h.tokens,
		Scope:       cellScope,
		Node:        node,
		Incarnation: inc,
	})
	assert.NoError(tb, err, "New must return a Follower of a complete config")
	h.follower = f
	return h
}

// TestFollower checks the construction, the snapshots, the watches and the steps of a
// Follower against a memory store.
func TestFollower(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give func(*follow.Config)
			name string
		}{
			{
				name: "returns ErrConfig for a config without a source of randomness",
				give: func(c *follow.Config) { c.Rand = nil },
			},
			{
				name: "returns ErrConfig for a config without a counter of tokens",
				give: func(c *follow.Config) { c.Tokens = nil },
			},
			{
				name: "returns ErrConfig for a config without a namespace",
				give: func(c *follow.Config) { c.Scope.Namespace = "" },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				var tokens machine.Token
				cfg := follow.Config{Rand: constant.FromFloat64(half), Tokens: &tokens, Scope: cellScope}
				tt.give(&cfg)
				_, err := follow.New(&cfg)
				assert.ErrorIs(t, err, follow.ErrConfig, "New must refuse a config without a required field")
			})
		}

		t.Run("returns ErrConfig for a nil config", func(t *testing.T) {
			t.Parallel()
			_, err := follow.New(nil)
			assert.ErrorIs(t, err, follow.ErrConfig, "New must refuse a nil config")
		})
	})

	t.Run("Start", func(t *testing.T) {
		t.Parallel()

		t.Run("reads the first page of the snapshot", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, nodeA, incA)
			assert.Equal(t, h.follower.Start(nil), []machine.Effect{machine.Read{
				Scope:       cellScope,
				Query:       store.Query{Node: nodeA},
				Token:       taken + 1,
				Incarnation: incA,
			}}, "Start must read the first page of the node in the incarnation of the config")
		})

		t.Run("returns out unchanged after the first call", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", incA)
			h.route(h.follower.Start(nil))
			out := []machine.Effect{foreign}
			assert.Equal(t, h.follower.Start(out), out, "a later Start must return out unchanged")
		})

		t.Run("returns out unchanged after Stop", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", incA)
			h.follower.Stop(nil)
			out := []machine.Effect{foreign}
			assert.Equal(t, h.follower.Start(out), out, "Start must not start a stopped Follower")
		})
	})

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("passes every placement of the scope over its pages", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", shard.NamespaceIncarnation{})
			h.write(t, incA, placements(5, nodeA)...)
			h.route(h.follower.Start(nil))
			h.drain(t)
			expect.Equal(t, h.state, index(placements(5, nodeA)...), "the machine must keep every placement")
			expect.Equal(t, h.generation, epoch.Epoch(1), "the snapshot must be at the generation of the scope")
		})

		t.Run("marks the first page of a snapshot", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", shard.NamespaceIncarnation{})
			h.write(t, incA, placements(5, nodeA)...)
			h.route(h.follower.Start(nil))
			h.drain(t)
			first := make([]bool, 0, len(h.results))
			for _, res := range h.results {
				assert.NotNil(t, res.Page, "each Result of a snapshot must pass a page")
				first = append(first, res.Page.First)
			}
			assert.Equal(t, first, []bool{true, false, false}, "only the first page must have First")
		})

		t.Run("marks the last page of a snapshot", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", shard.NamespaceIncarnation{})
			h.write(t, incA, placements(5, nodeA)...)
			h.route(h.follower.Start(nil))
			h.drain(t)
			last := make([]bool, 0, len(h.results))
			for _, res := range h.results {
				assert.NotNil(t, res.Page, "each Result of a snapshot must pass a page")
				last = append(last, res.Page.Last)
			}
			assert.Equal(t, last, []bool{false, false, true}, "only the last page must have Last")
		})

		t.Run("passes the placements of its node", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, nodeA, shard.NamespaceIncarnation{})
			h.write(t, incA, placement(1, nodeA), placement(2, nodeB), placement(3, nodeA), placement(4, nodeB))
			h.route(h.follower.Start(nil))
			h.drain(t)
			assert.Equal(t, h.state, index(placement(1, nodeA), placement(3, nodeA)),
				"the machine must keep the placements of its node only")
		})

		t.Run("watches the scope from the generation of the snapshot", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, nodeA, shard.NamespaceIncarnation{})
			h.write(t, incA, placement(1, nodeA))
			h.write(t, incA, placement(2, nodeA))
			h.route(h.follower.Start(nil))
			h.drain(t)
			assert.Equal(t, h.watches, []machine.Watch{{
				Scope:       cellScope,
				Node:        nodeA,
				Token:       h.tokens,
				From:        2,
				Incarnation: incA,
			}}, "the Follower must watch its node from the snapshot in the incarnation of the scope")
		})

		t.Run("keeps the incarnation of its config for a scope before its first commit", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", incA)
			h.route(h.follower.Start(nil))
			h.drain(t)
			assert.Length(t, h.watches, 1, "one watch must run")
			assert.Equal(t, h.watches[0].Incarnation, incA, "the watch must keep the incarnation of the config")
		})

		t.Run("reads the snapshot again from its first page after store.ErrCompacted", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", shard.NamespaceIncarnation{})
			h.write(t, incA, placements(5, nodeA)...)
			h.route(h.follower.Start(nil))
			h.next(t)
			h.write(t, incA, placement(6, nodeB))
			h.st.Compact()
			h.next(t)
			h.next(t)
			restart := take[machine.Read](t, h)
			expect.Equal(t, restart.Page, page.Page{}, "the read must start again at the first page")
			h.step(t, h.perform(t, restart))
			h.drain(t)
			expect.Equal(t, h.state, index(append(placements(5, nodeA), placement(6, nodeB))...),
				"the machine must keep every placement of the scope")
		})

		t.Run("reads a page again after a transient error", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", shard.NamespaceIncarnation{})
			h.write(t, incA, placements(5, nodeA)...)
			h.route(h.follower.Start(nil))
			h.next(t)
			second := take[machine.Read](t, h)
			h.st.StartOutage()
			h.step(t, h.perform(t, second))
			h.st.EndOutage()
			h.next(t)
			retry := take[machine.Read](t, h)
			assert.Equal(t, retry.Page, second.Page, "the retry must read the same page")
		})

		t.Run("returns the error of a page that is not transient", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", shard.NamespaceIncarnation{})
			assert.NoError(t, h.st.Close(), "the store must close")
			h.route(h.follower.Start(nil))
			assert.ErrorIs(t, h.failure(t), memory.ErrClosed,
				"the Follower must stop at an error that a retry cannot fix")
		})

		t.Run("returns shard.ErrIncarnation for a scope of another incarnation", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", incB)
			h.write(t, incA, placement(1, nodeA))
			h.route(h.follower.Start(nil))
			assert.ErrorIs(t, h.failure(t), shard.ErrIncarnation,
				"the Follower must stop at a scope of another incarnation")
		})

		t.Run("passes each later generation of the watch", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", shard.NamespaceIncarnation{})
			h.write(t, incA, placement(1, nodeA))
			h.route(h.follower.Start(nil))
			h.drain(t)
			h.write(t, incA, placement(2, nodeA))
			h.write(t, incA, placement(1, nodeB))
			h.drain(t)
			expect.Equal(t, h.state, index(placement(1, nodeB), placement(2, nodeA)),
				"the machine must keep the placements of each generation")
			expect.Equal(t, h.generation, epoch.Epoch(3), "the machine must be at the last generation")
		})

		t.Run("drops an event at or below the generation that it passed", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", shard.NamespaceIncarnation{})
			h.write(t, incA, placement(1, nodeA))
			h.route(h.follower.Start(nil))
			h.drain(t)
			h.write(t, incA, placement(2, nodeA))
			h.drain(t)
			out := []machine.Effect{foreign}
			res, got, ok := h.follower.Step(h.last, out)
			assert.True(t, ok, "the Follower must take an item of its own watch")
			expect.Equal(t, res, follow.Result{}, "the Follower must not pass a generation again")
			expect.Equal(t, got, out, "Step must return out unchanged for an item that passes nothing")
		})

		t.Run("passes the progress of the watch of a node", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, nodeA, shard.NamespaceIncarnation{})
			h.write(t, incA, placement(1, nodeA))
			h.route(h.follower.Start(nil))
			h.drain(t)
			h.write(t, incA, placement(2, nodeB))
			h.drain(t)
			assert.NotEmpty(t, h.results, "the Follower must pass a Result")
			assert.Equal(t, h.results[len(h.results)-1].Event, store.Event{Generation: 2},
				"the Follower must pass the empty event of a generation without a change for its node")
		})

		t.Run("starts a new snapshot after store.ErrCompacted from the watch", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", shard.NamespaceIncarnation{})
			h.write(t, incA, placement(1, nodeA))
			h.route(h.follower.Start(nil))
			h.drain(t)
			h.write(t, incA, placement(2, nodeA))
			h.write(t, incA, placement(3, nodeA))
			h.st.Compact()
			h.next(t)
			h.next(t)
			restart := take[machine.Read](t, h)
			expect.Equal(t, restart.Page, page.Page{}, "the new snapshot must start at the first page")
			h.step(t, h.perform(t, restart))
			h.drain(t)
			expect.Equal(t, h.state, index(placements(3, nodeA)...), "the machine must keep every placement")
			expect.Equal(t, h.generation, epoch.Epoch(3), "the machine must be at the generation of the scope")
		})

		t.Run("resumes the watch from the last generation after a transient error", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", shard.NamespaceIncarnation{})
			h.write(t, incA, placement(1, nodeA))
			h.route(h.follower.Start(nil))
			h.drain(t)
			h.write(t, incA, placement(2, nodeA))
			h.drain(t)
			h.deliverDuringOutage(t)
			h.next(t)
			assert.Length(t, h.watches, 1, "one watch must run")
			assert.Equal(t, h.watches[0].From, epoch.Epoch(2),
				"the Follower must resume the watch after the last generation that it passed")
		})

		t.Run("returns shard.ErrIncarnation for a scope that another incarnation created again", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", shard.NamespaceIncarnation{})
			h.write(t, incA, placement(1, nodeA))
			h.route(h.follower.Start(nil))
			h.drain(t)
			h.st.Delete(cellScope)
			h.write(t, incB, placement(1, nodeA))
			assert.ErrorIs(t, h.failure(t), shard.ErrIncarnation,
				"the Follower must stop at a scope of another incarnation")
		})

		t.Run("takes the incarnation of the first commit of a new scope", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", shard.NamespaceIncarnation{})
			h.route(h.follower.Start(nil))
			h.drain(t)
			h.write(t, incA, placement(1, nodeA))
			h.drain(t)
			assert.Length(t, h.watches, 1, "the Follower must end the watch without an incarnation")
			expect.Equal(t, h.watches[0].Incarnation, incA, "the new watch must have the incarnation of the commit")
			expect.Equal(t, h.watches[0].From, epoch.Epoch(1), "the new watch must start after the first commit")
		})

		t.Run(
			"returns shard.ErrIncarnation for a new scope that another incarnation created again",
			func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, "", shard.NamespaceIncarnation{})
				h.route(h.follower.Start(nil))
				h.drain(t)
				h.write(t, incA, placement(1, nodeA))
				h.drain(t)
				h.st.Delete(cellScope)
				h.write(t, incB, placement(1, nodeA))
				assert.ErrorIs(t, h.failure(t), shard.ErrIncarnation,
					"the Follower must stop at a scope of another incarnation")
			},
		)

		t.Run("keeps the watch of a new scope through a generation without a change for its node", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, nodeA, shard.NamespaceIncarnation{})
			h.route(h.follower.Start(nil))
			h.drain(t)
			assert.Length(t, h.watches, 1, "one watch must run")
			before := h.watches[0]
			h.write(t, incA, placement(1, nodeB))
			h.drain(t)
			assert.Length(t, h.watches, 1, "one watch must run")
			expect.Equal(t, h.watches[0].Token, before.Token, "the Follower must keep the watch")
			expect.Equal(t, h.watches[0].Incarnation, before.Incarnation,
				"an empty event must not give the Follower an incarnation")
		})

		tests := []struct {
			give func(machine.Token) machine.Event
			name string
		}{
			{
				name: "reports false for an Elapsed of another token",
				give: func(tok machine.Token) machine.Event { return machine.Elapsed{Token: tok} },
			},
			{
				name: "reports false for a PageRead of another token",
				give: func(tok machine.Token) machine.Event { return machine.PageRead{Token: tok} },
			},
			{
				name: "reports false for a Watched of another token",
				give: func(tok machine.Token) machine.Event { return machine.Watched{Token: tok} },
			},
			{
				name: "reports false for an event that a Follower does not take",
				give: func(machine.Token) machine.Event { return machine.Started{} },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, "", incA)
				h.route(h.follower.Start(nil))
				out := []machine.Effect{foreign}
				_, got, ok := h.follower.Step(tt.give(h.tokens+1), out)
				expect.False(t, ok, "Step must not take an event that the Follower does not wait for")
				expect.Equal(t, got, out, "Step must return out unchanged for an event that it does not take")
			})
		}

		t.Run("reports false before Start", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", incA)
			out := []machine.Effect{foreign}
			_, got, ok := h.follower.Step(machine.Elapsed{Token: h.tokens + 1}, out)
			expect.False(t, ok, "a Follower must not take an event before Start")
			expect.Equal(t, got, out, "Step must return out unchanged before Start")
		})

		t.Run("reports false after Stop", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", incA)
			h.write(t, incA, placement(1, nodeA))
			h.route(h.follower.Start(nil))
			read := take[machine.Read](t, h)
			h.follower.Stop(nil)
			_, _, ok := h.follower.Step(h.perform(t, read), nil)
			assert.False(t, ok, "a stopped Follower must not take an event")
		})

		t.Run("reports false for an item of the watch after Stop", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", incA)
			h.write(t, incA, placement(1, nodeA))
			h.route(h.follower.Start(nil))
			h.drain(t)
			assert.Length(t, h.watches, 1, "one watch must run")
			item := machine.Watched{Token: h.watches[0].Token, Event: store.Event{Generation: 2}}
			h.follower.Stop(nil)
			_, _, ok := h.follower.Step(item, nil)
			assert.False(t, ok, "a stopped Follower must not take an item of the watch that Stop ended")
		})

		t.Run("reports false after an error has stopped it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", incB)
			h.write(t, incA, placement(1, nodeA))
			h.route(h.follower.Start(nil))
			ev := h.perform(t, take[machine.Read](t, h))
			h.step(t, ev)
			_, _, ok := h.follower.Step(ev, nil)
			assert.False(t, ok, "a Follower that an error stopped must not take an event")
		})

		t.Run("reports false for the result of a read that it has taken", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", incA)
			h.write(t, incA, placement(1, nodeA))
			h.route(h.follower.Start(nil))
			ev := h.perform(t, take[machine.Read](t, h))
			h.step(t, ev)
			_, _, ok := h.follower.Step(ev, nil)
			assert.False(t, ok, "Step must take the result of a read once")
		})

		t.Run("takes the token of each effect from the counter of the machine", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", incA)
			h.write(t, incA, placement(1, nodeA))
			h.route(h.follower.Start(nil))
			read := take[machine.Read](t, h)
			h.st.StartOutage()
			h.step(t, h.perform(t, read))
			h.st.EndOutage()
			timer := take[machine.Timer](t, h)
			h.step(t, h.perform(t, timer))
			retry := take[machine.Read](t, h)
			h.step(t, h.perform(t, retry))
			assert.Length(t, h.watches, 1, "one watch must run")
			assert.Equal(t,
				[]machine.Token{read.Token, timer.Token, retry.Token, h.watches[0].Token},
				[]machine.Token{taken + 1, taken + 2, taken + 3, taken + 4},
				"each effect must take the next token of the counter")
		})

		t.Run("waits a backoff that doubles with each retry up to RetryMax", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", incA)
			h.st.StartOutage()
			h.route(h.follower.Start(nil))
			for range 9 {
				h.next(t)
				h.next(t)
			}
			h.st.EndOutage()
			h.drain(t)
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
			h := newHarness(t, "", shard.NamespaceIncarnation{})
			h.write(t, incA, placements(3, nodeA)...)
			h.route(h.follower.Start(nil))
			for range 2 {
				h.deliverDuringOutage(t)
				h.next(t)
				h.next(t)
			}
			h.deliverDuringOutage(t)
			h.next(t)
			h.write(t, incA, placement(4, nodeA))
			h.next(t)
			h.deliverDuringOutage(t)
			h.drain(t)
			assert.Equal(t, h.waits, []time.Duration{
				50 * time.Millisecond,
				50 * time.Millisecond,
				50 * time.Millisecond,
				50 * time.Millisecond,
			}, "each page and each generation of the watch must start the next backoff at the first ceiling")
		})

		faults := []prop.Option{
			prop.Require(outage, faultShare),
			prop.Require(compaction, faultShare),
			prop.Require(repeated, faultShare),
		}

		t.Run("brings a machine of every node to the state of the store", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "a Follower of every node passes each generation once, and its machine ends with "+
				"the state of the store", func(c *prop.Case) { follows(c, "") }, faults...)
		})

		t.Run("brings a machine of one node to the state of the store", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "a Follower of one node passes each generation once, and its machine ends with "+
				"the state of the store", func(c *prop.Case) { follows(c, nodeA) }, faults...)
		})
	})

	t.Run("Stop", func(t *testing.T) {
		t.Parallel()

		t.Run("ends the watch that runs", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", incA)
			h.write(t, incA, placement(1, nodeA))
			h.route(h.follower.Start(nil))
			h.drain(t)
			assert.Length(t, h.watches, 1, "one watch must run")
			assert.Equal(t, h.follower.Stop(nil), []machine.Effect{machine.Unwatch{Token: h.watches[0].Token}},
				"Stop must end the watch that runs")
		})

		t.Run("returns out unchanged without a watch", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", incA)
			h.route(h.follower.Start(nil))
			out := []machine.Effect{foreign}
			assert.Equal(t, h.follower.Stop(out), out, "Stop must return out unchanged without a watch")
		})

		t.Run("returns out unchanged while it waits to resume the watch", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", incA)
			h.write(t, incA, placement(1, nodeA))
			h.route(h.follower.Start(nil))
			h.drain(t)
			h.deliverDuringOutage(t)
			out := []machine.Effect{foreign}
			assert.Equal(t, h.follower.Stop(out), out, "Stop must not end a watch that has ended")
		})

		t.Run("returns out unchanged on a later call", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, "", incA)
			h.write(t, incA, placement(1, nodeA))
			h.route(h.follower.Start(nil))
			h.drain(t)
			h.follower.Stop(nil)
			out := []machine.Effect{foreign}
			assert.Equal(t, h.follower.Stop(out), out, "a later Stop must return out unchanged")
		})
	})
}

// placement returns the placement of the shard n of the tests on the node on. Its range
// starts at the position n, so the placements of the tests are in position order by n.
func placement(n int, on shard.NodeID) shard.Placement {
	pos := binary.BigEndian.AppendUint32(nil, uint32(n))
	return shard.Placement{
		Shard:    shard.ID{ID128: id.ID128{0: tagShard, 12: pos[0], 13: pos[1], 14: pos[2], 15: pos[3]}},
		Range:    shard.Range{Start: pos},
		Replicas: []shard.Replica{{Node: on, Role: shard.RolePrimary}},
	}
}

// follows runs the steps of the case c over a Follower of node. The steps commit, deliver
// the events of the Follower, fail them in outages, compact the store, and pass an item of
// the watch again. Each step must keep the Follower running, and the machine of the harness
// must end with the placements and the generation of the store.
func follows(c *prop.Case, node shard.NodeID) {
	h := newHarness(c, node, shard.NamespaceIncarnation{})
	h.write(c, incA, placement(1, nodeA))
	h.route(h.follower.Start(nil))
	h.drain(c)
	busy := func(struct{}) bool { return len(h.pending) > 0 || len(h.watches) > 0 }
	passed := func(struct{}) bool { return len(h.watches) > 0 && h.last.Event.Generation != 0 }
	stateful.Steps(c, stateful.Machine[struct{}]{
		Actions: []stateful.Action[struct{}]{
			{Name: "write", Run: func(c *prop.Case, _ int, _ any) { h.write(c, incA, drawn(c)) }},
			{Name: "deliver", Weight: deliveries, Enabled: busy, Run: func(c *prop.Case, _ int, _ any) { h.next(c) }},
			{Name: "fail during an outage", Enabled: busy, Run: func(c *prop.Case, _ int, _ any) {
				c.Classify(outage)
				h.deliverDuringOutage(c)
			}},
			{Name: "compact after two commits", Run: func(c *prop.Case, _ int, _ any) {
				h.write(c, incA, drawn(c))
				h.write(c, incA, drawn(c))
				h.st.Compact()
			}},
			{Name: "pass an item again", Enabled: passed, Run: func(c *prop.Case, _ int, _ any) {
				c.Classify(repeated)
				again := h.last
				again.Token = h.watches[0].Token
				res, out, ok := h.follower.Step(again, nil)
				assert.True(c, ok, "the Follower must take an item of its own watch")
				assert.Equal(c, res, follow.Result{}, "the Follower must not pass a generation again")
				assert.Empty(c, out, "an item that passes nothing must not append an effect")
			}},
		},
		Invariant: func(c *prop.Case, _ struct{}) {
			assert.Total(c, func(res follow.Result) error { return res.Err }, h.results,
				"no fault may stop the Follower")
		},
		Settle: func(c *prop.Case, _ struct{}) {
			h.drain(c)
			if h.compacted {
				c.Classify(compaction)
			}
			assert.Equal(c, h.state, h.expected(c), "the machine must end with the placements of the store",
				assert.EquateEmpty())
			assert.Equal(c, h.generation, h.head(c).Generation, "the machine must end at the generation of the store")
		},
	}, stateful.Swarm(false))
}

// drawn returns a placement that the case c draws. The placement puts one of the shards of
// the properties on one of the nodes of the tests.
func drawn(c *prop.Case) shard.Placement {
	n := c.Draw(prop.Integer(1, shards), "shard")
	on := c.Draw(prop.SampledFrom(nodeA, nodeB), "node")
	return placement(n, on)
}

// placements returns the placements of the shards 1 to n of the tests on the node on, in
// position order.
func placements(n int, on shard.NodeID) []shard.Placement {
	ps := make([]shard.Placement, 0, n)
	for i := range n {
		ps = append(ps, placement(i+1, on))
	}
	return ps
}

// index returns the placements ps in a map by shard.
func index(ps ...shard.Placement) map[shard.ID]shard.Placement {
	m := make(map[shard.ID]shard.Placement, len(ps))
	for _, p := range ps {
		m[p.Shard] = p
	}
	return m
}

// take removes the oldest pending effect and returns it. The effect must be an E.
func take[E machine.Effect](tb assert.TB, h *harness) E {
	tb.Helper()
	assert.NotEmpty(tb, h.pending, "an effect of the Follower must be pending")
	e, ok := h.pending[0].(E)
	assert.True(tb, ok, "the oldest pending effect must have the type that the test expects")
	h.pending = slices.Delete(h.pending, 0, 1)
	return e
}

// route starts and ends the watches of effects, and keeps every other effect to perform
// later.
func (h *harness) route(effects []machine.Effect) {
	for _, e := range effects {
		switch e := e.(type) {
		case machine.Watch:
			h.watches = append(h.watches, e)
		case machine.Unwatch:
			h.watches = slices.DeleteFunc(h.watches, func(w machine.Watch) bool { return w.Token == e.Token })
		default:
			h.pending = append(h.pending, e)
		}
	}
}

// next performs the oldest pending effect, or takes the next item of the oldest watch,
// and passes the event to the Follower. It reports false when it has nothing to pass.
func (h *harness) next(tb assert.TB) bool {
	tb.Helper()
	if len(h.pending) > 0 {
		h.step(tb, h.perform(tb, take[machine.Effect](tb, h)))
		return true
	}
	ev, ok := h.item(tb)
	if ok {
		h.step(tb, ev)
	}
	return ok
}

// deliverDuringOutage passes the next event to the Follower during an outage of the
// store, so the read or the item of the watch fails.
func (h *harness) deliverDuringOutage(tb assert.TB) {
	tb.Helper()
	h.st.StartOutage()
	h.next(tb)
	h.st.EndOutage()
}

// drain passes events to the Follower until it has nothing to pass.
func (h *harness) drain(tb assert.TB) {
	tb.Helper()
	for range maxDeliveries {
		if !h.next(tb) {
			return
		}
	}
	tb.Fatalf("the Follower still reads after %d deliveries", maxDeliveries)
}

// failure passes events to the Follower until it has nothing to pass, and returns the
// error of the last Result.
func (h *harness) failure(tb assert.TB) error {
	tb.Helper()
	h.drain(tb)
	assert.NotEmpty(tb, h.results, "the Follower must report a Result")
	return h.results[len(h.results)-1].Err
}

// step passes ev to the Follower and keeps the effects that it appends. It applies the
// Result to the state of the machine.
func (h *harness) step(tb assert.TB, ev machine.Event) {
	tb.Helper()
	res, out, ok := h.follower.Step(ev, nil)
	assert.True(tb, ok, "the Follower must take the result of its own effect")
	h.route(out)
	if item, isItem := ev.(machine.Watched); isItem && item.Err == nil {
		h.last = item
	}
	if res.Err == nil && res.Page == nil && res.Event.Generation == 0 {
		return
	}
	h.results = append(h.results, res)
	if res.Page != nil {
		h.page(tb, res.Page)
	}
	if res.Event.Generation != 0 {
		h.event(tb, &res.Event)
	}
}

// page adds p to the snapshot whose last page has not arrived. The last page replaces the
// state of the machine with the snapshot.
func (h *harness) page(tb assert.TB, p *follow.Page) {
	tb.Helper()
	if p.First {
		h.partial = map[shard.ID]shard.Placement{}
	}
	assert.NotNil(tb, h.partial, "the first page of a snapshot must arrive first")
	for _, pl := range p.Placements {
		h.partial[pl.Shard] = pl
	}
	if p.Last {
		h.state, h.partial = h.partial, nil
		h.generation = p.State.Head.Generation
	}
}

// event applies the generation ev to the state of the machine.
func (h *harness) event(tb assert.TB, ev *store.Event) {
	tb.Helper()
	assert.True(tb, ev.Generation > h.generation, "the Follower must pass each generation once, in order")
	h.generation = ev.Generation
	for _, p := range ev.Mutation.Put {
		on := slices.ContainsFunc(p.Replicas, func(r shard.Replica) bool { return r.Node == h.node })
		if h.node == "" || on {
			h.state[p.Shard] = p
		} else {
			delete(h.state, p.Shard)
		}
	}
	for _, s := range ev.Removed {
		delete(h.state, s)
	}
}

// perform performs e against the store, and returns the event of its result.
func (h *harness) perform(tb assert.TB, e machine.Effect) machine.Event {
	tb.Helper()
	switch e := e.(type) {
	case machine.Read:
		return h.read(tb, &e)
	case machine.Timer:
		h.waits = append(h.waits, e.Duration)
		return machine.Elapsed{Token: e.Token}
	default:
		tb.Fatalf("the Follower appended a %T, which the harness does not perform", e)
		return nil
	}
}

// read reads the page of e from the store and returns the event of its result. Each page
// that it reads has at most pageLimit placements.
func (h *harness) read(tb assert.TB, e *machine.Read) machine.Event {
	tb.Helper()
	ctx := assert.Context(tb)
	p := e.Page
	p.Limit = pageLimit
	state, cursor, err := h.st.Read(ctx, e.Scope, e.Incarnation, e.Query, p)
	if err != nil {
		h.compacted = h.compacted || errors.Is(err, store.ErrCompacted)
		return machine.PageRead{Err: err, Token: e.Token}
	}
	var ps []shard.Placement
	for pl, err := range cursor.Seq(ctx) {
		assert.NoError(tb, err, "the cursor of the memory store must yield its placements")
		ps = append(ps, pl)
	}
	next := cursor.NextPage()
	assert.NoError(tb, cursor.Close(), "the cursor must close")
	return machine.PageRead{Placements: ps, Next: next, State: state, Token: e.Token}
}

// item takes the next item of the oldest watch, and reports whether the store has one. An
// item with an error ends the watch.
func (h *harness) item(tb assert.TB) (machine.Watched, bool) {
	tb.Helper()
	if len(h.watches) == 0 {
		return machine.Watched{}, false
	}
	w := h.watches[0]
	// A watch of an ended context returns once it has yielded the generations that the
	// store has, so the harness reads one item without waiting for a commit.
	ctx, cancel := context.WithCancel(assert.Context(tb))
	cancel()
	for ev, err := range h.st.Watch(ctx, w.Scope, w.Incarnation, w.From, w.Node) {
		if err != nil {
			h.compacted = h.compacted || errors.Is(err, store.ErrCompacted)
			h.watches = slices.Delete(h.watches, 0, 1)
			return machine.Watched{Err: err, Token: w.Token}, true
		}
		h.watches[0].From = ev.Generation
		return machine.Watched{Event: ev, Token: w.Token}, true
	}
	return machine.Watched{}, false
}

// write commits puts to the scope of the tests at its current generation. Its writer has
// the incarnation inc and the term of the tests.
func (h *harness) write(tb assert.TB, inc shard.NamespaceIncarnation, puts ...shard.Placement) {
	tb.Helper()
	ctx := assert.Context(tb)
	head, err := h.st.Head(ctx, cellScope)
	assert.NoError(tb, err, "the store must return the header of the scope")
	h.seq++
	m := &store.Mutation{ID: store.CommitID{Incarnation: inc, Term: term, Seq: h.seq}, Put: puts}
	_, err = h.st.Writer(cellScope, inc, term).Commit(ctx, head.Generation, m)
	assert.NoError(tb, err, "the controller must commit")
}

// head returns the header of the scope of the tests.
func (h *harness) head(tb assert.TB) store.Head {
	tb.Helper()
	head, err := h.st.Head(assert.Context(tb), cellScope)
	assert.NoError(tb, err, "the store must return the header of the scope")
	return head
}

// expected returns the placements of the scope of the tests that the Follower selects. It
// indexes them by shard.
func (h *harness) expected(tb assert.TB) map[shard.ID]shard.Placement {
	tb.Helper()
	ctx := assert.Context(tb)
	want := map[shard.ID]shard.Placement{}
	p := page.Page{}
	for {
		_, cursor, err := h.st.Read(ctx, cellScope, incA, store.Query{Node: h.node}, p)
		assert.NoError(tb, err, "the store must read the placements")
		for pl, err := range cursor.Seq(ctx) {
			assert.NoError(tb, err, "the cursor of the memory store must yield its placements")
			want[pl.Shard] = pl
		}
		next := cursor.NextPage()
		assert.NoError(tb, cursor.Close(), "the cursor must close")
		if next == "" {
			return want
		}
		p = page.Page{Token: next}
	}
}

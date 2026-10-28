// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes_test

import (
	"encoding/json"
	"log/slog"
	"slices"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/routes"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The specs of the root scope that the router cannot apply.
const (
	// truncatedSpec is a spec that does not decode.
	truncatedSpec = `{"namespace":`
	// unknownSpec is a spec whose preset no registry has.
	unknownSpec = `{"namespace":"orders","version":1,"preset":"unknown"}`
	// eightSlots is the configuration of fixed-slots with eight slots and every default.
	eightSlots = `{"hash":"xxhash64","slots":8,"hashTags":false}`
)

// logMapping is the message of the log of an installed namespace whose positions the router
// cannot compute.
const logMapping = "routes: the router cannot compute the positions of the namespace"

// recreatedKey is the 16 bytes of the ULID of the cell of a namespace that the root creates
// again.
var recreatedKey = id.ID128{0: 0x01, 15: 0xd0}

// The third cell of the tests that divide the namespace into three cells. The cells of
// cellKey and otherKey are the other two.
var (
	// thirdKey is the 16 bytes of the ULID of the third cell.
	thirdKey = id.ID128{0: 0x01, 15: 0xd1}
	// thirdScope is the scope of the third cell.
	thirdScope = shard.Scope{Namespace: namespace, Cell: shard.CellID(ulid.Format(thirdKey.ID()))}
	// slotOne is the position of slot 1, where the cell of otherKey starts.
	slotOne = []byte{0, 0, 0, 1}
)

// TestDiscovery checks the reads of the namespace: the root scope, the resolution of the
// spec, the cell map, the installs and the rediscoveries.
func TestDiscovery(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		mappings := []struct {
			give func(m *shard.Mapping)
			name string
		}{
			{
				name: "returns ErrMapping to Started for a recorded mapping of another partitioner",
				give: func(m *shard.Mapping) { m.Partitioner = singleton.Name },
			},
			{
				name: "returns ErrMapping to Started for a recorded mapping of another hash",
				give: func(m *shard.Mapping) { m.Hash = "" },
			},
			{
				name: "returns ErrMapping to Started for a recorded mapping of another version of its hash",
				give: func(m *shard.Mapping) { m.HashVersion++ },
			},
			{
				name: "returns ErrMapping to Started for a recorded mapping of another configuration",
				give: func(m *shard.Mapping) { m.Config = []byte(eightSlots) },
			},
		}
		for _, tt := range mappings {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, func(s *setup) { s.create = false })
				recorded := h.mapping
				tt.give(&recorded)
				h.commit(t, rootScope, h.creation(cellKey, recorded))
				to := h.start(t)
				assert.ErrorIs(t, h.replied(t, to).Err, routes.ErrMapping, "the router must refuse the namespace")
			})
		}

		specs := []struct {
			name string
			give string
		}{
			{name: "returns ErrMapping to Started for a spec that does not decode", give: truncatedSpec},
			{name: "returns ErrMapping to Started for a spec that does not resolve", give: unknownSpec},
		}
		for _, tt := range specs {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, func(s *setup) { s.root.Spec = []byte(tt.give) })
				to := h.start(t)
				assert.ErrorIs(
					t,
					h.replied(t, to).Err,
					routes.ErrMapping,
					"the router must refuse a spec that does not apply",
				)
			})
		}

		t.Run("returns the error of the decoder to Started for a spec that does not decode", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.root.Spec = []byte(truncatedSpec) })
			to := h.start(t)
			_ = assert.ErrorAs[*json.SyntaxError](
				t,
				h.replied(t, to).Err,
				"the error must wrap the error of the decoder",
			)
		})

		t.Run(
			"returns ErrMapping to Started for a cell whose range the partitioner does not divide",
			func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, func(s *setup) {
					s.root.Put = []shard.Placement{
						{Shard: shard.ID{ID128: cellKey}, Range: shard.Range{Start: between}},
					}
				})
				to := h.start(t)
				assert.ErrorIs(
					t,
					h.replied(t, to).Err,
					routes.ErrMapping,
					"the router must refuse a cell without slots",
				)
			},
		)

		t.Run("waits for the spec of a root scope with a cell", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.create = false })
			h.commit(t, rootScope, &store.Mutation{Put: []shard.Placement{{Shard: shard.ID{ID128: cellKey}}}})
			to := h.start(t)
			_, ok := h.replyTo(to)
			assert.False(t, ok, "the router must wait for the spec of the namespace")
			h.commit(t, rootScope, &store.Mutation{Spec: h.spec, Mapping: &h.mapping})
			h.follow(t)
			h.view(t, cellScope, nodeA)
			assert.Nil(t, h.replied(t, to).Err, "the router must serve once the root has the spec")
		})

		t.Run("waits for a cell of a root scope with the spec", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.root.Put = nil })
			to := h.start(t)
			_, ok := h.replyTo(to)
			assert.False(t, ok, "the router must wait for a cell of the namespace")
			h.commit(t, rootScope, &store.Mutation{Put: []shard.Placement{{Shard: shard.ID{ID128: cellKey}}}})
			h.follow(t)
			h.view(t, cellScope, nodeA)
			assert.Nil(t, h.replied(t, to).Err, "the router must serve once the root has a cell")
		})

		t.Run("reads a namespace that the root creates after the start", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.create = false })
			to := h.start(t)
			h.commit(t, rootScope, h.creation(cellKey, h.mapping))
			h.follow(t)
			h.view(t, cellScope, nodeA)
			assert.Nil(t, h.replied(t, to).Err, "the router must serve the namespace that the root created")
			got, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "the router must route the keys of the namespace")
			assert.Equal(t, got.Incarnation, incA, "the route must be of the incarnation that the root created")
		})

		t.Run("does not read a cell before the last page of the root scope", func(t *testing.T) {
			t.Parallel()
			h := thirds(t)
			h.pause = func(e machine.Effect) bool {
				r := e.(machine.Read)
				return r.Scope == rootScope && r.Page.Token != ""
			}
			h.step(t, machine.Started{Reply: h.call()})
			h.run(t)
			assert.Length(t, h.pending, 1, "the read of the second page of the root scope must wait")
			assert.Empty(t, h.viewers, "the router must not read a cell before the last page of the root scope")
		})

		t.Run("returns ErrNoRoute for a key of a cell that the root adds until its first snapshot", func(t *testing.T) {
			t.Parallel()
			h := halved(t)
			h.pause = func(e machine.Effect) bool { return e.(machine.Read).Scope == cellScope }
			h.commit(t, rootScope, &store.Mutation{Put: []shard.Placement{{
				Shard: shard.ID{ID128: cellKey},
				Range: shard.Range{End: middle},
			}}})
			h.follow(t)
			h.view(t, cellScope, nodeA)
			_, err := h.machine.Route(t.Context(), h.keys[1])
			assert.ErrorIs(t, err, routes.ErrNoRoute, "a cell without its first snapshot must not have a route")
		})

		t.Run("reads every cell of a cell map of several pages", func(t *testing.T) {
			t.Parallel()
			h := thirds(t)
			h.step(t, machine.Started{Reply: h.call()})
			h.run(t)
			h.follow(t)
			scopes := make([]shard.Scope, 0, len(h.viewers))
			for _, w := range h.viewers {
				scopes = append(scopes, w.Scope)
			}
			assert.Permutation(t, scopes, []shard.Scope{cellScope, otherScope, thirdScope},
				"the router must watch the views of every cell of the cell map")
		})

		t.Run("keeps the first range of a cell that the root puts again", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.commit(t, rootScope, &store.Mutation{Put: []shard.Placement{{
				Shard: shard.ID{ID128: cellKey},
				Range: shard.Range{Start: middle},
			}}})
			h.follow(t)
			_, err := h.machine.Route(t.Context(), h.keys[0])
			assert.NoError(t, err, "the cell must keep the range that the router read first")
		})

		t.Run("reads the namespace again when the watch of the root reports another incarnation", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			recreated := h.recreate(t, incB, recreatedKey)
			h.follow(t)
			h.view(t, recreated, nodeA)
			got, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "the router must route the keys of the namespace created again")
			expect.Equal(t, got.Incarnation, incB, "the route must be of the new incarnation")
			expect.Equal(t, got.Cell, recreated.Cell, "the route must be of the new cell")
		})

		t.Run("keeps the installed routes while a rediscovery runs", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.recreate(t, incB, recreatedKey)
			h.follow(t)
			got, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "the installed routes must serve until the new read is complete")
			assert.Equal(t, got.Incarnation, incA, "the route must be of the installed incarnation")
		})

		t.Run("ends the reads of the incarnation that it replaces", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			old := slices.Clone(h.viewers)
			recreated := h.recreate(t, incB, recreatedKey)
			h.follow(t)
			h.view(t, recreated, nodeA)
			assert.Length(t, old, 1, "the router must have watched the views of the old cell")
			expect.Contains(t, h.unwatched, old[0].Token, "the router must end the watch of the views of the old cell")
			for _, w := range h.watches {
				expect.Equal(t, w.Incarnation, incB, "every watch that runs must be of the new incarnation")
			}
		})

		t.Run("reads the namespace again when the watch of a cell reports another incarnation", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.pause = func(machine.Effect) bool { return true }
			h.watchFails(t, cellScope, shard.ErrIncarnation)
			assert.Length(t, h.pending, 1, "the router must read the root scope again")
			expect.Equal(t, h.pending[0].(machine.Read).Scope, rootScope, "the read must be of the root scope")
		})

		t.Run("reads the namespace again when the views of a cell report another incarnation", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.pause = func(machine.Effect) bool { return true }
			h.viewFails(t, cellScope, shard.ErrIncarnation)
			assert.Length(t, h.pending, 1, "the router must read the root scope again")
			expect.Equal(t, h.pending[0].(machine.Read).Scope, rootScope, "the read must be of the root scope")
		})

		t.Run("shares one rediscovery between two triggers", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.pause = func(machine.Effect) bool { return true }
			h.viewFails(t, cellScope, shard.ErrIncarnation)
			h.watchFails(t, cellScope, shard.ErrIncarnation)
			assert.Length(t, h.pending, 1, "the second trigger must share the read of the first")
		})

		t.Run("restarts a rediscovery whose read reports another incarnation", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.recreate(t, incB, recreatedKey)
			h.follow(t)
			incoming := slices.IndexFunc(h.viewers, func(w machine.WatchViews) bool { return w.Incarnation == incB })
			assert.True(t, incoming >= 0, "the rediscovery must watch the views of the new cell")
			token := h.viewers[incoming].Token
			h.pause = func(machine.Effect) bool { return true }
			h.viewFails(t, h.viewers[incoming].Scope, shard.ErrIncarnation)
			assert.Length(t, h.pending, 1, "the router must read the root scope again")
			expect.Equal(t, h.pending[0].(machine.Read).Scope, rootScope, "the read must be of the root scope")
			expect.NotContains(t, h.unwatched, token, "the failed watch must not get an Unwatch")
		})

		t.Run("restarts the first read when a watch of its cell reports another incarnation", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			to := h.call()
			h.step(t, machine.Started{Reply: to})
			h.run(t)
			h.follow(t)
			h.viewFails(t, cellScope, shard.ErrIncarnation)
			h.view(t, cellScope, nodeA)
			assert.Nil(t, h.replied(t, to).Err, "the router must serve after it has read the namespace again")
		})

		t.Run("ends a rediscovery that finds the installed incarnation", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			before := len(h.unwatched)
			h.refresh(t, incB, cellID, h.placed)
			assert.Length(t, h.unwatched, before+1, "the router must end the read of the root of the rediscovery")
			got, err := h.machine.Route(t.Context(), h.keys[1])
			assert.NoError(t, err, "the installed routes must keep serving")
			assert.Equal(t, got.Incarnation, incA, "the route must be of the installed incarnation")
		})

		t.Run("installs a namespace whose spec does not apply after a rediscovery", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.st.Delete(rootScope)
			h.incarnation = incB
			clear(h.seqs)
			recorded := h.mapping
			recorded.Config = []byte(eightSlots)
			h.commit(t, rootScope, h.creation(recreatedKey, recorded))
			h.follow(t)
			_, err := h.machine.Route(t.Context(), h.keys[1])
			expect.ErrorIs(
				t,
				err,
				routes.ErrMapping,
				"the router must refuse the keys of a namespace that it cannot compute",
			)
			expect.True(
				t,
				slices.ContainsFunc(h.logs.All(), func(r slog.Record) bool { return r.Message == logMapping }),
				"the router must log the namespace that it cannot compute",
			)
			expect.Nil(t, h.machine.Err(), "the router must keep running")
		})

		t.Run("does not read a cell of a namespace whose spec does not apply", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.st.Delete(rootScope)
			h.incarnation = incB
			clear(h.seqs)
			recorded := h.mapping
			recorded.Config = []byte(eightSlots)
			h.commit(t, rootScope, &store.Mutation{Spec: h.spec, Mapping: &recorded})
			h.commit(t, rootScope, &store.Mutation{Put: []shard.Placement{{Shard: shard.ID{ID128: recreatedKey}}}})
			h.follow(t)
			h.commit(t, rootScope, &store.Mutation{Put: []shard.Placement{{Shard: shard.ID{ID128: otherKey}}}})
			h.follow(t)
			for _, w := range h.watches {
				expect.Equal(t, w.Scope, rootScope, "the router must not read a cell of the namespace")
			}
		})

		t.Run("stops at an error of the root scope that is not transient", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.watchFails(t, rootScope, errBroken)
			assert.ErrorIs(t, h.machine.Err(), errBroken, "Err must return the error of the root scope")
		})
	})
}

// thirds returns a harness of a router of a namespace whose root lists three cells. The
// cells of cellKey, otherKey and thirdKey start at slot 0, slot 1 and slot 2. The root scope
// of three cells has two pages, and the controllers have not placed a slot.
func thirds(tb testCase) *harness {
	tb.Helper()
	return newHarness(tb, func(s *setup) {
		s.root.Put = []shard.Placement{
			{Shard: shard.ID{ID128: cellKey}, Range: shard.Range{End: slotOne}},
			{Shard: shard.ID{ID128: otherKey}, Range: shard.Range{Start: slotOne, End: middle}},
			{Shard: shard.ID{ID128: thirdKey}, Range: shard.Range{Start: middle}},
		}
		s.place = false
	})
}

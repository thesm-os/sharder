// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control_test

import (
	"slices"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/handoff/breakbeforemake"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/internal/engine"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// otherProtocol is the name of a handoff protocol that the registry does not have.
const otherProtocol shard.StrategyName = "make-before-break"

// offSlot is the start of a range that is not the position of a slot of fixed-slots.
var offSlot = []byte{0x01}

// TestAssignment checks the shards of a scope, their placements and the transitions that a
// controller continues from its scope.
func TestAssignment(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("keeps the ID of each shard that its scope places", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, crowded(t, nodeA))
			h.start(t)
			h.view(t, nodeA, nodeB)
			got := h.shardIDs(t)
			assert.Length(t, got, shards, "the scope must place every shard")
			for i := range shards - 1 {
				expect.Contains(t, got, shard.ID{ID128: id.ID128{0: tagShard, 15: byte(i + 1)}},
					"the controller must keep the ID of a placed shard")
			}
		})

		tests := []struct {
			name string
			give []shard.Node
			want []shard.Replica
		}{
			{
				name: "passes a writer on the live incarnation of its node to a pass",
				give: []shard.Node{nodeA, nodeB},
				want: []shard.Replica{{Node: nodeA.ID, Role: shard.RolePrimary}},
			},
			{
				name: "leaves a writer whose node has a new incarnation out of a pass",
				give: []shard.Node{restarted(nodeA), nodeB},
			},
			{
				name: "passes a writer on a node that has left to a pass",
				give: []shard.Node{nodeB},
				want: []shard.Replica{{Node: nodeA.ID, Role: shard.RolePrimary}},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, crowded(t, nodeA))
				h.start(t)
				h.paused = true
				h.view(t, tt.give...)
				assert.NotEmpty(t, h.plans, "the first view must start a pass")
				current := h.plans[0].Input.Current.Placements
				for i := range shards - 1 {
					sid := shard.ID{ID128: id.ID128{0: tagShard, 15: byte(i + 1)}}
					expect.Equal(
						t,
						current[sid].Replicas,
						tt.want,
						"the pass must have the writers of the live incarnations",
						expect.EquateEmpty(),
					)
				}
			})
		}

		t.Run("passes the other replicas of a placement whose writer it leaves out to a pass", func(t *testing.T) {
			t.Parallel()
			secondary := shard.Replica{Node: nodeB.ID, Role: shard.RoleSecondary}
			h := newHarness(t, crowded(t, nodeA), func(s *setup) {
				for i := range s.prior[0].Put {
					s.prior[0].Put[i].Replicas = append(s.prior[0].Put[i].Replicas, secondary)
				}
			})
			h.start(t)
			h.paused = true
			h.view(t, restarted(nodeA), nodeB)
			assert.NotEmpty(t, h.plans, "the first view must start a pass")
			current := h.plans[0].Input.Current.Placements
			for i := range shards - 1 {
				sid := shard.ID{ID128: id.ID128{0: tagShard, 15: byte(i + 1)}}
				expect.Equal(t, current[sid].Replicas, []shard.Replica{secondary},
					"the pass must have the replicas of a placement other than its lost writer")
			}
		})

		t.Run("gives a new ID to each shard that its scope does not place", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			got := h.shardIDs(t)
			assert.Length(t, got, shards, "the scope must place every shard")
			assert.NoDuplicates(t, func() ([]shard.ID, error) { return got, nil },
				"each shard must have an ID of its own")
		})

		t.Run("fails with ErrSpec for a placement that starts where no span starts", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, stray(t))
			h.start(t)
			assert.ErrorIs(t, h.err, control.ErrSpec,
				"the controller must fail when its shards differ from the spans of its partitioner")
		})

		t.Run("fails with ErrSpec before it continues the transitions of its scope", func(t *testing.T) {
			t.Parallel()
			p := protocolOf(t)
			_, opt := transfer(t, p.Name(), p.Version(), handoff.Drop)
			h := newHarness(t, opt, stray(t))
			h.start(t)
			assert.ErrorIs(t, h.err, control.ErrSpec,
				"the controller must fail at the placements of its scope before it continues a transition")
		})

		t.Run("fails with partition.ErrRange for a range that the partitioner does not divide", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.rng = shard.Range{Start: offSlot} })
			h.start(t)
			assert.ErrorIs(t, h.err, partition.ErrRange,
				"the controller must fail when its partitioner does not divide its range")
		})

		t.Run("continues a transition of its scope", func(t *testing.T) {
			t.Parallel()
			p := protocolOf(t)
			sid, opt := transfer(t, p.Name(), p.Version(), handoff.Commit)
			h := newHarness(t, opt)
			h.start(t)
			h.view(t, nodeA, nodeB)
			expect.Equal(t, h.placements(t)[sid].Replicas, []shard.Replica{{Node: nodeB.ID, Role: shard.RolePrimary}},
				"the controller must finish the move of the writer")
			expect.Empty(t, h.state(t).Transitions, "the controller must end the transition")
		})

		t.Run("keeps the transitions of its scope in flight", func(t *testing.T) {
			t.Parallel()
			p := protocolOf(t)
			_, opt := transfer(t, p.Name(), p.Version(), handoff.Drop)
			h := newHarness(t, opt)
			h.start(t)
			got := h.metrics.value(transitionsInstrument, string(breakbeforemake.Name), shard.PhaseRunning.String())
			assert.Equal(t, got, 1.0, "the controller must count the transition of its scope before the first view")
		})

		t.Run("fails with engine.ErrProtocol for a transition of another protocol", func(t *testing.T) {
			t.Parallel()
			p := protocolOf(t)
			_, opt := transfer(t, otherProtocol, p.Version(), handoff.Commit)
			h := newHarness(t, opt)
			h.start(t)
			assert.ErrorIs(t, h.err, engine.ErrProtocol,
				"the controller must fail at a transition that its protocol does not continue")
		})
	})
}

// protocolOf returns the protocol break-before-make with its default configuration.
func protocolOf(tb assert.TB) handoff.Protocol {
	tb.Helper()
	f := builtin.BreakBeforeMake()
	parsed, err := f.Parse(nil)
	assert.NoError(tb, err, "the default configuration of the protocol must parse")
	p, err := f.New(parsed)
	assert.NoError(tb, err, "the protocol must build")
	return p
}

// transfer returns the shard of the first span and an option whose prior controller
// started the move of its writer from the node A to the node B. The record of the move has
// the protocol name at the version version, and its next step is the first step of the
// script with the action action.
func transfer(tb assert.TB, name shard.StrategyName, version uint32, action handoff.Action) (shard.ID, option) {
	tb.Helper()
	ss := spans(tb)
	script, err := protocolOf(tb).Script(handoff.Transfer{From: nodeA.ID, To: nodeB.ID})
	assert.NoError(tb, err, "the protocol must script a move")
	step := slices.IndexFunc(script.Steps, func(s handoff.Step) bool { return s.Action == action })
	assert.NotEqual(tb, step, -1, "the script must have a step with the action")
	sid := shard.ID{ID128: id.ID128{0: tagShard, 15: 1}}
	m := &store.Mutation{
		Put: []shard.Placement{{
			Shard:       sid,
			Range:       ss[0].Range,
			Replicas:    []shard.Replica{{Node: nodeA.ID, Role: shard.RolePrimary}},
			Epoch:       1,
			Incarnation: nodeA.Incarnation,
			Reserved:    3,
			Sequence:    2,
		}},
		Transitions: []store.Transition{{
			Started:         origin,
			Protocol:        name,
			From:            store.Endpoint{Node: nodeA.ID, Incarnation: nodeA.Incarnation, Role: shard.RolePrimary},
			To:              store.Endpoint{Node: nodeB.ID, Incarnation: nodeB.Incarnation, Role: shard.RolePrimary},
			Operation:       shard.OperationID{Incarnation: incA, Shard: sid, Sequence: 2},
			SpecVersion:     firstVersion,
			OldEpoch:        1,
			NewEpoch:        2,
			AbortEpoch:      3,
			Step:            step,
			ProtocolVersion: version,
			Phase:           shard.PhaseRunning,
		}},
	}
	return sid, func(s *setup) { s.prior = append(s.prior, m) }
}

// stray returns an option whose prior controller placed a shard whose range starts after
// the start of the second span, where no span starts.
func stray(tb assert.TB) option {
	tb.Helper()
	ss := spans(tb)
	p := shard.Placement{
		Shard:       shard.ID{ID128: id.ID128{0: tagShard, 15: 2}},
		Range:       shard.Range{Start: append(slices.Clone(ss[1].Range.Start), offSlot...)},
		Replicas:    []shard.Replica{{Node: nodeA.ID, Role: shard.RolePrimary}},
		Epoch:       1,
		Incarnation: nodeA.Incarnation,
		Reserved:    1,
		Sequence:    1,
	}
	return func(s *setup) { s.prior = append(s.prior, &store.Mutation{Put: []shard.Placement{p}}) }
}

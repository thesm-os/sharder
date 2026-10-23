// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control_test

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/crypto/sha256"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/rand/pcg"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/partition/xxhash64"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// interval is the interval of the periodic passes of the planner of the tests: the
// default of the planner.
const interval = 30 * time.Second

// strictConfig is the configuration of the planner of the tests that set it: at most one
// replica per node, and the goal count.
const strictConfig = `{"constraints":[{"name":"max-per-node","config":{"max":1}}],` +
	`"goals":[{"name":"count","tier":0,"weight":1}],"triggers":{"onMembership":false}}`

// pairedMax is the most replicas of a node under pairedConfig and boundedConfig.
const pairedMax = 2

// The configurations of the planner of the tests that bound the replicas of a node to
// pairedMax, with the goal count.
var (
	// pairedConfig does not start a pass for a change of the membership.
	pairedConfig = `{"constraints":[{"name":"max-per-node","config":{"max":` + strconv.Itoa(pairedMax) + `}}],` +
		`"goals":[{"name":"count","tier":0,"weight":1}],"triggers":{"onMembership":false}}`
	// boundedConfig keeps the triggers of the planner.
	boundedConfig = `{"constraints":[{"name":"max-per-node","config":{"max":` + strconv.Itoa(pairedMax) + `}}],` +
		`"goals":[{"name":"count","tier":0,"weight":1}]}`
)

// tagShard is the first byte of the IDs of the shards that a prior controller placed.
const tagShard byte = 0x5d

// TestPlan checks the passes of a controller and the moves that it starts from their
// plans.
func TestPlan(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("starts no pass before the first view", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.elapse(t, interval)
			assert.Empty(t, h.plans, "the controller must not plan before it knows its nodes")
		})

		t.Run("runs one placement pass at a time", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.paused = true
			h.view(t, nodeA, nodeB)
			h.view(t, nodeA, nodeB, nodeC)
			assert.Length(t, h.plans, 1, "a trigger during a pass must not start a second pass")
		})

		t.Run("starts a queued pass after the pass in flight", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.paused = true
			h.view(t, nodeA, nodeB)
			h.view(t, nodeA, nodeB, nodeC)
			h.pass(t, h.plans[0].Token)
			assert.Length(t, h.plans, 2, "the trigger during the pass must start one more pass after it")
		})

		t.Run("starts a pass at the interval of the schedule", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.elapse(t, interval)
			expect.Length(t, h.plans, 2, "the interval must start a pass")
			expect.Contains(t, durations(h.timers), interval, "the controller must arm the next interval")
		})

		t.Run("restores missing replicas first in the first pass", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.elapse(t, interval)
			expect.True(t, h.plans[0].Input.Emergency, "a pass over unplaced shards must restore them first")
			expect.False(t, h.plans[1].Input.Emergency, "a pass over placed shards must not be an emergency")
		})

		t.Run("restores a faulted replica first", func(t *testing.T) {
			t.Parallel()
			h := served(t, quiet)
			h.step(t, h.report(&nodeA, 1, faulty(h.shardOn(t, nodeA.ID))))
			h.run(t)
			assert.Length(t, h.plans, 2, "the fault must start a pass")
			assert.True(t, h.plans[1].Input.Emergency, "a pass with a faulted replica must restore it first")
		})

		t.Run("finishes a drain while a shard without a writer has a fault on every node", func(t *testing.T) {
			t.Parallel()
			h := served(t, quiet)
			broken := h.shardOn(t, nodeA.ID)
			h.answer = func(r *protocol.Request) error {
				if r.Shard == broken && r.Call == protocol.Add {
					return errRefused
				}
				return nil
			}
			h.step(t, h.report(&nodeA, 1, faulty(broken)))
			h.run(t)
			assert.Empty(t, h.placements(t)[broken].Replicas, "the refused shard must remain without a writer")
			assert.NotEqual(t, h.writers(t)[nodeB.ID], 0, "node B must host writers before its drain")
			h.disrupt(t, disruption(1, nodeB.ID), operator)
			h.elapse(t, interval)
			assert.Equal(t, h.writers(t)[nodeB.ID], 0, "the drain must move every writer off node B")
		})

		t.Run("records the duration of each pass by its mode", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			expect.Equal(t, h.metrics.value(passInstrument, modeEmergency), 1.0,
				"the controller must record the first pass as an emergency")
			expect.Equal(t, h.metrics.value(passInstrument, modePeriodic), 0.0,
				"the controller must not record the first pass as periodic")
			h.elapse(t, interval)
			expect.Equal(t, h.metrics.value(passInstrument, modePeriodic), 1.0,
				"the controller must record the periodic pass")
		})

		t.Run("starts each pass at the repair cursor of the last pass", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.elapse(t, interval)
			assert.Length(t, h.planned, 2, "the harness must have performed both passes")
			assert.Equal(t, h.plans[1].Input.RepairFrom, h.planned[0].RepairNext,
				"the second pass must start at the cursor of the first")
		})

		t.Run("passes the transitions in flight to each pass", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.answer = func(*protocol.Request) error { return protocol.ErrUnreachable }
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			h.elapse(t, interval)
			got := h.plans[len(h.plans)-1].Input.Transitions
			assert.Length(t, got, 2, "the pass must have both moves off the draining node")
			for _, f := range got {
				expect.Equal(t, f.Role, shard.RolePrimary, "each transition must place the writer role")
			}
		})

		t.Run("passes the draining nodes to each pass", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.disrupt(t, disruption(1, nodeB.ID), operator)
			assert.Equal(t, h.plans[len(h.plans)-1].Input.Draining, map[shard.NodeID]bool{nodeB.ID: true},
				"the pass must have the draining node alone")
		})

		t.Run("restores a writer on a node that has left first", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.view(t, nodeB)
			assert.True(t, h.plans[len(h.plans)-1].Input.Emergency,
				"a pass over a writer that has left must be an emergency")
		})

		t.Run("does not count a shard in transition as an emergency", func(t *testing.T) {
			t.Parallel()
			h := served(t, delayed)
			h.view(t, nodeB)
			h.elapse(t, interval)
			assert.False(t, h.plans[len(h.plans)-1].Input.Emergency,
				"a pass over writers that move off a node that has left must not be an emergency")
		})

		t.Run("waits for the lock delay of its spec before a drop on a node that has left completes",
			func(t *testing.T) {
				t.Parallel()
				h := served(t, delayed)
				h.view(t, nodeB)
				assert.Contains(t, durations(h.timers), lockDelay, "the controller must wait for the lock delay")
			})

		t.Run("gives a move of a replica without a fault its source as the fallback", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			for _, n := range []shard.Node{nodeA, nodeB} {
				var replicas []shard.Observed
				for sid, p := range h.placements(t) {
					if p.Replicas[0].Node == n.ID {
						replicas = append(replicas, healthy(sid))
					}
				}
				h.step(t, h.report(&n, 1, replicas...))
			}
			before := len(h.commits)
			h.view(t, nodeA, nodeB, nodeC)
			got := records(h.commits[before:])
			assert.NotEmpty(t, got, "the join must start a rebalance")
			expect.Equal(t, got[0].Fallback, got[0].From, "the fallback must be the source")
		})

		t.Run("seeds each pass from its snapshot", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.paused = true
			h.view(t, nodeA, nodeB)
			snap := h.plans[0].Input.Snapshot
			fields := []uint64{
				uint64(snap.Term), uint64(snap.SpecVersion), uint64(snap.Generation), uint64(snap.Membership),
				snap.Reports,
			}
			b := []byte(seedDomain)
			for _, v := range fields {
				b = binary.BigEndian.AppendUint64(b, v)
			}
			d := sha256.Hasher{}.Hash(b)
			want := pcg.New(rand.Seed(binary.BigEndian.Uint64(d.Bytes()))).Uint64()
			assert.Equal(t, h.plans[0].Input.Rand.Uint64(), want,
				"the source of the pass must have the values of the seed of its snapshot")
		})

		t.Run("applies the budget of a newer spec to the moves", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.accept(t, 2, func(s *config.Spec) { s.Budget.MaxUnavailable = new(int) })
			h.view(t, nodeA, nodeB, nodeC)
			assert.Equal(t, h.writers(t)[nodeC.ID], 0, "the budget of the newer spec must defer the rebalance")
		})

		t.Run("arms one Timer of the interval at a time", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.accept(t, 2, nil)
			ivs := slices.DeleteFunc(durations(h.timers), func(d time.Duration) bool { return d != interval })
			assert.Length(t, ivs, 1, "the controller must arm one Timer of the interval")
		})

		t.Run("reads a snapshot of the committed state in each pass", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA, nodeB)
			gen := h.state(t).Head.Generation
			h.reportAll(t)
			h.elapse(t, interval)
			got := h.plans[1].Input.Snapshot
			expect.Equal(t, got.Term, cellTerm, "the snapshot must contain the term of the cell")
			expect.Equal(t, got.SpecVersion, firstVersion, "the snapshot must contain the version of the spec")
			expect.Equal(t, got.Generation, gen, "the snapshot must contain the generation of the assignment")
			expect.Equal(t, got.Membership, h.revision, "the snapshot must contain the revision of the view")
			expect.Equal(t, got.Reports, uint64(2), "the snapshot must count the accepted reports")
		})

		t.Run("gives passes over the same snapshot the same source of randomness", func(t *testing.T) {
			t.Parallel()
			first, second := newHarness(t), newHarness(t)
			for _, h := range []*harness{first, second} {
				h.start(t)
				h.paused = true
				h.view(t, nodeA, nodeB)
			}
			assert.Equal(t, first.plans[0].Input.Rand.Uint64(), second.plans[0].Input.Rand.Uint64(),
				"two passes over the same snapshot must have the same first value")
		})

		t.Run("gives a pass over another snapshot another source of randomness", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.paused = true
			h.elapse(t, interval)
			again := newHarness(t)
			again.start(t)
			again.paused = true
			again.view(t, nodeA, nodeB)
			assert.NotEqual(t, h.plans[1].Input.Rand.Uint64(), again.plans[0].Input.Rand.Uint64(),
				"passes over two snapshots must have other first values")
		})

		t.Run("logs a pass that fails", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t, nodeA, nameless)
			got := h.logged(logPass)
			assert.Length(t, got, 1, "the controller must log the pass that failed")
			expect.Equal(t, got[0][attrNamespaceKey], string(namespace), "the log must name the namespace")
			expect.Equal(t, got[0][attrCellKey], string(cellID), "the log must name the cell")
		})

		t.Run("logs each replica that breaks a constraint", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, strict, crowded(t, nodeA))
			h.start(t)
			h.view(t, nodeA, nodeB)
			got := h.logged(logViolation)
			assert.NotEmpty(t, got, "the controller must log the replicas that break a constraint")
			expect.Equal(t, got[0][attrConstraintKey], string(constraint.MaxPerNodeName),
				"the log must name the constraint")
			expect.Equal(t, got[0][attrNodeKey], string(nodeA.ID), "the log must name the node")
		})

		t.Run("starts no move for a target without a writer", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.view(t)
			expect.Empty(t, h.commits[1:], "a pass without nodes must not start a move")
			expect.Empty(t, h.logged(logMove), "a pass without nodes must not give the engine a move")
		})

		t.Run("does not start a move to a node that started to drain after the input of the pass", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			h.paused = true
			h.elapse(t, interval)
			pass := h.plans[len(h.plans)-1].Token
			before := len(h.commits)
			h.disrupt(t, disruption(1, nodeC.ID), operator)
			h.paused = false
			h.pass(t, pass)
			assert.NotContains(t, destinations(h.commits[before:]), nodeC.ID,
				"the controller must not move a shard to a node that drains")
		})

		t.Run("does not start a move to a node whose drain is in flight", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			h.paused = true
			h.elapse(t, interval)
			pass := h.plans[len(h.plans)-1].Token
			before := len(h.commits)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Commit); return ok }
			d := disruption(1, nodeC.ID)
			h.step(t, h.disruptCall(&d, operator))
			h.paused = false
			h.pass(t, pass)
			h.pause = nil
			h.run(t)
			assert.NotContains(t, destinations(h.commits[before:]), nodeC.ID,
				"the controller must not move a shard to a node whose drain is in flight")
		})

		t.Run("starts a move of an unplaced shard without a source", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			got := records(h.commits)
			assert.NotEmpty(t, got, "the first pass must start moves")
			expect.Equal(t, got[0].From, store.Endpoint{}, "the move of an unplaced shard must not have a source")
			expect.Equal(t, got[0].Fallback, store.Endpoint{}, "the move of an unplaced shard must not have a fallback")
		})

		t.Run("gives a move of an available writer its source as the fallback", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			before := len(h.commits)
			h.view(t, nodeA, nodeB, nodeC)
			got := records(h.commits[before:])
			assert.NotEmpty(t, got, "the join must start a rebalance")
			expect.NotEqual(t, got[0].From, store.Endpoint{}, "the rebalance must move a placed writer")
			expect.Equal(t, got[0].Fallback, got[0].From, "the fallback must be the source")
			expect.Equal(t, h.metrics.value(movesInstrument, reasonRebalance), 1.0,
				"the controller must count the rebalance")
		})

		t.Run("counts the move of an unplaced shard as a failure", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			assert.Equal(t, h.metrics.value(movesInstrument, reasonFailure), float64(shards),
				"the controller must count each placement of a missing replica as a failure")
		})

		t.Run("gives no fallback to a move off a node that has left", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			before, failures := len(h.commits), h.metrics.value(movesInstrument, reasonFailure)
			h.view(t, nodeB)
			got := records(h.commits[before:])
			assert.NotEmpty(t, got, "the end of the node must start repairs")
			expect.Equal(
				t,
				got[0].From,
				store.Endpoint{Node: nodeA.ID, Incarnation: nodeA.Incarnation, Role: shard.RolePrimary},
				"the source that has left must have the incarnation of its placement",
			)
			expect.Equal(t, got[0].Fallback, store.Endpoint{}, "a source that has left must not be the fallback")
			expect.Equal(t, h.metrics.value(movesInstrument, reasonFailure)-failures, float64(len(got)),
				"the controller must count each repair as a failure")
		})

		t.Run("gives no fallback to a move of a writer whose node has a new incarnation", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			before, failures := len(h.commits), h.metrics.value(movesInstrument, reasonFailure)
			h.view(t, restarted(nodeA), nodeB)
			got := records(h.commits[before:])
			assert.NotEmpty(t, got, "the new incarnation must start repairs")
			for _, r := range got {
				expect.Equal(
					t,
					r.From,
					store.Endpoint{Node: nodeA.ID, Incarnation: nodeA.Incarnation, Role: shard.RolePrimary},
					"the source must be the writer at the incarnation of its placement",
				)
				expect.Equal(
					t,
					r.Fallback,
					store.Endpoint{},
					"a source of an ended incarnation must not be the fallback",
				)
			}
			expect.Equal(t, h.metrics.value(movesInstrument, reasonFailure)-failures, float64(len(got)),
				"the controller must count each repair as a failure")
		})

		t.Run("places each writer again whose node has a new incarnation", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			later := restarted(nodeA)
			h.view(t, later, nodeB)
			h.live(t, later, nodeB)
		})

		t.Run("places each writer again that a prior controller placed on an ended incarnation", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, crowded(t, nodeA))
			h.start(t)
			later := restarted(nodeA)
			h.view(t, later, nodeB)
			h.live(t, later, nodeB)
		})

		t.Run("gives no fallback to a move of a faulted replica", func(t *testing.T) {
			t.Parallel()
			h := served(t, quiet)
			before, failures := len(h.commits), h.metrics.value(movesInstrument, reasonFailure)
			sid := h.shardOn(t, nodeA.ID)
			fault := faulty(sid)
			fault.Epoch = h.placements(t)[sid].Epoch
			h.step(t, h.report(&nodeA, 1, fault))
			h.run(t)
			got := records(h.commits[before:])
			assert.Length(t, got, 1, "the fault must start one repair")
			expect.Equal(t, got[0].Fallback, store.Endpoint{}, "a faulted source must not be the fallback")
			expect.Equal(t, h.metrics.value(movesInstrument, reasonFailure)-failures, 1.0,
				"the controller must count the repair as a failure")
		})

		t.Run("gives no fallback to a move off a draining node", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			before := len(h.commits)
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			got := records(h.commits[before:])
			assert.NotEmpty(t, got, "the drain must start moves")
			expect.Equal(t, got[0].Fallback, store.Endpoint{}, "a draining source must not be the fallback")
			expect.Equal(t, h.metrics.value(movesInstrument, reasonDrain), 2.0,
				"the controller must count each move off the draining node as a drain")
		})

		t.Run("defers a move of an available writer at maxUnavailable zero", func(t *testing.T) {
			t.Parallel()
			h := served(t, unavailable)
			before := len(h.commits)
			h.view(t, nodeA, nodeB, nodeC)
			expect.Empty(t, records(h.commits[before:]), "the budget must defer the rebalance")
			expect.Equal(t, h.metrics.value(deferredInstrument), 1.0, "the controller must count the deferred move")
		})

		t.Run("admits the repair of a writer with a permanent fault at maxUnavailable zero", func(t *testing.T) {
			t.Parallel()
			h := served(t, unavailable, quiet)
			sid := h.shardOn(t, nodeA.ID)
			fault := faulty(sid)
			fault.Epoch = h.placements(t)[sid].Epoch
			h.step(t, h.report(&nodeA, 1, fault))
			h.run(t)
			assert.Equal(t, h.placements(t)[sid].Replicas, []shard.Replica{{Node: nodeB.ID, Role: shard.RolePrimary}},
				"the budget must admit the repair of a writer that is unavailable already")
		})

		t.Run("defers the repair of a writer whose fault is of another tenure at maxUnavailable zero",
			func(t *testing.T) {
				t.Parallel()
				h := served(t, unavailable, quiet)
				sid := h.shardOn(t, nodeA.ID)
				fault := faulty(sid)
				fault.Epoch = h.placements(t)[sid].Epoch - 1
				h.step(t, h.report(&nodeA, 1, fault))
				h.run(t)
				expect.Equal(
					t,
					h.placements(t)[sid].Replicas,
					[]shard.Replica{{Node: nodeA.ID, Role: shard.RolePrimary}},
					"the budget must defer the move of a writer whose tenure has no fault",
				)
				expect.Equal(t, h.metrics.value(deferredInstrument), 1.0, "the controller must count the deferred move")
			})

		t.Run("defers a move of a pass that a constraint refuses at its admission", func(t *testing.T) {
			t.Parallel()
			h := served(t, paired)
			h.view(t, nodeA, nodeB, nodeC)
			h.paused = true
			moved := h.shardOn(t, nodeB.ID)
			p := proposalOf(t, 1, proposal.Move, h.placements(t)[moved].Epoch,
				proposal.MoveArgs{Node: nodeC.ID, Shard: moved})
			h.step(t, h.submitCall(&p, operator))
			proposed := h.plans[len(h.plans)-1].Token
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			drain := h.plans[len(h.plans)-1].Token
			h.pass(t, proposed)
			before := h.metrics.value(deferredInstrument)
			h.pass(t, drain)
			expect.Equal(t, h.writers(t)[nodeC.ID], 2, "node C must host two writers at most")
			expect.Equal(t, h.writers(t)[nodeA.ID], 1, "the refused move must leave its shard on the draining node")
			expect.Equal(t, h.metrics.value(deferredInstrument)-before, 1.0,
				"the controller must count the move that the constraint refuses")
		})

		t.Run("defers a move of a pass to a node that refused an add of its shard during the pass", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			h.paused = true
			h.elapse(t, interval)
			periodic := h.plans[len(h.plans)-1].Token
			h.answer = func(r *protocol.Request) error {
				if r.Call == protocol.Add && r.Node == nodeC.ID {
					return errRefused
				}
				return nil
			}
			for i, sid := range h.shardIDs(t) {
				p := proposalOf(t, byte(i+1), proposal.Move, h.placements(t)[sid].Epoch,
					proposal.MoveArgs{Node: nodeC.ID, Shard: sid})
				h.step(t, h.submitCall(&p, operator))
				h.pass(t, h.plans[len(h.plans)-1].Token)
			}
			before := h.metrics.value(deferredInstrument)
			h.pass(t, periodic)
			expect.Equal(t, h.writers(t)[nodeC.ID], 0, "the pass must not move a shard to a node that refused it")
			expect.Equal(t, h.metrics.value(deferredInstrument)-before, 1.0,
				"the controller must count the move that the admission refuses")
			got := h.logged(logRefusal)
			assert.NotEmpty(t, got, "the controller must log the move that the admission refuses")
			expect.Contains(t, got[len(got)-1][attrErrorKey], "has a fault of shard", "the log must state the fault")
		})

		t.Run("defers a move of a pass to a node that has left the view during the pass", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.paused = true
			h.view(t, nodeA, nodeB, nodeC)
			joined := h.plans[len(h.plans)-1].Token
			h.view(t, nodeA, nodeB)
			before, deferred := len(h.commits), h.metrics.value(deferredInstrument)
			h.pass(t, joined)
			expect.Empty(t, destinations(h.commits[before:]), "the pass must not move a shard to a node that has left")
			expect.Equal(t, h.metrics.value(deferredInstrument)-deferred, 1.0,
				"the controller must count the move that the admission refuses")
		})

		t.Run("defers a move of a writer whose tenure reports no fault at maxUnavailable zero", func(t *testing.T) {
			t.Parallel()
			h := served(t, unavailable)
			sid := h.shardOn(t, nodeA.ID)
			replica := healthy(sid)
			replica.Epoch = h.placements(t)[sid].Epoch
			h.step(t, h.report(&nodeA, 1, replica))
			before := len(h.commits)
			h.view(t, nodeA, nodeB, nodeC)
			expect.Empty(t, records(h.commits[before:]), "the budget must defer the rebalance of a healthy writer")
			expect.Equal(t, h.metrics.value(deferredInstrument), 1.0, "the controller must count the deferred move")
		})

		t.Run("builds no admission for a pass without targets", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.paused = true
			h.elapse(t, interval)
			periodic := h.plans[len(h.plans)-1].Token
			h.view(t, nodeA, nodeB, nameless)
			h.pass(t, periodic)
			assert.Empty(
				t,
				h.planned[len(h.planned)-1].Targets,
				"the periodic pass of a balanced cell must have no target",
			)
			expect.Empty(t, h.logged(logAdmission), "a pass without targets must not build the state of an admission")
		})

		t.Run("starts no move of a pass whose admission does not build", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			h.paused = true
			h.view(t, nodeA, nodeB, nodeC)
			joined := h.plans[len(h.plans)-1].Token
			h.view(t, nodeA, nodeB, nodeC, nameless)
			before := len(h.commits)
			h.pass(t, joined)
			expect.Empty(t, records(h.commits[before:]), "a pass whose admission does not build must start no move")
			expect.NotEmpty(t, h.logged(logAdmission), "the controller must log the admission that does not build")
		})

		t.Run("admits a move off a node that has left at maxUnavailable zero", func(t *testing.T) {
			t.Parallel()
			h := served(t, unavailable)
			h.view(t, nodeB)
			assert.Equal(t, h.writers(t)[nodeB.ID], shards, "the budget must admit the repairs")
		})

		t.Run("admits a move of a writer whose node has a new incarnation at maxUnavailable zero", func(t *testing.T) {
			t.Parallel()
			h := served(t, unavailable)
			later := restarted(nodeA)
			h.view(t, later, nodeB)
			h.live(t, later, nodeB)
		})

		t.Run("admits a move off a node that an admitted reservation covers at maxUnavailable zero",
			func(t *testing.T) {
				t.Parallel()
				h := served(t, unavailable)
				h.reportAll(t)
				h.paused = true
				forced := disruption(1, nodeA.ID)
				forced.Negotiable = false
				h.disrupt(t, forced, operator)
				h.elapse(t, startAfter)
				h.paused = false
				h.pass(t, h.plans[len(h.plans)-1].Token)
				assert.Equal(t, h.writers(t)[nodeA.ID], 0, "the budget must admit the moves off the covered node")
			})

		t.Run("starts a pass after the end of a transition while the bounds refused a move", func(t *testing.T) {
			t.Parallel()
			h := served(t, func(s *setup) { s.spec.Budget.MaxTransitions = new(1) })
			expect.Equal(t, h.writers(t)[nodeA.ID]+h.writers(t)[nodeB.ID], shards,
				"the passes after each transition must place every shard")
			expect.Length(t, h.plans, shards, "each end of a transition must start a pass")
			// The pass of k unplaced shards starts one move, and the bounds refuse k-1.
			expect.Equal(t, h.metrics.value(deferredInstrument), float64(shards*(shards-1)/2),
				"the controller must count each move that the bounds refuse")
		})

		t.Run("starts no pass at the end of a transition without refused moves", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			assert.Length(t, h.plans, 1, "the end of a transition must not start a pass")
		})

		t.Run("starts a pass for a commit that leaves a shard without a writer", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			refused := false
			h.answer = func(*protocol.Request) error {
				if refused {
					return nil
				}
				refused = true
				return errRefused
			}
			h.view(t, nodeA, nodeB)
			expect.Length(t, h.plans, 2, "the move that ends without a writer must start a pass")
			expect.Equal(t, h.writers(t)[nodeA.ID]+h.writers(t)[nodeB.ID], shards,
				"the second pass must place the shard again")
		})

		t.Run("starts a pass for a commit that ends a transition of a writer whose node has a new incarnation",
			func(t *testing.T) {
				t.Parallel()
				h := served(t)
				sid := h.shardOn(t, nodeA.ID)
				h.answer = func(r *protocol.Request) error {
					if r.Call == protocol.Add && r.Node == nodeB.ID {
						return errRefused
					}
					return nil
				}
				h.pause = func(e machine.Effect) bool {
					cb, ok := e.(machine.Callback)
					return ok && cb.Request.Call == protocol.Drop && cb.Request.Node == nodeB.ID
				}
				p := proposalOf(t, 1, proposal.Move, h.placements(t)[sid].Epoch,
					proposal.MoveArgs{Node: nodeB.ID, Shard: sid})
				h.submit(t, &p, operator)
				later := restarted(nodeA)
				h.view(t, later, nodeB)
				h.pause = nil
				h.run(t)
				h.live(t, later, nodeB)
			})

		t.Run("starts a pass for a commit that ends a transition of a writer on a draining node", func(t *testing.T) {
			t.Parallel()
			h := spare(t)
			h.pause = func(e machine.Effect) bool {
				cb, ok := e.(machine.Callback)
				return ok && cb.Request.Call == protocol.Add && cb.Request.Node == nodeC.ID
			}
			before := len(h.commits)
			h.elapse(t, interval)
			assert.Contains(t, destinations(h.commits[before:]), nodeC.ID,
				"the periodic pass must start a move to node C")
			h.disrupt(t, disruption(1, nodeC.ID), operator)
			h.pause = nil
			h.run(t)
			assert.Equal(t, h.writers(t)[nodeC.ID], 0,
				"the end of the move to the draining node must start the pass that moves its writer off")
		})

		t.Run("logs a move that the protocol cannot script", func(t *testing.T) {
			t.Parallel()
			h := served(t, selects(config.Handoff, "", func(r *config.Registry) error {
				return config.Register[handoff.Protocol](
					r,
					config.Handoff,
					refusing{Factory: builtin.BreakBeforeMake()},
				)
			}))
			sid := h.shardOn(t, nodeA.ID)
			h.disrupt(t, disruption(1, nodeA.ID), operator)
			got := h.logged(logMove)
			assert.NotEmpty(t, got, "the controller must log a move that the protocol cannot script")
			expect.Equal(t, got[0][attrShardKey], sid.String(), "the log must name the shard")
		})

		t.Run("starts a pass after the end of a transition while the engine refused a move of a shard in flight",
			func(t *testing.T) {
				t.Parallel()
				h := newHarness(t)
				h.start(t)
				h.paused = true
				h.view(t, nodeA)
				first := h.plans[len(h.plans)-1].Token
				h.view(t, nodeA, nodeB)
				h.paused = false
				h.pass(t, first)
				expect.True(t, len(h.plans) > 2, "the end of the transitions of the first pass must start a third pass")
				expect.NotEqual(t, h.writers(t)[nodeB.ID], 0, "a later pass must move shards to the node that joined")
			})

		t.Run("fails at an error of a commit of a pass while a trigger waits", func(t *testing.T) {
			t.Parallel()
			full := false
			h := served(t, func(s *setup) {
				s.fits = func(*store.Mutation) error {
					if full {
						return store.ErrTooLarge
					}
					return nil
				}
			})
			h.paused = true
			h.view(t, nodeA, nodeB, nodeC)
			joined := h.plans[len(h.plans)-1].Token
			h.view(t, nodeA, nodeB, nodeC, nodeD)
			full = true
			h.pass(t, joined)
			assert.ErrorIs(t, h.err, store.ErrTooLarge,
				"the controller must fail at the commit of the pass before it starts the pass that waits")
		})

		t.Run("passes the load metrics of the spec to each pass", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) { s.spec.Load.Metrics = []config.Metric{{Name: cpuMetric}} })
			h.start(t)
			h.view(t, nodeA, nodeB)
			assert.NotEmpty(t, h.plans, "the first view must start a pass")
			assert.Equal(t, h.plans[0].Input.Metrics, []placement.Metric{{Name: cpuMetric}},
				"the pass must have the load metrics of the spec")
		})

		t.Run("passes the load of the config for each shard without a reported load", func(t *testing.T) {
			t.Parallel()
			load := shard.Load{cpuMetric: 1}
			h := newHarness(t, func(s *setup) { s.cfg.Load = load })
			h.start(t)
			h.view(t, nodeA, nodeB)
			assert.NotEmpty(t, h.plans, "the first view must start a pass")
			got := h.plans[0].Input.ShardLoad
			assert.Length(t, got, shards, "the pass must have a load of each shard")
			for _, sid := range h.shardIDs(t) {
				expect.Equal(t, got[sid], load, "each shard must have the load of the config")
			}
		})

		t.Run("passes the reported load over the load of the config", func(t *testing.T) {
			t.Parallel()
			h := served(t, func(s *setup) { s.cfg.Load = shard.Load{cpuMetric: 1} })
			sid := h.shardOn(t, nodeA.ID)
			reported := shard.Load{cpuMetric: 2}
			replica := shard.Observed{Shard: sid, Role: shard.RolePrimary, Status: shard.StatusServing, Load: reported}
			h.step(t, h.report(&nodeA, 1, replica))
			h.elapse(t, interval)
			assert.Equal(t, h.plans[len(h.plans)-1].Input.ShardLoad[sid], reported,
				"the pass must have the load that the writer of the shard reports")
		})

		t.Run("ignores the result of a pass that it did not send", func(t *testing.T) {
			t.Parallel()
			h := served(t)
			out := []machine.Effect{marker}
			_, got, ok := h.controller.Step(machine.Planned{Token: 99}, out)
			expect.False(t, ok, "the controller must not take the result of a pass that it did not send")
			expect.Equal(t, got, out, "the controller must ignore the result of a pass that it did not send")
		})
	})
}

// The values of the attributes of the instruments that the tests read.
const (
	modeEmergency   = "emergency"
	modePeriodic    = "periodic"
	reasonFailure   = "failure"
	reasonDrain     = "drain"
	reasonRebalance = "rebalance"
	reasonPlanned   = "planned"
)

// The keys of the attributes of the logs that the tests read.
const (
	attrNamespaceKey  = "namespace"
	attrCellKey       = "cell"
	attrNodeKey       = "node"
	attrShardKey      = "shard"
	attrErrorKey      = "error"
	attrConstraintKey = "constraint"
)

// errRefused is the error of an agent that refuses a callback. A retry does not fix it.
var errRefused = errs.WithClass(errors.New("test: the agent refuses the call"), errs.Invalid)

// seedDomain is the domain of the hash that derives the seed of a pass from its snapshot.
const seedDomain = "sharder.pass.seed"

// lockDelay is the lock delay of the spec of the tests that set it.
const lockDelay = 2 * time.Second

// strict sets the planner of the spec to at most one replica per node.
func strict(s *setup) {
	s.spec.Placement = config.Selection{Name: planner.Name, Config: json.RawMessage(strictConfig)}
}

// paired sets the planner of the spec to pairedConfig.
func paired(s *setup) {
	s.spec.Placement = config.Selection{Name: planner.Name, Config: json.RawMessage(pairedConfig)}
}

// bounded sets the planner of the spec to boundedConfig.
func bounded(s *setup) {
	s.spec.Placement = config.Selection{Name: planner.Name, Config: json.RawMessage(boundedConfig)}
}

// unavailable sets maxUnavailable of the spec to zero.
func unavailable(s *setup) {
	s.spec.Budget.MaxUnavailable = new(int)
}

// delayed sets the lock delay of the fenced exclusive ownership of the spec to lockDelay.
func delayed(s *setup) {
	raw := fmt.Sprintf(`{"fenced":true,"lease":{"lockDelay":%q}}`, lockDelay.String())
	s.spec.Ownership = config.Selection{Name: builtin.ExclusiveName, Config: json.RawMessage(raw)}
}

// crowded returns an option whose prior controller placed every shard of the cell but the
// last on the incarnation of the node n. The first pass is then an emergency, which
// restores the last shard and keeps the other replicas where they are.
func crowded(tb assert.TB, n shard.Node) option {
	ss := spans(tb)
	return func(s *setup) {
		m := &store.Mutation{}
		for i, sp := range ss[:len(ss)-1] {
			m.Put = append(m.Put, shard.Placement{
				Shard:       shard.ID{ID128: id.ID128{0: tagShard, 15: byte(i + 1)}},
				Range:       sp.Range,
				Replicas:    []shard.Replica{{Node: n.ID, Role: shard.RolePrimary}},
				Epoch:       1,
				Incarnation: n.Incarnation,
				Reserved:    1,
				Sequence:    1,
			})
		}
		s.prior = append(s.prior, m)
	}
}

// live checks that every shard of the scope of the cell has a writer, on a node of nodes,
// and that its placement records the incarnation of that node in nodes.
func (h *harness) live(tb assert.TB, nodes ...shard.Node) {
	tb.Helper()
	incarnations := make(map[shard.NodeID]epoch.Epoch, len(nodes))
	for _, n := range nodes {
		incarnations[n.ID] = n.Incarnation
	}
	for _, p := range h.placements(tb) {
		assert.Length(tb, p.Replicas, 1, "every shard must have a writer")
		inc, ok := incarnations[p.Replicas[0].Node]
		assert.True(tb, ok, "the writer of a shard must be a node of the view")
		expect.Equal(tb, p.Incarnation, inc, "the placement must record the live incarnation of the node of its writer")
	}
}

// logged returns the attributes of each record that the controller has logged with the
// message msg, in order. Each map has the text of the value of each attribute by its key.
func (h *harness) logged(msg string) []map[string]string {
	var got []map[string]string
	for _, r := range h.logs.All() {
		if r.Message != msg {
			continue
		}
		attrs := make(map[string]string, r.NumAttrs())
		r.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value.String()
			return true
		})
		got = append(got, attrs)
	}
	return got
}

// spans returns the spans of the cell: the four slots of fixed-slots.
func spans(tb assert.TB) []partition.Span {
	tb.Helper()
	p, err := fixedslots.New(fixedslots.Config{Hash: xxhash64.Name, Slots: shards}, xxhash64.Hash{})
	assert.NoError(tb, err, "the partitioner of the tests must build")
	ss, err := p.Initial(shard.Range{})
	assert.NoError(tb, err, "the partitioner must divide the whole range")
	return ss
}

// records returns the records of the transitions that the mutations ms start, each with
// its first record, in the order of the mutations.
func records(ms []*store.Mutation) []store.Transition {
	var got []store.Transition
	seen := make(map[shard.ID]bool)
	for _, m := range ms {
		for _, t := range m.Transitions {
			if !seen[t.Operation.Shard] {
				seen[t.Operation.Shard] = true
				got = append(got, t)
			}
		}
		for _, s := range m.Done {
			delete(seen, s)
		}
	}
	return got
}

// destinations returns the node of the destination of each transition that the mutations
// ms start, in the order of the mutations.
func destinations(ms []*store.Mutation) []shard.NodeID {
	started := records(ms)
	nodes := make([]shard.NodeID, 0, len(started))
	for _, t := range started {
		nodes = append(nodes, t.To.Node)
	}
	return nodes
}

// durations returns the Duration of each Timer of timers.
func durations(timers []machine.Timer) []time.Duration {
	ds := make([]time.Duration, 0, len(timers))
	for _, tm := range timers {
		ds = append(ds, tm.Duration)
	}
	return ds
}

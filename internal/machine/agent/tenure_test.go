// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent_test

import (
	"encoding/json"
	"runtime"
	"slices"
	"testing"
	"time"
	"weak"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The default timeouts of break-before-make, which the spec of the tests keeps.
const (
	addTimeout  = 30 * time.Second
	dropTimeout = 10 * time.Second
)

// logCleanup is the log message of a cleanup that fails.
const logCleanup = "agent: the cleanup of a tenure failed"

// releasedRange is the length of the range of each request whose release a test checks. The
// runtime allocates a range of more than 16 bytes as an object of its own.
const releasedRange = 64

// TestTenure checks the tenures that an agent starts, drops and cleans up.
func TestTenure(t *testing.T) {
	t.Parallel()

	t.Run("Step", func(t *testing.T) {
		t.Parallel()

		t.Run("acquires the fence of an add at the epoch of the add", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, secondEpoch)
			assert.Length(t, h.acquires, 1, "the add must acquire the fence once")
			want := fence.Token{
				Namespace: namespace, Cell: cellID, Incarnation: incA, Shard: shardA, Epoch: secondEpoch,
			}
			expect.Equal(t, h.acquires[0].Fence, want, "the fencing token must name the tenure of the add")
			expect.Equal(t, h.acquires[0].Timeout, addTimeout, "the acquisition must have the timeout of an add")
		})

		t.Run("calls the handler with the add after the fence acquisition", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			assert.Length(t, h.handles, 1, "the add must call the handler once")
			expect.Equal(t, *h.handles[0].Request, *h.request(protocol.Add, shardA, 1, firstEpoch),
				"the handler must get the request of the add")
			expect.Equal(t, h.handles[0].Timeout, addTimeout, "the handler must have the timeout of an add")
		})

		t.Run("calls the handler with the fencing token of the tenure of the add", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, secondEpoch)
			assert.Length(t, h.handles, 1, "the add must call the handler once")
			expect.Equal(t, h.handles[0].Fence, h.acquires[0].Fence,
				"the handler must get the token that the add acquired")
		})

		t.Run("calls the handler of an add without a fence acquisition as the agent of the root", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, rooted)
			h.start(t)
			to := h.host(t, firstEpoch)
			assert.NoError(t, replyTo[struct{}](t, h, to).Err, "the agent of the root must serve the add of a cell")
			expect.Empty(t, h.acquires, "the agent of the root must not acquire a fence")
			expect.Length(t, h.handles, 1, "the agent of the root must call the handler of the add once")
		})

		t.Run("serves the tenure of an add that succeeds", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			assert.Equal(t, h.observed(t, shardA).Status, shard.StatusServing, "the replica of the add must serve")
		})

		t.Run("returns the error of a fence acquisition that fails", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.fence = func(*fence.Token) error { return epoch.ErrFenced }
			h.place(t, shardA, firstEpoch)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			expect.ErrorIs(t, got.Err, epoch.ErrFenced, "the add must fail with the error of the fence")
			expect.Empty(t, h.handles, "an add whose fence acquisition failed must not call the handler")
			expect.Empty(
				t,
				h.reports[len(h.reports)-1].Replicas,
				"an add whose fence acquisition failed has no replica",
			)
		})

		t.Run("removes the token of the shard when the fence acquisition of a later add fails", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.fence = func(*fence.Token) error { return epoch.ErrFenced }
			later := h.request(protocol.Add, shardA, 1, firstEpoch)
			later.Step = 1
			got := replyTo[struct{}](t, h, h.do(t, later))
			assert.ErrorIs(t, got.Err, epoch.ErrFenced, "the later add must fail with the error of the fence")
			_, err := h.machine.Token(shardA)
			assert.ErrorIs(t, err, agent.ErrNotWriter, "a failed add must not leave the token of the tenure before it")
		})

		t.Run("leaves the tenure failed when the handler of an add fails", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.handler = func(*protocol.Request) error { return errBroken }
			h.place(t, shardA, firstEpoch)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch)))
			expect.ErrorIs(t, got.Err, errBroken, "the add must fail with the error of the handler")
			expect.Equal(t, h.observed(t, shardA).Status, shard.StatusFailed, "the replica must be failed")
			_, err := h.machine.Token(shardA)
			expect.ErrorIs(t, err, agent.ErrNotWriter, "a failed add must not leave a token")
		})

		t.Run("returns ErrStale for an add whose placement changed before its turn", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Acquire); return ok }
			h.place(t, shardA, firstEpoch)
			h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			queued := h.request(protocol.Add, shardA, 1, firstEpoch)
			queued.Path = protocol.Abort
			to := h.do(t, queued)
			h.unplace(t, shardA, firstEpoch)
			h.release(t)
			assert.ErrorIs(t, replyTo[struct{}](t, h, to).Err, agent.ErrStale,
				"an add whose placement no longer lists the node must fail")
		})

		t.Run("returns ErrStale for an add whose session ends before its turn", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.step(t, machine.SessionEnded{Token: h.join})
			h.place(t, shardA, firstEpoch)
			expect.ErrorIs(t, replyTo[struct{}](t, h, to).Err, agent.ErrStale,
				"an add must not start without the session of its incarnation")
			expect.Empty(t, h.acquires, "an add of an ended session must not acquire the fence")
		})

		t.Run("returns ErrStale for an add of an earlier session at its turn", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.step(t, machine.SessionEnded{Token: h.join})
			h.elapse(t, firstBackoff)
			h.place(t, shardA, firstEpoch)
			expect.ErrorIs(t, replyTo[struct{}](t, h, to).Err, agent.ErrStale,
				"an add of an earlier incarnation must not start")
			expect.Empty(t, h.acquires, "an add of an earlier incarnation must not acquire the fence")
		})

		t.Run("returns ErrStale for an add whose acquisition ends after its session under stop", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) {
				s.spec.Ownership = config.Selection{Config: json.RawMessage(stopOnLoss)}
			})
			h.start(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Acquire); return ok }
			h.place(t, shardA, firstEpoch)
			to := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.step(t, machine.SessionEnded{Token: h.join})
			h.release(t)
			expect.ErrorIs(t, replyTo[struct{}](t, h, to).Err, agent.ErrStale,
				"an add must fail once its session has ended under stop")
			expect.Empty(t, h.handles, "the add must not call the handler")
			_, err := h.machine.Token(shardA)
			expect.ErrorIs(t, err, agent.ErrNotWriter, "the add must not leave a token")
		})

		t.Run("returns the error of a fence acquisition that fails after its session under stop", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(s *setup) {
				s.spec.Ownership = config.Selection{Config: json.RawMessage(stopOnLoss)}
			})
			h.start(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Acquire); return ok }
			h.fence = func(*fence.Token) error { return epoch.ErrFenced }
			h.place(t, shardA, firstEpoch)
			to := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.step(t, machine.SessionEnded{Token: h.join})
			h.release(t)
			assert.ErrorIs(t, replyTo[struct{}](t, h, to).Err, epoch.ErrFenced,
				"the error of the fence must take precedence over the end of the session")
		})

		t.Run("serves an add whose acquisition ends after its session under continue-fenced", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Acquire); return ok }
			h.place(t, shardA, firstEpoch)
			to := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.step(t, machine.SessionEnded{Token: h.join})
			h.release(t)
			expect.NoError(t, replyTo[struct{}](t, h, to).Err, "the add must serve under continue-fenced")
			_, err := h.machine.Token(shardA)
			expect.NoError(t, err, "the tenure must keep its token under continue-fenced")
		})

		t.Run("drops the tenure of a drop", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch)))
			expect.NoError(t, got.Err, "the drop must succeed")
			expect.Equal(
				t,
				h.handles[len(h.handles)-1].Timeout,
				dropTimeout,
				"the drop must have the timeout of a drop",
			)
			expect.Empty(t, h.reports[len(h.reports)-1].Replicas, "the node must not report a dropped replica")
			_, err := h.machine.Token(shardA)
			expect.ErrorIs(t, err, agent.ErrNotWriter, "a dropped tenure must not leave a token")
		})

		t.Run("completes a drop of a tenure that the node does not have without the handler", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Drop, shardA, 1, firstEpoch)))
			expect.NoError(t, got.Err, "a drop without a tenure must succeed")
			expect.Empty(t, h.handles, "a drop without a tenure must not call the handler")
		})

		t.Run("completes a drop of another epoch than the tenure without the handler", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.do(t, h.request(protocol.Drop, shardA, 2, secondEpoch))
			assert.Length(t, h.handles, 1, "a drop of another tenure must not call the handler")
		})

		t.Run("calls the handler of a drop with the fencing token of its tenure", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch))
			assert.Length(t, h.handles, 2, "the drop must call the handler")
			expect.Equal(t, h.handles[1].Fence, h.acquires[0].Fence,
				"the handler must get the token of the tenure that the drop ends")
		})

		t.Run("revokes the token of a tenure while its drop runs", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Handle); return ok }
			h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch))
			_, err := h.machine.Token(shardA)
			expect.ErrorIs(t, err, epoch.ErrFenced, "the drop must revoke the token of the tenure")
			expect.Equal(t, h.observed(t, shardA).Status, shard.StatusStopping, "the replica must be stopping")
		})

		t.Run("leaves the tenure failed with its token revoked when the drop fails", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.handler = func(*protocol.Request) error { return errBroken }
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch)))
			expect.ErrorIs(t, got.Err, errBroken, "the drop must fail with the error of the handler")
			expect.Equal(t, h.observed(t, shardA).Status, shard.StatusFailed, "the replica must be failed")
			_, err := h.machine.Token(shardA)
			expect.ErrorIs(t, err, epoch.ErrFenced, "the token must remain revoked")
		})

		t.Run("cleans up a tenure that the placements no longer list", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.unplace(t, shardA, secondEpoch)
			assert.Length(t, h.handles, 2, "the removal must clean up the tenure")
			want := h.request(protocol.Drop, shardA, 1, firstEpoch)
			want.Path = protocol.Cleanup
			expect.Equal(t, *h.handles[1].Request, *want,
				"the cleanup must be a drop on the path of a cleanup with the request of the add")
			expect.Equal(t, h.handles[1].Timeout, dropTimeout, "the cleanup must have the timeout of a drop")
			expect.Empty(
				t,
				h.reports[len(h.reports)-1].Replicas,
				"the node must not report a replica that it cleaned up",
			)
		})

		t.Run("cleans up a tenure once when the cleanup fails", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Handle); return ok }
			h.unplace(t, shardA, secondEpoch)
			h.commitCell(t, &store.Mutation{})
			h.follow(t)
			h.handler = func(*protocol.Request) error { return errBroken }
			h.release(t)
			assert.Length(t, h.handles, 2, "a second generation must not clean up the tenure again")
		})

		t.Run("cleans up a tenure again after a transient error", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			failed := false
			h.handler = func(r *protocol.Request) error {
				if r.Path == protocol.Cleanup && !failed {
					failed = true
					return errBusy
				}
				return nil
			}
			h.unplace(t, shardA, secondEpoch)
			expect.Length(t, h.handles, 2, "the cleanup must wait for its backoff")
			h.elapse(t, firstBackoff)
			expect.Length(t, h.handles, 3, "the cleanup must run again after its backoff")
			expect.Equal(t, *h.handles[2].Request, *h.handles[1].Request, "the retry must have the same request")
			expect.Empty(t, h.reports[len(h.reports)-1].Replicas, "the retry must remove the tenure")
		})

		t.Run("leaves the tenure failed after a cleanup error that is not transient", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.handler = func(*protocol.Request) error { return errBroken }
			h.unplace(t, shardA, secondEpoch)
			h.commitCell(t, &store.Mutation{})
			h.follow(t)
			expect.Length(t, h.handles, 2, "the agent must not run a failed cleanup again")
			expect.Equal(t, h.observed(t, shardA).Status, shard.StatusFailed, "the replica must be failed")
			expect.Contains(t, h.logged(), logCleanup, "the agent must log the failed cleanup")
			_, err := h.machine.Token(shardA)
			expect.ErrorIs(t, err, epoch.ErrFenced, "the token of the tenure must remain revoked")
		})

		t.Run("calls the handler for a drop of a tenure whose cleanup failed", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.handler = func(*protocol.Request) error { return errBroken }
			h.unplace(t, shardA, secondEpoch)
			h.handler = nil
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch)))
			expect.NoError(t, got.Err, "the drop must succeed")
			expect.Length(t, h.handles, 3, "the drop must call the handler")
		})

		t.Run("cleans up a tenure whose drop failed once the placements no longer list it", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.handler = func(*protocol.Request) error { return errBroken }
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch)))
			assert.ErrorIs(t, got.Err, errBroken, "the drop must fail with the error of the handler")
			h.handler = nil
			h.unplace(t, shardA, secondEpoch)
			assert.Length(t, h.handles, 3, "the agent must clean up the tenure that its drop left")
			expect.Equal(t, h.handles[2].Request.Path, protocol.Cleanup, "the last call must be the cleanup")
		})

		t.Run("does not run a failed cleanup again after a drop of the tenure fails", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.handler = func(*protocol.Request) error { return errBroken }
			h.unplace(t, shardA, secondEpoch)
			h.handler = func(*protocol.Request) error { return errBusy }
			got := replyTo[struct{}](t, h, h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch)))
			assert.ErrorIs(t, got.Err, errBusy, "the drop must fail with the error of the handler")
			h.commitCell(t, &store.Mutation{})
			h.follow(t)
			assert.Length(t, h.handles, 3,
				"the agent must not run a cleanup again after an error that is not transient")
		})

		t.Run("ends the cleanup of a tenure that a drop removed without an effect", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Handle); return ok }
			h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch))
			h.unplace(t, shardA, secondEpoch)
			h.release(t)
			assert.Length(t, h.handles, 2, "the cleanup of a dropped tenure must not call the handler")
		})

		t.Run("cleans up a tenure that a snapshot after a compaction omits", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.commitCell(t, &store.Mutation{Put: []shard.Placement{placement(shardA, secondEpoch, otherNode)}})
			h.compact(t)
			assert.Length(t, h.handles, 2, "the snapshot must clean up the tenure")
			expect.Equal(t, h.handles[1].Request.Path, protocol.Cleanup, "the snapshot must start a cleanup")
		})

		t.Run("drops an add that waits for its placement when its caller stops waiting", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.step(t, machine.Cancelled{Reply: to})
			h.place(t, shardA, firstEpoch)
			assert.Empty(t, h.acquires, "an add without a caller must not start")
		})

		t.Run("drops a callback that waits for its turn when its caller stops waiting", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Handle); return ok }
			h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch))
			queued := h.request(protocol.Add, shardA, 2, firstEpoch)
			queued.Path, queued.Step = protocol.Abort, 1
			to := h.do(t, queued)
			h.step(t, machine.Cancelled{Reply: to})
			h.release(t)
			assert.Length(t, h.acquires, 1, "a callback without a caller must not run")
		})

		t.Run("runs the other callbacks that wait for their turn when one caller stops waiting", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.serve(t, shardA, 1, firstEpoch)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Handle); return ok }
			h.do(t, h.request(protocol.Drop, shardA, 2, firstEpoch))
			add := h.request(protocol.Add, shardA, 2, firstEpoch)
			add.Path, add.Step = protocol.Abort, 1
			h.do(t, add)
			drop := h.request(protocol.Drop, shardA, 2, firstEpoch)
			drop.Path, drop.Step = protocol.Abort, 2
			to := h.do(t, drop)
			h.step(t, machine.Cancelled{Reply: to})
			h.release(t)
			expect.Length(t, h.acquires, 2, "the callback with a caller must run")
			expect.Length(t, h.handles, 3, "the callback without a caller must not run")
		})

		t.Run("starts the other adds that wait for their placement when one caller stops waiting", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			second := h.request(protocol.Add, shardA, 1, firstEpoch)
			second.Step = 1
			to := h.do(t, second)
			h.step(t, machine.Cancelled{Reply: to})
			h.place(t, shardA, firstEpoch)
			assert.Length(t, h.acquires, 1, "the add with a caller must start once its placement arrives")
		})

		t.Run("runs an add that its caller sends again after it stopped waiting", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.step(t, machine.Cancelled{Reply: to})
			again := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.place(t, shardA, firstEpoch)
			assert.NoError(t, replyTo[struct{}](t, h, again).Err, "the add must run for the call that sent it again")
		})

		t.Run("runs a callback once when its caller stops waiting and sends it again", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Handle); return ok }
			h.place(t, shardA, firstEpoch)
			to := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.step(t, machine.Cancelled{Reply: to})
			again := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.release(t)
			expect.NoError(
				t,
				replyTo[struct{}](t, h, again).Err,
				"the repeated call must get the result of the callback",
			)
			expect.Length(t, h.handles, 1, "the callback must run once")
		})

		t.Run("keeps the result of a callback that runs when its caller stops waiting", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Handle); return ok }
			h.place(t, shardA, firstEpoch)
			to := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.step(t, machine.Cancelled{Reply: to})
			h.release(t)
			again := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			expect.NoError(t, replyTo[struct{}](t, h, again).Err, "the repeated key must get the kept result")
			expect.Length(t, h.handles, 1, "the callback must run once")
		})

		t.Run("replies with ErrNotServing to a callback that runs at Stopped", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.pause = func(e machine.Effect) bool { _, ok := e.(machine.Handle); return ok }
			h.place(t, shardA, firstEpoch)
			to := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.step(t, machine.Stopped{})
			assert.ErrorIs(t, replyTo[struct{}](t, h, to).Err, agent.ErrNotServing,
				"a callback that runs must get ErrNotServing when the run ends")
		})

		t.Run("ignores the result of a fence acquisition that it does not wait for", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Acquired{Token: 999}, out)
			assert.Equal(t, got, out, "the agent must ignore the result of another acquisition")
		})

		t.Run("ignores the result of a handler that it does not wait for", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			out := []machine.Effect{marker}
			got := h.machine.Step(machine.Handled{Token: 999}, out)
			assert.Equal(t, got, out, "the agent must ignore the result of another handler call")
		})

		t.Run("replies to the other caller of a key when one caller stops waiting", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			to := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			other := h.do(t, h.request(protocol.Add, shardA, 1, firstEpoch))
			h.step(t, machine.Cancelled{Reply: to})
			h.place(t, shardA, firstEpoch)
			expect.NoError(t, replyTo[struct{}](t, h, other).Err, "the other caller must get the result")
			expect.False(t, slices.ContainsFunc(h.replies, func(e machine.Effect) bool {
				return e.(machine.Reply[struct{}]).To == to
			}), "the agent must not reply to a call that the runtime cancelled")
		})

		t.Run("releases the request of each callback that ended", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.place(t, shardA, firstEpoch)
			gone := func() weak.Pointer[byte] {
				rng := shard.Range{Start: make([]byte, releasedRange)}
				add := h.request(protocol.Add, shardA, 1, firstEpoch)
				add.Range = rng
				drop := h.request(protocol.Drop, shardA, 2, firstEpoch)
				drop.Range = rng
				h.do(t, add)
				h.do(t, drop)
				h.handles = nil
				return weak.Make(&rng.Start[0])
			}()
			runtime.GC()
			assert.Nil(t, gone.Value(), "the agent must release the request of each callback that ended")
			runtime.KeepAlive(h)
		})

		t.Run("releases the request of a cleanup that ran again", func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.start(t)
			h.place(t, shardA, firstEpoch)
			failed := false
			h.handler = func(r *protocol.Request) error {
				if r.Path == protocol.Cleanup && !failed {
					failed = true
					return errBusy
				}
				return nil
			}
			gone := func() weak.Pointer[byte] {
				add := h.request(protocol.Add, shardA, 1, firstEpoch)
				add.Range = shard.Range{Start: make([]byte, releasedRange)}
				h.do(t, add)
				return weak.Make(&add.Range.Start[0])
			}()
			h.unplace(t, shardA, secondEpoch)
			h.elapse(t, firstBackoff)
			h.handles = nil
			runtime.GC()
			assert.Nil(t, gone.Value(), "the agent must release the request of a cleanup that ended")
			runtime.KeepAlive(h)
		})
	})
}

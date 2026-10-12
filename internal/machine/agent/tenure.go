// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"go.thesmos.sh/core/crypto"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/resilience"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// result is the kept result of a callback: the key and the digest of its request, and its
// error.
type result struct {
	// err is the error of the callback, or nil.
	err error
	// key is the key of the request of the callback.
	key protocol.Key
	// digest is the digest of the request of the callback.
	digest crypto.Digest
}

// tenure is the replica of a shard on the node at one epoch, from the callback that started
// it until a drop or a cleanup removes it.
type tenure struct {
	// cause is at most the first 1,024 bytes of the message of the error that the
	// application reported with a fault, or empty without a fault.
	cause string
	// token is the fencing token of the tenure.
	token fence.Token
	// request is the request of the callback that started the tenure.
	request protocol.Request
	// status is the condition of the replica.
	status shard.Status
	// fault is the fault that the application reported in the replica.
	fault shard.Fault
	// cleaned is true once a cleanup of the tenure has ended with an error that is not
	// transient. The agent does not run the cleanup again, and a drop of the tenure runs the
	// handler.
	cleaned bool
}

// work is a callback or a cleanup of one shard that waits for its turn or runs.
type work struct {
	// state is the state of the shard of the work.
	state *shardState
	// waiters lists the tokens of the calls that wait for the result of the work, in their
	// order. A cleanup has none.
	waiters []machine.ReplyToken
	// request is the request of the callback, or the request that the agent builds for a
	// cleanup.
	request protocol.Request
	// effect is the token of the Acquire, the Handle or the Timer of the work in flight.
	effect machine.Token
	// attempt counts the transient errors of a cleanup in a row.
	attempt int
	// digest is the digest of the request of a callback.
	digest crypto.Digest
	// running is true from the start of the work until its result.
	running bool
	// cleanup is true for the cleanup of a tenure that the watched placements no longer
	// list.
	cleanup bool
}

// operation is what the agent knows about the newest operation of a shard.
type operation struct {
	// results lists the result of each callback and cleanup that ended without a transient
	// error while the operation was the newest of the shard, in the order of their ends. A
	// lookup costs O(k) for the k results.
	results []result
	// id is the operation.
	id shard.OperationID
	// aborted is true once the agent has seen the abort path of the operation.
	aborted bool
	// ended is true once the agent has seen the operation end. The agent then forgets the
	// results.
	ended bool
	// watched is true while the watch of the scope shows the transition of the operation in
	// flight.
	watched bool
}

// shardState is what the agent knows about one shard: its newest operation with the kept
// results of its callbacks, the highest epoch of a callback, the work of the shard, and the
// tenure of the node.
type shardState struct {
	// inflight maps the key of each callback and cleanup that waits or runs to its work.
	inflight map[protocol.Key]*work
	// tenure is the tenure of the node, and nil without one.
	tenure *tenure
	// running is the work that runs, and nil while no work runs.
	running *work
	// queue lists the work that waits for its turn, in its order.
	queue []*work
	// listing lists the callbacks that start a tenure and wait until the watched placements
	// list the tenure, in their order.
	listing []*work
	// op is the newest operation of the shard that the agent has seen.
	op operation
	// epoch is the highest epoch of the shard that a callback has named.
	epoch epoch.Epoch
}

// newer takes op as the newest operation of the shard when its sequence is above the
// sequence of the newest operation that the agent has seen. The agent then forgets what it
// knew about the older operation.
func (st *shardState) newer(op shard.OperationID) {
	if op.Sequence > st.op.id.Sequence {
		st.op = operation{id: op}
	}
}

// cleanup queues the cleanup of the tenure t, unless a cleanup of t waits or runs. The
// cleanup is a drop on the path protocol.Cleanup at step 0. Its request has the operation,
// the node, the incarnation of the node, the shard, the range, the role and the epoch of the
// callback that started t, so a removal event and a snapshot build the same key for t.
func (st *shardState) cleanup(t *tenure) {
	r := t.request
	r.Call, r.Path, r.Step = protocol.Drop, protocol.Cleanup, 0
	key := r.Key()
	if _, ok := st.inflight[key]; ok {
		return
	}
	w := &work{state: st, request: r, cleanup: true}
	st.inflight[key] = w
	st.queue = append(st.queue, w)
}

// forget removes the work w, which waits for its turn or for its placement, after its last
// caller stopped waiting.
func (st *shardState) forget(w *work) {
	st.listing = slices.DeleteFunc(st.listing, func(x *work) bool { return x == w })
	st.queue = slices.DeleteFunc(st.queue, func(x *work) bool { return x == w })
	delete(st.inflight, w.request.Key())
}

// waiting returns the work of the shard that callers can wait for: the callbacks that wait
// for their placement or their turn, and the work that runs.
func (st *shardState) waiting() []*work {
	ws := slices.Concat(st.listing, st.queue)
	if st.running != nil {
		ws = append(ws, st.running)
	}
	return ws
}

// finish ends the work w of the shard with err. The agent keeps the result unless err is
// transient, so a request with the same key gets the result without the handler. It replies
// to each call that waits for w.
func (st *shardState) finish(w *work, err error, out []machine.Effect) []machine.Effect {
	key := w.request.Key()
	delete(st.inflight, key)
	w.running, st.running = false, nil
	if !errs.Retryable(err) {
		st.op.results = append(st.op.results, result{err: err, key: key, digest: w.digest})
	}
	return reply(w, err, out)
}

// state returns the state of the shard sid, and creates it for a shard that the agent has
// not seen.
func (m *Machine) state(sid shard.ID) *shardState {
	st, ok := m.shards.Get(sid)
	if !ok {
		st = &shardState{inflight: make(map[protocol.Key]*work)}
		m.shards.Set(sid, st)
	}
	return st
}

// run starts the work at the head of the queue of the shard st while no work of the shard
// runs.
func (m *Machine) run(st *shardState, out []machine.Effect) []machine.Effect {
	for st.running == nil && len(st.queue) > 0 {
		w := st.queue[0]
		st.queue = slices.Delete(st.queue, 0, 1)
		out = m.begin(st, w, out)
	}
	return out
}

// begin starts the work w of the shard st:
//   - A drop of a tenure that the node does not have completes without the handler. A
//     cleanup of a tenure that is no longer active ends without an effect.
//   - A drop and a cleanup of the tenure revoke its fencing token and call the handler.
//   - An add fails with an error that wraps ErrStale when the session of its incarnation has
//     ended, or when the watched placements no longer list its tenure.
//   - Any other add acquires the fence of the tenure and then calls the handler.
func (m *Machine) begin(st *shardState, w *work, out []machine.Effect) []machine.Effect {
	r := &w.request
	if r.Call == protocol.Drop {
		t := st.tenure
		if t == nil || t.request.Epoch != r.Epoch {
			return st.finish(w, nil, out)
		}
		m.withdraw(r.Shard)
		t.status = shard.StatusStopping
		return m.report(m.handle(st, w, m.settings.drop, out))
	}
	if !m.session.live || r.NodeIncarnation != m.session.incarnation {
		err := fmt.Errorf(
			"%w: the session of the incarnation %d of node %s has ended",
			ErrStale,
			r.NodeIncarnation,
			r.Node,
		)
		return st.finish(w, err, out)
	}
	if m.watched.placements[r.Shard].Epoch != r.Epoch {
		err := fmt.Errorf("%w: the placement of shard %v no longer names node %s at epoch %d",
			ErrStale, r.Shard, r.Node, r.Epoch)
		return st.finish(w, err, out)
	}
	t := &tenure{request: *r, status: shard.StatusStarting, token: fence.Token{
		Namespace:   m.rootScope.Namespace,
		Cell:        m.cellScope.Cell,
		Incarnation: m.incarnation,
		Shard:       r.Shard,
		Epoch:       r.Epoch,
	}}
	st.tenure = t
	w.running, st.running = true, w
	w.effect = m.next()
	m.effects[w.effect] = w
	out = append(out, machine.Acquire{Fence: t.token, Token: w.effect, Timeout: m.settings.add})
	return m.report(out)
}

// handle appends the Handle of the work w of the shard st to out. The runtime cancels the
// context of the handler once timeout has passed.
func (m *Machine) handle(st *shardState, w *work, timeout time.Duration, out []machine.Effect) []machine.Effect {
	w.running, st.running = true, w
	w.effect = m.next()
	m.effects[w.effect] = w
	return append(out, machine.Handle{Request: &w.request, Token: w.effect, Timeout: timeout})
}

// acquired takes the result of the fence acquisition of an add. After the acquisition,
// Machine.Token returns the token of the tenure, and the agent calls the handler. An
// acquisition that fails ends the add with its error, and the tenure does not start. Under
// ownership.Stop, an acquisition that completes after the session of the incarnation of the
// add has ended fails with an error that wraps ErrStale.
func (m *Machine) acquired(ev machine.Acquired, out []machine.Effect) []machine.Effect {
	w, ok := m.effects[ev.Token]
	if !ok {
		return out
	}
	delete(m.effects, ev.Token)
	st, r := w.state, &w.request
	if ev.Err == nil && m.settings.lease.OnLoss == ownership.Stop &&
		(!m.session.live || r.NodeIncarnation != m.session.incarnation) {

		ev.Err = fmt.Errorf("%w: the session of the incarnation %d of node %s has ended", ErrStale,
			r.NodeIncarnation, r.Node)
	}
	if ev.Err != nil {
		if errors.Is(ev.Err, epoch.ErrFenced) {
			m.metrics.refusals.Add(context.Background(), 1)
		}
		st.tenure = nil
		err := fmt.Errorf("agent: the fence of shard %v at epoch %d: %w", w.request.Shard, w.request.Epoch, ev.Err)
		return m.report(m.run(st, st.finish(w, err, out)))
	}
	m.issue(w.request.Shard, &st.tenure.token)
	return m.handle(st, w, m.settings.add, out)
}

// handled takes the result of the handler for the work w:
//   - An add that succeeds serves its tenure. After an add that fails, the tenure is failed,
//     and Machine.Token returns ErrNotWriter for the shard.
//   - A drop and a cleanup that succeed remove the tenure. A drop that fails leaves the
//     tenure failed, and its fencing token revoked.
//   - A cleanup that fails with a transient error runs again with the same request after a
//     backoff. Any other error leaves the tenure failed, and the agent logs it.
//
// The agent then reports the replicas of the node and runs the next work of the shard.
func (m *Machine) handled(ev machine.Handled, out []machine.Effect) []machine.Effect {
	w, ok := m.effects[ev.Token]
	if !ok {
		return out
	}
	delete(m.effects, ev.Token)
	st, t, err := w.state, w.state.tenure, ev.Err
	if w.cleanup && errs.Retryable(err) {
		w.attempt++
		w.effect = m.next()
		m.effects[w.effect] = w
		delay := resilience.Backoff(m.rand, w.attempt, RetryBase, RetryMax)
		return append(out, machine.Timer{Token: w.effect, Duration: delay})
	}
	if err == nil && w.request.Call == protocol.Add {
		t.status = shard.StatusServing
	} else if err == nil {
		st.tenure = nil
		m.unissue(w.request.Shard)
	} else if w.request.Call == protocol.Add {
		t.status = shard.StatusFailed
		m.unissue(w.request.Shard)
	} else {
		t.status, t.cleaned = shard.StatusFailed, w.cleanup
	}
	if err != nil && w.cleanup {
		m.logger.LogAttrs(context.Background(), slog.LevelError, "agent: the cleanup of a tenure failed",
			slog.String(attrShard, w.request.Shard.String()), slog.Any(attrError, err))
	}
	return m.report(m.run(st, st.finish(w, err, out)))
}

// retried takes the end of the backoff of a cleanup, which runs again with the same
// request. It returns out unchanged for a Timer of no cleanup.
func (m *Machine) retried(tok machine.Token, out []machine.Effect) []machine.Effect {
	w, ok := m.effects[tok]
	if !ok {
		return out
	}
	delete(m.effects, tok)
	return m.handle(w.state, w, m.settings.drop, out)
}

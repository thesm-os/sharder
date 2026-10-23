// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"fmt"
	"slices"

	"go.thesmos.sh/core/crypto"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/protocol"
)

// do takes a callback of the controller of the cell. The agent checks the call and refuses a
// stale request before it changes its state. It returns the kept result of a key that it
// has seen, and a request that repeats the key of a callback that waits or runs waits for
// the result of that callback. A callback that starts a tenure waits until the watched
// placements list the tenure. The agent runs the callbacks of one shard one at a time.
func (m *Machine) do(ev *machine.Do, out []machine.Effect) []machine.Effect {
	r := ev.Request
	digest, err := m.check(&ev.Metadata, r)
	if err != nil {
		return append(out, machine.Reply[struct{}]{Err: err, To: ev.Reply})
	}
	st := m.state(r.Shard)
	key := r.Key()
	i := slices.IndexFunc(st.op.results, func(res result) bool { return res.key == key })
	w, waits := st.inflight[key]
	if i >= 0 && st.op.results[i].digest != digest || waits && w.digest != digest {
		err := fmt.Errorf("%w: the key of step %d on the path %v of %v", ErrReused, r.Step, r.Path, r.Operation)
		return append(out, machine.Reply[struct{}]{Err: err, To: ev.Reply})
	}
	st.epoch = r.Epoch
	st.newer(r.Operation)
	if r.Path == protocol.Abort {
		st.op.aborted = true
	}
	if i >= 0 {
		return append(out, machine.Reply[struct{}]{Err: st.op.results[i].err, To: ev.Reply})
	}
	if waits {
		w.waiters = append(w.waiters, ev.Reply)
		return out
	}
	w = &work{state: st, request: *r, digest: digest, waiters: []machine.ReplyToken{ev.Reply}}
	st.inflight[key] = w
	if p, _ := m.watched.placements.Get(r.Shard); r.Call == protocol.Add && p.Epoch != r.Epoch {
		st.listing = append(st.listing, w)
		return out
	}
	st.queue = append(st.queue, w)
	return m.run(st, out)
}

// check returns the digest of the request r of a call with the metadata md. It returns the
// error of the reply for a request that the agent refuses, in this order:
//   - an error that wraps protocol.ErrUnsupported for metadata that this release does not
//     implement.
//   - an error that wraps ErrRequest for a request that does not encode.
//   - an error that wraps ErrUnseen for a controller epoch above the record of the
//     controller that the agent has read.
//   - an error that wraps ErrDenied for a caller other than the controller of that record.
//   - an error that wraps protocol.ErrUnsupported for a callback other than an add or a drop,
//     and for a role other than the writer role of the replica state model.
//   - an error that wraps ErrRequest for a request on the path protocol.Cleanup, and for a
//     request without an epoch. Only the agent builds a request on the path
//     protocol.Cleanup, and every tenure has an epoch above zero.
//   - an error that wraps ErrStale for another scope, another namespace incarnation or
//     another node.
//   - an error that wraps ErrNotServing for a later incarnation of the node than the
//     agent has joined.
//   - an error that wraps ErrStale for an earlier incarnation of the node, or for the
//     incarnation of a session that has ended.
//   - an error that wraps ErrStale for a controller epoch below the highest that the agent
//     has seen, an epoch of the shard below the highest that it has seen, an operation older
//     than the newest operation of the shard, an operation that has ended, and a forward
//     callback of an operation on its abort path.
func (m *Machine) check(md *protocol.Metadata, r *protocol.Request) (crypto.Digest, error) {
	if err := md.Check(); err != nil {
		return crypto.Digest{}, fmt.Errorf("agent: the metadata of the callback: %w", err)
	}
	digest, err := r.Digest()
	if err != nil {
		return crypto.Digest{}, fmt.Errorf("%w: %w", ErrRequest, err)
	}
	record := &m.watched.record
	if r.ControllerEpoch > record.Epoch {
		return digest, fmt.Errorf("%w: the callback has the controller epoch %d, and the record has %d",
			ErrUnseen, r.ControllerEpoch, record.Epoch)
	}
	if !m.unauthenticated && md.Caller != record.ControllerIdentity {
		return digest, fmt.Errorf(
			"%w: %q calls, and the controller is %q",
			ErrDenied,
			md.Caller,
			record.ControllerIdentity,
		)
	}
	if !r.Call.IsValid() || r.Role != m.settings.writer {
		return digest, fmt.Errorf("%w: the callback %v in the role %v", protocol.ErrUnsupported, r.Call, r.Role)
	}
	if r.Path != protocol.Forward && r.Path != protocol.Abort {
		return digest, fmt.Errorf("%w: a callback on the path %v", ErrRequest, r.Path)
	}
	if r.Epoch.IsZero() {
		return digest, fmt.Errorf("%w: a callback without an epoch", ErrRequest)
	}
	if r.Scope != m.cellScope || r.Operation.Incarnation != m.incarnation || r.Node != m.node.ID {
		return digest, fmt.Errorf("%w: the callback is for node %s in %v of the incarnation %v",
			ErrStale, r.Node, r.Scope, r.Operation.Incarnation.ID)
	}
	if r.NodeIncarnation > m.session.incarnation {
		return digest, fmt.Errorf("%w: the callback is for the incarnation %d of node %s, and the node has joined %d",
			ErrNotServing, r.NodeIncarnation, r.Node, m.session.incarnation)
	}
	if r.NodeIncarnation < m.session.incarnation || !m.session.live {
		return digest, fmt.Errorf("%w: the incarnation %d of node %s has ended", ErrStale, r.NodeIncarnation, r.Node)
	}
	if r.ControllerEpoch < m.epoch {
		return digest, fmt.Errorf("%w: the controller epoch %d is below %d", ErrStale, r.ControllerEpoch, m.epoch)
	}
	st, ok := m.shards.Get(r.Shard)
	if !ok {
		return digest, nil
	}
	op := &st.op
	if r.Epoch < st.epoch || r.Operation.Sequence < op.id.Sequence ||
		r.Operation == op.id && (op.ended || op.aborted && r.Path == protocol.Forward) {

		return digest, fmt.Errorf("%w: %v at epoch %d on the path %v, and the shard has seen %v at epoch %d",
			ErrStale, r.Operation, r.Epoch, r.Path, op.id, st.epoch)
	}
	return digest, nil
}

// reply appends the reply with err to each call that waits for the work w, in their order.
func reply(w *work, err error, out []machine.Effect) []machine.Effect {
	for _, to := range w.waiters {
		out = append(out, machine.Reply[struct{}]{Err: err, To: to})
	}
	return out
}

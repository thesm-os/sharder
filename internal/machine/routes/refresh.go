// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes

import (
	"context"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/shard"
)

// wait is a Refresh that waits for its reply.
type wait struct {
	// cell is the cell of the generation.
	cell shard.CellID
	// reply is the token of the call.
	reply machine.ReplyToken
	// min is the generation that the Refresh waits for.
	min epoch.Epoch
	// incarnation is the namespace incarnation of the generation.
	incarnation shard.NamespaceIncarnation
}

// refresh takes a Refresh, which waits until the router has applied generation Min of its
// cell in its incarnation. A Refresh of another incarnation than the installed one starts a
// rediscovery. A Refresh before the first install waits for it.
//
// Error modes:
//   - the error of rediscover.
func (m *Machine) refresh(ev machine.Refresh, out []machine.Effect) ([]machine.Effect, error) {
	m.metrics.redirects.Add(context.Background(), 1)
	m.waits = append(m.waits, wait{cell: ev.Cell, reply: ev.Reply, min: ev.Min, incarnation: ev.Incarnation})
	d := m.current
	if d == nil {
		return out, nil
	}
	if ev.Incarnation != d.incarnation {
		var err error
		out, err = m.rediscover(out)
		return m.settle(out), err
	}
	if c := d.cell(ev.Cell); c != nil {
		c.highest = max(c.highest, ev.Min)
		c.report()
	}
	return m.settle(out), nil
}

// settle replies to each Refresh that waits and whose reply the installed discovery decides:
//   - shard.ErrIncarnation for another incarnation, once no rediscovery runs.
//   - the error of an installed namespace whose positions the router cannot compute.
//   - ErrCell for a cell that the cell map does not have.
//   - nil once the cell has applied the generation that the Refresh waits for.
//
// Every other Refresh keeps its wait. settle costs O(n) for the n Refresh calls that wait.
func (m *Machine) settle(out []machine.Effect) []machine.Effect {
	d := m.current
	if d == nil {
		return out
	}
	waits := m.waits[:0]
	for _, w := range m.waits {
		var err error
		done := true
		switch {
		case w.incarnation != d.incarnation:
			done, err = m.incoming == nil, shard.ErrIncarnation
		case d.err != nil:
			err = d.err
		default:
			if c := d.cell(w.cell); c == nil {
				err = ErrCell
			} else {
				done = c.generation >= w.min
			}
		}
		if done {
			out = append(out, machine.Reply[struct{}]{Err: err, To: w.reply})
		} else {
			waits = append(waits, w)
		}
	}
	m.waits = waits
	return out
}

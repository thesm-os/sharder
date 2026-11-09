// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package maintenance_test

import (
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/maintenance"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// The disruptions of the tests.
var (
	// disruptionID is the ID of a disruption that a request contains.
	disruptionID = budget.ID{ID: id.New128([16]byte{0: 0xd1, 15: 0x01})}
	// restart is a disruption of node B for an hour, without an ID and an incarnation.
	restart = budget.Disruption{
		Start: origin,
		End:   origin.Add(time.Hour),
		Scope: cellScope,
		Nodes: []shard.NodeID{nodeB.ID},
	}
)

// TestBudget checks the disruptions that a client requests from the budgets of the cells.
func TestBudget(t *testing.T) {
	t.Parallel()

	t.Run("Disrupt", func(t *testing.T) {
		t.Parallel()

		t.Run("requests the disruption from the controller of its cell", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			_, err := w.client(t).Disrupt(t.Context(), &maintenance.DisruptRequest{Disruption: restart})
			assert.NoError(t, err, "Disrupt must request the disruption")
			assert.Length(t, w.disrupted, 1, "Disrupt must request one disruption")
			got := w.disrupted[0]
			expect.Equal(t, got.record, w.cell.Cell, "Disrupt must call the controller of the cell")
			expect.Equal(t, got.disruption.Scope, cellScope, "the disruption must be of the scope of the cell")
			expect.Equal(t, got.disruption.Nodes, restart.Nodes, "the disruption must have the nodes of the request")
			expect.Equal(t, got.disruption.End, restart.End, "the disruption must have the end of the request")
		})

		t.Run("gives the disruption the namespace incarnation of its scope", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			_, err := w.client(t).Disrupt(t.Context(), &maintenance.DisruptRequest{Disruption: restart})
			assert.NoError(t, err, "Disrupt must request the disruption")
			assert.Length(t, w.disrupted, 1, "Disrupt must request one disruption")
			assert.Equal(t, w.disrupted[0].disruption.Incarnation, inc,
				"the disruption must have the namespace incarnation of its scope")
		})

		t.Run("returns the ID that the client gives the disruption", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			res, err := w.client(t).Disrupt(t.Context(), &maintenance.DisruptRequest{Disruption: restart})
			assert.NoError(t, err, "Disrupt must request the disruption")
			assert.Length(t, w.disrupted, 1, "Disrupt must request one disruption")
			expect.False(t, res.ID.IsZero(), "the disruption must have an ID")
			expect.Equal(t, res.ID, w.disrupted[0].disruption.ID, "the result must have the ID of the disruption")
		})

		t.Run("requests the disruption with the ID of the request", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			req := &maintenance.DisruptRequest{Disruption: restart}
			req.Disruption.ID = disruptionID
			res, err := w.client(t).Disrupt(t.Context(), req)
			assert.NoError(t, err, "Disrupt must request the disruption")
			assert.Length(t, w.disrupted, 1, "Disrupt must request one disruption")
			got := w.disrupted[0].disruption
			expect.Equal(t, got.ID, disruptionID, "the disruption must have the ID of the request")
			expect.Equal(t, res.ID, disruptionID, "the result must have the ID of the request")
		})

		t.Run("returns the decision of the budget", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			res, err := w.client(t).Disrupt(t.Context(), &maintenance.DisruptRequest{Disruption: restart})
			assert.NoError(t, err, "Disrupt must request the disruption")
			assert.Equal(t, res.Decision, w.decision, "Disrupt must return the decision of the budget")
		})

		t.Run("does not modify the disruption of the request", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			c := w.client(t)
			req := &maintenance.DisruptRequest{Disruption: restart}
			var err error
			assert.Pure(t, func() budget.Disruption { return req.Disruption }, func() {
				_, err = c.Disrupt(t.Context(), req)
			}, "Disrupt must leave the disruption of the request unchanged")
			assert.NoError(t, err, "Disrupt must request the disruption")
		})

		t.Run("returns shard.ErrIncarnation for a disruption of another namespace incarnation", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			req := &maintenance.DisruptRequest{Disruption: restart}
			req.Disruption.Incarnation = other
			_, err := w.client(t).Disrupt(t.Context(), req)
			assert.ErrorIs(t, err, shard.ErrIncarnation, "Disrupt must refuse a disruption of another incarnation")
		})

		t.Run("returns ErrNoController for a cell without a controller", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.cell.Cell.Controller = ""
			_, err := w.client(t).Disrupt(t.Context(), &maintenance.DisruptRequest{Disruption: restart})
			assert.ErrorIs(t, err, maintenance.ErrNoController, "Disrupt must refuse a cell without a controller")
		})

		t.Run("returns the error of a read of the scope of the cell", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.readFails, w.readErr = cellScope, errRead
			_, err := w.client(t).Disrupt(t.Context(), &maintenance.DisruptRequest{Disruption: restart})
			assert.ErrorIs(t, err, errRead, "Disrupt must return the error of the read")
		})

		t.Run("returns the error of the call", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.callErr = errCall
			_, err := w.client(t).Disrupt(t.Context(), &maintenance.DisruptRequest{Disruption: restart})
			assert.ErrorIs(t, err, errCall, "Disrupt must return the error of the call")
		})
	})

	t.Run("Done", func(t *testing.T) {
		t.Parallel()

		t.Run("ends the reservation at the controller of its cell", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			_, err := w.client(t).Done(t.Context(), &maintenance.DoneRequest{Scope: cellScope, ID: disruptionID})
			assert.NoError(t, err, "Done must end the reservation")
			assert.Length(t, w.done, 1, "Done must end one reservation")
			expect.Equal(t, w.done[0].record, w.cell.Cell, "Done must call the controller of the cell")
			expect.Equal(t, w.done[0].args, protocol.DoneArgs{Scope: cellScope, Incarnation: inc, ID: disruptionID},
				"Done must name the reservation in the namespace incarnation of its scope")
		})

		t.Run("returns ErrNoController for a cell without a controller", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.cell.Cell.Controller = ""
			_, err := w.client(t).Done(t.Context(), &maintenance.DoneRequest{Scope: cellScope, ID: disruptionID})
			assert.ErrorIs(t, err, maintenance.ErrNoController, "Done must refuse a cell without a controller")
		})

		t.Run("returns the error of a read of the scope of the cell", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.readFails, w.readErr = cellScope, errRead
			_, err := w.client(t).Done(t.Context(), &maintenance.DoneRequest{Scope: cellScope, ID: disruptionID})
			assert.ErrorIs(t, err, errRead, "Done must return the error of the read")
		})

		t.Run("returns the error of the call", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.callErr = errCall
			_, err := w.client(t).Done(t.Context(), &maintenance.DoneRequest{Scope: cellScope, ID: disruptionID})
			assert.ErrorIs(t, err, errCall, "Done must return the error of the call")
		})
	})
}

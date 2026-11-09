// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package maintenance_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/maintenance"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// TestStatus checks the status of a namespace that a client reads from the store and the
// membership.
func TestStatus(t *testing.T) {
	t.Parallel()

	t.Run("Status", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the namespace incarnation", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			got, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.NoError(t, err, "Status must read the namespace")
			assert.Equal(t, got.Incarnation, inc, "the status must have the namespace incarnation")
		})

		t.Run("returns the version of the accepted spec", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			got, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.NoError(t, err, "Status must read the namespace")
			assert.Equal(t, got.Version, accepted, "the status must have the version of the accepted spec")
		})

		t.Run("returns the status of the root scope", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			got, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.NoError(t, err, "Status must read the namespace")
			assert.Equal(t, got.Root, maintenance.ScopeStatus{
				Applied:    w.root.Applied,
				Record:     w.root.Cell,
				Scope:      rootScope,
				Generation: rootGeneration,
			}, "the status of the root scope must have what the root scope records")
		})

		t.Run("returns the status of the scope of each cell", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			got, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.NoError(t, err, "Status must read the namespace")
			assert.Length(t, got.Cells, 1, "the status must have the one cell of the namespace")
			assert.Equal(t, got.Cells[0].ScopeStatus, maintenance.ScopeStatus{
				Record:     w.cell.Cell,
				Scope:      cellScope,
				Generation: cellGeneration,
			}, "the status of the cell must have what its scope records", assert.EquateEmpty())
		})

		t.Run("returns the state of each cell", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.cell.Transitions = []store.Transition{
				{Operation: shard.OperationID{Incarnation: inc, Shard: shardA, Sequence: 2}},
			}
			w.cell.Reservations = []budget.Reservation{{Disruption: budget.Disruption{Scope: cellScope}}}
			got, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.NoError(t, err, "Status must read the namespace")
			assert.Length(t, got.Cells, 1, "the status must have the one cell of the namespace")
			cell := got.Cells[0]
			expect.Equal(t, cell.Transitions, w.cell.Transitions, "the status must have the transitions of the cell")
			expect.Equal(t, cell.Reservations, w.cell.Reservations, "the status must have the reservations of the cell")
			expect.Equal(t, cell.Drains, w.cell.Drains, "the status must have the drains of the cell")
		})

		t.Run("returns each node with its replicas in the order of the node IDs", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			got, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.NoError(t, err, "Status must read the namespace")
			assert.Length(t, got.Cells, 1, "the status must have the one cell of the namespace")
			assert.Equal(t, got.Cells[0].Nodes, []maintenance.NodeStatus{
				{
					Replicas: []maintenance.Replica{
						{Shard: shardB, Role: shard.RolePrimary},
						{Shard: shardC, Role: shard.RoleSecondary},
					},
					Node: shard.Node{ID: gone},
				},
				{Replicas: []maintenance.Replica{{Shard: shardA, Role: shard.RolePrimary}}, Node: nodeA, Live: true},
				{Node: nodeB, Live: true, Draining: true},
			}, "the status must list each node of the view and each node of a replica", assert.EquateEmpty())
		})

		t.Run("returns each shard without a writer as unplaced", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			got, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.NoError(t, err, "Status must read the namespace")
			assert.Length(t, got.Cells, 1, "the status must have the one cell of the namespace")
			cell := got.Cells[0]
			assert.Equal(t, cell.Unplaced, []shard.ID{shardC}, "the status must list the shard without a writer")
		})

		t.Run("returns ErrNoNamespace for a namespace without an incarnation", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.root.Head.Incarnation = shard.NamespaceIncarnation{}
			_, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.ErrorIs(t, err, maintenance.ErrNoNamespace, "Status must refuse a namespace that does not exist")
		})

		t.Run("returns an error for an accepted spec that does not decode", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.root.Spec = []byte("{")
			_, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.HasError(t, err, "Status must refuse an accepted spec that does not decode")
		})

		t.Run("returns shard.ErrIncarnation for a cell of another namespace incarnation", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.cell.Head.Incarnation = other
			_, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.ErrorIs(t, err, shard.ErrIncarnation, "Status must refuse a cell that another incarnation created")
		})

		t.Run("returns the error of a read of the root scope", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.readFails, w.readErr = rootScope, errRead
			_, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.ErrorIs(t, err, errRead, "Status must return the error of the read")
		})

		t.Run("returns the error of a read of the scope of a cell", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.readFails, w.readErr = cellScope, errRead
			_, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.ErrorIs(t, err, errRead, "Status must return the error of the read")
		})

		t.Run("returns the error of a page of the placements of a cell", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.readFails, w.pageErr = cellScope, errPage
			_, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.ErrorIs(t, err, errPage, "Status must return the error of the page")
		})

		t.Run("returns the error of the close of a cursor", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.readFails, w.closeErr = cellScope, errClose
			_, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.ErrorIs(t, err, errClose, "Status must return the error of the close")
		})

		t.Run("returns the error of a watch of the views", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.watchErr = errWatch
			_, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.ErrorIs(t, err, errWatch, "Status must return the error of the watch")
		})

		t.Run("returns an error for a watch that ends before its first view", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.viewless = true
			_, err := w.client(t).Status(t.Context(), &maintenance.StatusRequest{Namespace: namespace})
			assert.HasError(t, err, "Status must refuse a watch without a view")
		})
	})
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package maintenance_test

import (
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/maintenance"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/shard"
)

// The proposals of the tests.
var (
	// requested is the ID of a proposal that a request contains.
	requested = proposal.ID{ID: id.New128([16]byte{0: 0xb7, 15: 0x01})}
	// proposed is the spec that an Apply of the tests proposes.
	proposed = config.Spec{Namespace: namespace}
	// unknown is a shard that no cell of the namespace has.
	unknown = shard.ID{ID128: id.ID128{0: 0x5d, 15: 0x09}}
)

// TestProposals checks the proposals that a client submits to the controllers of a namespace.
func TestProposals(t *testing.T) {
	t.Parallel()

	t.Run("Apply", func(t *testing.T) {
		t.Parallel()

		t.Run("submits a spec change to the leader of the root", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			_, err := w.client(t).Apply(t.Context(), &maintenance.ApplyRequest{Spec: proposed})
			assert.NoError(t, err, "Apply must submit the spec")
			assert.Length(t, w.submitted, 1, "Apply must submit one proposal")
			got := w.submitted[0]
			expect.Equal(t, got.record, w.root.Cell, "Apply must call the leader of the root")
			expect.Equal(t, got.proposal.Scope, rootScope, "the proposal must be of the root scope")
			expect.Equal(t, got.proposal.Kind, proposal.SpecChange, "the proposal must change the spec")
			expect.Equal(t, got.proposal.Base, accepted, "the base must be the version of the accepted spec")
			expect.Equal(t, got.proposal.Incarnation, inc, "the proposal must have the namespace incarnation")
			expect.Equal(t, got.proposal.Expires, origin.Add(lifetime), "the proposal must expire after the lifetime")
			expect.False(t, got.proposal.ID.IsZero(), "the proposal must have an ID")
		})

		t.Run("proposes the version after the accepted spec", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			res, err := w.client(t).Apply(t.Context(), &maintenance.ApplyRequest{Spec: proposed})
			assert.NoError(t, err, "Apply must submit the spec")
			assert.Length(t, w.submitted, 1, "Apply must submit one proposal")
			var spec config.Spec
			assert.NoError(t, json.Unmarshal(w.submitted[0].proposal.Body, &spec), "the body must be a spec")
			expect.Equal(t, spec.Version, accepted+1, "the body must propose the next version")
			expect.Equal(t, res.Version, accepted+1, "the result must have the proposed version")
		})

		t.Run("returns the receipt of the root", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			res, err := w.client(t).Apply(t.Context(), &maintenance.ApplyRequest{Spec: proposed})
			assert.NoError(t, err, "Apply must submit the spec")
			assert.Equal(t, res.Receipt, w.receipt, "Apply must return the receipt of the root")
		})

		t.Run("submits the proposal with the ID of the request", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			req := &maintenance.ApplyRequest{Spec: proposed, ID: requested}
			_, err := w.client(t).Apply(t.Context(), req)
			assert.NoError(t, err, "Apply must submit the spec")
			assert.Length(t, w.submitted, 1, "Apply must submit one proposal")
			assert.Equal(t, w.submitted[0].proposal.ID, requested, "the proposal must have the ID of the request")
		})

		t.Run("does not modify the spec of the request", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			c := w.client(t)
			req := &maintenance.ApplyRequest{Spec: proposed}
			var err error
			assert.Pure(t, func() epoch.Epoch { return req.Spec.Version }, func() {
				_, err = c.Apply(t.Context(), req)
			}, "Apply must leave the version of the spec of the request unchanged")
			assert.NoError(t, err, "Apply must submit the spec")
		})

		t.Run("returns ErrNoController for a root scope without a leader", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.root.Cell.Controller = ""
			_, err := w.client(t).Apply(t.Context(), &maintenance.ApplyRequest{Spec: proposed})
			assert.ErrorIs(t, err, maintenance.ErrNoController, "Apply must refuse a root scope without a leader")
		})

		t.Run("returns ErrNoNamespace for a namespace without an incarnation", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.root.Head.Incarnation = shard.NamespaceIncarnation{}
			_, err := w.client(t).Apply(t.Context(), &maintenance.ApplyRequest{Spec: proposed})
			assert.ErrorIs(t, err, maintenance.ErrNoNamespace, "Apply must refuse a namespace that does not exist")
		})

		t.Run("returns an error for an accepted spec that does not decode", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.root.Spec = []byte("{")
			_, err := w.client(t).Apply(t.Context(), &maintenance.ApplyRequest{Spec: proposed})
			assert.HasError(t, err, "Apply must refuse an accepted spec that does not decode")
			assert.Empty(t, w.submitted, "Apply must not submit a proposal")
		})

		t.Run("returns an error for a spec that does not encode", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			req := &maintenance.ApplyRequest{
				Spec: config.Spec{Namespace: namespace, Placement: config.Selection{Config: json.RawMessage("{")}},
			}
			_, err := w.client(t).Apply(t.Context(), req)
			assert.HasError(t, err, "Apply must refuse a spec that does not encode")
			assert.Empty(t, w.submitted, "Apply must not submit a proposal")
		})

		t.Run("returns the error of a read of the root scope", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.readFails, w.readErr = rootScope, errRead
			_, err := w.client(t).Apply(t.Context(), &maintenance.ApplyRequest{Spec: proposed})
			assert.ErrorIs(t, err, errRead, "Apply must return the error of the read")
		})

		t.Run("returns the error of the call", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.callErr = errCall
			_, err := w.client(t).Apply(t.Context(), &maintenance.ApplyRequest{Spec: proposed})
			assert.ErrorIs(t, err, errCall, "Apply must return the error of the call")
		})
	})

	t.Run("Drain", func(t *testing.T) {
		t.Parallel()

		t.Run("submits the drain of a node to the controller of its cell", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			_, err := w.client(t).Drain(t.Context(), &maintenance.DrainRequest{Namespace: namespace, Node: nodeA.ID})
			assert.NoError(t, err, "Drain must submit the drain")
			assert.Length(t, w.submitted, 1, "Drain must submit one proposal")
			got := w.submitted[0]
			expect.Equal(t, got.record, w.cell.Cell, "Drain must call the controller of the cell")
			expect.Equal(t, got.proposal.Scope, cellScope, "the proposal must be of the scope of the cell")
			expect.Equal(t, got.proposal.Kind, proposal.Drain, "the proposal must drain")
			expect.Equal(t, got.proposal.Base, nodeA.Incarnation, "the base must be the incarnation of the node")
			expect.Equal(t, got.proposal.Incarnation, inc, "the proposal must have the namespace incarnation")
			var args proposal.NodeArgs
			assert.NoError(t, json.Unmarshal(got.proposal.Body, &args), "the body must be the arguments of a node")
			expect.Equal(t, args, proposal.NodeArgs{Node: nodeA.ID}, "the body must name the node")
		})

		t.Run("returns the receipt of the controller", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			res, err := w.client(t).Drain(t.Context(), &maintenance.DrainRequest{Namespace: namespace, Node: nodeA.ID})
			assert.NoError(t, err, "Drain must submit the drain")
			assert.Equal(t, res.Receipt, w.receipt, "Drain must return the receipt of the controller")
		})

		t.Run("returns the decision of the budget", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			res, err := w.client(t).Drain(t.Context(), &maintenance.DrainRequest{Namespace: namespace, Node: nodeA.ID})
			assert.NoError(t, err, "Drain must submit the drain")
			assert.Equal(t, res.Decision, deferral, "Drain must decode the decision of the receipt")
		})

		t.Run("returns an error for a decision that does not decode", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.receipt.Result = []byte("{")
			_, err := w.client(t).Drain(t.Context(), &maintenance.DrainRequest{Namespace: namespace, Node: nodeA.ID})
			assert.HasError(t, err, "Drain must refuse a decision that does not decode")
		})

		t.Run("returns ErrNoNode for a node that no view lists", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			_, err := w.client(t).Drain(t.Context(), &maintenance.DrainRequest{Namespace: namespace, Node: gone})
			assert.ErrorIs(t, err, maintenance.ErrNoNode, "Drain must refuse a node that no view lists")
		})

		t.Run("returns ErrNoController for a cell without a controller", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.cell.Cell.Controller = ""
			_, err := w.client(t).Drain(t.Context(), &maintenance.DrainRequest{Namespace: namespace, Node: nodeA.ID})
			assert.ErrorIs(t, err, maintenance.ErrNoController, "Drain must refuse a cell without a controller")
		})

		t.Run("returns the error of a read of the root scope", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.readFails, w.readErr = rootScope, errRead
			_, err := w.client(t).Drain(t.Context(), &maintenance.DrainRequest{Namespace: namespace, Node: nodeA.ID})
			assert.ErrorIs(t, err, errRead, "Drain must return the error of the read")
		})

		t.Run("returns the error of a watch of the views", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.watchErr = errWatch
			_, err := w.client(t).Drain(t.Context(), &maintenance.DrainRequest{Namespace: namespace, Node: nodeA.ID})
			assert.ErrorIs(t, err, errWatch, "Drain must return the error of the watch")
		})

		t.Run("returns the error of a read of the scope of a cell", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.readFails, w.readErr = cellScope, errRead
			_, err := w.client(t).Drain(t.Context(), &maintenance.DrainRequest{Namespace: namespace, Node: nodeA.ID})
			assert.ErrorIs(t, err, errRead, "Drain must return the error of the read")
		})

		t.Run("returns the error of the close of a cursor", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.readFails, w.closeErr = cellScope, errClose
			_, err := w.client(t).Drain(t.Context(), &maintenance.DrainRequest{Namespace: namespace, Node: nodeA.ID})
			assert.ErrorIs(t, err, errClose, "Drain must return the error of the close")
		})

		t.Run("returns the error of the call", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.callErr = errCall
			_, err := w.client(t).Drain(t.Context(), &maintenance.DrainRequest{Namespace: namespace, Node: nodeA.ID})
			assert.ErrorIs(t, err, errCall, "Drain must return the error of the call")
		})

		t.Run("submits the proposal with the ID of the request", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			req := &maintenance.DrainRequest{Namespace: namespace, Node: nodeA.ID, ID: requested}
			_, err := w.client(t).Drain(t.Context(), req)
			assert.NoError(t, err, "Drain must submit the drain")
			assert.Length(t, w.submitted, 1, "Drain must submit one proposal")
			assert.Equal(t, w.submitted[0].proposal.ID, requested, "the proposal must have the ID of the request")
		})

		t.Run("gives each proposal an ID of its own", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			c := w.client(t)
			for range 2 {
				_, err := c.Drain(t.Context(), &maintenance.DrainRequest{Namespace: namespace, Node: nodeA.ID})
				assert.NoError(t, err, "Drain must submit the drain")
			}
			assert.Length(t, w.submitted, 2, "Drain must submit one proposal per call")
			assert.NotEqual(t, w.submitted[0].proposal.ID, w.submitted[1].proposal.ID,
				"each call must submit a proposal of its own")
		})
	})

	t.Run("Undrain", func(t *testing.T) {
		t.Parallel()

		t.Run("submits the undrain of a node to the controller of its cell", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			req := &maintenance.UndrainRequest{Namespace: namespace, Node: nodeB.ID}
			res, err := w.client(t).Undrain(t.Context(), req)
			assert.NoError(t, err, "Undrain must submit the undrain")
			assert.Length(t, w.submitted, 1, "Undrain must submit one proposal")
			got := w.submitted[0]
			expect.Equal(t, got.record, w.cell.Cell, "Undrain must call the controller of the cell")
			expect.Equal(t, got.proposal.Kind, proposal.Undrain, "the proposal must undrain")
			expect.Equal(t, got.proposal.Base, nodeB.Incarnation, "the base must be the incarnation of the node")
			expect.Equal(t, res.Receipt, w.receipt, "Undrain must return the receipt of the controller")
		})

		t.Run("returns ErrNoNode for a node that no view lists", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			_, err := w.client(t).Undrain(t.Context(), &maintenance.UndrainRequest{Namespace: namespace, Node: gone})
			assert.ErrorIs(t, err, maintenance.ErrNoNode, "Undrain must refuse a node that no view lists")
		})
	})

	t.Run("Move", func(t *testing.T) {
		t.Parallel()

		t.Run("submits the move of a shard to the controller of its cell", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			req := &maintenance.MoveRequest{Namespace: namespace, Shard: shardB, Node: nodeB.ID}
			res, err := w.client(t).Move(t.Context(), req)
			assert.NoError(t, err, "Move must submit the move")
			assert.Length(t, w.submitted, 1, "Move must submit one proposal")
			got := w.submitted[0]
			expect.Equal(t, got.record, w.cell.Cell, "Move must call the controller of the cell")
			expect.Equal(t, got.proposal.Scope, cellScope, "the proposal must be of the scope of the cell")
			expect.Equal(t, got.proposal.Kind, proposal.Move, "the proposal must move")
			expect.Equal(t, got.proposal.Base, epochB, "the base must be the epoch of the placement")
			var args proposal.MoveArgs
			assert.NoError(t, json.Unmarshal(got.proposal.Body, &args), "the body must be the arguments of a move")
			expect.Equal(t, args, proposal.MoveArgs{Node: nodeB.ID, Shard: shardB}, "the body must name the shard")
			expect.Equal(t, res.Receipt, w.receipt, "Move must return the receipt of the controller")
		})

		t.Run("returns ErrNoShard for a shard that no cell contains", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			req := &maintenance.MoveRequest{Namespace: namespace, Shard: unknown, Node: nodeB.ID}
			_, err := w.client(t).Move(t.Context(), req)
			assert.ErrorIs(t, err, maintenance.ErrNoShard, "Move must refuse a shard that no cell contains")
		})

		t.Run("returns the error of a read of the root scope", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.readFails, w.readErr = rootScope, errRead
			req := &maintenance.MoveRequest{Namespace: namespace, Shard: shardA, Node: nodeB.ID}
			_, err := w.client(t).Move(t.Context(), req)
			assert.ErrorIs(t, err, errRead, "Move must return the error of the read")
		})

		t.Run("returns the error of a read of the scope of a cell", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.readFails, w.readErr = cellScope, errRead
			req := &maintenance.MoveRequest{Namespace: namespace, Shard: shardA, Node: nodeB.ID}
			_, err := w.client(t).Move(t.Context(), req)
			assert.ErrorIs(t, err, errRead, "Move must return the error of the read")
		})

		t.Run("returns the error of the call", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.callErr = errCall
			req := &maintenance.MoveRequest{Namespace: namespace, Shard: shardA, Node: nodeB.ID}
			_, err := w.client(t).Move(t.Context(), req)
			assert.ErrorIs(t, err, errCall, "Move must return the error of the call")
		})
	})

	t.Run("Explain", func(t *testing.T) {
		t.Parallel()

		t.Run("submits the explanation of a shard to the controller of its cell", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.receipt.Result = []byte("{}")
			_, err := w.client(t).Explain(t.Context(), &maintenance.ExplainRequest{Namespace: namespace, Shard: shardA})
			assert.NoError(t, err, "Explain must submit the explanation")
			assert.Length(t, w.submitted, 1, "Explain must submit one proposal")
			got := w.submitted[0]
			expect.Equal(t, got.record, w.cell.Cell, "Explain must call the controller of the cell")
			expect.Equal(t, got.proposal.Kind, proposal.Explain, "the proposal must explain")
			expect.Equal(t, got.proposal.Base, cellGeneration, "the base must be the generation of the cell")
			var args proposal.ShardArgs
			assert.NoError(t, json.Unmarshal(got.proposal.Body, &args), "the body must be the arguments of a shard")
			expect.Equal(t, args, proposal.ShardArgs{Shard: shardA}, "the body must name the shard")
		})

		t.Run("returns the explanation of the controller", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			want := proposal.Explanation{
				Violations: []proposal.Violation{
					{Constraint: "zone", Node: nodeA.ID, Reason: "test: the zone is full", Role: shard.RolePrimary},
				},
				Costs: []float64{0.5},
			}
			result, err := json.Marshal(&want)
			assert.NoError(t, err, "the explanation of the test encodes")
			w.receipt.Result = result
			req := &maintenance.ExplainRequest{Namespace: namespace, Shard: shardA}
			res, err := w.client(t).Explain(t.Context(), req)
			assert.NoError(t, err, "Explain must return the explanation")
			expect.Equal(t, res.Explanation, want, "Explain must decode the explanation of the receipt")
			expect.Equal(t, res.Generation, w.receipt.Generation, "Explain must return the generation of the receipt")
		})

		t.Run("returns an error for a result that does not decode", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.receipt.Result = []byte("{")
			_, err := w.client(t).Explain(t.Context(), &maintenance.ExplainRequest{Namespace: namespace, Shard: shardA})
			assert.HasError(t, err, "Explain must refuse a result that does not decode")
		})

		t.Run("returns ErrNoShard for a shard that no cell contains", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			req := &maintenance.ExplainRequest{Namespace: namespace, Shard: unknown}
			_, err := w.client(t).Explain(t.Context(), req)
			assert.ErrorIs(t, err, maintenance.ErrNoShard, "Explain must refuse a shard that no cell contains")
		})

		t.Run("returns the error of the call", func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.callErr = errCall
			_, err := w.client(t).Explain(t.Context(), &maintenance.ExplainRequest{Namespace: namespace, Shard: shardA})
			assert.ErrorIs(t, err, errCall, "Explain must return the error of the call")
		})
	})
}

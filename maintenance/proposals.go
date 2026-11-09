// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package maintenance

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id/ulid"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// ApplyRequest is the request of Client.Apply.
type ApplyRequest struct {
	_ incomparable
	// Spec is the spec that the call proposes for the namespace Spec.Namespace. Apply
	// proposes it as the version after the accepted spec, whatever its Version.
	Spec config.Spec
	// ID is the ID of the proposal, or zero for a new ID.
	ID proposal.ID
}

// ApplyResult is the result of Client.Apply.
type ApplyResult struct {
	_ incomparable
	// Receipt is the receipt of the leader of the root.
	Receipt protocol.Receipt
	// Version is the version of the spec that the call proposed.
	Version epoch.Epoch
}

// DrainRequest is the request of Client.Drain.
type DrainRequest struct {
	_ incomparable
	// Namespace is the namespace of the node.
	Namespace shard.Namespace
	// Node is the node that the call drains.
	Node shard.NodeID
	// ID is the ID of the proposal, or zero for a new ID.
	ID proposal.ID
}

// DrainResult is the result of Client.Drain.
type DrainResult struct {
	_ incomparable
	// Decision is the decision of the budget of the cell for a disruption of the node. It allows
	// the disruption once no replica of the cell is on the node.
	Decision budget.Decision
	// Receipt is the receipt of the controller of the cell.
	Receipt protocol.Receipt
}

// UndrainRequest is the request of Client.Undrain.
type UndrainRequest struct {
	_ incomparable
	// Namespace is the namespace of the node.
	Namespace shard.Namespace
	// Node is the node that the call undrains.
	Node shard.NodeID
	// ID is the ID of the proposal, or zero for a new ID.
	ID proposal.ID
}

// UndrainResult is the result of Client.Undrain.
type UndrainResult struct {
	_ incomparable
	// Receipt is the receipt of the controller of the cell.
	Receipt protocol.Receipt
}

// MoveRequest is the request of Client.Move.
type MoveRequest struct {
	_ incomparable
	// Namespace is the namespace of the shard.
	Namespace shard.Namespace
	// Node is the node of the cell of the shard that receives its writer.
	Node shard.NodeID
	// Shard is the shard that the call moves.
	Shard shard.ID
	// ID is the ID of the proposal, or zero for a new ID.
	ID proposal.ID
}

// MoveResult is the result of Client.Move.
type MoveResult struct {
	_ incomparable
	// Receipt is the receipt of the controller of the cell.
	Receipt protocol.Receipt
}

// ExplainRequest is the request of Client.Explain.
type ExplainRequest struct {
	_ incomparable
	// Namespace is the namespace of the shard.
	Namespace shard.Namespace
	// Shard is the shard whose placement the call explains.
	Shard shard.ID
	// ID is the ID of the proposal, or zero for a new ID.
	ID proposal.ID
}

// ExplainResult is the result of Client.Explain.
type ExplainResult struct {
	_ incomparable
	// Explanation contains the constraints that the replicas of the shard break, and the cost
	// of each tier of the goals of the placement.
	Explanation proposal.Explanation
	// Generation is the generation of the cell at which the controller explained the placement.
	Generation epoch.Epoch
}

// located is the cell of a shard: the scope of the cell, the placement of the shard and the
// state of the cell.
type located struct {
	// scope is the scope of the cell.
	scope shard.Scope
	// placement is the placement of the shard.
	placement shard.Placement
	// state is the state of the scope of the cell.
	state store.State
}

// Apply proposes req.Spec as the next version of the spec of its namespace. It submits the
// proposal to the leader of the root, with the version of the accepted spec as the base. The
// root refuses a base that is not the version of the accepted spec, and a spec that the
// namespace cannot take.
//
// Error modes:
//   - an error that wraps ErrNoNamespace or ErrNoController.
//   - an error that wraps the error of a read or of the transport.
//   - an error for an accepted spec that does not decode, and for a spec that does not
//     encode.
func (c *Client) Apply(ctx context.Context, req *ApplyRequest) (ApplyResult, error) {
	root, _, err := c.root(ctx, req.Spec.Namespace)
	if err != nil {
		return ApplyResult{}, err
	}
	base, err := accepted(req.Spec.Namespace, &root)
	if err != nil {
		return ApplyResult{}, err
	}
	spec := req.Spec
	spec.Version = base + 1
	p := &proposal.Proposal{
		Scope: shard.Scope{Namespace: spec.Namespace},
		Base:  base,
		ID:    req.ID,
		Kind:  proposal.SpecChange,
	}
	r, err := c.submit(ctx, &root, p, &spec)
	if err != nil {
		return ApplyResult{}, err
	}
	return ApplyResult{Receipt: r, Version: spec.Version}, nil
}

// Drain adds the manual drain of the node req.Node. The controller of its cell then moves
// every replica off the node under the budget of the cell. Drain submits the proposal to the
// controller of the cell whose view lists the node, with the incarnation of the node in that
// view as the base. The controller refuses a base that is not the live incarnation of the
// node.
//
// Error modes:
//   - an error that wraps ErrNoNamespace, ErrNoNode or ErrNoController.
//   - an error that wraps the error of a read, of a watch or of the transport.
//   - an error for a watch that ends before its first view, and for a receipt whose decision
//     does not decode.
func (c *Client) Drain(ctx context.Context, req *DrainRequest) (DrainResult, error) {
	r, err := c.node(ctx, proposal.Drain, req.Namespace, req.Node, req.ID)
	if err != nil {
		return DrainResult{}, err
	}
	var d budget.Decision
	if err := json.Unmarshal(r.Result, &d); err != nil {
		return DrainResult{}, fmt.Errorf("maintenance: the decision of the drain of node %s: %w", req.Node, err)
	}
	return DrainResult{Receipt: r, Decision: d}, nil
}

// Undrain removes the manual drain of the node req.Node. A drain of a disruption of the node
// remains in effect. Undrain submits the proposal as Drain does.
//
// Error modes:
//   - an error that wraps ErrNoNamespace, ErrNoNode or ErrNoController.
//   - an error that wraps the error of a read, of a watch or of the transport.
//   - an error for a watch that ends before its first view.
func (c *Client) Undrain(ctx context.Context, req *UndrainRequest) (UndrainResult, error) {
	r, err := c.node(ctx, proposal.Undrain, req.Namespace, req.Node, req.ID)
	return UndrainResult{Receipt: r}, err
}

// Move moves the writer of the shard req.Shard to the node req.Node of its cell. It submits
// the proposal to the controller of the cell whose placements contain the shard, with the
// epoch of the placement of the shard as the base. The controller refuses a base that is not
// the epoch of the placement, a shard with a transition in flight, and a node that its view
// does not list.
//
// Error modes:
//   - an error that wraps ErrNoNamespace, ErrNoShard or ErrNoController.
//   - an error that wraps the error of a read or of the transport.
func (c *Client) Move(ctx context.Context, req *MoveRequest) (MoveResult, error) {
	l, err := c.locate(ctx, req.Namespace, req.Shard)
	if err != nil {
		return MoveResult{}, err
	}
	p := &proposal.Proposal{Scope: l.scope, Base: l.placement.Epoch, ID: req.ID, Kind: proposal.Move}
	r, err := c.submit(ctx, &l.state, p, &proposal.MoveArgs{Node: req.Node, Shard: req.Shard})
	return MoveResult{Receipt: r}, err
}

// Explain returns the constraints that the replicas of the shard req.Shard break and the
// costs of the goals of its placement. It submits the proposal to the controller of the cell
// whose placements contain the shard, with the generation of the cell as the base. The
// controller refuses a base that is not the generation of its cell, and commits nothing for
// the proposal.
//
// Error modes:
//   - an error that wraps ErrNoNamespace, ErrNoShard or ErrNoController.
//   - an error that wraps the error of a read or of the transport.
//   - an error for a receipt whose explanation does not decode.
func (c *Client) Explain(ctx context.Context, req *ExplainRequest) (ExplainResult, error) {
	l, err := c.locate(ctx, req.Namespace, req.Shard)
	if err != nil {
		return ExplainResult{}, err
	}
	p := &proposal.Proposal{Scope: l.scope, Base: l.state.Head.Generation, ID: req.ID, Kind: proposal.Explain}
	r, err := c.submit(ctx, &l.state, p, &proposal.ShardArgs{Shard: req.Shard})
	if err != nil {
		return ExplainResult{}, err
	}
	var e proposal.Explanation
	if err := json.Unmarshal(r.Result, &e); err != nil {
		return ExplainResult{}, fmt.Errorf("maintenance: the explanation of shard %v: %w", req.Shard, err)
	}
	return ExplainResult{Explanation: e, Generation: r.Generation}, nil
}

// node submits the drain or the undrain of the node n of the namespace ns, as kind states,
// with the proposal ID pid. It reads the cells from the root scope, and watches the views of
// each cell until a view lists n. The base is the incarnation of n in that view.
//
// Error modes:
//   - the error of root, view, state and submit.
//   - an error that wraps ErrNoNode when no view lists n.
func (c *Client) node(ctx context.Context, kind proposal.OperationKind, ns shard.Namespace, n shard.NodeID,
	pid proposal.ID,
) (protocol.Receipt, error) {
	root, cells, err := c.root(ctx, ns)
	if err != nil {
		return protocol.Receipt{}, err
	}
	for i := range cells {
		s := shard.Scope{Namespace: ns, Cell: shard.CellID(ulid.Format(cells[i].Shard.ID()))}
		view, err := c.view(ctx, s, root.Head.Incarnation)
		if err != nil {
			return protocol.Receipt{}, err
		}
		node, ok := view.Node(n)
		if !ok {
			continue
		}
		st, err := c.state(ctx, s, root.Head.Incarnation)
		if err != nil {
			return protocol.Receipt{}, err
		}
		p := &proposal.Proposal{Scope: s, Base: node.Incarnation, ID: pid, Kind: kind}
		return c.submit(ctx, &st, p, &proposal.NodeArgs{Node: n})
	}
	return protocol.Receipt{}, fmt.Errorf("%w: node %s of namespace %s", ErrNoNode, n, ns)
}

// locate returns the cell of the namespace ns whose placements contain the shard sid. It reads
// the cells from the root scope, and reads the placements of each cell until a cell has sid.
//
// Error modes:
//   - the error of root and of read.
//   - an error that wraps ErrNoShard when no cell has sid.
func (c *Client) locate(ctx context.Context, ns shard.Namespace, sid shard.ID) (located, error) {
	root, cells, err := c.root(ctx, ns)
	if err != nil {
		return located{}, err
	}
	for i := range cells {
		s := shard.Scope{Namespace: ns, Cell: shard.CellID(ulid.Format(cells[i].Shard.ID()))}
		st, placements, err := c.read(ctx, s, root.Head.Incarnation)
		if err != nil {
			return located{}, err
		}
		if j := slices.IndexFunc(placements, func(p shard.Placement) bool { return p.Shard == sid }); j >= 0 {
			return located{scope: s, placement: placements[j], state: st}, nil
		}
	}
	return located{}, fmt.Errorf("%w: shard %v of namespace %s", ErrNoShard, sid, ns)
}

// submit completes the proposal p and sends it to the controller of its scope, whose state is
// st. It encodes args as JSON into the body of p. It gives p the namespace incarnation of st,
// an expiry Lifetime after the time of the clock, and a new ID when p has none.
//
// Error modes:
//   - an error that wraps ErrNoController for a scope without a controller.
//   - an error that wraps the error of encoding/json for args that do not encode.
//   - an error that wraps the error of the transport.
func (c *Client) submit[A any](ctx context.Context, st *store.State, p *proposal.Proposal, args *A) (
	protocol.Receipt, error,
) {
	record, err := recordOf(st, p.Scope)
	if err != nil {
		return protocol.Receipt{}, err
	}
	body, err := json.Marshal(args)
	if err != nil {
		return protocol.Receipt{}, fmt.Errorf("maintenance: the body of the %v proposal: %w", p.Kind, err)
	}
	p.Body, p.Incarnation, p.Expires = body, st.Head.Incarnation, c.clock.Time().Add(c.lifetime)
	if p.ID.IsZero() {
		p.ID = proposal.ID{ID: c.ids.Generate()}
	}
	r, err := c.transport.Submit(ctx, record, p)
	if err != nil {
		return protocol.Receipt{}, fmt.Errorf("maintenance: submit the %v proposal %v: %w", p.Kind, p.ID, err)
	}
	return r, nil
}

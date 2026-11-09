// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package maintenance

import (
	"context"
	"fmt"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// DisruptRequest is the request of Client.Disrupt.
type DisruptRequest struct {
	_ incomparable
	// Disruption is the disruption that the call requests, of the nodes of the cell
	// Disruption.Scope. A zero ID gets a new ID, and a zero Incarnation gets the namespace
	// incarnation of the scope.
	Disruption budget.Disruption
}

// DisruptResult is the result of Client.Disrupt.
type DisruptResult struct {
	_ incomparable
	// Decision is the decision of the budget of the cell.
	Decision budget.Decision
	// ID is the ID of the disruption. Done ends its reservation.
	ID budget.ID
}

// DoneRequest is the request of Client.Done.
type DoneRequest struct {
	_ incomparable
	// Scope is the scope of the cell of the reservation.
	Scope shard.Scope
	// ID is the ID of the disruption of the reservation.
	ID budget.ID
}

// DoneResult is the result of Client.Done.
type DoneResult struct {
	_ incomparable
}

// Disrupt requests the disruption req.Disruption from the budget of its cell. It sends the
// disruption to the controller of the scope that the disruption names. The decision allows the
// disruption or defers it. A caller whose disruption the budget defers calls Disrupt again
// with the same ID after RetryAfter, and calls Done once the disruption has ended.
//
// Error modes:
//   - an error that wraps ErrNoController.
//   - an error that wraps the error of the read or of the transport, such as
//     shard.ErrIncarnation for a disruption of another namespace incarnation.
func (c *Client) Disrupt(ctx context.Context, req *DisruptRequest) (DisruptResult, error) {
	d := req.Disruption
	st, err := c.state(ctx, d.Scope, d.Incarnation)
	if err != nil {
		return DisruptResult{}, err
	}
	record, err := recordOf(&st, d.Scope)
	if err != nil {
		return DisruptResult{}, err
	}
	if d.Incarnation.IsZero() {
		d.Incarnation = st.Head.Incarnation
	}
	if d.ID.IsZero() {
		d.ID = budget.ID{ID: c.ids.Generate()}
	}
	decision, err := c.transport.Disrupt(ctx, record, &d)
	if err != nil {
		return DisruptResult{}, fmt.Errorf("maintenance: request disruption %v: %w", d.ID, err)
	}
	return DisruptResult{Decision: decision, ID: d.ID}, nil
}

// Done ends the reservation of the disruption req.ID in the cell req.Scope. It releases an
// admitted reservation, and cancels a pending one together with its drains.
//
// Error modes:
//   - an error that wraps ErrNoController.
//   - an error that wraps the error of the read or of the transport.
func (c *Client) Done(ctx context.Context, req *DoneRequest) (DoneResult, error) {
	st, err := c.state(ctx, req.Scope, shard.NamespaceIncarnation{})
	if err != nil {
		return DoneResult{}, err
	}
	record, err := recordOf(&st, req.Scope)
	if err != nil {
		return DoneResult{}, err
	}
	a := &protocol.DoneArgs{Scope: req.Scope, Incarnation: st.Head.Incarnation, ID: req.ID}
	if err := c.transport.Done(ctx, record, a); err != nil {
		return DoneResult{}, fmt.Errorf("maintenance: end the reservation of disruption %v: %w", req.ID, err)
	}
	return DoneResult{}, nil
}

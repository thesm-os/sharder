// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package local

import (
	"context"
	"fmt"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// Local is the client of one process of a Network. It implements protocol.AgentClient and
// protocol.ControllerClient, passes the metadata of the process with every call, and
// checks the metadata with Metadata.Check before it calls a server, as a transport adapter
// does. It routes a callback by the namespace of the request and the node, and every other
// call by the scope of its message. It does not check the identity of a controller,
// because its network registers none.
//
// The zero Local is not usable. Construct one with New or Network.Local.
//
// # Concurrency
//
// A Local is safe for concurrent use.
type Local struct {
	// n is the network of the servers.
	n *Network
	// md is the metadata of every call of the process.
	md protocol.Metadata
}

// The interfaces that a Local implements.
var (
	_ protocol.AgentClient      = (*Local)(nil)
	_ protocol.ControllerClient = (*Local)(nil)
)

// New returns the client of a single process with the metadata md, over a network of its
// own. The process registers its own servers on the network that Network returns, so a
// controller calls the agent of its own process without an encoding.
func New(md protocol.Metadata) *Local {
	return NewNetwork().Local(md)
}

// Network returns the network that l calls through.
func (l *Local) Network() *Network {
	return l.n
}

// Do passes r to the agent that the network contains for the namespace of r and node.ID.
//
// Error modes:
//   - the error of Metadata.Check for the metadata of l.
//   - an error that wraps protocol.ErrUnreachable without such an agent.
//   - an error that wraps the error of the agent.
func (l *Local) Do(ctx context.Context, node *shard.Node, r *protocol.Request) error {
	if err := l.md.Check(); err != nil {
		return fmt.Errorf("local: callback to %s: %w", node.ID, err)
	}
	s, ok := l.n.agentOf(r.Scope.Namespace, node.ID)
	if !ok {
		return fmt.Errorf("%w: no agent of %s in the namespace %s", protocol.ErrUnreachable, node.ID, r.Scope.Namespace)
	}
	if err := s.Do(ctx, l.md, r); err != nil {
		return fmt.Errorf("local: callback to %s: %w", node.ID, err)
	}
	return nil
}

// Report passes r to the controller that the network contains for the scope of r, and
// returns its ack.
//
// Error modes:
//   - the error of Metadata.Check for the metadata of l.
//   - an error that wraps protocol.ErrUnreachable without such a controller.
//   - an error that wraps the error of the controller.
func (l *Local) Report(ctx context.Context, _ *shard.Cell, r *protocol.Report) (protocol.Ack, error) {
	srv, err := l.controller(r.Scope)
	if err != nil {
		return protocol.Ack{}, err
	}
	ack, err := srv.Report(ctx, l.md, r)
	if err != nil {
		return protocol.Ack{}, fmt.Errorf("local: report of %s: %w", r.Node, err)
	}
	return ack, nil
}

// Submit passes p to the controller that the network contains for the scope of p, and
// returns its receipt.
//
// Error modes:
//   - the error of Metadata.Check for the metadata of l.
//   - an error that wraps protocol.ErrUnreachable without such a controller.
//   - an error that wraps the error of the controller.
func (l *Local) Submit(ctx context.Context, _ *shard.Cell, p *proposal.Proposal) (protocol.Receipt, error) {
	srv, err := l.controller(p.Scope)
	if err != nil {
		return protocol.Receipt{}, err
	}
	receipt, err := srv.Submit(ctx, l.md, p)
	if err != nil {
		return protocol.Receipt{}, fmt.Errorf("local: proposal %v: %w", p.ID, err)
	}
	return receipt, nil
}

// Disrupt passes d to the controller that the network contains for the scope of d, and
// returns its decision.
//
// Error modes:
//   - the error of Metadata.Check for the metadata of l.
//   - an error that wraps protocol.ErrUnreachable without such a controller.
//   - an error that wraps the error of the controller.
func (l *Local) Disrupt(ctx context.Context, _ *shard.Cell, d *budget.Disruption) (budget.Decision, error) {
	srv, err := l.controller(d.Scope)
	if err != nil {
		return budget.Decision{}, err
	}
	decision, err := srv.Disrupt(ctx, l.md, d)
	if err != nil {
		return budget.Decision{}, fmt.Errorf("local: disruption %v: %w", d.ID, err)
	}
	return decision, nil
}

// Done passes a to the controller that the network contains for the scope of a.
//
// Error modes:
//   - the error of Metadata.Check for the metadata of l.
//   - an error that wraps protocol.ErrUnreachable without such a controller.
//   - an error that wraps the error of the controller.
func (l *Local) Done(ctx context.Context, _ *shard.Cell, a *protocol.DoneArgs) error {
	srv, err := l.controller(a.Scope)
	if err != nil {
		return err
	}
	if err := srv.Done(ctx, l.md, a); err != nil {
		return fmt.Errorf("local: end of the reservation %v: %w", a.ID, err)
	}
	return nil
}

// controller returns the controller of the scope s for a call of l.
//
// Error modes:
//   - the error of Metadata.Check for the metadata of l.
//   - an error that wraps protocol.ErrUnreachable for a scope without a controller.
func (l *Local) controller(s shard.Scope) (protocol.ControllerServer, error) {
	if err := l.md.Check(); err != nil {
		return nil, fmt.Errorf("local: call of the controller of %v: %w", s, err)
	}
	srv, ok := l.n.controllerOf(s)
	if !ok {
		return nil, fmt.Errorf("%w: no controller of %v", protocol.ErrUnreachable, s)
	}
	return srv, nil
}

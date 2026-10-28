// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol

import (
	"context"
	"fmt"

	"go.thesmos.sh/sharder/shard"
)

// AgentServer receives callbacks: an agent receives those of the controller of its cell,
// and a controller receives those of the root of a namespace. Do calls Metadata.Check
// first, and does not modify or keep its request.
//
// # Concurrency
//
// An AgentServer is safe for concurrent use.
type AgentServer interface {
	// Do runs the callback r.
	Do(ctx context.Context, md Metadata, r *Request) error
}

// AgentClient sends callbacks to the agents of other processes. Transport adapters
// implement it.
//
// # Concurrency
//
// An AgentClient is safe for concurrent use.
//
//dokimi:stub
//dokimi:out path=protocoltest/
type AgentClient interface {
	// Do sends r to the agent of node.
	Do(ctx context.Context, node *shard.Node, r *Request) error
}

// dispatch is the AgentServer that Dispatch returns.
type dispatch struct {
	// root receives the requests for the root scopes.
	root AgentServer
	// agents maps each namespace to the agent that receives the requests for its cells.
	agents map[shard.Namespace]AgentServer
}

// Dispatch returns an AgentServer that passes a request for the root scope of a namespace
// to root, and a request for a cell to the agent that agents maps the namespace of the
// request to. A process serves the result on its one callback endpoint. Dispatch keeps
// agents, so the caller does not modify the map afterwards.
//
// The Do of the result returns the error of Metadata.Check before it passes a request,
// and an error that wraps [ErrUnreachable] for a namespace without an agent, as a
// transport does for an unreachable node.
func Dispatch(root AgentServer, agents map[shard.Namespace]AgentServer) AgentServer {
	return &dispatch{root: root, agents: agents}
}

// Do passes r to the server of its scope, and wraps the error that the server returns.
// Dispatch states which server receives r.
func (d *dispatch) Do(ctx context.Context, md Metadata, r *Request) error {
	if err := md.Check(); err != nil {
		return err
	}
	server := d.root
	if r.Scope.Cell != "" {
		agent, ok := d.agents[r.Scope.Namespace]
		if !ok {
			return fmt.Errorf("%w: no agent of the namespace %s", ErrUnreachable, r.Scope.Namespace)
		}
		server = agent
	}
	if err := server.Do(ctx, md, r); err != nil {
		return fmt.Errorf("protocol: callback for %v: %w", r.Scope, err)
	}
	return nil
}

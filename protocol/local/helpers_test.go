// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package local_test

import (
	"context"
	"errors"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// The names and the addresses of the servers of the tests.
const (
	namespace shard.Namespace = "orders"
	cell      shard.CellID    = "c1"
	nodeID    shard.NodeID    = "n1"
	// controllerA is the address of the first controller process of the tests.
	controllerA = "controller-a:7000"
	// controllerB is the address of the second controller process of the tests.
	controllerB = "controller-b:7000"
)

// The scope, the node and the metadata of the tests.
var (
	// scope is the scope of the messages of the tests.
	scope = shard.Scope{Namespace: namespace, Cell: cell}
	// node is the node of the agent of the tests.
	node = shard.Node{ID: nodeID, Cell: cell}
	// current is the metadata of a process of this release.
	current = protocol.Metadata{Version: protocol.Current}
)

// The errors that the servers of the tests return, one per server.
var (
	errFirst  = errors.New("local_test: the first server")
	errSecond = errors.New("local_test: the second server")
)

// failing is an agent and a controller that return err from every call.
type failing struct {
	err error
}

// Do returns the error of the server.
func (f failing) Do(context.Context, protocol.Metadata, *protocol.Request) error {
	return f.err
}

// Report returns the error of the server.
func (f failing) Report(context.Context, protocol.Metadata, *protocol.Report) (protocol.Ack, error) {
	return protocol.Ack{}, f.err
}

// Submit returns the error of the server.
func (f failing) Submit(context.Context, protocol.Metadata, *proposal.Proposal) (protocol.Receipt, error) {
	return protocol.Receipt{}, f.err
}

// Disrupt returns the error of the server.
func (f failing) Disrupt(context.Context, protocol.Metadata, *budget.Disruption) (budget.Decision, error) {
	return budget.Decision{}, f.err
}

// Done returns the error of the server.
func (f failing) Done(context.Context, protocol.Metadata, *protocol.DoneArgs) error {
	return f.err
}

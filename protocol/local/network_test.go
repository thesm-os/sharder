// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package local_test

import (
	"context"
	"errors"
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/local"
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

// TestNetwork checks that a later server of an address replaces the earlier one.
func TestNetwork(t *testing.T) {
	t.Parallel()

	t.Run("AddAgent", func(t *testing.T) {
		t.Parallel()

		t.Run("replaces the agent of the same namespace and node", func(t *testing.T) {
			t.Parallel()
			n := local.NewNetwork()
			n.AddAgent(namespace, nodeID, failing{err: errFirst})
			n.AddAgent(namespace, nodeID, failing{err: errSecond})
			err := n.Local(current).Do(t.Context(), &node, &protocol.Request{Scope: scope})
			assert.ErrorIs(t, err, errSecond, "the network passes a callback to the later agent")
		})

		t.Run("adds an agent for the clients that the network returned before", func(t *testing.T) {
			t.Parallel()
			n := local.NewNetwork()
			l := n.Local(current)
			r := protocol.Request{Scope: scope}
			assert.ErrorIs(t, l.Do(t.Context(), &node, &r), protocol.ErrUnreachable,
				"a callback before AddAgent returns ErrUnreachable")
			n.AddAgent(namespace, nodeID, failing{err: errFirst})
			assert.ErrorIs(t, l.Do(t.Context(), &node, &r), errFirst,
				"a callback after AddAgent returns the error of the agent")
		})
	})

	t.Run("AddController", func(t *testing.T) {
		t.Parallel()

		t.Run("replaces the controller of the same scope", func(t *testing.T) {
			t.Parallel()
			n := local.NewNetwork()
			n.AddController(scope, failing{err: errFirst})
			n.AddController(scope, failing{err: errSecond})
			_, err := n.Local(current).Report(t.Context(), nil, &protocol.Report{Scope: scope})
			assert.ErrorIs(t, err, errSecond, "the network passes a report to the later controller")
		})

		t.Run("adds a controller for the clients that the network returned before", func(t *testing.T) {
			t.Parallel()
			n := local.NewNetwork()
			l := n.Local(current)
			r := protocol.Report{Scope: scope}
			_, err := l.Report(t.Context(), nil, &r)
			assert.ErrorIs(t, err, protocol.ErrUnreachable, "a report before AddController returns ErrUnreachable")
			n.AddController(scope, failing{err: errFirst})
			_, err = l.Report(t.Context(), nil, &r)
			assert.ErrorIs(t, err, errFirst, "a report after AddController returns the error of the controller")
		})
	})
}

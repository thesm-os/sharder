// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocoltest_test

import (
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/local"
	"go.thesmos.sh/sharder/protocol/protocoltest"
)

// TestTransport runs the suite against the transport of package local.
func TestTransport(t *testing.T) {
	t.Parallel()

	t.Run("AssertTransport", func(t *testing.T) {
		t.Parallel()

		t.Run("passes the transport of package local", func(t *testing.T) {
			t.Parallel()
			protocoltest.AssertTransport(t, func(_ assert.TB, md protocol.Metadata, e *protocoltest.Endpoints) (
				protocol.AgentClient, protocol.ControllerClient,
			) {
				n := local.NewNetwork()
				n.AddAgent(e.Scope.Namespace, e.Node.ID, e.Agent)
				n.AddController(e.Scope, e.Controller)
				l := n.Local(md)
				return l, l
			})
		})
	})
}

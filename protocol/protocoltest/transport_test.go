// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocoltest_test

import (
	"testing"

	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/local"
	"go.thesmos.sh/sharder/protocol/protocoltest"
)

// controllerAddress is the address of the controller of the suite on the network of package
// local.
const controllerAddress = "controller-1:7000"

// connector connects a client of a local network of its own to the servers of a check.
type connector struct{}

// Connect returns the client of a process with the metadata md on a network with the agent
// and the controller of e, at controllerAddress.
func (connector) Connect(md protocol.Metadata, e *protocoltest.Endpoints) (
	protocol.AgentClient, protocol.ControllerClient,
) {
	n := local.NewNetwork()
	n.AddAgent(e.Scope.Namespace, e.Node.ID, e.Agent)
	e.Cell.Controller = controllerAddress
	n.AddController(controllerAddress, e.Controller)
	l := n.Local(md)
	return l, l
}

// TestTransport checks that the transport of package local passes the checks of the suite
// of protocoltest.Connector.
func TestTransport(t *testing.T) {
	t.Parallel()

	protocoltest.RunConnectorSuite(t, func() connector { return connector{} })
}

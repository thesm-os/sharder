// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package local

import (
	"sync"

	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// agent is the address of an agent on a network: its namespace and its node.
type agent struct {
	namespace shard.Namespace
	node      shard.NodeID
}

// Network connects the servers of the processes of one simulation or test by direct calls,
// without an encoding.
//
// The zero Network is not usable. Construct one with NewNetwork.
//
// # Concurrency
//
// A Network is safe for concurrent use.
type Network struct {
	// agents maps the namespace and the node of each agent to the agent.
	agents map[agent]protocol.AgentServer
	// controllers maps the scope of each controller to the controller.
	controllers map[shard.Scope]protocol.ControllerServer
	mu          sync.RWMutex
}

// NewNetwork returns a network without servers.
func NewNetwork() *Network {
	return &Network{
		agents:      make(map[agent]protocol.AgentServer),
		controllers: make(map[shard.Scope]protocol.ControllerServer),
	}
}

// AddAgent makes s the agent of node in the namespace ns for every Local of the network. A
// later call for the same namespace and node replaces s.
func (n *Network) AddAgent(ns shard.Namespace, node shard.NodeID, s protocol.AgentServer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.agents[agent{namespace: ns, node: node}] = s
}

// AddController makes srv the controller of the scope s, the root when s.Cell is empty, for
// every Local of the network. A later call for s replaces srv.
func (n *Network) AddController(s shard.Scope, srv protocol.ControllerServer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.controllers[s] = srv
}

// Local returns the client of the process whose calls have the metadata md: md.Caller is
// the identity of the process, and md.Version and md.Requires are what its calls state. A
// test gives a process another version or a feature to check that a receiver refuses
// them.
func (n *Network) Local(md protocol.Metadata) *Local {
	return &Local{n: n, md: md}
}

// agentOf returns the agent of node in the namespace ns, and reports whether the network
// has one.
func (n *Network) agentOf(ns shard.Namespace, node shard.NodeID) (protocol.AgentServer, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	s, ok := n.agents[agent{namespace: ns, node: node}]
	return s, ok
}

// controllerOf returns the controller of the scope s, and reports whether the network has
// one.
func (n *Network) controllerOf(s shard.Scope) (protocol.ControllerServer, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	srv, ok := n.controllers[s]
	return srv, ok
}

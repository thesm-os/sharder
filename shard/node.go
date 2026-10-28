// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=Node

import (
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/tag"
)

// NodeID is the identity of a node in a namespace. The node chooses it, such as the
// name of its pod, and keeps it across restarts. The empty NodeID is no node.
type NodeID string

// Identity is the identity of a process as the transport of the embedder
// authenticates it, such as a SPIFFE ID or the digest of the key of a certificate.
// The library compares identities and does not interpret them. The empty Identity is
// the identity of a caller that the transport did not authenticate.
type Identity string

// Node is the description of one live member of a cell. A published Node is never
// modified.
//
// The zero Node is no node.
//
// # Encoding
//
// kanon generates the codec of Node. ID is field 1, Incarnation is field 2, Cell is
// field 3, Address is field 4, Labels is field 5, Capacity is field 6, and Identity is
// field 7.
//
// # Concurrency
//
// A copy of a Node shares the elements of Labels and the map of Capacity with the
// original.
//
// # Allocation contract
//
// Construction does not allocate.
//
//dokimi:builder
//dokimi:out path=shardtest/
type Node struct {
	// ID is the identity of the node.
	ID NodeID
	// Cell is the cell of the node: the value of its "cell" label, or empty for the
	// only cell of a namespace.
	Cell CellID
	// Address is the address at which the node serves the callbacks of its
	// controller, in the form that the transport of the embedder dials.
	Address string
	// Identity is the identity of the node process. It is empty when the embedder
	// does not authenticate peers.
	Identity Identity
	// Capacity maps a load metric, such as "cpu" or "bytes", to the capacity of the
	// node for that metric.
	Capacity map[string]float64
	// Labels are the topology and the attributes of the node, such as "region",
	// "zone" and "host", which placement constraints and goals read.
	Labels tag.Tags
	// Incarnation increases each time the node process starts. Every callback names
	// the incarnation that it is for, and a node refuses a callback for another
	// incarnation of itself. A report or a lease of an older incarnation of the same
	// ID is stale.
	Incarnation epoch.Epoch
}

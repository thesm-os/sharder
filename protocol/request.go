// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol

//go:generate go tool kanon -type=Request,Key

import (
	"fmt"

	"go.thesmos.sh/core/crypto"
	"go.thesmos.sh/core/crypto/sha256"
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/shard"
)

// digestBuffer is the size of the buffer on the stack that Request.Digest encodes a request
// into. A request with a longer encoding allocates its buffer.
const digestBuffer = 256

// Request is one callback for one replica. The controller of the cell of the replica sends
// it, and the agent checks it before the application receives it. Its [Request.Key] and
// its [Request.Digest] identify it across retries. A request on the path [Cleanup] comes
// from the agent itself.
//
// The zero Request is no callback.
//
// # Encoding
//
// kanon generates the codec of Request. Call is field 1, Scope is field 2, Operation is
// field 3, Path is field 4, Step is field 5, Node is field 6, NodeIncarnation is field 7,
// Shard is field 8, Range is field 9, Role is field 10, Epoch is field 11, and
// ControllerEpoch is field 12. The codec returns an error that wraps [ErrUnknownCall],
// [ErrUnknownPath] or shard.ErrUnknownRole for a Call, a Path or a Role that is not Valid.
//
// # Concurrency
//
// A copy of a Request shares the bytes of Range with the original.
type Request struct {
	// Scope is the scope of the cell of the replica.
	Scope shard.Scope
	// Node is the node that the callback is for.
	Node shard.NodeID
	// Range is the range of positions of the shard.
	Range shard.Range
	// Operation is the transition of the callback. It contains the namespace incarnation
	// for which the controller acts.
	Operation shard.OperationID
	// Step is the index of the callback in its path.
	Step int
	// NodeIncarnation is the incarnation of the process of the node that the callback is
	// for.
	NodeIncarnation epoch.Epoch
	// Epoch is the fence epoch of the tenure that the callback acts on.
	Epoch epoch.Epoch
	// ControllerEpoch is the fence epoch of the controller that sends the request. A node
	// refuses a request below the highest controller epoch that it has seen. It is the
	// authority of the sender, and no part of the payload of the callback.
	ControllerEpoch epoch.Epoch
	// Shard is the shard of the replica.
	Shard shard.ID
	// Call is the callback.
	Call Call
	// Path is the path of the transition that the callback belongs to.
	Path Path
	// Role is the role that the callback gives the replica.
	Role shard.Role
}

// Key returns the idempotency key of r. No two callbacks share a key.
//
// # Allocation contract
//
// Zero alloc.
func (r *Request) Key() Key {
	return Key{
		Operation:       r.Operation,
		Path:            r.Path,
		Step:            r.Step,
		Node:            r.Node,
		NodeIncarnation: r.NodeIncarnation,
		Epoch:           r.Epoch,
	}
}

// Digest returns the digest of the payload of the callback. It hashes the encoding of r
// with ControllerEpoch set to zero with SHA-256. A new controller that sends a callback
// again under its own epoch sends the same digest, and an agent refuses a key that it has
// seen with another digest. Digest does not modify r.
//
// Error modes:
//   - an error that wraps the error of the codec when r fails to encode, such as for a Role
//     that is not Valid.
//
// # Allocation contract
//
// Zero alloc for a request whose encoding is at most 256 bytes.
func (r *Request) Digest() (crypto.Digest, error) {
	payload := *r
	payload.ControllerEpoch = 0
	var buf [digestBuffer]byte
	b, err := payload.AppendBinary(buf[:0])
	if err != nil {
		return crypto.Digest{}, fmt.Errorf("protocol: digest of the request of %v at step %d: %w",
			r.Operation, r.Step, err)
	}
	return sha256.Hasher{}.Hash(b), nil
}

// Key is the idempotency key of one callback. It contains the operation, the path and the
// step of the callback, the incarnation of the process of its node, and the epoch of its
// tenure. The operation of a cleanup is the operation of the callback that started the
// tenure, so a removal event and a replacement snapshot build the same key for one tenure.
//
// The zero Key is the key of no callback.
//
// # Encoding
//
// kanon generates the codec of Key. Operation is field 1, Path is field 2, Step is field
// 3, Node is field 4, NodeIncarnation is field 5, and Epoch is field 6.
//
// # Concurrency
//
// A Key is a comparable value, and a copy is independent of the original.
type Key struct {
	// Node is the node that the callback is for.
	Node shard.NodeID
	// Operation is the transition of the callback.
	Operation shard.OperationID
	// Step is the index of the callback in its path.
	Step int
	// NodeIncarnation is the incarnation of the process of the node.
	NodeIncarnation epoch.Epoch
	// Epoch is the fence epoch of the tenure that the callback acts on.
	Epoch epoch.Epoch
	// Path is the path of the callback.
	Path Path
}

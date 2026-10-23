// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine

import (
	"time"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/protocol"
)

// Handle is a call of the handler of the application. The runtime calls the AddShard of
// the handler for a request whose Call is protocol.Add, and its DropShard for
// protocol.Drop. It passes the result as a [Handled].
type Handle struct {
	// Request is the request of a callback, or the request of a cleanup that the agent
	// builds itself.
	Request *protocol.Request
	// Fence is the fencing token of the tenure of the request: the tenure that an add starts,
	// or the tenure that a drop ends. The agent of the root starts a tenure without a fence
	// acquisition.
	Fence fence.Token
	// Token is the token of the effect.
	Token Token
	// Timeout is the timeout of the step of the request. The runtime cancels the context
	// of the handler once the timeout has passed.
	Timeout time.Duration
}

var _ Effect = Handle{}

// effect marks Handle as an Effect.
func (Handle) effect() {}

// Handled is the result of a [Handle].
type Handled struct {
	// Err is the error of the handler, or nil.
	Err error
	// Token is the token of the Handle.
	Token Token
}

var _ Event = Handled{}

// event marks Handled as an Event.
func (Handled) event() {}

// Acquire is the acquisition of the fence of a shard through fence.Acquirer.Acquire. An
// agent acquires the fence of a writer before the AddShard of the writer. The runtime
// passes the result as an [Acquired].
type Acquire struct {
	// Fence is the fencing token of the tenure that the acquisition starts.
	Fence fence.Token
	// Token is the token of the effect.
	Token Token
	// Timeout is the longest time that the call can take.
	Timeout time.Duration
}

var _ Effect = Acquire{}

// effect marks Acquire as an Effect.
func (Acquire) effect() {}

// Acquired is the result of an [Acquire].
type Acquired struct {
	// Err is the error of fence.Acquirer.Acquire, or nil. It wraps epoch.ErrFenced when the
	// watermark of the fence key is above the epoch of the fencing token.
	Err error
	// Token is the token of the Acquire.
	Token Token
}

var _ Event = Acquired{}

// event marks Acquired as an Event.
func (Acquired) event() {}

// Revoke is the revocation of a tenure whose session has ended. The runtime calls the
// RevokeShard of a handler that has one, so the handler ends the work of the tenure. A
// handler without RevokeShard has no call to make. The runtime passes the result as a
// [Revoked].
type Revoke struct {
	// Fence is the fencing token of the tenure.
	Fence fence.Token
	// Token is the token of the effect.
	Token Token
	// Timeout is the longest time that the call can take.
	Timeout time.Duration
}

var _ Effect = Revoke{}

// effect marks Revoke as an Effect.
func (Revoke) effect() {}

// Revoked is the result of a [Revoke].
type Revoked struct {
	// Err is the error of RevokeShard, or nil.
	Err error
	// Token is the token of the Revoke.
	Token Token
}

var _ Event = Revoked{}

// event marks Revoked as an Event.
func (Revoked) event() {}

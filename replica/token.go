// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica

import (
	"context"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/shard"
)

// TokenRequest is the request of Agent.Token.
type TokenRequest struct {
	_ incomparable
	// Shard is the shard of the token.
	Shard shard.ID
}

// TokenResult is the result of Agent.Token.
type TokenResult struct {
	_ incomparable
	// Token is the fencing token of the tenure of the shard on the node.
	Token fence.Token
}

// Token returns the fencing token of the tenure of the shard req.Shard on the node. The
// application passes the token to the fence with each write of the shard. Token reads a copy
// of the tokens that the agent publishes after each change, so it does not wait for the
// agent, and it does not read ctx.
//
// Error modes:
//   - ErrNotWriter for a shard without a tenure on the node, such as a shard whose add has
//     not acquired its fence, a shard whose add failed, and a shard that the node has
//     dropped.
//   - an error that wraps epoch.ErrFenced for a tenure whose drop runs or has failed, after
//     the end of the session of the node under ownership.Stop, and after the run of the
//     agent has ended.
//
// # Concurrency
//
// Token is safe for concurrent use.
//
// # Allocation contract
//
// Zero alloc. A request in a local variable of the caller remains on the stack of the
// caller.
func (a *Agent) Token(_ context.Context, req *TokenRequest) (TokenResult, error) {
	t, err := a.machine.Token(req.Shard)
	//nolint:wrapcheck // Token returns the sentinels of the agent as they are, so errors.Is matches them without an allocation.
	return TokenResult{Token: t}, err
}

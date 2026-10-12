// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"slices"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/shard"
)

// grant is the fencing token of a tenure of the node, as Machine.Token returns it.
type grant struct {
	// token is the fencing token of the tenure.
	token fence.Token
	// revoked is true from the start of a drop or a cleanup of the tenure, and after the
	// lease of the node has passed under ownership.Stop. Token then returns an error that
	// wraps epoch.ErrFenced.
	revoked bool
}

// Token returns the fencing token of the tenure of the shard id on the node. Token reads a
// copy of the tokens that the agent publishes after each change, so it does not wait for a
// Step.
//
// Error modes:
//   - ErrNotWriter for a shard without a tenure on the node, such as a shard whose add has
//     not acquired its fence, a shard whose add failed, and a shard that the node dropped.
//   - an error that wraps epoch.ErrFenced for a tenure whose drop or cleanup runs or has
//     failed, after the end of the session of the node under ownership.Stop, and after the
//     agent has stopped.
//
// # Concurrency
//
// Token is safe for concurrent use with itself and with Step.
//
// # Allocation contract
//
// Zero alloc.
func (m *Machine) Token(id shard.ID) (fence.Token, error) {
	g, ok := m.published.Load().Get(id)
	if !ok {
		return fence.Token{}, ErrNotWriter
	}
	if g.revoked {
		return fence.Token{}, errRevoked
	}
	return g.token, nil
}

// issue makes t the fencing token of the shard sid, once the tenure has acquired its fence,
// and publishes the tokens.
func (m *Machine) issue(sid shard.ID, t *fence.Token) {
	m.grants.Set(sid, grant{token: *t})
	m.published.Store(m.grants.Clone())
}

// withdraw revokes the fencing token of the shard sid, when the node has one, and publishes
// the tokens.
func (m *Machine) withdraw(sid shard.ID) {
	if g, ok := m.grants.Get(sid); ok {
		g.revoked = true
		m.grants.Set(sid, g)
		m.published.Store(m.grants.Clone())
	}
}

// unissue removes the fencing token of the shard sid, and publishes the tokens.
func (m *Machine) unissue(sid shard.ID) {
	m.grants.Delete(sid)
	m.published.Store(m.grants.Clone())
}

// revoke revokes every fencing token of the node, and publishes the tokens.
func (m *Machine) revoke() {
	for _, sid := range slices.Collect(m.grants.Keys()) {
		m.grants.Update(sid, func(g grant, _ bool) grant {
			g.revoked = true
			return g
		})
	}
	m.published.Store(m.grants.Clone())
}

---
adr: 0002
title: Readers of the store take store.Reader
status: Accepted
date: 2026-10-10
supersedes: none
superseded-by: none
rfc: RFC-0007, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0002: Readers of the store take store.Reader

## Status

Accepted

## Context

`store.Store` has nine methods: `Head`, `Read`, `Watch`, `Writer`, `Outcome`, `Campaign`, `Limits`, `Fits` and `Close`. A controller calls all nine. The agent, the router and `sharderctl` only read:

- The agent reads and watches its node's placements and its cell's record.
- The router reads and watches the cell map and the routes of each cell.
- `sharderctl` reads the records of the controllers, the status and the history.

Each of them still takes a `store.Store`. A test double of any of them implements nine methods, and nothing stops the component from committing.

## Decision

We will declare `store.Reader` with `Head`, `Read` and `Watch`, embed it in `store.Store`, and give the agent, the router and `sharderctl` a `store.Reader`, because a component that only reads the store then cannot commit to it.

```go
// Reader reads the scopes of namespaces. Implementations are safe for concurrent use.
type Reader interface {
	Head(ctx context.Context, s shard.Scope) (Head, error)
	Read(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation, q Query, p page.Page) (State, page.Cursor[shard.Placement], error)
	Watch(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation, from epoch.Epoch, node shard.NodeID) iter.Seq2[Event, error]
}

// Store persists namespaces, one scope at a time. Implementations are safe for concurrent use.
type Store interface {
	Reader
	Writer(s shard.Scope, inc shard.NamespaceIncarnation, term epoch.Epoch) Writer
	Outcome(ctx context.Context, s shard.Scope, id CommitID) (Receipt, bool, error)
	Campaign(ctx context.Context, s shard.Scope, election, candidate string) (Leadership, error)
	Limits(s shard.Scope) Limits
	Fits(s shard.Scope, m Mutation) error
	Close() error
}
```

The contracts of `Head`, `Read` and `Watch` do not change. `replica.Config.Store` and `router.Config.Store` have the type `store.Reader`.

## Alternatives Considered

### store.Store everywhere

Every component takes `store.Store`, as the interface of the store declares, so an embedder passes one type to every component.

Rejected because the agent, the router and `sharderctl` would depend on six methods that none of them calls, and a test double of each would implement those six methods.

## Consequences

**Positive:**

- The compiler rejects a commit, a campaign or a `Close` from the agent, the router or `sharderctl`.
- A test double of a reader implements three methods.

**Negative:**

- Package `store` documents one more interface.
- A reader that needs another method of `store.Store` needs a decision that widens its type.

**Neutral:**

- An embedder passes the same `store.Store` value to every component, because every `Store` is a `Reader`.

---
adr: 0042
title: The agent revokes each tenure of the node at the end of its session under stop
status: Accepted
date: 2026-10-13
supersedes: none
superseded-by: none
rfc: RFC-0006, RFC-0011
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0042: The agent revokes each tenure of the node at the end of its session under stop

## Status

Accepted

## Context

RFC-0006 states that a node stops its writers when the deadline of its lease passes under `stop`. ADR-0026 implements the end of a session in the agent: `Token` returns an error that wraps `epoch.ErrFenced` for every tenure, and the agent joins again with a new incarnation. ADR-0026 records the consequence that the agent does not tell the application that its session ended.

RFC-0011 adds `replica.Runner`. The runner passes the token of the add to `Opener.Open`, and runs the work that `Open` returns until a drop, a newer add or the end of the agent. The end of the session is none of the three. The review of 2026-10-13 reproduced the consequence through the public API: during a store outage longer than the TTL, `Token` returns `epoch.ErrFenced`, and the work of the tenure keeps running with its copied token. The fence still admits that token until a newer tenure acquires, so the work writes through the whole outage, as under `continue-fenced`.

## Decision

We will tell the handler of the application when a tenure loses its authority under `stop`:

1. At the end of the session under `stop`, the agent queues one revocation for each tenure whose token `Token` returned until then, in the order of the shard IDs. It then revokes the tokens. Under `continue-fenced` the agent queues none.
2. A revocation runs in the turn of its shard. It waits for the work of the shard that runs, and runs before the work that waits. An add whose call of the handler runs at the end of the session gets its revocation once the call returns.
3. A revocation is the effect `machine.Revoke`, with the token of the tenure and the timeout of a drop. A runtime calls the optional `RevokeShard` of its handler, and passes the result as `machine.Revoked`. The agent logs the error of a revocation that fails, and then runs the next work of the shard.
4. Package `replica` gains the optional interface `Revoker`, which a `Handler` implements: `RevokeShard(ctx, *RevokeShardRequest) (RevokeShardResult, error)`, with the token of the tenure in the request. The agent finds it with a type assertion, as the interfaces standard requires for a new call of a released interface.
5. `Runner` implements `Revoker`. `RevokeShard` ends the work of the tenure of the token and waits until the work has returned. The runner then forgets the tenure. A revocation of an older tenure leaves a newer tenure of the shard running, because the runner compares the epochs.

A revocation is cooperative. The work ends when it honours the end of its context. The fence remains the protection against a work that does not.

## Alternatives Considered

### The runner polls Token

The runner asks `Agent.Token` for each tenure at an interval. It ends the work of a tenure whose token is revoked.

Rejected because the work then runs up to one interval past the deadline. The runner also reads every token at every interval while nothing changes.

### A context for each tenure

The driver keeps one context for each tenure and ends it at the end of the session. It passes that context to `AddShard` beside the context of the add.

Rejected because a second context parameter would change `Handler`, a released interface. The context of the add ends with the timeout of the add. The lifetime of a tenure is longer.

### A revocation outside the queue of its shard

The agent appends a `Revoke` for each tenure at the end of the session, and the runtime performs it at once.

Rejected because `RevokeShard` then runs while `AddShard` of the same tenure runs. A revocation that arrives before `Open` returns does not find the work, and the work that `Open` returns then runs under a revoked token. The handler would also lose the rule that the agent calls one method at a time for each shard.

### A new method of Handler

`Handler` gains `RevokeShard`.

Rejected because a released interface never gains a method. Every handler of an application would stop compiling.

## Consequences

**Positive:**

- The work of a `Runner` stops at the end of the session under `stop`, as RFC-0006 states.
- A handler of its own can stop its work the same way by implementing `Revoker`.
- A late revocation does not end a newer tenure of the shard.

**Negative:**

- A handler without `Revoker` still learns of the end of the session from `Token` alone.
- A work that ignores its context keeps running until it returns, and its writes fail at the fence once a newer tenure acquires.
- The next work of a shard waits until `RevokeShard` returns, as it waits for a drop.

**Neutral:**

- The closed vocabulary of the machines gains the effect `Revoke` and the event `Revoked`.
- Under `continue-fenced`, a tenure keeps its work until a drop or a cleanup ends it, as before.

## References

| What | Where |
|---|---|
| `stop` and `continue-fenced` | RFC-0006, The lease, and `ownership/lease.go` |
| The end of a session and the next join | ADR-0026, and `internal/machine/agent/session.go` |
| The runner and the token of the add | RFC-0011 |
| A new call of a released interface | `docs/standards/interfaces.md`, Growth without a break |
| The work of a tenure | `replica/runner.go` |

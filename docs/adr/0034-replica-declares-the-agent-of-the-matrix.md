---
adr: 0034
title: Package replica declares the agent of the support matrix in the form of the interfaces standard
status: Accepted
date: 2026-10-13
supersedes: none
superseded-by: none
rfc: RFC-0006, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0034: Package replica declares the agent of the support matrix in the form of the interfaces standard

## Status

Accepted

## Context

RFC-0008 declares package `replica`, which an application process embeds to host replicas:

- `Handler` with `AddShard` and `DropShard`, each with a `protocol.Request` by value.
- The optional interfaces `RoleChanger`, `Preparer`, `Stager`, `MembershipUpdater`, `Resharder`, `LoadReporter` and `Elector`.
- `Config`, and `Start(ctx, cfg Config, h Handler) (*Agent, error)`.
- `Agent` with `Token`, `Close`, `Do`, `ReportFault` and `Recovered`, and with `Elected`, `Deposed`, `Hosted` and `Released` for the `delegated` ownership mode.

The support matrix contains the `primary-only` model, exclusive ownership with a fence and `break-before-make` (RFC-0001). Of the interfaces, it needs `Handler` and `LoadReporter`. The other interfaces and the four calls of the `delegated` mode serve models, protocols and modes outside the matrix. ADR-0008, ADR-0012, ADR-0013, ADR-0015 and ADR-0017 removed such declarations from their packages.

The interfaces standard of the repository requires that a call takes a context and one request, and returns one result and an error. A field that a request or a result gains does not break a caller. RFC-0008 passes a `protocol.Request` of 248 bytes by value, and gocritic's hugeParam refuses a value parameter of 80 bytes or more. Unit 7 adopted the standard for the components, and ADR-0031 changed their constructors to take a `*Config`.

These facts of the agent machine also bear on the declarations:

- The machine stops itself when the root creates its namespace again, and at an error of the machine. RFC-0008 has no call that reports the end of the agent to the application. The controller returns such an error from `Run`.
- The machine takes the jitter of each backoff and the ID of the disruption of a `Close` from a source of randomness. The `Config` of RFC-0008 has none.
- RFC-0006 makes `Start` refuse an exclusive namespace without a fence guard. Every namespace of the support matrix is exclusive and fenced.

## Decision

We will declare in package `replica` only the agent of the support matrix, and give each of its calls the form of the interfaces standard:

- `Handler` has `AddShard(ctx, *AddShardRequest) (AddShardResult, error)` and `DropShard(ctx, *DropShardRequest) (DropShardResult, error)`. Each request contains the callback as a `*protocol.Request`. The agent hands out each request from a pool, and a handler does not keep it after it returns.
- `LoadReporter` keeps `ShardLoad(id) shard.Load`. The standard exempts an accessor that does not perform I/O and cannot fail.
- `Agent` has these calls:
  - `Token(ctx, *TokenRequest) (TokenResult, error)`. The standard exempts only an accessor that cannot fail, and `Token` fails for a shard that the node does not write. `Token` does not block or read `ctx`.
  - `ReportFault(ctx, *ReportFaultRequest) (ReportFaultResult, error)` and `Recovered(ctx, *RecoveredRequest) (RecoveredResult, error)`.
  - `Do` keeps the form of `protocol.AgentServer`, because package `protocol` is finished.
  - `Close(ctx) error` is a lifecycle method. It refuses a context without a deadline (ADR-0028).
  - `Done() <-chan struct{}` is closed once the run of the agent has ended and every goroutine of the agent has returned. `Err()` then returns the error that ended the run: the cause of the context of `Start`, or the error with which the agent stopped itself. It returns nil after a `Close`.
- `Config` has the fields of RFC-0008 that the support matrix uses. `Store` is a `store.Reader` (ADR-0002), and `Reporter` replaces `Telemetry` (ADR-0031). `Rand` is a new required field. `Guard` is required, because every namespace of the matrix is exclusive and fenced. `Trust` is not a field, because ADR-0008 removed approvals.
- The package does not declare `RoleChanger`, `Preparer`, `Stager`, `MembershipUpdater`, `Resharder`, `Elector`, `Election`, `Placed`, `Elected`, `Deposed`, `Hosted` and `Released`.

## Alternatives Considered

### The declarations of RFC-0008

`Handler` and the calls of `Agent` keep their parameters.

Rejected because a field that `protocol.Request` gains would then change every handler, and gocritic refuses a value parameter of 248 bytes.

### Wait in place of Done and Err

`Agent.Wait()` blocks until the run has ended and returns its error.

Rejected because an application selects on the end of its agent together with a signal or a context. `Done` gives it a channel for that select. `Wait` needs a goroutine of its own to take part in the select.

## Consequences

**Positive:**

- A request or a result gains a field without a change of the applications.
- An application can select on the end of its agent, such as after its namespace was created again, and start a new agent.
- `Start` refuses a missing guard at once, before it reads the namespace.

**Negative:**

- A call of the application writes a request, such as `a.Token(ctx, &replica.TokenRequest{Shard: id})`.
- `Token` takes a context that it does not read.

**Neutral:**

- RFC-0008 keeps its declarations, and this ADR replaces them for package `replica`.
- `replica` exports the sentinels that applications check as the values of the agent machine, as `os.ErrNotExist` is the value of `fs.ErrNotExist`.

## References

| What | Where |
|---|---|
| The declarations of the agent | RFC-0008, The agent |
| The form of a call | `docs/standards/interfaces.md` |
| The deadline of a Close | ADR-0028 |
| The constructors of the components | ADR-0031 |
| The machine of the agent | `internal/machine/agent` |

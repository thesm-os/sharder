---
adr: 0026
title: The agent joins its cell again with a new incarnation after its session ends
status: Accepted
date: 2026-10-12
supersedes: none
superseded-by: none
rfc: RFC-0006, RFC-0007, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0026: The agent joins its cell again with a new incarnation after its session ends

## Status

Accepted

## Context

A node has a membership session in its cell for as long as its lease lasts. The session ends when the deadline of the lease passes, such as during a store outage longer than the TTL. The controller of the cell then ends the incarnation of the node, and moves the shards of the node at new epochs.

The ownership configuration states what the writers of the node do at the deadline:

- Under `stop`, every writer of the node stops.
- Under `continue-fenced`, the writers keep their tokens, and the fence rejects their writes once a newer tenure acquires.

The design does not state what the agent does when its session ends while the agent still runs.

## Decision

We will make the agent join its cell again with a new incarnation after a backoff, because the process can then serve without a restart and the new incarnation separates the callbacks of the old session from those of the new one.

- Under `stop`, `Token` returns an error that wraps `epoch.ErrFenced` for every tenure from the end of the session on. Under `continue-fenced`, the tenures keep their tokens.
- The agent refuses a callback of the ended incarnation with `ErrStale`, and a callback of an incarnation that it has not joined yet with `ErrNotServing`.
- The tenures of the ended incarnation end through cleanups, once the controller moves their shards.
- The reports of the new incarnation start again at sequence one.

## Alternatives Considered

### A stop of the agent at the end of its session

The agent stops with an error at the end of its session. The application then restarts its process to join again.

Rejected because a store outage longer than the TTL would stop every agent of the cell, and every process would need a restart once the store returns. A new session gives the node the same new incarnation that a restart gives it.

## Consequences

**Positive:**

- A node returns to its cell after a store outage without a restart.
- The controller sees a rejoin and a restart the same way: as a new incarnation of the node.

**Negative:**

- Under `continue-fenced`, one process can run the tenures of an ended incarnation beside the tenures of its new incarnation. The fence rejects the writes of an old tenure once a newer tenure acquires.
- The agent does not tell the application that its session ended. Under `stop`, the application learns it from `Token`.

**Neutral:**

- The backoff between two joins has the bounds `RetryBase` and `RetryMax` of the agent.

## References

| What | Where |
|---|---|
| The end of a session and the next join | `internal/machine/agent/session.go`, `ended` and `joined` |
| The checks of the incarnation of a callback | `internal/machine/agent/callback.go`, `check` |

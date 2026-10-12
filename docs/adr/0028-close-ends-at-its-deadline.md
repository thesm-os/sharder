---
adr: 0028
title: The disruption of a Close ends at the deadline of the call
status: Accepted
date: 2026-10-12
supersedes: none
superseded-by: none
rfc: RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0028: The disruption of a Close ends at the deadline of the call

## Status

Accepted

## Context

During a `Close`, the agent sends a disruption of the node to the controller of the cell. The agent sends the disruption again after each `RetryAfter` of a deferral. The controller admits the disruption once the replicas of the node have moved away. When the context of the call ends first, `Close` returns the error of the context.

The budget needs the end of each disruption:

- The controller refuses a disruption whose `End` is not after its `Start`.
- A pending reservation ends when its `End` passes before the admission. The commit that ends it also removes the drain of the node.

An application calls `Close` from its shutdown hook, and the platform bounds the shutdown. Kubernetes gives a pod 30 seconds between the termination signal and a forced kill by default.

## Decision

We will end the disruption of a `Close` at the deadline of the context of the call, because the budget needs the end of each disruption and the deadline is the last time at which the node waits for its admission.

- The runtime fills the deadline of `machine.Close` from the context of the call.
- A `Close` without a deadline after now gets an error that wraps `ErrConfig`, because its disruption would have no end.
- The disruption starts at the `Close` and names the node at its incarnation. Its impact is `ProcessRestart`. It is negotiable, so the node stops only after the admission.
- When the deadline passes before the admission, the runtime replies with the error of the context. The agent then ends the session of the node without the end of the reservation, and the controller ends the reservation once the incarnation of the node has ended.
- A node without a session has no replicas to move, so its `Close` ends at once.

## Alternatives Considered

### A fixed window for a Close without a deadline

The agent ends the disruption of a `Close` without a deadline at a fixed time after its start.

Rejected because no fixed time fits every drain. A shorter window abandons a `Close` that could complete, and a longer window is a deadline that the application did not give.

### A new disruption for each window

The agent sends a disruption with a short window, and sends a new disruption with a new ID when the window passes.

Rejected because the end of each window ends the pending reservation and the drain of the node. The controller can then place replicas on the node again until the next reservation drains it.

## Consequences

**Positive:**

- The budget knows the last time at which the node stops.
- A `Close` never waits longer than its caller.

**Negative:**

- `replica.Agent.Close` refuses a context without a deadline. The design of the agent has no such error mode.

**Neutral:**

- The controller ends the pending reservation of a `Close` at its deadline, as it ends the reservation of any disruption whose window has closed.

## References

| What | Where |
|---|---|
| The disruption of a Close | `internal/machine/agent/close.go`, `close` |
| The deadline of a Close | `internal/machine/inbound.go`, `Close` |
| The end of a pending reservation | `internal/control/budget.go`, `arm` and `expire` |

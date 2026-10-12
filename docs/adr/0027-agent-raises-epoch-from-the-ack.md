---
adr: 0027
title: The agent raises its controller epoch from the ack of a report
status: Accepted
date: 2026-10-12
supersedes: none
superseded-by: none
rfc: RFC-0006, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0027: The agent raises its controller epoch from the ack of a report

## Status

Accepted

## Context

An agent keeps the highest controller epoch that it has seen, and refuses a callback with a lower one. The controller of the cell returns its fence epoch in the ack of each report.

The design states two rules for the source of that epoch:

- A node refuses a replaced controller's callbacks as soon as it has heard from the replacement in the reply to a report. The docblock of `protocol.Ack` states the same.
- The agent raises its highest controller epoch only from its watch and from a callback that passed its checks.

The agent sends each report to the controller of the record that its watch delivered. The transport checks that the controller presents the identity of that record. A callback whose controller epoch is above that record gets `ErrUnseen`, an error of class `errs.Transient`.

## Decision

We will make the agent raise its controller epoch from the ack of each accepted report and from the record on its watch, because the ack comes from the controller that the record names.

- The agent refuses the callbacks of a replaced controller with `ErrStale` from the first ack of its successor on.
- A callback of the successor gets `ErrUnseen` until the watch delivers the record of the successor.
- A callback that passes the checks has the controller epoch of the record, so a callback does not raise the epoch.

## Alternatives Considered

### The epoch from the watch alone

The agent raises its controller epoch from the record on its watch and from the callbacks that pass its checks, and ignores the epoch of the ack.

Rejected because the callbacks of a replaced controller then pass until the watch delivers the new record, and the watch can deliver it after the ack. The ack would also have a field that the agent does not read.

## Consequences

**Positive:**

- The agent refuses a replaced controller as soon as the ack or the watch has shown its successor.

**Negative:**

- Between the ack and the new record, the agent refuses the callbacks of both controllers: the old one with `ErrStale`, and the new one with `ErrUnseen`. The engine retries the transient error, so the callbacks of the new controller succeed once the record arrives.

**Neutral:**

- The epoch of an ack is trustworthy because the transport checks the identity of the controller. In an unauthenticated deployment, the transport does not check the identity of the controller for an ack or for a callback.

## References

| What | Where |
|---|---|
| The ack of a report | `internal/machine/agent/report.go`, `reported` |
| The epoch of the record | `internal/machine/agent/watch.go`, `record` |
| The checks of a callback | `internal/machine/agent/callback.go`, `check` |

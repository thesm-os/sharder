---
adr: 0050
title: Applications run the maintenance of their namespaces through package maintenance
status: Accepted
date: 2026-11-09
supersedes: RFC-0001 in part, RFC-0002 in part, RFC-0004 in part, RFC-0006 in part, RFC-0007 in part, RFC-0008 in part, RFC-0009 in part, RFC-0010 in part, RFC-0011 in part, RFC-0017 in part, ADR-0002 in part
superseded-by: none
rfc: RFC-0018
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0050: Applications run the maintenance of their namespaces through package maintenance

## Status

Accepted

## Context

sharder is a library for building distributed systems. RFC-0001 and RFC-0008 give the maintenance of a namespace to a command `sharderctl` of sharder: its status, a spec change, the drain of a node, the move of a shard and the explanation of a placement. Each system that embeds sharder has a command line of its own. A command of sharder would also need a network transport and a store that it shares with the processes.

An application that performs an operation through `protocol.ControllerClient` finds the controller of the operation and the base that the controller checks. It then builds the proposal and submits it. RFC-0010 keeps these operations out of the examples, because `sharderctl` performs them.

## Decision

We will give the applications that embed sharder package `maintenance`, as RFC-0018 designs it, because each application builds the command line of its operators on the calls of the library.

- A `maintenance.Client` has the calls `Status`, `Apply`, `Drain`, `Undrain`, `Move`, `Explain`, `Disrupt` and `Done`.
- Each call finds the controller from the records of the store, and each proposal gets its base, its ID and its expiry from the client.
- sharder does not provide a command or a program. An application that runs its controllers in processes of their own composes package `controller` in a program of its own.
- The catalogue of examples gains the kind `operation`, with one example for each kind of proposal and one for a disruption.

## Alternatives Considered

### `sharderctl`

sharder provides the command of RFC-0008.

Rejected because each system that embeds sharder provides its own command line, and the command would need a network transport and a shared store that the library does not require.

### Methods of `embedded.Process`

The process of an application runs the maintenance calls.

Rejected because an application can run its maintenance from a process that hosts no namespace, such as the process of its command line.

## Consequences

**Positive:**

- An application performs each operation with one call, and builds its command line on the calls.
- The examples show each operation of an operator.

**Negative:**

- Each application writes its own command line or console.
- The catalogue gains six programs, and each change of the maintenance calls changes them.

**Neutral:**

- The controllers, the protocol and the proposals do not change.

## References

| What | Where |
|---|---|
| The design of the maintenance calls | RFC-0018 |
| The commands that the calls replace | RFC-0008, Operations |
| The catalogue of examples | RFC-0010 |

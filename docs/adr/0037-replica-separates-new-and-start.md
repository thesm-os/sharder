---
adr: 0037
title: Package replica constructs an agent with New and starts it with Agent.Start
status: Accepted
date: 2026-10-13
supersedes: none
superseded-by: none
rfc: RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0037: Package replica constructs an agent with New and starts it with Agent.Start

## Status

Accepted

## Context

ADR-0034 declares `replica.Start(ctx, *Config, Handler) (*Agent, error)`. Start returns the agent once the node has a session in its cell and the agent has read the scope of the cell.

The controller of the cell places shards on a node once the view of the cell lists the node. Its first callbacks to the node can arrive before `Start` returns. An application process of the embedded topology serves its controller and its agent on one endpoint, and `protocol.Dispatch` passes each callback to the server of its scope. The process can put its agent into `Dispatch` only after `Start` returns. A callback of the cell that arrives first reaches `Controller.Do`, which refuses it with `ErrScope`. That error is not transient, so the controller of the cell aborts the move.

`controller.New` returns a controller that serves its calls before `Controller.Run`. The agent machine already answers a callback before the node serves with `ErrNotServing`, which classifies as `errs.Transient`.

## Decision

We will construct an agent and start it in two calls:

- `New(cfg *Config, h Handler) (*Agent, error)` checks `cfg` and `h`, and returns the agent without a goroutine or a read.
- `Agent.Start(ctx) error` reads the namespace, joins the cell of the node and reads the scope of the cell. It returns once the node has a session and the agent has read that scope. The agent then runs until `ctx` ends or a `Close` returns, as under ADR-0034. A second call of `Start` returns `ErrStarted`, which classifies as `errs.Invalid`.
- Before `Start`, `Do`, `ReportFault`, `Recovered` and `Close` return an error that wraps `ErrNotServing`. `Token` returns `ErrNotWriter`.

An application process creates its agent, serves it through `Dispatch` together with its controller, and then starts both.

## Alternatives Considered

### Start, with a late registration in Dispatch

`protocol.Dispatch` takes the agent of a namespace after the endpoint serves.

Rejected because every embedder would then order the registration and the start by hand. `controller.New` and `Controller.Run` already separate construction from the run.

### Start, and an agent of its own in each example

An example gives the controller process and the node different node IDs, and registers the agent after `Start` returns. The network retries the early callbacks.

Rejected because the gap remains for a process with one endpoint over a transport adapter.

## Consequences

**Positive:**

- A process serves its agent before the agent joins the cell, so no callback reaches the wrong server.
- `replica` and `controller` construct and run their components in the same two steps.

**Negative:**

- An application calls two functions where it called one.

**Neutral:**

- ADR-0034 keeps every other declaration of package `replica`, and this ADR replaces its `Start`.

## References

| What | Where |
|---|---|
| The declarations of the agent | ADR-0034 |
| The constructors of the components | ADR-0031 |
| One endpoint for the callbacks of a process | RFC-0008, The protocol |
| The catalogue of examples | RFC-0010 |

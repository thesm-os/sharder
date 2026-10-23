---
adr: 0044
title: The router reports the lag of the generations that its Refresh calls wait for
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

# ADR-0044: The router reports the lag of the generations that its Refresh calls wait for

## Status

Accepted

## Context

The instrument table of RFC-0008 defines `sharder.generation.lag` as the generations between the store and each agent or router. The agent sets the gauge from the generation in the ack of each report, which the controller of the cell has committed. The router has no such source. It sets the gauge to the highest generation that a Refresh of the cell asked for, less the generation that the router has applied.

The review of 2026-10-13 reproduced the gap. A router at generation 1 whose watch has not delivered 20 later commits reports a lag of zero while no Refresh waits. A Refresh that a client cancels keeps its generation in the gauge until the router applies that generation.

## Decision

We will report the lag of the router under a name that states what it measures:

1. The router reports `sharder.router.refresh.lag`, a gauge in `{generation}` per cell. Its value is the highest generation that a waiting Refresh of the cell waits for, less the generation that the router has applied, and zero without such a Refresh.
2. A Refresh that a client cancels no longer counts in the gauge.
3. The router no longer reports `sharder.generation.lag`. The agent reports it as before, so its meaning becomes the generations between the store and each agent.
4. The router does not probe the head of a scope. A server refuses a request on a stale route with the generation of its node, and the Refresh of the client then shows the lag of the router in the new gauge.

## Alternatives Considered

### Probe the head of each scope

The router reads the header of each scope of its cells at an interval, and reports the head less the applied generation.

Rejected because every router then reads the store once per cell and interval, also while nothing changes. A router whose watch stalls without a stale route serves correct routes, and the next request on a moved shard shows its lag through a redirect.

### Keep the name and change the description

`sharder.generation.lag` keeps one name for two quantities, and the description of the instrument states both.

Rejected because one name for two quantities lets a dashboard that aggregates the series of agents and routers mix them.

### Report the state of the watch and the time since progress

The router reports whether each watch is connected and the time since it applied a generation.

Rejected because a watch that delivers nothing without an error looks connected, and a quiet namespace does not apply a generation while it is fresh. An operator then cannot tell a stalled router from a fresh one.

## Consequences

**Positive:**

- Each instrument measures one quantity, and its name states which.
- A cancelled Refresh no longer inflates the lag of the router.

**Negative:**

- A router whose watch stalls does not report a lag until a client sends a Refresh.
- A dashboard that reads `sharder.generation.lag` for routers loses those series.

**Neutral:**

- The instrument table of RFC-0008 has one row more, and the row of `sharder.generation.lag` covers agents alone.

## References

| What | Where |
|---|---|
| The instruments of the agent and the router | RFC-0008, the table of instruments |
| The lag of the agent | `internal/machine/agent/report.go`, `reported` |
| The lag of the router | `internal/machine/routes/metrics.go`, and `refresh.go` |

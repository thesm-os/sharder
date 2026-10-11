---
adr: 0011
title: The churn budget limits the moves out of a node
status: Accepted
date: 2026-10-11
supersedes: none
superseded-by: none
rfc: RFC-0004
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0011: The churn budget limits the moves out of a node

## Status

Accepted

## Context

The churn budget caps the improvement of one periodic pass:

- `maxShardFraction` limits the moved shards to 9% of the cell by default.
- `perNode` limits the moves that involve one node to 2 by default, whether the node is their source or their destination.
- `perShard` limits the moves of one shard to 1 by default.

A node that joins a busy cell receives replicas from the next periodic pass. It receives at most `perNode` of them per pass. With the defaults, a node that needs 50 replicas fills in 25 passes of 30 seconds, about 12.5 minutes. An autoscaler that added the node measures most of that time as unrelieved load.

`maxNodeTransitions`, 16 by default, already bounds the transitions in flight that count against one node. A transition counts against its destination, against its fallback, and against its source while the session of the source is live.

The limit per node spreads the writer gaps of one pass over the sources. It also throttles a destination that the planner fills.

## Decision

We will apply `churn.perNode` to the moves out of a node only, because `maxNodeTransitions` already bounds the moves into a node at once.

- A node far below the mean then fills within a few passes.
- A pass still moves at most `perNode` replicas off each node.
- `maxShardFraction` and `perShard` keep their rules and defaults.

## Alternatives Considered

### The limit per node of the design

Limit the moves of one pass that involve a node in either direction, so that no node takes part in more than two moves per pass.

Rejected because a node that joins then fills at two replicas per pass. A node that needs 50 replicas takes about 12.5 minutes.

### A larger default for perNode

Keep both directions, and raise the default, such as to 16.

Rejected because a pass could then move 16 replicas off every node, eight times the writer gaps per node of the default.

## Consequences

**Positive:**

- A node that joins a cell of 20 nodes and needs 50 replicas fills in two passes, because each of the 20 sources gives up to two replicas per pass.
- The metric of an autoscaler falls within about a minute of the join.

**Negative:**

- A node that joins can receive up to `maxShardFraction` of the shards of the cell in one pass. Its application then starts many replicas in a row, 16 at a time.
- The planner counts the budget of each move against its source alone, so a source and a destination have different limits to remember.

**Neutral:**

- Repair remains outside the churn budget.

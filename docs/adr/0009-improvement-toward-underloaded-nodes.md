---
adr: 0009
title: The planner improves a cell whose least loaded node is far below the mean
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

# ADR-0009: The planner improves a cell whose least loaded node is far below the mean

## Status

Accepted

## Context

A periodic pass of the planner repairs first, and then improves the assignment unless one of two thresholds of `triggers` skips the improvement:

- The most loaded node is below `activeAbove` of its capacity, 25% by default.
- The ratio of the highest node load to the mean node load is below `minImbalance`, 1.1 by default.

Both thresholds depend on the most loaded node alone. When k empty nodes join a cell of N nodes with equal load, the mean falls to N/(N+k) of that load, and the ratio becomes 1 + k/N. A cell that grows by fewer than N/10 nodes skips the improvement. Its new nodes remain empty. Repair does not move replicas to them either, because no replica on the old nodes needs repair.

While the load of the old nodes does not move, the metric of an autoscaler that added the new nodes does not fall. The autoscaler then adds more nodes until one step crosses a tenth of the cell.

The thresholds keep a cell where no node is at risk from moving replicas. In such a cell a move has a cost and no benefit. A cell with busy old nodes and empty new nodes has nodes at risk. The ratio of the highest load to the mean does not show them.

## Decision

We will also run the improvement while the mean node load is more than `minImbalance` times the load of the least loaded node, because only the least loaded node shows the capacity that a cell leaves unused.

- The planner computes both ratios over the live nodes that accept new replicas. Repair empties a draining node, so a draining node never starts an improvement.
- `activeAbove` still skips the improvement while the most loaded node is below that fraction of its capacity. An idle cell then does not move replicas to a new node.
- Repair, the churn budget and the interval keep their rules and defaults.

## Alternatives Considered

### The thresholds of the design

Skip the improvement while the highest load is within `minImbalance` of the mean. Only an overloaded node then starts moves.

Rejected because k empty nodes raise the ratio only to 1 + k/N. A step of fewer than N/10 nodes then moves nothing.

### One improvement after each change of the membership

Run the improvement once after a node joins, whatever the thresholds report.

Rejected because one pass moves at most `churn.perNode` replicas to a node, and the default of `churn.perNode` is 2. The passes after it skip the improvement again.

## Consequences

**Positive:**

- A node that joins a busy cell receives replicas from the next periodic pass, whatever the size of the step.
- The metric of an autoscaler falls as the planner moves load to the nodes that the autoscaler adds.

**Negative:**

- A node fills at the rate of the churn budget: at most `churn.perNode` replicas per pass. With the defaults, a node that needs 50 replicas fills in 25 passes of 30 seconds, about 12.5 minutes.
- The improvement runs in more passes. With 10 replicas on 3 nodes, the counts 4, 3 and 3 start it in every pass. The ratio of the highest load to the mean starts it there already. Each such pass costs its search and applies only the moves that lower the cost of a tier.
- A node whose labels every placement constraint rejects does not receive a shard. It starts the improvement in every pass.

**Neutral:**

- CockroachDB's allocator and Kafka Cruise Control also move load toward a node that is too far below the mean.

## References

| What | Where |
|---|---|
| CockroachDB, `shouldRebalanceBasedOnThresholds`: a store above the mean rebalances when another store is underfull | https://github.com/cockroachdb/cockroach/blob/d30c905fff79ef825adc96bcc647f1872a90f2ff/pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go#L498-L534 |
| Kafka Cruise Control, `ResourceDistributionGoal`: the upper and lower balance limits around the average utilization | https://github.com/linkedin/cruise-control/blob/24a555b631b1fd134c784a33817ca7f8f89480c9/cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/analyzer/goals/ResourceDistributionGoal.java#L245-L329 |

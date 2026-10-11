---
adr: 0010
title: The budget counts only the disrupted nodes that host a replica
status: Accepted
date: 2026-10-11
supersedes: none
superseded-by: none
rfc: RFC-0002, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0010: The budget counts only the disrupted nodes that host a replica

## Status

Accepted

## Context

The disruption budget admits a negotiable disruption only while two limits remain met after it starts:

- No shard has more unavailable replicas than `maxUnavailable`.
- At most `maxConcurrentNodes` nodes are covered by admitted reservations at once. The default is one.

Before the budget admits a disruption, the core moves the replicas of the roles in `drainRoles` off its nodes. The drain starts with the first deferral, which commits a pending reservation, so the nodes of all pending disruptions drain at once. `drainRoles` defaults to the writer roles of the model, and every replica of `primary-only` is a writer. So in the support matrix, each node is empty when the budget admits its disruption.

An empty node does not make a replica unavailable. When k pods of a deployment stop at once, they drain in parallel, and `maxConcurrentNodes` then orders their exits. Each pod waits for the `Done` of the pod before it, and for its own next `Disrupt`, one `RetryAfter` later. Kubernetes kills a pod whose grace period ends during that wait, and the process does not finish its own shutdown.

The limit on nodes protects the shards whose replicas remain on a disrupted node. It also delays the nodes that host no replica, and the delay protects no shard.

## Decision

We will count an admitted reservation against `maxConcurrentNodes` only while one of its nodes hosts a replica, because a node without replicas does not make a shard unavailable.

- A node that a reservation covers is draining, and it does not accept new replicas. So a reservation whose nodes are empty at its admission never counts.
- `maxUnavailable` and the rest of the budget keep their rules.

## Alternatives Considered

### Every admitted reservation counts

Count every node of an admitted reservation, so that planned disruptions never take out more than `maxConcurrentNodes` processes at once.

Rejected because every admitted node of the support matrix is empty. The limit then delays the exit of each drained pod and protects no shard.

### A larger default for maxConcurrentNodes

Keep counting every node, and raise the default so that the pods of one step exit together.

Rejected because a namespace whose disruptions keep replicas on their nodes would then take more of those replicas down at once.

## Consequences

**Positive:**

- A drained pod exits once its own drain ends. A step of k pods needs the time of its drain and one `RetryAfter`.
- A namespace whose disruptions keep replicas on their nodes still has at most `maxConcurrentNodes` such nodes down at once.

**Negative:**

- In the support matrix, `maxConcurrentNodes` limits nothing, because every admitted node is empty.
- `maxConcurrentNodes` no longer bounds how many processes of a deployment are down at once. The platform bounds them, such as with the `maxUnavailable` of the rolling update of a Kubernetes Deployment.

**Neutral:**

- Kubernetes' cluster autoscaler also deletes empty nodes in bulk, and drains one node with pods at a time.

## References

| What | Where |
|---|---|
| Kubernetes cluster autoscaler, FAQ: one non-empty node at a time, and empty nodes in bulk | https://github.com/kubernetes/autoscaler/blob/a4f00a378b42142a3ae5c053192e7cdb10e0c03e/cluster-autoscaler/FAQ.md#L843-L847 |

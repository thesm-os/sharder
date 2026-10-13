---
rfc: 0013
title: The generation that the agent of a node has observed, for the refusal of a request
author: Roy Klopper
status: Accepted
created: 2026-10-13
updated: 2026-10-13
discussion: none
supersedes: none
superseded-by: none
produces-adr: tbd
research: none
related: RFC-0008, RFC-0010, RFC-0011
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# RFC-0013: The generation that the agent of a node has observed, for the refusal of a request

## Summary

A server refuses a request for a shard that its node does not serve with a `router.NotOwnerError`. The error contains the generation of the cell that the server has observed and the namespace incarnation of the server. `router.Do` waits for that generation before it routes the key again. The public API of an agent does not return either value. A server takes them from a router of its own process, or cannot fill them at all.

This RFC proposes `replica.Agent.Observed`. It returns the cell of the node, the generation of the scope of the cell that the agent has applied, and the namespace incarnation of the agent.

## Motivation

### The fields that a refusal needs

RFC-0008 defines the refusal: "Incarnation is the server's namespace incarnation, and Generation is the cell's generation in that incarnation that the server has observed." `router.Do` calls `Router.Refresh` with the cell, the generation and the incarnation of the error. It routes the key again once the router of the client has applied that generation. `Refresh` returns at once when the router of the client has applied the generation already.

The agent of the node checks whether the node serves a shard. `Agent.Token` returns the token of a tenure, or `ErrNotWriter` for a shard without one. `TokenResult` contains the token alone, and no other method of the agent returns a generation or an incarnation.

### The refusal of preset-actor-entities

The server of `preset-actor-entities` takes the fields from the router of its own process. It routes the key of the request, and copies the cell, the generation, the incarnation and the writer of that route into the error. This has two defects:

- A process that serves requests without routing them does not run a router. `embedded.Namespace.Route` is false for it, as RFC-0011 decided for a worker. Its server cannot fill the cell, the generation or the incarnation.
- A route contains the generation of the scope of the cell from which the router took the placement of the shard. The router of the process and the agent of the node read the scope through two watches, and each applies the generations at its own pace. When the router of the server has not applied the move, its route is the placement whose writer is the server. Its generation is one that the client has applied already. `Refresh` then returns at once, and `Do` calls the same server again with the same route until its attempts end.

We have not measured the lag between the router of a process and the agent of its node.

### Prior art

| System | The version in a request | The refusal |
|---|---|---|
| Apache Kafka 4.3.1 with KIP-951 | A fetch request contains `CurrentLeaderEpoch` | `NOT_LEADER_OR_FOLLOWER` or `FENCED_LEADER_EPOCH`. Since KIP-951 the response also contains the ID, the epoch and the endpoint of the new leader when the broker has them. The client applies that leader only when its epoch is greater than the epoch that the client knows |
| CockroachDB v26.2.7 | Each batch contains `ClientRangeInfo`: the descriptor generation and the lease sequence of the range cache of the client | `NotLeaseHolderError` with the replica that refused, the range, the lease when the replica knows one, and the descriptor of the replica. DistSender keeps the newer information by the descriptor generation and then by the lease sequence |
| Redis Cluster | None | `-MOVED <slot> <endpoint>:<port>`. The specification states that a node with a stale map can redirect to a node that redirects again |
| TiKV v8.5.8 | Each request context contains the region ID, the region epoch, the peer and the Raft term | `NotLeader` with the leader when the follower knows it, and `EpochNotMatch` with the current regions for a stale epoch |
| MongoDB r8.2.12 | The router attaches the shard version of its routing table | `StaleConfig` with the received version and the version of the shard, `vWanted`. The router moves its cache to `vWanted` and refreshes before it routes again |

The design takes these rules from them:

- The request contains the version of the route that the client took. Kafka, CockroachDB, TiKV and MongoDB do this. In sharder, the request contains the shard and the epoch of the route, and `NotOwnerError.Shard` and `NotOwnerError.Epoch` contain the same values.
- The refusal contains the version that the server has observed. MongoDB returns `vWanted`, CockroachDB the descriptor of the replica, and TiKV the current regions. In sharder, the refusal contains the generation that the agent of the node has applied.
- The new owner is optional. Kafka sends it only when the broker has it, and the client then avoids a full refresh. `router.Do` refreshes by the generation and does not read `Hint`.

## Detailed design

### Components

| Component | Package | Responsibility |
|---|---|---|
| `Agent.Observed` | `replica` | Returns the cell of the node, the generation of the scope of the cell that the agent has applied, and the namespace incarnation of the agent |
| The published observation | `internal/machine/agent` | Publishes the generation after each generation that the agent applies, and the cell and the incarnation once the node serves |

### The observation of an agent

```go
// Observed returns the cell of the node, the last generation of the scope of the cell that
// the agent has applied, and the namespace incarnation of the agent. A server copies them
// into the router.NotOwnerError of a request that it refuses.
// Observed reads the values that the agent publishes, so it does not wait for the agent,
// and it does not read ctx.
//
// Error modes:
//   - an error that wraps ErrNotServing before the agent serves and after its run has ended.
//
// # Concurrency
//
// Observed is safe for concurrent use.
//
// # Allocation contract
//
// Zero alloc. A request in a local variable of the caller remains on the stack of the
// caller.
func (a *Agent) Observed(ctx context.Context, req *ObservedRequest) (ObservedResult, error)

// ObservedRequest is the request of Agent.Observed.
type ObservedRequest struct {
	_ incomparable
}

// ObservedResult is the result of Agent.Observed.
type ObservedResult struct {
	_ incomparable
	// Cell is the cell of the node.
	Cell shard.CellID
	// Generation is the last generation of the scope of the cell that the agent has
	// applied.
	Generation epoch.Epoch
	// Incarnation is the namespace incarnation of the agent.
	Incarnation shard.NamespaceIncarnation
}
```

The machine of the agent publishes the generation in an atomic value after each generation that it applies, also after a report of progress of the watch. It publishes the cell and the incarnation once the node serves, as it publishes its tokens after each change.

### The refusal of a server

A server takes these steps for a request:

1. The request contains the shard and the epoch of the route that the client took, as `router.Route` returns them.
2. The server serves the request when its node has the tenure of the shard at that epoch: `Agent.Token` returns a token at the epoch, or the application has an activation of the tenure, as the `Opener` of `preset-actor-entities` keeps.
3. The server otherwise calls `Agent.Observed`, and returns `&router.NotOwnerError{Cell: o.Cell, Shard: shard, Epoch: epoch, Generation: o.Generation, Incarnation: o.Incarnation}`. A server whose process runs a router can set `Hint` from its route.
4. When `Observed` returns an error, the server returns that error. `router.Do` returns it without a retry, and the client retries the request itself.

`preset-actor-entities` then sends the shard and the epoch of its route with each deposit, and its server refuses through `Agent.Observed`. The server no longer routes the account.

### Invariants

- A server returns a `NotOwnerError` before it starts the request. `Observed` does not start a request.
- The generation that `Observed` returns does not decrease during the run of an agent.
- The generation of a refusal is at least the generation that the agent had applied when the server checked the tenure, because the server calls `Observed` after the check.

### Bounds

- `Observed` costs two atomic loads, and does not allocate.
- The agent stores one atomic value per generation that it applies.

### Failure handling

| Failure | Behaviour |
|---|---|
| The agent does not serve | `Observed` returns `ErrNotServing`, of class `errs.Transient`. The server returns it, and `router.Do` returns it without a retry |
| The generation of the agent is not newer than the route of the client | `Refresh` returns at once, and `Do` calls the server again. `Do` returns the refusal after its last attempt, as RFC-0008 states |
| The namespace has a new incarnation | `Refresh` returns `shard.ErrIncarnation`, and the router of the client reads the root scope again, as RFC-0008 states |

### Verdicts

| Capability | Verdict | Reason | Evidence that would reverse it |
|---|---|---|---|
| `Agent.Observed` with the cell, the generation and the incarnation | In | A server without a router cannot fill a refusal, and the route of a router of the process can have an older generation than the agent that checked the tenure | None. RFC-0008 defines the generation as the observation of the server |
| The epoch of the request in `TokenRequest`, with an error that contains the observation | Out | A server with an activation of its own serves without a call of `Token`, and the error would allocate on each refusal | A server that calls `Token` for every request and refuses often enough that a second call costs more than the allocation |
| The writer of the shard in the observation | Out | The agent removes the placement of a shard once the shard leaves the node, and `router.Do` does not read `Hint` | A client that routes by `Hint`, such as a router in `lookup` mode |
| A refusal that makes the client wait for a generation after the observation of the server | Out | RFC-0008 waits for the generation of the server. During the writer gap of a move the placement of the shard has no writer, so `Route` returns `ErrNoRoute`, `Do` returns it, and the application retries | A measured share of the requests of a planned move that fail with a `NotOwnerError` after the last attempt of `Do` |

## Alternatives considered

### The epoch of the request in Token

`TokenRequest` gains the epoch of the request. `Token` returns an error that contains the observation when the node does not have the tenure at that epoch.

Rejected because an application that keeps an activation of each tenure serves without a call of `Token`, as the server of `preset-actor-entities` does. It would call `Token` only for the error. The error would also allocate on each refusal, and `Token` does not allocate.

### The fields in TokenResult

`TokenResult` gains the cell, the generation and the incarnation.

Rejected because `Token` returns `ErrNotWriter` with a zero result for a shard without a tenure, which is the case of a refusal.

### A NotOwnerError from the agent

The agent returns a `router.NotOwnerError` itself.

Rejected because package `replica` does not import package `router`, and the error needs the shard and the epoch of the request, which the application parses.

## Drawbacks

- A server calls the agent twice for a refusal: once for the tenure, and once for the observation.
- Each request of an application protocol contains the shard and the epoch of its route.

## Open questions

None.

## Unresolved and future work

None.

## References

| What | Where |
|---|---|
| The router, `NotOwnerError` and `Do` | RFC-0008, The router |
| A router per namespace on request | RFC-0011, Verdicts |
| The interfaces standard | `docs/standards/interfaces.md` |
| Apache Kafka 4.3.1, the errors of the protocol | <https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java> |
| Apache Kafka, KIP-951 | <https://cwiki.apache.org/confluence/display/KAFKA/KIP-951%3A+Leader+discovery+optimizations+for+the+client> |
| CockroachDB v26.2.7, `NotLeaseHolderError` | <https://github.com/cockroachdb/cockroach/blob/v26.2.7/pkg/kv/kvpb/errors.proto> |
| CockroachDB v26.2.7, the range cache | <https://github.com/cockroachdb/cockroach/blob/v26.2.7/pkg/kv/kvclient/rangecache/range_cache.go> |
| Redis, the cluster specification | <https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/> |
| TiKV kvproto 07aa8c6, the errors of a region | <https://github.com/pingcap/kvproto/blob/07aa8c6a46fab0a4cd577119c249c9c8e718c553/proto/errorpb.proto> |
| MongoDB r8.2.12, the versioning protocols | <https://github.com/mongodb/mongo/blob/r8.2.12/src/mongo/db/s/README_versioning_protocols.md> |

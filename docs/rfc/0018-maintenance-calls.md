---
rfc: 0018
title: Maintenance calls for the applications that embed sharder
author: Roy Klopper
status: Accepted
created: 2026-11-09
updated: 2026-11-09
discussion: none
supersedes: RFC-0001 in part, RFC-0002 in part, RFC-0004 in part, RFC-0006 in part, RFC-0007 in part, RFC-0008 in part, RFC-0009 in part, RFC-0010 in part, RFC-0011 in part, RFC-0017 in part, ADR-0002 in part
superseded-by: none
produces-adr: ADR-0050
research: none
related: RFC-0007, RFC-0009, RFC-0012
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# RFC-0018: Maintenance calls for the applications that embed sharder

## Summary

sharder is a library for building distributed systems. An application that embeds it runs the maintenance of its namespaces through package `maintenance`:

- It reads the status of a namespace.
- It applies a spec, drains and undrains a node, moves a shard and explains the placement of a shard.
- It requests a disruption through the budget of a cell and ends its reservation.

Each application builds its own command line or console on these calls, and sharder does not provide a command. The catalogue of examples gains the kind `operation`, with one example for each kind of proposal and one for a disruption.

## Motivation

An operator changes a namespace through `protocol.ControllerClient`. Each call takes the record of the controller that applies it. Each proposal has a base that the controller checks: the version of the accepted spec, the epoch of a shard, the incarnation of a node or the generation of a cell. A drain of a node then takes these steps in the application:

1. Read the root scope, and find the cell whose view lists the node.
2. Read the record of the controller of that cell, and the incarnation of the node in its view.
3. Build the proposal with an ID, an expiry, the base and the encoded arguments, and submit it.

RFC-0001 and RFC-0008 give these steps to a command `sharderctl` of sharder. Each system that embeds sharder has a command line of its own, and a second command would duplicate it. The command would also need a network transport and a store that it shares with the processes, which a deployment of the library does not have to provide.

RFC-0010 keeps the actions of an operator out of the examples, because `sharderctl` performs them. No example shows a call of `protocol.ControllerClient`.

## Detailed design

### Components

| Component | Responsibility |
|---|---|
| `maintenance.Client` | The maintenance calls of the namespaces of one store |
| `maintenance.Config` | The store reader, the membership, the transport, the clock, the source of randomness and the lifetime of a proposal |
| The examples of the kind `operation` | One program for each maintenance call that changes a namespace |

`maintenance` is a package of the root module. It imports `budget`, `config`, `membership`, `proposal`, `protocol`, `shard` and `store`, and core's `clock`, `epoch`, `errs`, `id/ulid`, `page` and `rand`. A `Client` is safe for concurrent use when its clock and its source of randomness are.

### The calls

Each call takes a context and one request, and returns one result and an error, as the interfaces standard requires.

| Call | Does | Controller | Base |
|---|---|---|---|
| `Status` | Reads the cells of a namespace, the generation of each, the accepted spec version and the version that each cell applies, the replicas of each node, the transitions in flight, the reservations, the drains, the shards without a writer, and the proposals that each scope has applied | None. The reads go to the store and the membership | None |
| `Apply` | Proposes a spec as the next version, and returns the receipt of the root | The leader of the root, from the record of the root scope | The version of the accepted spec |
| `Drain` and `Undrain` | Add or remove the manual drain of a node. `Drain` returns the decision of the budget for a disruption of the node, which the receipt of the controller contains | The controller of the cell whose view lists the node | The incarnation of the node in that view |
| `Move` | Moves the writer of a shard to a node of its cell | The controller of the cell whose range contains the shard | The epoch of the placement of the shard |
| `Explain` | Returns the constraints and the costs of the goals that placed a shard, as a `proposal.Explanation` | The controller of the cell of the shard | The generation of the cell |
| `Disrupt` and `Done` | Request a disruption of the nodes of a cell, and end its reservation | The controller of the cell that the disruption names | None |

- A proposal gets an ID from core's `ulid` and expires `Config.Lifetime` after the time of `Config.Clock`.
- A request may contain the ID of its proposal. An application that retries a call with the same ID submits the same proposal, and the controller applies it once.
- A call returns an error that wraps the error of the transport, so the caller classifies a refusal with `errs.Classify`, as `protocol.CallError` keeps the class of the error of the controller.
- The client does not retry a call. A caller that retries reads the status again, and names the same proposal ID.

The client returns these errors of its own:

- `ErrConfig`, of class `errs.Invalid`, from `New` for a config without a store, a membership, a transport, a clock or a source of randomness, or with a lifetime that is not positive.
- `ErrNoNamespace`, of class `errs.NotFound`, for a namespace whose root scope has no namespace incarnation, because the root has not created the namespace.
- `ErrNoController`, of class `errs.NotFound`, for a scope whose record does not contain a controller.
- `ErrNoNode` and `ErrNoShard`, of class `errs.NotFound`, for a node that is not in a view of the namespace and a shard that is not in a cell.

### The examples

- The catalogue adds the kind `operation`. `Keys` returns `operation-<kind>` for each kind that `proposal.OperationKindValues` returns, and `operation-disruption` for the budget of a cell.
- The catalogue then has six more examples: `operation-disruption`, `operation-drain`, `operation-explain`, `operation-move`, `operation-spec` and `operation-undrain`.
- Each example starts the processes of a namespace over `store/memory` and `protocol/local`, as the examples of the presets do. It runs its operation through a `maintenance.Client`, and writes the status before and after the operation.
- The other rules of RFC-0010 hold. Each example is one program in the directory of its key, `README.md` lists every example, and the test of the catalogue derives the keys from their authorities.

### Superseded parts

| Record | Part | Replaced by |
|---|---|---|
| RFC-0001 | The row of the module `go.thesmos.sh/sharder/cmd` in the table of modules, the sentence "Operators use the `sharderctl` command and the metrics", and `sharderd` in the topology `service` of the support matrix, of the table of topologies and of the alternative of a standalone service | sharder does not provide a command or a program. Operators use the maintenance calls through their application, and the metrics. The controllers of the topology `service` run in a program of the application that composes package `controller` |
| RFC-0002 | The sentences on `sharderctl`: "`sharderctl` changes it", of the accepted spec, the warnings that `sharderctl apply` prints, the steps of `sharderctl apply` and `sharderctl status` in the change of a running namespace, the commands `sign`, `submit` and `anchor` of the approvals, the signatures of operators, and the change-management workflow in front of `sharderctl` | `Apply`, which returns the receipt of the root, and `Status`. Approvals are outside the support matrix, and no maintenance call signs a proposal. A change-management workflow runs in front of the command line of the application |
| RFC-0004 | "`sharderctl move` and `drain` each become a plan that the core validates the same way" | `Move` and `Drain` |
| RFC-0006 | "so that routers and `sharderctl` see it", of a writer that an agent reports in the `delegated` mode, and "`sharderctl status` reports such a namespace as best effort", of `exclusive` without a fence | The routers and the maintenance calls see it. No call reports a namespace as best effort, because every namespace of the support matrix needs the capability `fenced` |
| RFC-0007 | The sentences on `sharderctl`: the manual drain that `sharderctl drain` adds, the report of an applied proposal from `State.Applied`, and the read of the record of a controller. The `sharderd` processes in the docblock of the topology `Service` | The proposals of `Drain` and `Undrain`, the applied proposals of each scope in the result of `Status`, and the record that each call reads to find its controller. The controllers of the topology `service` run in a program of the application |
| RFC-0008 | The sentence on `sharderctl` in the summary of the operations, the section Operations, the credentials of `sharderctl` in the rules of trust, the alternative of operations through the store, and `sharderd` as the embedder of the topology `service` and in the unresolved work | The maintenance calls. The client of package `maintenance` reads the store with read credentials. The program of the application that runs the controllers of the topology `service` is their embedder |
| RFC-0009 | The command `sharderctl cell split`, and the `sharderd` process as a node of the root in the topology `service` | No call, because a cell split is outside the support matrix. A node of the root in the topology `service` is a process of the application that runs controllers |
| RFC-0010 | The verdict Out on one example per action of an operator, and the kinds `preset` and `topology` as the only kinds | The kind `operation` |
| RFC-0011 | `sharderd` in the topology `service` of the unresolved work | A program of the application that composes package `controller` |
| RFC-0017 | The verdict Out on the report of `sharderctl` in the simulation | The verdict Out on the maintenance calls in the simulation, because they are not a machine. The specs of `maintenance` check the applied proposals in the result of `Status` |
| ADR-0002 | `sharderctl` among the readers of the store | The client of package `maintenance` |

### Verdicts

| Candidate | Verdict | Reason | Evidence that would reverse it |
|---|---|---|---|
| A command `sharderctl` | Out | sharder is a library, and each system that embeds it has a command line of its own | A consumer of sharder without a command line of its own |
| A program `sharderd` for the topology `service` | Out | sharder is a library, and the topology `service` is outside the support matrix. An application that runs its controllers in processes of their own composes package `controller` in a program of its own | A consumer of sharder that cannot build a program on package `controller` |
| A call that waits for the end of a drain | Out | `Disrupt` with a deadline waits for its admission, which the budget gives once the nodes host no replica (RFC-0012) | A drain that must wait without a reservation of the budget |
| A call of the history of a cell | Out | `store.Reader.Watch` yields the generations of a scope with their mutations, so the call would rename it | A history that needs a read that `Watch` does not have |
| Retries inside the client | Out | The caller chooses its retries, and a retry with the same proposal ID is safe | A call whose retry needs state that only the client has |
| `Status` through a controller | Out | The store and the membership have every fact of the status, and a read of them needs no controller | A fact of the status that only a controller has |
| Calls of a reshard, of a cell split and of signed proposals | Out | The support matrix has no kind of proposal for them, and a controller refuses each with an error of class `errs.Unsupported` (RFC-0008, RFC-0009) | A kind of proposal in the support matrix for one of them |

## Alternatives considered

### Methods of `embedded.Process`

The process of an application runs the maintenance calls of its namespaces.

Rejected because an application can run its maintenance from a process that hosts no namespace, such as the process of its command line. That process has a store reader, a membership and a transport, and no embedded process.

### `sharderctl` as RFC-0008 designs it

sharder provides the command of RFC-0008.

Rejected because each system that embeds sharder provides its own command line, and the command of sharder would need a network transport and a shared store that the library does not require.

## Drawbacks

- Each application writes its own command line or console for its operators.
- The catalogue gains six programs, and each change of the maintenance calls changes them.

## Open questions

None.

## References

| What | Where |
|---|---|
| The commands that the maintenance calls replace | RFC-0008, Operations |
| The calls of a controller and their protocol | RFC-0008, The protocol |
| A Disrupt that waits for its admission | RFC-0012 |
| The catalogue of examples | RFC-0010 |
| The kinds of proposals | `proposal/kind.go` |
| The drain of kubectl over the drain helper of its library | <https://github.com/kubernetes/kubectl/blob/master/pkg/drain/drain.go> |
| The maintenance calls of the Go client of etcd, on which its command line builds | <https://github.com/etcd-io/etcd/blob/main/client/v3/maintenance.go> |

---
adr: 0051
title: sharder does not provide an etcd adapter
status: Accepted
date: 2026-11-09
supersedes: RFC-0001 in part, RFC-0007 in part, RFC-0009 in part, RFC-0017 in part, ADR-0001 in part
superseded-by: none
rfc: none
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0051: sharder does not provide an etcd adapter

## Status

Accepted

## Context

sharder is a library for building distributed systems, such as a datastore like etcd. The store of sharder is an interface, and an adapter implements it over a backend. The design of the store proposes one adapter, a module over an etcd cluster. The support matrix lists that adapter as the store of a deployment. Its feasibility check against a real etcd cluster sets the supported limits of a cell, and the conformance kits of the store and the membership list it as a subject. The adapter has no code.

A system built on sharder that runs over that adapter operates an etcd cluster beside itself. A datastore built on sharder would then depend on a second datastore. Designs in draft propose stores without etcd: a file store for the scopes of one process, and a consensus group for the root that replicates the scopes of the cells.

## Decision

We will not provide an etcd adapter, because sharder is a library for building systems such as etcd, and an adapter over etcd makes each of those systems operate an etcd cluster beside itself.

- The support matrix lists `store/memory` in tests and simulations only. It has no store of a deployment, and no accepted design proposes one.
- A store of a deployment passes `store/storetest` and `membership/membershiptest`. These cases run against its backend: a commit whose reply is lost after it applies, two controllers that race to acquire one scope, a watch whose history is compacted during a paged snapshot, and every supported transaction shape at the limits of the backend.
- The measurements of that store set the supported limits of a cell.
- The designs and the research keep etcd as prior art: its limits, its benchmarks, its election and its client.

This decision supersedes these parts:

| Record | Part | Replaced by |
|---|---|---|
| RFC-0001 | "Adapter modules implement the store and the membership over etcd", of the summary, and the etcd client among the dependencies of the adapters | Adapter modules implement the store and the membership over the backend of a deployment |
| RFC-0001 | The etcd adapter in the store row of the support matrix | `store/memory` in tests and simulations only |
| RFC-0001 | The row of the module `go.thesmos.sh/sharder/etcd` in the table of modules | No module |
| RFC-0001 | The feasibility check of the etcd adapter against a real etcd cluster, in the evidence of the properties and in the verification | The same four cases against the backend of a store of a deployment |
| RFC-0001 | The etcd store and the etcd client in the alternative of adapters as packages of the root module | The store adapters and the clients of their backends |
| RFC-0007 | The etcd adapter in the support matrix of the summary, and its row in the table of the implementations of the membership | `store/memory` in tests and simulations only |
| RFC-0007 | The feasibility check of the etcd adapter, its run of the conformance suite against a real etcd cluster, and the default limits of etcd in the failure of a mutation that does not fit | The checks of a store of a deployment against its backend |
| RFC-0007 | The section "A backend that meets the contract" | No adapter. A store of a deployment meets the same requirements with the features of its backend |
| RFC-0007 | The operation of the etcd adapter in the drawbacks, the commits of about 30 replica moves with the etcd adapter, and the etcd module among the implementations to keep correct | The operation of the backend of a deployment, one commit per `Limits.MaxPlacements` placements, and the store and the membership of a deployment |
| RFC-0009 | The feasibility measurements of the etcd adapter, from which the supported limits of a cell follow | The measurements of a store of a deployment |
| RFC-0017 | The client of an etcd store, which gets the end of the term of a root from the lease of its election keys | The client of a store of a deployment |
| ADR-0001 | The etcd adapter among the implementations of `store.Store` and `membership.Membership`, and among the subjects of `store/storetest` and `membership/membershiptest` | A store of a deployment |

## Alternatives Considered

### An etcd adapter

A module `go.thesmos.sh/sharder/etcd` implements the store and the membership over the etcd v3 client. Transactions with compares make the conditional commits, a watch from a revision makes the change feed, `concurrency.Election` grants the terms of a root, and leases end the sessions of the nodes.

Rejected because a system built on sharder, such as a datastore, would then operate an etcd cluster beside itself, and sharder exists to build such systems.

## Consequences

**Positive:**

- A system built on sharder runs without an etcd cluster.
- The modules of sharder do not depend on the etcd client, so its requirements do not enter the module graph of an application.

**Negative:**

- The support matrix has no store of a deployment, so it contains no deployment of a namespace.
- The supported limits of a cell have no measurement, because no store of a deployment exists.
- An application that already operates etcd writes its own adapter, and runs `store/storetest` and `membership/membershiptest` against it.

**Neutral:**

- The code does not change, because the adapter had no code.

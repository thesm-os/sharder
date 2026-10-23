<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# Roadmap

What we are building, in what order, and what has to be true first. Milestone numbers are permanent. The order is the order of this table, and it changes.

| Order | Milestone | Status | Depends on | Released in |
|---|---|---|---|---|
| 1 | [0001](0001-exclusive-fenced-shards.md) Exclusive fenced shards in one cell | Planned | none | unscheduled |
| 2 | [0002](0002-delegated-ownership.md) Delegated ownership for consensus applications | Planned | 0001 | unscheduled |
| — | [0003](0003-replication-models.md) Replication models beyond primary-only | Deferred | 0001 | unscheduled |
| — | [0004](0004-resharding.md) Shards that split and merge | Deferred | 0001 | unscheduled |
| — | [0005](0005-multiple-cells.md) Namespaces of more than one cell | Deferred | 0001 | unscheduled |

## Authority architecture and downstream integrations

The direction in draft RFC-0015 replaces the old assumed etcd dependency with an external consensus root and replicated cell scopes. RFC-0014 supplies a local file-store proposal. Both RFCs remain Draft: this roadmap does not accept their protocols or claim an available backend. Milestone 0001 first records the architecture decision, selects the external root-quorum library and passes the backend and cross-process acceptance gates. Historical etcd tests remain useful scenarios, not a release dependency.

Theke v2's default replicated design uses a separate primary-backup application adapter: a consensus configuration master, all-counted data commits, fenced configuration activation and committed-cut reads. Local theke fences are not the transactional Guard of milestone 0001. Milestone 0003 must specify and test this capability before the integration is advertised. Milestone 0002 is the alternative for applications whose own consensus authority elects and commits data. Neither a control-store implementation nor a replica-count setting provides the application adapter automatically.

| Consumer requirement | Gate |
|---|---|
| Exclusive writer over an authoritative transactional store | 0001 and that store's fence adapter |
| Application consensus authority chooses writers | 0002 and its effective-membership adapter |
| Theke all-counted primary-backup data groups | 0003's authority, publication and configuration-transition gates |
| Graceful copy, drain, stable cut and activation | 0003's planned-move gates; no claim from routing changes alone |

All gates are pending implementation evidence. Deferred means the design remains in scope but its release is not scheduled.

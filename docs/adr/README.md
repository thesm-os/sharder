<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# Decision records

Each ADR records one decision and what it cost. Once an ADR is accepted, do not change its argument. A later decision supersedes it, and both records link to each other.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-conformance-kits-per-contract.md) | Conformance kits for every contract with more than one implementation | Accepted |
| [0002](0002-store-reader-for-readers.md) | Readers of the store take store.Reader | Accepted |
| [0003](0003-implementations-below-contracts.md) | Built-in implementations are subpackages of their contracts | Accepted |
| [0004](0004-proposals-in-their-own-package.md) | Proposals and approvals form package proposal | Superseded by [0008](0008-declarations-of-the-support-matrix.md) |
| [0005](0005-core-assigns-shard-ids.md) | The core assigns shard IDs from core's identifiers | Accepted |
| [0006](0006-roles-and-statuses-are-enumerations.md) | Roles and statuses are closed enumerations | Accepted |
| [0007](0007-partitioners-return-spans.md) | Partitioners divide a range into spans, and the core assigns their IDs | Accepted |
| [0008](0008-declarations-of-the-support-matrix.md) | Replication, ownership, fence and proposal declare only the capabilities of the support matrix | Accepted |
| [0009](0009-improvement-toward-underloaded-nodes.md) | The planner improves a cell whose least loaded node is far below the mean | Accepted |
| [0010](0010-budget-counts-nodes-with-replicas.md) | The budget counts only the disrupted nodes that host a replica | Accepted |
| [0011](0011-churn-limits-moves-out-of-a-node.md) | The churn budget limits the moves out of a node | Accepted |
| [0012](0012-config-and-budget-declare-the-matrix.md) | Config and budget declare only the capabilities of the support matrix | Accepted |
| [0013](0013-store-membership-protocol-declare-the-matrix.md) | Store, membership and protocol declare only the capabilities of the support matrix | Accepted |
| [0014](0014-cell-ids-are-root-shard-ids.md) | The ID of a cell is the canonical text of its shard ID in the root | Accepted |
| [0015](0015-placement-declares-the-planner-of-the-matrix.md) | Placement declares only the planner of the support matrix, with sparse plans and a concrete state | Accepted |
| [0016](0016-improvement-weighs-every-tier-at-once.md) | The improvement weighs every tier at once and examines a sample of the shards of each node | Accepted |
| [0017](0017-handoff-steps-on-the-sides-of-a-transfer.md) | Handoff declares only the protocol of the support matrix, with steps on the sides of a transfer | Accepted |

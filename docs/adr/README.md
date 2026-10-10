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

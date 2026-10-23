<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# RFCs

Each RFC proposes one part of the design of `sharder` and records the alternatives it rejected.

| # | Title | Status |
|---|---|---|
| [0001](0001-architecture-and-core-model.md) | Pluggable sharding strategies over a fixed correctness core | Accepted |
| [0002](0002-configuration-and-strategy-registry.md) | Namespace specs, the strategy registry and validation | Accepted |
| [0003](0003-partitioning-strategies.md) | Partitioning strategies and resharding | Accepted |
| [0004](0004-placement-and-rebalancing.md) | Placement and rebalancing strategies | Accepted |
| [0005](0005-replica-state-models.md) | Replica state models and replica counts | Accepted |
| [0006](0006-ownership-handoff-and-fencing.md) | Ownership modes, handoff protocols and fencing | Accepted |
| [0007](0007-membership-store-and-distribution.md) | Membership, the assignment store and assignment distribution | Accepted |
| [0008](0008-agent-router-and-budgets.md) | The replica agent, the router, disruption budgets and the protocol | Accepted |
| [0009](0009-horizontal-scaling-with-cells.md) | Horizontal scaling with cells and a root controller | Accepted |
| [0010](0010-keyed-examples.md) | A catalogue of examples keyed by registered names | Accepted |
| [0011](0011-embedded-process-and-tenure-runner.md) | An embedded process, a runner of tenures and a pass after a refused move | Accepted |
| [0012](0012-prompt-close.md) | A Disrupt that waits for its admission, and a Close that ends the root sessions of a controller | Accepted |
| [0013](0013-observed-generation-of-a-node.md) | The generation that the agent of a node has observed, for the refusal of a request | Accepted |
| [0014](0014-filesystem-store.md) | A filesystem store for the scopes of one process | Draft |
| [0015](0015-consensus-only-for-the-root.md) | Consensus only for the root, and cell scopes that the root replicates | Draft |

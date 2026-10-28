<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# Decision records

Each ADR records one decision and what it cost. Once an ADR is accepted, do not change its argument. A later decision supersedes it, and both records link to each other.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-conformance-kits-per-contract.md) | Conformance kits for every contract with more than one implementation | Superseded in part by [0045](0045-dokimi-generates-the-test-code.md) |
| [0002](0002-store-reader-for-readers.md) | Readers of the store take store.Reader | Accepted |
| [0003](0003-implementations-below-contracts.md) | Built-in implementations are subpackages of their contracts | Accepted |
| [0004](0004-proposals-in-their-own-package.md) | Proposals and approvals form package proposal | Superseded by [0008](0008-declarations-of-the-support-matrix.md) |
| [0005](0005-core-assigns-shard-ids.md) | The core assigns shard IDs from core's identifiers | Superseded by [0018](0018-shard-ids-are-128-bit-identifiers.md) |
| [0006](0006-roles-and-statuses-are-enumerations.md) | Roles and statuses are closed enumerations | Superseded in part by [0045](0045-dokimi-generates-the-test-code.md) |
| [0007](0007-partitioners-return-spans.md) | Partitioners divide a range into spans, and the core assigns their IDs | Accepted |
| [0008](0008-declarations-of-the-support-matrix.md) | Replication, ownership, fence and proposal declare only the capabilities of the support matrix | Accepted |
| [0009](0009-improvement-toward-underloaded-nodes.md) | The planner improves a cell whose least loaded node is far below the mean | Accepted |
| [0010](0010-budget-counts-nodes-with-replicas.md) | The budget counts only the disrupted nodes that host a replica | Accepted |
| [0011](0011-churn-limits-moves-out-of-a-node.md) | The churn budget limits the moves out of a node | Accepted |
| [0012](0012-config-and-budget-declare-the-matrix.md) | Config and budget declare only the capabilities of the support matrix | Superseded in part by [0045](0045-dokimi-generates-the-test-code.md) |
| [0013](0013-store-membership-protocol-declare-the-matrix.md) | Store, membership and protocol declare only the capabilities of the support matrix | Superseded in part by [0045](0045-dokimi-generates-the-test-code.md) |
| [0014](0014-cell-ids-are-root-shard-ids.md) | The ID of a cell is the canonical text of its shard ID in the root | Accepted |
| [0015](0015-placement-declares-the-planner-of-the-matrix.md) | Placement declares only the planner of the support matrix, with sparse plans and a concrete state | Superseded in part by [0045](0045-dokimi-generates-the-test-code.md) |
| [0016](0016-improvement-weighs-every-tier-at-once.md) | The improvement weighs every tier at once and examines a sample of the shards of each node | Accepted |
| [0017](0017-handoff-steps-on-the-sides-of-a-transfer.md) | Handoff declares only the protocol of the support matrix, with steps on the sides of a transfer | Accepted |
| [0018](0018-shard-ids-are-128-bit-identifiers.md) | The core assigns shard IDs as 128-bit identifiers of core | Accepted |
| [0019](0019-builtin-registers-with-a-constructor-per-factory.md) | Builtin registers the support matrix with Register and one constructor per factory | Accepted |
| [0020](0020-machines-return-effects-with-tokens.md) | Machines take events and return effects through a closed vocabulary with tokens | Accepted |
| [0021](0021-engine-bounds-callbacks-per-node.md) | The transition engine bounds the callbacks to each node with a queue of its own | Accepted |
| [0022](0022-engine-commits-one-batch-at-a-time.md) | The transition engine commits its changes with one batch in flight | Accepted |
| [0023](0023-membership-ends-node-incarnations.md) | The transition engine takes the end of a node incarnation from the membership view | Accepted |
| [0024](0024-controller-makes-the-source-the-fallback.md) | A controller makes the source of a move its fallback, or gives the move none | Accepted |
| [0025](0025-cell-logs-the-cell-above-its-bounds.md) | The controller of a cell logs the cell above its bounds | Accepted |
| [0026](0026-agent-joins-again-after-its-session.md) | The agent joins its cell again with a new incarnation after its session ends | Accepted |
| [0027](0027-agent-raises-epoch-from-the-ack.md) | The agent raises its controller epoch from the ack of a report | Accepted |
| [0028](0028-close-ends-at-its-deadline.md) | The disruption of a Close ends at the deadline of the call | Accepted |
| [0029](0029-router-watches-every-cell.md) | The router watches every cell of the cell map | Accepted |
| [0030](0030-routes-take-addresses-from-views.md) | A route takes the address of each replica from the view of its cell | Accepted |
| [0031](0031-constructors-take-a-config.md) | The constructors of the components take a pointer to their configuration | Accepted |
| [0032](0032-controller-hosts-cells-through-a-root-agent.md) | A controller process hosts its cells through an agent of the root | Accepted |
| [0033](0033-placement-records-the-writer-incarnation.md) | A placement records the incarnation of its writer | Accepted |
| [0034](0034-replica-declares-the-agent-of-the-matrix.md) | Package replica declares the agent of the support matrix in the form of the interfaces standard | Accepted |
| [0035](0035-router-declares-the-router-of-the-matrix.md) | Package router declares the router of the support matrix in the form of the interfaces standard | Accepted |
| [0036](0036-local-routes-controller-calls-by-address.md) | The local transport routes the calls of a controller by the address in the record of the cell | Accepted |
| [0037](0037-replica-separates-new-and-start.md) | Package replica constructs an agent with New and starts it with Agent.Start | Accepted |
| [0038](0038-example-programs-are-one-file.md) | An example keeps its program in main.go and its application in a package under internal | Superseded by [0039](0039-example-applications-are-in-main.md) |
| [0039](0039-example-applications-are-in-main.md) | An example keeps its application in main.go, and examples/internal keeps the storage that the examples share | Accepted |
| [0040](0040-failed-add-is-a-fault-of-the-node-incarnation.md) | A definite failure of an add is a fault of the shard on the node incarnation | Accepted |
| [0041](0041-admission-counts-transitions-in-flight.md) | The controller admits each move against the replicas that the transitions in flight can place | Accepted |
| [0042](0042-agent-revokes-tenures-under-stop.md) | The agent revokes each tenure of the node at the end of its session under stop | Accepted |
| [0043](0043-emergency-pass-without-progress-repairs-the-rest.md) | An emergency pass that restores no replica repairs the other replicas | Accepted |
| [0044](0044-router-reports-the-lag-of-its-refreshes.md) | The router reports the lag of the generations that its Refresh calls wait for | Accepted |
| [0045](0045-dokimi-generates-the-test-code.md) | dokimi generates the enumerations, the error checks, the conformance suites, the doubles and the builders | Accepted |
| [0046](0046-specs-build-structs-through-fixtures.md) | The specs build the structs with generated builders through the fixtures of their package | Accepted |

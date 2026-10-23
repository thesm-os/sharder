---
milestone: 0002
title: Delegated ownership for consensus applications
status: Planned
depends-on: 0001
released-in: unscheduled
deadline: none
deadline-source: none
prd: none
rfc: RFC-0001, RFC-0002, RFC-0003, RFC-0005, RFC-0006, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# Milestone 0002: Delegated ownership for consensus applications

## Goal

A consensus application can let its own election choose the writers of its shards. sharder then records the replicas and writers that the application's authority reports, routes clients to them, asks the authority to move a writer, and admits planned disruptions against the authority's effective quorum. It follows the shards that the authority splits, and a planned restart ends only when the member has returned or the authority has replaced it.

This milestone covers an application's own consensus authority. Theke's default all-counted primary-backup adapter is a separate capability in milestone 0003; its local k-acknowledgement wait is not a consensus election or majority proof.

## Done when

- [ ] `builtin.Support` admits `delegated` ownership with the `elect` handoff, and `Resolve` rejects `delegated` with any other handoff.
- [ ] A writer that the authority elects without a request from the core appears in the cell's next generation, and routers route to it, within the authority's failover time plus one commit.
- [ ] A move that the core requests runs through `elect` or `abdicate`. The budget keeps a disruption deferred while the authority has not moved a writer off the disrupted node.
- [ ] Reports of `Hosted`, `Elected`, `Deposed` and `Released` that arrive twice and in reverse order never bring back a released replica or a deposed writer.
- [ ] Every report of a replica is a versioned observation. It carries the authority's epoch, and the agent's sequence per node incarnation orders the observations that one epoch contains. A removal leaves a tombstone, so a delayed `Hosted` for a removed replica restores nothing. Two observations with the same epoch and sequence and different payloads are refused. After a reconnect, the agent reports the node's complete current state, and the core reconciles it with the tombstones. An authority can coalesce the roles that it reports to a slow reader, so the contract states which changes of state an observation exposes.
- [ ] The adapter identifies the authoritative configuration version, counted voters, learners, commit rule and any transition. Local storage registrations cannot silently change acknowledgement eligibility.
- [ ] The availability rule counts only voters. With voters A, B and C and learners D and E, where A has failed, the budget refuses a planned disruption of B.
- [ ] During a joint configuration, the budget requires a majority of each of the two voter sets. Without a current report of a shard's effective configuration, it defers every planned disruption of the shard.
- [ ] An `Elect` whose target already leads at an epoch at or above the request's completes without another election, and returns that epoch. A retry after a lost reply, and a new controller that resumes the transition, receive the same outcome under the operation's identity. A request that a newer operation of the shard superseded is refused, so it cannot restore an obsolete placement.
- [ ] The core imports the authority's layout when the namespace starts, and keeps the authority's shard IDs, ranges and epochs. A split's children are published in one generation that replaces the parent, with no overlap between parent and children. After a report that covers one child only, or a coalesced report, the core recovers the complete layout from later reports or a snapshot of the authority's layout.
- [ ] The budget admits a planned disruption only against the authority's assessment of every affected data shard and of the authority's metadata group. A configuration change between the admission and the node's stop either keeps the admission valid through the authority's reservation or revalidation, or defers the disruption. Admitted outages count in every later assessment.
- [ ] A disruption with the impact process restart abdicates the node's writers and keeps its voter replicas. Its reservation ends when the member has returned and caught up, or when the authority has replaced it, and never when the process exits. The returning process or an external coordinator completes it through a defined call. A permanent removal follows the authority's replacement lifecycle instead.
- [ ] Acceptance cases pass for: an initial snapshot and a reconnect; duplicate, reordered and equal-epoch observations; a removed replica followed by a delayed `Hosted`; a lost `Elect` reply and a controller takeover; a superseded placement request; a split interrupted after one child's report; a restart with voters retained; a permanent removal; two requested restarts whose actual outages overlap; and a configuration change while a disruption is pending or admitted.
- [ ] A replica with a permanent fault in a delegated namespace loses the writer role through `abdicate`, and counts as unavailable until the application releases it.
- [ ] A Raft group whose members each run an agent fails over a leader, and sharder commits the new writer. A rolling upgrade of one member at a time completes under `maxConcurrentNodes: 1`.

## Why now

This milestone follows milestone 0001 because it extends that milestone's agent, protocol, store and budget.

## Scope

The scope is the `delegated` ownership mode with the `elect` handoff: the authority's reports through `Hosted`, `Elected`, `Deposed` and `Released`, the agent's `Elector` interface, and the budget's rules for consensus models. The rules for consensus models are the voter-aware availability rule, the authority's effective quorum, and the deferral of disruptions without a current configuration.

The scope also covers versioned observations with removal tombstones, a repeat-safe `Elect`, the authority's splits, admission against the authority's assessment of its data shards and metadata group, and the completion of delegated restarts.

## Not in this milestone

- Merges by the authority, and splits or merges that the core plans: milestone 0004.
- Consensus models under the core's own election, with `UpdateMembership`: nobody has scheduled them.
- Weighted voting and witness replicas: nobody has scheduled them.

## Risks to the sequence

| Risk | What it delays | What we would do |
|---|---|---|
| The authority's assessment of effective configurations and disruptions is not available when this milestone starts | Admission of planned disruptions in a delegated namespace | The budget defers every planned disruption of the namespace, and no automated disruption is admitted until the authority can assess it; an external operator must establish the same safety preconditions |

## Changes

| Date | What changed | Why |
|---|---|---|
| 2026-10-20 | Distinguished draft control-store decisions, delegated consensus and the primary-backup application adapter | The theke v2 integration needs explicit authority and configuration gates; no draft protocol or implementation is accepted by this update |
| 2026-10-10 | Removed the thesmos ledger as this milestone's consumer and example, and the ledger contracts that only its withdrawn cluster design stated | Ledger ADR-0006 grants writer leases on ranges of ledger IDs apart from shard leadership, and ledger RFC-0011 is withdrawn, so the ledger's storage roles do not choose sharder's writers |
| 2026-09-29 | Added versioned observations, a repeat-safe `Elect`, the authority's splits, admission against the authority's assessment, and the completion of delegated restarts, with their acceptance cases. Moved splits by the authority from milestone 0004 into this one, as the risk of 2026-09-28 foresaw, and replaced that risk | The thesmos kernel follows the ledger's leadership and uses this milestone for load-aware leadership and maintenance (kernel RFC-0010). Its integration review of 2026-09-29 found these contracts missing from the milestone |
| 2026-09-29 | Removed the risk that the ledger reports leadership in a form that `Elected` and `Deposed` do not fit | The ledger's `Member.Replicas` yields each role with the epoch in which it took effect, and `Hosted`, `Elected`, `Deposed` and `Released` report every role that it yields |
| 2026-09-28 | Created as the follow-on of the first release | The design review of 2026-09-28 moved delegated ownership out of the first release, and the thesmos ledger's integration depends on it |

## References

| What | Where |
|---|---|
| Ongaro and Ousterhout, "In Search of an Understandable Consensus Algorithm (Extended Version)", 2014, §6: joint consensus and non-voting members | https://raft.github.io/raft.pdf |
| go.thesmos.sh/ledger ADR-0006, "Fence each host's appends with writer leases, apart from shard leadership" | `docs/adr/0006-writer-leases-for-every-host.md` in the ledger repository |
| go.thesmos.sh/kernel RFC-0010, "Cluster: Cells, Ledger Leadership and Configuration": sharder's delegated mode | `docs/rfc/0010-cluster.md` in the kernel repository |

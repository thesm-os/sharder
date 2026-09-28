---
milestone: 0002
title: Delegated ownership for consensus applications
status: Planned
depends-on: 0001
released-in: unscheduled
deadline: none
deadline-source: none
prd: none
rfc: RFC-0001, RFC-0002, RFC-0005, RFC-0006, RFC-0008
---

# Milestone 0002: Delegated ownership for consensus applications

## Goal

A consensus application, such as the thesmos ledger, can let its own election choose the writers of its shards. sharder then records the replicas and writers that the application's authority reports, routes clients to them, asks the authority to move a writer, and admits planned disruptions against the authority's effective quorum.

## Done when

- [ ] `builtin.Support` admits `delegated` ownership with the `elect` handoff, and `Resolve` rejects `delegated` with any other handoff.
- [ ] A writer that the authority elects without a request from the core appears in the cell's next generation, and routers route to it, within the authority's failover time plus one commit.
- [ ] A move that the core requests runs through `elect` or `abdicate`. The budget keeps a disruption deferred while the authority has not moved a writer off the disrupted node.
- [ ] Reports of `Hosted`, `Elected`, `Deposed` and `Released` that arrive twice and in reverse order never bring back a released replica or a deposed writer.
- [ ] The availability rule counts only voters. With voters A, B and C and learners D and E, where A has failed, the budget refuses a planned disruption of B.
- [ ] During a joint configuration, the budget requires a majority of each of the two voter sets. Without a current report of a shard's effective configuration, it defers every planned disruption of the shard.
- [ ] A replica with a permanent fault in a delegated namespace loses the writer role through `abdicate`, and counts as unavailable until the application releases it.
- [ ] A cluster of ledger members, each with an agent, fails over a leader, and sharder commits the new writer. A rolling upgrade of one member at a time completes under `maxConcurrentNodes: 1`.

## Why now

This milestone follows milestone 0001 because it extends that milestone's agent, protocol, store and budget. It comes before milestones 0003, 0004 and 0005 because the thesmos ledger's integration with sharder waits for it, and none of those three has a consumer waiting.

The ledger's integration depends on this milestone.

## Scope

The scope is the `delegated` ownership mode with the `elect` handoff: the authority's reports through `Hosted`, `Elected`, `Deposed` and `Released`, the agent's `Elector` interface, and the budget's rules for consensus models. The rules for consensus models are the voter-aware availability rule, the authority's effective quorum, and the deferral of disruptions without a current configuration.

## Not in this milestone

- Splits and merges by the authority: milestone 0004, whose fresh shard identities they need.
- Consensus models under the core's own election, with `UpdateMembership`: nobody has scheduled them.
- Weighted voting and witness replicas: nobody has scheduled them.

## Risks to the sequence

| Risk | What it delays | What we would do |
|---|---|---|
| The ledger's integration needs splits and merges by its authority | The ledger's integration | Move them into this milestone, together with the fresh-identity rule that they need from milestone 0004 |
| The ledger reports leadership in a form that `Elected` and `Deposed` do not fit | This milestone, and the ledger's integration | Change the agent's reporting calls before the milestone starts |

## Changes

| Date | What changed | Why |
|---|---|---|
| 2026-09-28 | Created as the follow-on of the first release | The design review of 2026-09-28 moved delegated ownership out of the first release, and the thesmos ledger's integration depends on it |

## References

| What | Where |
|---|---|
| Ongaro and Ousterhout, "In Search of an Understandable Consensus Algorithm (Extended Version)", 2014, §6: joint consensus and non-voting members | https://raft.github.io/raft.pdf |
| go.thesmos.sh/ledger RFC-0011, "Replicated storage": leader election and membership changes | `docs/rfc/0011-replicated-storage.md` in the ledger repository |
| go.thesmos.sh/ledger RFC-0014, "Operations resilience": upgrades one member at a time | `docs/rfc/0014-operations-resilience.md` in the ledger repository |

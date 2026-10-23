---
milestone: 0003
title: Replication models beyond primary-only
status: Deferred
depends-on: 0001
released-in: unscheduled
deadline: none
deadline-source: none
prd: none
rfc: RFC-0005, RFC-0006, RFC-0015
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# Milestone 0003: Replication models beyond primary-only

## Goal

An application can keep more than one replica of each shard, as read replicas with `secondary-only` or as followers of one writer with `primary-secondary`, and sharder moves those replicas without exceeding a role's bound.

## Done when

- [ ] `builtin.Support` admits `secondary-only` and `primary-secondary`, replica counts above one, per-shard overrides and `scale`.
- [ ] A `secondary-only` shard whose R secondaries all serve moves one of them with `make-before-break`, through a temporary allowance that the core reserves before the move and releases when the old replica is dropped. The allowance never raises the bound of a writer role.
- [ ] A shard scaled down from five replicas to two, and up from two to five, ends at its new count without a deadlock and without exceeding the temporary allowance.
- [ ] Every preset that selects one of the two models completes its first move under its default budget.

### Theke primary-backup authority adapter

This is a separate capability from milestone 0001's transactional Guard and milestone 0002's application-consensus ownership. It uses a consensus configuration master and all-counted data replication. The exact public capability and handoff names are fixed by the protocol RFC before implementation; `Resolve` rejects the combination until its gates pass.

- [ ] The protocol RFC specifies the master record: configuration version, primary epoch, counted members, learners, predecessor, transition ID, phase and activation cut. Draft RFC-0015 is a precedent for control scopes, not implicit acceptance of this application-data protocol.
- [ ] A local theke durable result is preparation only. The adapter returns authoritative success after every counted member has durably accepted the prefix, and serves reads, change feeds and checkpoints through the committed `volume.Cut`. No public route exposes a raw latest-local view. The engine and every exposed kind pass the committed-cut visibility tests.
- [ ] The adapter's sequencer envelope records stable client operation outcomes, including outcome-only batches for refusals computed from mutable state, and publishes them only after their predecessor records and outcome record commit. A local condition failure based on a discarded prepared prefix cannot escape.
- [ ] A read uses a fresh all-counted barrier at the current configuration and epoch. Old barriers cannot authorize new reads; a reusable read lease needs a separate accepted expiry and restart contract.
- [ ] Adding a learner changes neither the counted set nor the commit rule. Activation fences a surviving old counted member, preserves its complete accepted prefix, copies it and an activation cut to all intended counted members, then conditionally commits the new configuration at the master. A learner cannot recover a group whose old counted members are all lost.
- [ ] Transitions, callbacks and acknowledgements bind operation identity, expected configuration version, epoch and storage registration. Crash or lose replies at every phase; retry resumes that transition, and an abort never reuses a superseded epoch.
- [ ] A learner target first becomes counted under an activated configuration with a surviving old member as primary. Only a later fenced activation can transfer leadership to it; admission remains closed across those activations.
- [ ] Planned moves close old-leader admission durably, drain previously admitted writes and outcomes, establish a stable cut, activate the target, then publish routing and retire the source. Delayed writes from stale routers cannot cross the admission barrier. A lost client reply still resolves through its operation ID.
- [ ] Disruption admission uses the adapter's active all-counted configuration and pending transition, not a majority predicate for arbitrary replicas. It reconfigures away from a planned outage before the outage, or defers it.
- [ ] With old counted members A/B/C and learner D, an acknowledgement from A/D never commits a batch under the old configuration. Partition A/D from B/C through every activation phase and prove that acknowledged writes remain in each subsequently activated leader.
- [ ] Pause an old writer before persistence, activate a successor, then resume the old writer. No old prepared write becomes a new authoritative result or feed entry, and no read invoked after activation obtains an old-config barrier.
- [ ] An external-package integration test restores the master and replicas after every boundary, including a committed batch whose watermark report was lost. Recovery preserves the selected old member's accepted prefix rather than truncating it to its last known committed watermark.
- [ ] The integration is measured under finite retention, bounded catch-up, source/target device limits and aggregate buffer caps. Leadership-restoration time and full-redundancy restoration time are reported separately.

## Why now

This milestone is deferred, and has no position in the order. It extends milestone 0001's core. Its theke adapter also needs an accepted configuration-master protocol and theke milestone 0007's committed-cut and replication contracts. It does not require milestone 0002's data consensus mode. The order among the three deferred milestones is arbitrary.

Theke v2's sharder integration and its shared-WAL failure measurement wait for this milestone's adapter gates. Theke can test its local primitives with a clearly labelled test authority before that integration exists.

## Scope

The scope is the two built-in models beyond `primary-only`, replica-count configuration, temporary allowances and the separately gated primary-backup authority adapter. The adapter includes planned move admission, drain, stable cut and activation; it does not change the safety contract of the existing transactional Guard.

## Not in this milestone

- Consensus models: milestone 0002.
- Generic `graceful` and `staged` handoffs. The concrete primary-backup move above has its own protocol gates; those do not enable every handoff or ownership combination.
- The `replicated-store` and `cache-affinity` presets, which also need range partitioners, the `service` topology or `affinity` ownership: nobody has scheduled them.

## Risks to the sequence

The configuration-master protocol and theke committed-cut publication API may change while their RFCs are drafts. Keep the integration capability disabled until both sides pass the same traces; do not replace it with a local Guard or a generic k-acknowledgement wait.

## Changes

| Date | What changed | Why |
|---|---|---|
| 2026-10-20 | Distinguished draft control-store decisions, delegated consensus and the primary-backup application adapter | The theke v2 integration needs explicit authority and configuration gates; no draft protocol or implementation is accepted by this update |
| 2026-09-28 | Created as deferred | The design review of 2026-09-28 found that `secondary-only` cannot start a move under its bound, and the first release supports `primary-only` alone |

---
milestone: 0003
title: Replication models beyond primary-only
status: Deferred
depends-on: 0001
released-in: unscheduled
deadline: none
deadline-source: none
prd: none
rfc: RFC-0005, RFC-0006
---

# Milestone 0003: Replication models beyond primary-only

## Goal

An application can keep more than one replica of each shard, as read replicas with `secondary-only` or as followers of one writer with `primary-secondary`, and sharder moves those replicas without exceeding a role's bound.

## Done when

- [ ] `builtin.Support` admits `secondary-only` and `primary-secondary`, replica counts above one, per-shard overrides and `scale`.
- [ ] A `secondary-only` shard whose R secondaries all serve moves one of them with `make-before-break`, through a temporary allowance that the core reserves before the move and releases when the old replica is dropped. The allowance never raises the bound of a writer role.
- [ ] A shard scaled down from five replicas to two, and up from two to five, ends at its new count without a deadlock and without exceeding the temporary allowance.
- [ ] Every preset that selects one of the two models completes its first move under its default budget.

## Why now

This milestone is deferred, and has no position in the order. It extends milestone 0001's core and needs nothing from milestones 0002, 0004 or 0005. The order among the three deferred milestones is arbitrary.

Nothing waits for this milestone.

## Scope

The scope is the two built-in models beyond `primary-only`, the replica-count configuration, and the temporary allowance that `secondary-only` needs.

## Not in this milestone

- Consensus models: milestone 0002.
- The `graceful` and `staged` handoffs: nobody has scheduled them.
- The `replicated-store` and `cache-affinity` presets, which also need range partitioners, the `service` topology or `affinity` ownership: nobody has scheduled them.

## Risks to the sequence

None. No milestone depends on this one.

## Changes

| Date | What changed | Why |
|---|---|---|
| 2026-09-28 | Created as deferred | The design review of 2026-09-28 found that `secondary-only` cannot start a move under its bound, and the first release supports `primary-only` alone |

---
milestone: 0005
title: Namespaces of more than one cell
status: Deferred
depends-on: 0001
released-in: unscheduled
deadline: none
deadline-source: none
prd: none
rfc: RFC-0007, RFC-0009
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# Milestone 0005: Namespaces of more than one cell

## Goal

An operator can split a cell that exceeds its bounds, so that one namespace spans more than one cell, each with its own controller, its own scope in the store and its own nodes.

## Done when

- [ ] `builtin.Support` admits more than one cell, and `Submit` applies a proposal of the kind `cell-split`.
- [ ] A split records each of its phases durably, from the root's record of the split to the source's tombstone, and one authority advances the transferred shards in every phase.
- [ ] A crash of the source controller, of the destination controller or of the root, before or after every phase and every copied batch, resumes the split without a second grant of any epoch and without a lost high-water mark.
- [ ] A router with a cached route of a transferred shard is redirected to the destination cell, and no router compares the generations of two cells.
- [ ] A node whose `cell` label names the destination joins it, and a node without a label is refused.

## Why now

This milestone is deferred, and has no position in the order. It extends milestone 0001's core and needs nothing from milestones 0002, 0003 or 0004. The order among the three deferred milestones is arbitrary.

Nothing waits for this milestone.

## Scope

The scope is the journaled cell split, routing across cells, and the joining of nodes by their `cell` label.

## Not in this milestone

- The `service` topology with `sharderd`: nobody has scheduled it.
- The `lookup` routing source: nobody has scheduled it.
- Moving a shard between cells without a split, and balancing load across cells: nobody has scheduled them.
- Moving a live scope to another backend cluster: nobody has scheduled it.

## Risks to the sequence

None. No milestone depends on this one.

## Changes

| Date | What changed | Why |
|---|---|---|
| 2026-09-28 | Created as deferred | The design review of 2026-09-28 found that a cell split copies a range between two scopes without a durable record of its progress |

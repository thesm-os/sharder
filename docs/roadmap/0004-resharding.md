---
milestone: 0004
title: Shards that split and merge
status: Deferred
depends-on: 0001
released-in: unscheduled
deadline: none
deadline-source: none
prd: none
rfc: RFC-0002, RFC-0003, RFC-0006, RFC-0007
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# Milestone 0004: Shards that split and merge

## Goal

A namespace can partition its keys with `hash-range` or `key-range`, whose shards split and merge with load or size, and no writer of a parent shard can write into a child.

## Done when

- [ ] `builtin.Support` admits `hash-range` and `key-range`, and `Submit` applies a split or a merge that the partitioner proposes.
- [ ] Every new shard gets an ID that no earlier shard of the namespace incarnation used. After a split, a merge and another split of one range, every active child acquires its fence and writes, and every retired ancestor remains fenced.
- [ ] A layout change larger than one commit never shows a reader a layout that is partly applied, and a crash before or after every commit of the change leaves either the old layout or the new one.
- [ ] An application reports a split position through a public interface, and the controller splits at it. A cooldown applies the same way before and after a restart of the controller.
- [ ] A change of the slot count of `fixed-slots` remains rejected.

## Why now

This milestone is deferred, and has no position in the order. It extends milestone 0001's core and needs nothing from milestones 0002, 0003 or 0005. The order among the three deferred milestones is arbitrary.

Splits and merges by the authority of a delegated namespace need this milestone's fresh shard identities.

## Scope

The scope is the two range partitioners, the fresh identities of new shards, a publication of layout changes that fits the store's commit limits, and the inputs that `Reshape` needs: split positions from the agents' reports, durable cooldown state and a logical time.

## Not in this milestone

- Changes of the slot count of `fixed-slots`, which move keys between cells: nobody has scheduled them.
- `explicit`: nobody has scheduled it.
- Detection of a hot key: nobody has scheduled it.

## Risks to the sequence

| Risk | What it delays | What we would do |
|---|---|---|
| The ledger's integration needs splits and merges by its authority | The ledger's integration, after milestone 0002 | Move the fresh-identity rule into milestone 0002 |

## Changes

| Date | What changed | Why |
|---|---|---|
| 2026-09-28 | Created as deferred | The design review of 2026-09-28 found that a split reuses retired shard IDs, and that a large layout change does not fit one commit |

---
adr: 0006
title: Roles and statuses are closed enumerations
status: Accepted
date: 2026-10-10
supersedes: none
superseded-by: none
rfc: RFC-0001, RFC-0002, RFC-0005, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0006: Roles and statuses are closed enumerations

## Status

Accepted

## Context

In the design, `shard.Role` and `shard.Status` are strings, and `shard.Fault` is a `uint8` enumeration:

- A model of package `replication` declares its roles by name, such as `primary`, and a model in a spec lists its roles in JSON.
- An agent reports the status of a replica as `starting`, `serving`, `stopping` or `failed`.

A string type has no closed set of values. The compiler accepts a misspelled role, the `exhaustive` linter cannot check a switch over the values, and every message spends the bytes of each name.

## Decision

We will declare `Role` and `Status` as `uint8` enumerations with a `String` method from stringer and a `Valid` method, because a closed set lets the compiler, the linter and the codecs reject a value outside it.

| Type | Values |
|---|---|
| `Role` | none 0, offline 1, dropped 2, primary 3, secondary 4, leader 5, follower 6, learner 7 |
| `Status` | starting 1, serving 2, stopping 3, failed 4 |

- The zero `Role` is no role, such as the `FromRole` of a callback other than `change-role`. The zero `Status` is not valid.
- The codecs of the messages call `Valid` through kanon's `-validate=valid`, so a message with a value outside the set fails to encode and to decode. An encoding leaves out a field of the zero value, so the receiver of a report checks the zero `Status` itself.
- A model declares its roles from the enumeration. `replication.Model` has no `Offline` field, because every model uses `RoleOffline`, and `replication.Dropped` becomes `shard.RoleDropped`.
- The Raft example of the design uses `follower` for a voter that does not lead.

## Alternatives Considered

### Strings, as in the design

A model declares its roles by name, and a spec can declare a model with roles of its own, such as `voter`.

Rejected because the core and the handlers then compare names that no closed set constrains.

## Consequences

**Positive:**

- The compiler rejects a role or a status that the enumeration does not declare.
- The `exhaustive` linter checks every switch over the values.
- A role or a status takes one byte of a message.

**Negative:**

- A model that needs a role outside the enumeration needs a new constant in a release of sharder.
- A spec writes roles in JSON. `Role` needs text methods for that, and this decision does not declare them.
- The numbers are persisted. A new value takes the next number, so every number keeps its meaning.

**Neutral:**

- `Fault` already has this form.

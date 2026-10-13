---
adr: 0031
title: The constructors of the components take a pointer to their configuration
status: Accepted
date: 2026-10-12
supersedes: none
superseded-by: none
rfc: RFC-0007, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0031: The constructors of the components take a pointer to their configuration

## Status

Accepted

## Context

RFC-0007 declares `controller.New(reg, st, m, agents, opts...)`. Its options set the node, the clock, the source of randomness, the reporter, the logger, the operators, the cell capacity, a volatile store and unauthenticated callers. `New` refuses a controller without the node, the clock, the source of randomness, the reporter or the logger, so five of its options are required. RFC-0008 declares `replica.Start(ctx, cfg Config, h)` and `router.New(ctx, cfg Config)`, which take their configurations by value.

The machines that the components run take a pointer to a `Config`, and so do `control.New` and `driver.New`. The interfaces standard of the repository requires that a constructor takes a pointer to its configuration.

## Decision

We will make the constructors of `controller`, `replica` and `router` take a pointer to a `Config`, as the machines and the driver do:

- `controller.New(cfg *Config) (*Controller, error)`. `Config` has a field for each argument and each option of RFC-0007 that the support matrix has: `Registry`, `Store`, `Membership`, `Agents`, `Node`, `Clock`, `Rand`, `Reporter`, `Logger`, `Operators`, `CellCapacity`, `Volatile` and `Unauthenticated`. It has no field for a trust anchor, because ADR-0008 removed approvals.
- `replica.Start(ctx, cfg *Config, h Handler) (*Agent, error)` and `router.New(ctx, cfg *Config) (*Watcher, error)`.
- The field of the reporter is `Reporter` in each `Config`, as in the machines, in place of the `Telemetry` of RFC-0008.
- Each constructor returns an error that wraps the `ErrConfig` of its package, of class `errs.Invalid`, for a nil `cfg` and for each required field that `cfg` leaves out.
- `controller.New` takes a `CellCapacity` of zero as one, the default of RFC-0007, and refuses a negative one.

## Alternatives Considered

### Functional options

`New` takes the dependencies as arguments and the rest as options, as RFC-0007 declares.

Rejected because five of the options are required, so the options hide fields that a caller must set. The controller would also differ from the machines that it runs.

### Configurations by value

`replica.Start` and `router.New` take their `Config` by value, as RFC-0008 declares.

Rejected because the interfaces standard requires a pointer. Each `Config` of RFC-0008 is also larger than the 80 bytes from which gocritic's hugeParam refuses a value parameter.

## Consequences

**Positive:**

- A component and the machines that it runs take their configuration in the same form.
- A new field keeps the behaviour of its zero value, so a caller that does not set it does not break.

**Negative:**

- A caller writes a struct literal in place of a list of options.

**Neutral:**

- RFC-0007 and RFC-0008 keep their declarations, and this ADR replaces them for the constructors.

## References

| What | Where |
|---|---|
| The declarations of the components | RFC-0007, The controller, and RFC-0008, The agent and The router |
| The rule for constructors | `docs/standards/interfaces.md` |
| The removal of approvals | ADR-0008 |

---
adr: 0035
title: Package router declares the router of the support matrix in the form of the interfaces standard
status: Accepted
date: 2026-10-13
supersedes: none
superseded-by: none
rfc: RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0035: Package router declares the router of the support matrix in the form of the interfaces standard

## Status

Accepted

## Context

RFC-0008 declares package `router`, which a client process embeds to route its requests:

- `Router` with `Route(ctx, key)` and `Refresh(ctx, inc, cell, min)`.
- `Source` with the values `Watch`, `Lookup` and `Compute`.
- `Watcher`, the router in watch mode, with `New(ctx, cfg Config)` and `Close() error`.
- `Do`, `NotOwnerError`, `Route` and `Endpoint`.
- `Config` with `Namespace`, `Store store.Store`, `Registry`, `Clock`, `Telemetry` and `Logger`.

The support matrix routes with the source `watch` (RFC-0001). RFC-0008 puts the sources `lookup` and `compute` outside the matrix.

The interfaces standard of the repository requires that a call takes a context and one request, and returns one result and an error. ADR-0034 applied the standard to package `replica`. ADR-0031 changed the constructors of the components to take a `*Config`, and replaced `Telemetry` with `Reporter`. ADR-0002 gives each reader a `store.Reader`.

These facts of the machine of the router also bear on the declarations:

- The machine takes the address of each replica from the views of its cell (ADR-0030), so the router needs a membership.
- The machine takes the jitter of each backoff from a source of randomness.
- The machine stops itself at an error of a read that a retry does not fix. `Route` then returns `ErrClosed` for every key.
- The machine publishes each route as a value of its own type, and returns that value without an allocation.

## Decision

We will declare in package `router` only the router of the support matrix, and give each call of `Router` the form of the interfaces standard:

- `Router` has `Route(ctx, *RouteRequest) (RouteResult, error)` and `Refresh(ctx, *RefreshRequest) (RefreshResult, error)`. `RouteRequest` contains the key, and `RouteResult` contains the route. `RefreshRequest` contains the namespace incarnation, the cell and the generation that the caller waits for. `Watcher.Refresh` returns `ErrConfig` for a request that lacks an incarnation or a cell, as the standard requires for a missing required field.
- `Do` keeps the signature of RFC-0008, because the standard covers the methods of interfaces and of concrete types, and `Do` is a function. `Do` passes `Route` and `Refresh` one call state from a pool. It calls the application at most `attempts` times, and routes the key again only after a `*NotOwnerError`. It does not refresh the router after the last call. It returns every other error at once, a lost reply included.
- `Watcher` implements `Router`. `New(ctx, *Config) (*Watcher, error)` returns once every cell has its first snapshot and its first view. The context of `New` bounds the wait of `New` alone. The router then runs until `Close`, as RFC-0008 states, or until it stops itself. `Close() error` is a lifecycle method. `Done()` is closed once the run of the router has ended, and `Err()` then returns the error that ended it, as in package `replica`.
- `Config` has `Namespace`, `Store store.Reader`, `Membership`, `Registry`, `Clock`, `Rand`, `Reporter` and `Logger`. `Membership` and `Rand` are new required fields.
- `Route` and `Endpoint` are aliases of the types of the machine, as `os.FileInfo` is an alias of `fs.FileInfo`. `ErrNoRoute`, `ErrClosed`, `ErrMapping` and `ErrCell` are the values of the machine.
- `NotOwnerError` keeps the declaration of RFC-0008, with the class `errs.Conflict`.
- The package does not declare `Source`, `Lookup` and `Compute`.

The property of the machine already replays the recreation case of milestone 0001 as `prop.Draws` traces. The tests of package `router` add the case end to end, through `Watcher` and `Do`.

## Alternatives Considered

### The declarations of RFC-0008

`Router` keeps its positional parameters.

Rejected because a new input of `Route` or `Refresh` would then change every implementation of `Router`.

### Types of the router for routes and endpoints

Package `router` declares `Route` and `Endpoint` with the fields of RFC-0008, and `Watcher.Route` copies each route of the machine into them.

Rejected because the endpoints of a route are a slice. A copy into a slice of another type allocates on every call of `Route`, which is the hot path of the router.

### A type Source with one value

Rejected because no caller reads the type: `New` builds a router in watch mode only.

## Consequences

**Positive:**

- A request or a result gains a field without a change of the applications.
- `Route` does not allocate, and neither does `Do` around the call of the application.
- An application can select on the end of its router and start a new one.

**Negative:**

- The documentation of package `router` shows `Route` and `Endpoint` as aliases, and documents their fields in an internal package.
- A call of the application writes a request, such as `w.Route(ctx, &router.RouteRequest{Key: k})`.

**Neutral:**

- RFC-0008 keeps its declarations, and this ADR replaces them for package `router`.

## References

| What | Where |
|---|---|
| The declarations of the router | RFC-0008, The router |
| The form of a call | `docs/standards/interfaces.md` |
| The constructors of the components | ADR-0031 |
| The addresses of the routes | ADR-0030 |
| The declarations of the agent | ADR-0034 |
| The machine of the router | `internal/machine/routes` |

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# Using assert

A test chooses the part of `go.dokimi.dev/assert` by the contract that it
states, not by the check that it replaces. This document covers release
v0.2.0. `go doc` of each package gives the details.

## Packages

| Package | Contains |
|---|---|
| `assert` | assertions that stop the case at the first failure, the chain `assert.That`, `Rejects`, and the options `ByIdentity`, `EquateEmpty` and `EquateNaNs` |
| `expect` | the same assertions, which record a failure and let the case continue |
| `prop` | `ForAll` and `Fuzz` over generated inputs, the property form of each assertion, and the generators |
| `stateful` | `Steps` of a `Machine` with its spec, actions, invariant and settle, inside a property |
| `history` | histories of concurrent calls: `Concurrently`, `Linearizable`, `Serializable` and `HasSnapshotIsolation` |
| `bench` | benchmark contracts: `Start`, `MaxAllocs`, `Warmup`, `Loop` and `End` |
| `golden` | golden files: `MatchAt`, `MatchTree` and `ShouldUpdate` |
| `files` | file trees: `Workspace`, `Equal`, `Contains` and `Unchanged` |

## A contract and the part that states it

| Contract | Part |
|---|---|
| A law over a domain, such as an order or an admission rule | `prop.ForAll`, or a property form such as `prop.Equal`, with `prop.Example` for each case that a table would pin |
| Encoding and decoding | `RoundTrip`, and `prop.RoundTrip` over generated inputs |
| A subject with state, such as a cache, an engine or a log service | `stateful.Steps` of a machine inside `prop.ForAll` |
| Concurrent calls | `history.Concurrently` and `history.Linearizable` against a `history.Spec` |
| The isolation of transactions | `history.Serializable` over a list-append workload |
| A call that leaves state unchanged | `Pure`, with a method value as the observer |
| A counter that grows by one step per call | `Accumulates` |
| A value that never falls | `Monotonic` |
| A call that has the same effect when it repeats | `Idempotent` |
| The same input gives the same output | `Deterministic` |
| A call under a context that ended | `HonoursCancellation` and `HonoursDeadline` |
| A call after `Close` | `FailsAfterClose` |
| A subject that refuses every call after a failure | `Poisoned` |
| A call whose goroutines end before it returns | `NoGoroutineLeaks` |
| Every input of a list succeeds | `Total` |
| No two results are equal | `NoDuplicates` |
| The same elements in any order | `Permutation` |
| The order of adjacent elements | `Pairwise` |
| A call returns one particular object | `Equal` with `ByIdentity()` |
| The error of a dependency | `Equal`, with the wanted error computed from the dependency |
| An allocation ceiling | `MaxAllocs` and `MaxAllocsWithSetup` in tests, and `bench.Start(b).MaxAllocs(n)` in benchmarks |
| A check can fail | `Rejects` against an implementation that breaks the contract |
| A generator writes a tree of files | `golden.MatchTree`, or `files.Equal` against a `files.Tree` |

Tests do not use the latency ceilings `MaxLatency` and `MaxMean`, because
latency differs per machine. They do not use `NilContextSafe` either,
because Go code never passes a nil context to a function.

## Stateful subjects

A subject whose calls change what later calls find is stateful, a cache
included. Write its machine before any other test. Each case of its
suite then derives from the machine.

1. Write the sequential specification as a `history.Spec`. `Initial`
   returns the state before any call. `Next` returns every state that an
   operation may leave. State `Hash` whenever you state `Equal`. Without
   it, the search compares every pair of states and slows by orders of
   magnitude.
2. Write the `stateful.Machine`. It has the spec, one `Action` per call,
   an `Invariant` and a `Settle` that ends work in progress. Each action
   has a `Name`, a `Weight`, `Enabled`, `Input` and `Run`.
3. Run it as `prop.ForAll(t, contract, func(c *prop.Case) {
   stateful.Steps(c, m, stateful.Clients(2)) })`. `Steps` checks the
   history with `Linearizable` after every step, and `Clients` adds a
   concurrent section.
4. Draw each input within fixed bounds. When the subject can return other
   results on a replay, as an engine that refuses a call for room does,
   `Enabled` and `Input` read a state of the machine's own. Otherwise a
   replay takes other steps and the run ends as flaky.
5. Give a blocking call an ended context inside the machine, so every
   step completes.

A spec reads a call's output with `op.Returned(v)`. A spec receives a
pending call with `Known` false and a nil `Output`, because
`Linearizable` lets it take effect at any point or never.

## Concurrent calls

- `history.Concurrently(clients, within, body)` releases the clients
  together and returns one outcome per client. Assert that every outcome
  is finished, and give `within` more time than the subject's own
  deadlines.
- Record each call with `Invoke` and its completion with `OK`, `Fail` or
  `Unknown`. Calls that share a key form one partition of the check.
- `Linearizable` takes the options `Budget`, `MemoLimit` and `TimeLimit`.
  A test harness sets them, so an undecided search fails within a known
  cost.

## Properties

- A body draws each input with `c.Draw(generator, label)`. The generators
  include `Integer`, `Bytes`, `String`, `List`, `Dict`, `OneOf`,
  `SampledFrom`, `Optional`, `Recursive` and `Composite`, with the methods
  `Map`, `Filter` and `Bind`.
- `prop.Require(label, share)` with `c.Classify(label)` proves that a
  share of the cases runs a branch. A body returns on a refusal instead
  of calling `c.Assume` on a rare condition, because a run fails when it
  rejects more than ten cases for each valid one.
- Each run also tries the bounds of every integer generator. The size
  bounds of `Bytes` and `String` are not always tried, so a length rule
  keeps explicit rows for its bounds.
- `prop.Fuzz` serves `go test -fuzz` only. The same property runs with
  `prop.ForAll` in a test.

## assert and expect

- Check one property per assertion: `Length`, `Nil`, `ErrorIs` and
  `Equal` on one value, and `True` on one bool. Split `a && b` into two
  assertions, and never pack values into an array to compare them at
  once.
- Use `assert` by default, and always for a precondition, such as
  `Length` before an index. Use `expect` where one case checks two or
  more independent properties of one result. `expect.That(t, v)` chains
  the checks of one value.
- Compare pointers, slices and maps by identity with `ByIdentity()`.
- `Permutation` and `NoDuplicates` compare every pair of values, so
  16,000 values take 128 million comparisons. Check thousands of results
  with `Linearizable`, or sort them and use `Equal`.

## Facts of v0.2.0

- `MaxAllocs` calls the function once to warm it and counts the next 100
  calls. A ceiling of 0 admits one allocation in all, and a count above
  the ceiling is repeated up to three times.
- `MaxAllocs` checks no ceiling under the race detector, msan or asan,
  with `-gcflags` that turn off optimisation, in a mutation run, or in a
  `go test` run whose result can be cached. Run `go test -count=1` to
  check the ceilings.
- `Rejects` returns the records of the assertions of its function. A check
  that reports through `Fatalf` or `Errorf` fails without a record, so a
  suite reports through assertions.
- `Equal` compares unexported fields. A nil slice differs from an empty
  one unless `EquateEmpty` applies.

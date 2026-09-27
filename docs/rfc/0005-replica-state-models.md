---
rfc: 0005
title: Replica state models and replica counts
author: Roy Klopper
status: Draft
created: 2026-09-27
updated: 2026-09-27
discussion: none
supersedes: none
superseded-by: none
produces-adr: tbd
research: Research-0001, Research-0002
related: RFC-0001, RFC-0002, RFC-0004, RFC-0006
---

# RFC-0005: Replica state models and replica counts

## Summary

A state model declares:

- the roles of a shard's replicas, and the transitions between roles
- the most replicas that may be in each role at one instant
- which role writes
- when a shard counts as available

We propose declaring models as data, compiling each into a validated state machine from core's `fsm` package when a spec resolves, and three built-in models: `primary-only`, `secondary-only` and `primary-secondary`. Applications define other models, such as a consensus group with a leader, followers and learners. The core starts a transition only when every role remains within its bound in every order in which the pending transitions can complete. Replica counts are part of the same configuration: a default, per-shard overrides, and optional scaling on load within limits.

## Motivation

Replicated applications differ in what their replicas do:

- SM supports three replication strategies because its applications split between them. Primary-only serves applications that keep state elsewhere, secondary-only serves read-only replicas such as model servers, and primary-secondary serves storage with a writer.
- Consensus applications need more. SM's maintainers added a call that tells a primary to change its replica set, for Paxos applications that must apply membership changes themselves.
- Helix lets every application declare its own state model, with bounds such as at most one leader per partition. Its built-in models are master-slave, leader-standby and online-offline.

A fixed set of roles serves the first group and forces the others to build their own. With undeclared roles, the core cannot keep two writers apart, because the model does not state which role writes. A declared model gives the core what it needs to enforce the writer bound and to order promotions after demotions, for any set of roles.

A model that users write is a lifecycle, and core validates lifecycles at construction. Core's `fsm.Spec` rejects a state that cannot be reached, a state that cannot be left, a state with no path to a terminal state and an edge that can never be taken, and it checks a stored status change in about half a nanosecond without an allocation.

## Detailed design

### The model

```go
// Package replication declares replica state models and replica counts.
package replication

import (
 "errors"
 "strconv"

 "go.thesmos.sh/core/errs"
 "go.thesmos.sh/core/fsm"
 "go.thesmos.sh/sharder/shard"
)

// Model declares the roles of a shard's replicas, the transitions between roles, and how
// many replicas of one shard may be in each role at the same instant.
type Model struct {
 Name string
 // Offline is the role of a replica that is assigned but not serving. A new replica
 // starts in it, and a replica passes through it before it is dropped.
 Offline shard.Role
 Roles   []Role
 // Transitions lists every allowed change of role, including to and from Offline.
 Transitions []Transition
 // Available is the rule under which a shard counts as available for the disruption
 // budget.
 Available Availability
}

// Role is one serving role.
type Role struct {
 Name shard.Role
 // Max is the most replicas of one shard that may be in the role at any instant,
 // counting replicas whose transition into the role has started.
 Max Bound
 // Writer marks the role whose replicas write shard data. A writer role has a Max of
 // one. The core issues fencing tokens to replicas in the writer role.
 Writer bool
}

// Bound is a replica count: Count, or the shard's replica count when Replicas is true.
type Bound struct {
 Count    int
 Replicas bool
}

// Transition is one allowed change of role. When more than one transition of a shard is
// pending, the core starts them in ascending Priority, subject to every role's Max.
type Transition struct {
 From     shard.Role
 To       shard.Role
 Priority int
}

// Availability selects the rule under which a shard counts as available.
type Availability string

const (
 // AnyReplica counts a shard as available while one replica serves in any role.
 AnyReplica Availability = "any"
 // WriterServes counts a shard as available while its writer role is served.
 WriterServes Availability = "writer"
 // Majority counts a shard as available while more than half of its replicas serve.
 Majority Availability = "majority"
 // AllReplicas counts a shard as available only while every replica serves.
 AllReplicas Availability = "all"
)

// ErrInvalidModel is returned by Compile, joined with every problem it finds. It
// classifies as errs.Invalid.
var ErrInvalidModel = errs.WithClass(errors.New("replication: invalid model"), errs.Invalid)

// Dropped is the role of a replica that has left its node. Every compiled model ends
// in it, and only the model's Offline role leads to it.
const Dropped shard.Role = "dropped"

// State is the index of a role in a compiled model. State 0 is the model's Offline
// role, the roles follow in declaration order, and the last state is Dropped.
type State uint8

// String returns "state" and the index. Compiled.Role returns the role's name.
func (s State) String() string { return "state " + strconv.Itoa(int(s)) }

// Compiled is a validated model and its transition table. It is immutable and safe
// for concurrent use.
type Compiled struct {
 Model Model
 // Spec is the model's transition table. The event that moves a replica to a state
 // is that state, so a transition from s to t is the edge Edge(s, t, t).
 Spec *fsm.Spec[State, State, struct{}]
 // unexported fields
}

// Compile validates m and builds its transition table with fsm.Builder, with Offline
// as the initial state, the transition from Offline to Dropped added, and Dropped as
// the only terminal state. It returns ErrInvalidModel joined with every problem it
// finds: a role declared twice, a transition that refers to an undeclared role, more
// than one writer role, a writer role whose Max is not one, more than 254 roles, and
// every problem that fsm.Builder.Build reports for the table.
func (m Model) Compile() (*Compiled, error)

// State returns the state of role r.
func (c *Compiled) State(r shard.Role) (State, bool)

// Role returns the role of state s.
func (c *Compiled) Role(s State) (shard.Role, bool)

// Allows reports whether the model moves a replica from role from to role to in one
// transition. The core checks it before it commits a role change.
func (c *Compiled) Allows(from, to shard.Role) bool
```

`fsm.Builder.Build` reports a role that nothing transitions into, a role with no transition out of it, and a role with no path to a terminal state. `Dropped` is the only terminal state and only `Offline` leads to it, so every role needs a path to `Offline`. The rule comes from Helix, which requires a path to its `DROPPED` state from every state. Without it, the core could strand a replica in a cycle of roles that it can neither drop nor demote.

The core compiles every model when a spec resolves, so an invalid model fails `Resolve` with each of its problems.

### Built-in models

| Model | Roles and bounds | Transitions in priority order | Available when |
|---|---|---|---|
| `primary-only` | `primary`: max 1, writer | `primary→offline`, `offline→primary` | The primary serves |
| `secondary-only` | `secondary`: max R | `offline→secondary`, `secondary→offline` | Any secondary serves |
| `primary-secondary` | `primary`: max 1, writer. `secondary`: max R | `primary→secondary`, `secondary→primary`, `offline→secondary`, `secondary→offline` | The primary serves |

R is the shard's replica count. Every model also has the transition `offline→dropped`, which `Compile` adds. The priorities put demotion of a writer before promotion of a new one in `primary-secondary`, and removal before addition in `primary-only`. They match the order in Helix's built-in master-slave model. `secondary-only` adds before it removes, and its bound of R lets a new secondary start while the old one still serves.

`primary-secondary` in Go:

```go
package replication

// PrimarySecondary is the built-in model with one writer and replicas that follow it.
var PrimarySecondary = Model{
 Name:    "primary-secondary",
 Offline: "offline",
 Roles: []Role{
  {Name: "primary", Max: Bound{Count: 1}, Writer: true},
  {Name: "secondary", Max: Bound{Replicas: true}},
 },
 Transitions: []Transition{
  {From: "primary", To: "secondary", Priority: 0},
  {From: "secondary", To: "primary", Priority: 1},
  {From: "offline", To: "secondary", Priority: 2},
  {From: "secondary", To: "offline", Priority: 3},
 },
 Available: WriterServes,
}
```

A consensus group whose learners catch up before they vote, declared in the spec:

```json
{
  "name": "consensus",
  "config": {
    "offline": "offline",
    "roles": [
      {"name": "leader", "max": {"count": 1}, "writer": true},
      {"name": "follower", "max": {"replicas": true}},
      {"name": "learner", "max": {"count": 2}}
    ],
    "transitions": [
      {"from": "leader", "to": "follower", "priority": 0},
      {"from": "follower", "to": "leader", "priority": 1},
      {"from": "learner", "to": "follower", "priority": 2},
      {"from": "offline", "to": "learner", "priority": 3},
      {"from": "follower", "to": "offline", "priority": 4},
      {"from": "learner", "to": "offline", "priority": 5}
    ],
    "available": "majority",
    "membershipUpdates": true
  }
}
```

With `membershipUpdates` set, the core calls the writer's `UpdateMembership` callback before it adds or removes a voting replica, so that the application changes its consensus configuration in its own protocol.

A consensus application that elects its own leaders selects the `delegated` ownership mode instead. Its authority then chooses the replicas and the writer. The core does not call `UpdateMembership` or `ChangeRole`, and the model only declares the roles that agents report.

### Starting transitions within bounds

The core compares each shard's target replicas with its current replicas and derives the role transitions that connect them. It starts a transition only if, for every role, the replicas in the role plus the replicas with a started transition into the role remain within the role's `Max` after the transition starts. It counts started transitions, not only completed ones, so that no order of completion can exceed a bound.

Every transition is also subject to these rules:

- A transition into a writer role waits until the transition out of the old writer has completed, or until the old writer's lease has expired and the clock-drift bound has passed. The handoff protocol selects which of the two applies.
- A transition that makes a replica unavailable starts only when the disruption budget admits it.
- A transition not listed in the model is never started. The core checks `Compiled.Allows` before it commits a role change, as core's `fsm` prescribes for a status that a compare-and-swap updates. The core moves a replica to its target through listed transitions, for example `secondary→offline` then `offline→secondary` on another node.

Helix's controller selects its state-transition messages the same way: it counts current and pending states against each state's upper bound, from the highest-priority transition to the lowest.

### Replica counts

```json
{
  "replicas": 3,
  "overrides": {"tenant-large": 5},
  "scale": {"metric": "qps", "perReplica": 1000, "min": 2, "max": 7, "cooldown": "10m"}
}
```

`replicas` is the default count, and `overrides` sets it per shard. With `scale`, the core sets each shard's count to its load on `metric` divided by `perReplica`, rounded up and clamped to `min` and `max`. It changes a shard's count at most once per `cooldown`. SM scales replicas the same way, adding replicas when the average load per replica leaves a configured range, with a limit on how many replicas it adds or drops in a period.

## Alternatives considered

### A fixed set of replication strategies

Offer primary-only, secondary-only and primary-secondary as an enumeration, as SM does. The core code handles three known cases, and users choose from a short list.

**Why not:** consensus applications need learners, a majority rule and membership changes, and SM had to add a call for them. An enumeration makes each new case a change to the core. SM's strategies remain as built-in models, so users who need nothing else still choose from a short list.

### Helix's general state models

Allow any states, including non-serving ones such as bootstrapping or error states, with constraints per transition and throttles per state.

**Why not:** the handoff protocols and the fencing rule need to identify which role writes and which roles serve. Arbitrary non-serving states would make every protocol handle states it cannot interpret. The model has one non-serving role, `Offline`. A replica whose callback fails is `failed` in its observed status, not in a model state.

### No roles

Treat every replica the same, and let the application elect its own leader if it needs one.

**Why not:** the application then needs its own election and fencing, which is the work this library removes. The disruption budget also has to treat the loss of a leader differently from the loss of a follower.

## Drawbacks

- Custom models expand the test space. Each model changes which transitions exist and which orders the core may choose, and the simulator has to cover them.
- The bound counts started transitions, so a slow transition blocks others. A secondary that takes an hour to catch up occupies its slot in the role's bound for that hour.
- `Priority` values interact with bounds in ways that are hard to predict for a user who writes a custom model. `Compile` checks structure, not whether the priorities produce the intended order.
- `fsm` limits a model to 256 states, so a model has at most 254 roles besides `offline` and `dropped`. Errors from `fsm` name states by index, and `Compile` adds each index's role name to them.
- Scaling on load moves replicas when load moves, which adds churn. The cooldown limits it and adds delay.
- A model allows one writer role with one replica. Multi-leader applications, where more than one replica accepts writes, cannot express their writers, and the core fences none of them.

## Open questions

1. Should a model allow more than one writer role, or more than one replica in a writer role, for multi-leader applications that fence writes themselves?
2. Should scaling on load be part of the replication configuration, as proposed, or a separate axis?
3. Is a failed callback best represented only as an observed status, or should a model be able to declare an error role with its own transitions?

## Unresolved and future work

- Weighted voting and witness replicas, which vote but do not store data, are not proposed here.
- Moving the writer role to the replica with the most recent data is not proposed here. Service Fabric's 2018 open-source code picks the first live replica with the highest log sequence number. The application's `ChangeRole` handler can refuse a promotion, and the placer then tries another replica.

## References

| What | Where |
|---|---|
| Lee et al., "Shard Manager", SOSP '21, §2.2.3 | https://dl.acm.org/doi/10.1145/3477132.3483546 |
| Guo and Kooburat, "Scaling services with Shard Manager", Engineering at Meta, 2020 | https://engineering.fb.com/2020/08/24/production-engineering/scaling-services-with-shard-manager/ |
| Apache Helix 2.0.1, state models | https://helix.apache.org/2.0.1-docs/tutorial_state.html |
| Apache Helix, MessageSelectionStage at 48674e6 | https://github.com/apache/helix/blob/48674e6ac2a94fb5365ce69a4b1ee1d57655da1d/helix-core/src/main/java/org/apache/helix/controller/stages/MessageSelectionStage.java |
| Microsoft Learn, replica roles | https://learn.microsoft.com/en-us/dotnet/api/system.fabric.replicarole?view=azure-dotnet |
| go.thesmos.sh/core at 8edf26a: finite state machines (RFC-0031), states that cannot finish (ADR-0024), `fsm/builder.go` | https://github.com/thesmos-ai/core |

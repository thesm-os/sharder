---
rfc: 0002
title: Namespace specs, the strategy registry and validation
author: Roy Klopper
status: Draft
created: 2026-09-27
updated: 2026-09-28
discussion: none
supersedes: none
superseded-by: none
produces-adr: tbd
research: Research-0002
related: RFC-0001, RFC-0003, RFC-0004, RFC-0005, RFC-0006, RFC-0007, RFC-0008, RFC-0009
---

# RFC-0002: Namespace specs, the strategy registry and validation

## Summary

A namespace has one declarative spec, which selects a preset and, per axis, a registered strategy with its own JSON configuration. Strategies register in an explicit `Registry` through a typed factory. The factory parses and validates the strategy's configuration, and declares the capabilities that the parsed configuration provides.

Each registry has a support matrix, and `Resolve` checks a spec against it first: a selection outside the matrix fails with an error of class `errs.Unsupported`. `Resolve` then merges the spec over its preset, applies defaults, parses every strategy configuration, and checks every cross-axis requirement, including the capabilities of the process's store and membership adapters. It reports all problems at once, each with its spec path, and warns about combinations that are valid but probably a mistake.

A running namespace accepts a new spec version only after `Change` confirms that the move from the old spec is possible by an edit alone. The mapping from keys to positions is not editable, because the root fixes it when it creates the namespace. `Submit` returns a rejected version's reasons, and the namespace continues on the last accepted spec. A controller authorises every operation by its kind, against the operators in its process's own configuration. The design also lets a namespace require signatures from a threshold of approvers, which is outside the support matrix.

## Motivation

A library that accepts any combination of strategies accepts combinations that cannot work:

- exclusive ownership with a handoff that runs two writers at once
- a leaderless topology with a placer that needs a controller
- a partitioner that splits and merges shards, in a topology without a controller to compute the splits

Each of these combinations fails at runtime as a correctness bug, not at start-up. Misconfiguration is the main cost of configurability, and this RFC specifies validation together with the spec for that reason. Validation is also where a release refuses the strategies whose design is not complete, so that no spec can select them.

In projects that make behaviour pluggable at this scale, a strategy is selected by name and parses its own typed configuration. Invalid input is rejected before anything runs.

- gRPC selects a load-balancing policy by name from JSON. The policy's `ParseConfig` turns its raw JSON into a typed configuration, and a service config that fails to parse is ignored as a whole while the channel keeps the previous one.
- Caddy selects modules by ID from JSON and decodes each strictly. It then runs each module's `Provision` and `Validate` methods. A reload that fails rolls back to the old configuration.
- The OpenTelemetry Collector decodes each component's configuration on top of the component's defaults and rejects unknown keys. It validates every component and reports all errors joined.
- Envoy rejects an invalid configuration update with a NACK that contains the error, and the last valid configuration remains in effect.
- Kubernetes validates custom resources with CEL rules, including transition rules that compare the new object with the old one, under a cost budget.

We propose the same mechanism for sharding strategies.

## Detailed design

### The spec

An application builds a spec in Go or decodes it from JSON. The namespace's root scope in the store contains the accepted spec, and `sharderctl` changes it.

The spec selects strategies, not infrastructure. The store and membership adapters are process wiring: the embedder builds them and passes them to the controller and the agent. The spec could not select its own store, because the store contains the spec.

```go
// Package config declares namespace specs, the strategy registry, the support matrix,
// presets and validation.
package config

import (
 "encoding/json"
 "time"

 "go.thesmos.sh/core/epoch"
 "go.thesmos.sh/sharder/shard"
)

// Spec declares how one namespace is sharded. A field left empty takes its value
// from the preset named in Preset, then from the library default.
type Spec struct {
 Namespace shard.Namespace `json:"namespace"`
 // Version increases by one with every accepted change of the spec.
 Version epoch.Epoch `json:"version"`
 // Preset names a registered preset. Resolve rejects a spec without one, so every
 // strategy of a namespace is one that the spec or its preset selected.
 Preset string `json:"preset"`

 Partitioning Selection `json:"partitioning,omitzero"`
 Placement    Selection `json:"placement,omitzero"`
 Replication  Selection `json:"replication,omitzero"`
 Ownership    Selection `json:"ownership,omitzero"`
 Handoff      Selection `json:"handoff,omitzero"`
 Topology     Selection `json:"topology,omitzero"`
 Routing      Selection `json:"routing,omitzero"`

 Budget    Budget     `json:"budget,omitzero"`
 Load      LoadModel  `json:"load,omitzero"`
 Cells     CellPolicy `json:"cells,omitzero"`
 Approvals Approvals  `json:"approvals,omitzero"`
}

// CellPolicy bounds the size of each cell of the namespace. The root reports a cell
// above either bound.
type CellPolicy struct {
 // MaxNodes is the most nodes of one cell. Zero selects 5,000.
 MaxNodes int `json:"maxNodes,omitempty"`
 // MaxShards is the most shards of one cell. Zero selects 1,000,000.
 MaxShards int `json:"maxShards,omitempty"`
}

// Selection selects a registered strategy by name. Config is the strategy's
// configuration as raw JSON, in the schema that the strategy's factory defines. A
// selection without a name selects the preset's strategy. The Config of a selection
// of the preset's strategy merges over the preset's configuration of that strategy.
type Selection struct {
 Name   string          `json:"name,omitempty"`
 Config json.RawMessage `json:"config,omitempty"`
}

// Budget bounds how many replicas of a shard may be unavailable at once, how many nodes
// planned disruptions may take out at once, and how much transition work runs at once.
// A nil field takes its value from the preset, then from the default that its comment
// names, so an explicit zero differs from an absent field.
type Budget struct {
 // MaxUnavailable is the most replicas of one shard that may be unavailable at once,
 // counting failures, maintenance and transitions. Nil selects the replication
 // model's default for Interruption. An explicit zero admits no planned
 // unavailability.
 MaxUnavailable *int `json:"maxUnavailable,omitempty"`
 // MaxConcurrentNodes is the most nodes that planned disruptions may take out at once.
 // Nil selects one.
 MaxConcurrentNodes *int `json:"maxConcurrentNodes,omitempty"`
 // MaxTransitions is the most transitions of a cell in flight at once, repair
 // included. Nil selects 64. Resolve rejects a value below one.
 MaxTransitions *int `json:"maxTransitions,omitempty"`
 // MaxNodeTransitions is the most transitions in flight at once that count against
 // one node. A transition counts against its destination and its fallback, and
 // against its source while the source's session is live. Nil selects 16. Resolve
 // rejects a value below one.
 MaxNodeTransitions *int `json:"maxNodeTransitions,omitempty"`
 // NodeCallbacks is the most callbacks that the controller has outstanding to one node
 // at once. It does not bound the handlers that still run on the node after the
 // controller's call has timed out. Nil selects the value of MaxNodeTransitions, so
 // that no callback waits for a permit. Resolve rejects a value below one.
 NodeCallbacks *int `json:"nodeCallbacks,omitempty"`
 // Interruption states whether planned work may leave a shard without a serving
 // writer for the handoff protocol's writer gap. Empty takes the preset's value, and
 // a preset without one selects NoInterruption.
 Interruption Interruption `json:"interruption,omitempty"`
 // DrainRoles lists the roles moved off a node before a planned disruption starts.
 // Replicas in other roles remain on the node and count as unavailable until the
 // node returns.
 DrainRoles []shard.Role `json:"drainRoles,omitempty"`
}

// Interruption states whether planned work may interrupt a shard's writer.
type Interruption string

const (
 // PlannedInterruption lets a planned move or disruption leave a shard without a
 // serving writer for the handoff protocol's writer gap. Every preset in the support
 // matrix selects it.
 PlannedInterruption Interruption = "planned"
 // NoInterruption admits no planned move or disruption that leaves a shard without a
 // serving writer. Resolve rejects it together with break-before-make, because every
 // move of break-before-make has a writer gap.
 NoInterruption Interruption = "none"
)

// LoadModel lists the load metrics that placers balance, and the window over which
// agents average the load they report.
type LoadModel struct {
 Metrics []Metric `json:"metrics,omitempty"`
 // Window is the averaging window. Zero selects one minute.
 Window Duration `json:"window,omitempty"`
}

// Metric is a load dimension that placers compare with the node capacity named by
// Capacity, or with the node capacity of the same name when Capacity is empty.
type Metric struct {
 Name     string `json:"name"`
 Capacity string `json:"capacity,omitempty"`
}

// Duration encodes a time.Duration in JSON as a Go duration string, such as "30s".
type Duration time.Duration

// MarshalJSON encodes d as a duration string.
func (d Duration) MarshalJSON() ([]byte, error)

// UnmarshalJSON decodes a duration string. A number is an error, because a bare
// number is ambiguous between seconds and nanoseconds.
func (d *Duration) UnmarshalJSON(b []byte) error
```

A spec that relies on its preset for everything but two settings:

```json
{
  "namespace": "orders",
  "version": 3,
  "preset": "exclusive-worker",
  "partitioning": {"config": {"slots": 4096}},
  "placement": {
    "config": {
      "constraints": [
        {"name": "capacity", "config": {"headroom": 0.2}}
      ]
    }
  }
}
```

Both selections omit the strategy's name, so each selects the preset's strategy and merges over the preset's configuration of it. The slot count replaces the preset's slot count and keeps its hash function. The list of constraints replaces the preset's list and keeps the preset's goals.

### The registry

A strategy registers through a factory for its axis. The factory's type parameter is the axis interface, such as `partition.Partitioner`. `Build` returns an error for a factory whose type parameter does not match the axis, such as a partitioner factory registered on the placement axis, and the controller calls `Build` for every axis when it starts. The root also builds every strategy of a spec before it accepts the spec, the constraints, goals and hash functions that its configurations name included. A live edit that selects such a factory, or one whose `New` fails, is therefore rejected with its spec path, and the namespace continues on its current spec.

The planner's constraints and goals register the same way, under the kinds `constraint` and `goal`. An application adds a rule that no built-in covers as a Go factory of `placement.Constraint` or `placement.Goal`, and a spec names it in the planner's configuration.

Hash functions register under the kind `hash`, as factories of `partition.Hash`, and a partitioner's configuration names one. The one built-in is `xxhash64`. An application that migrates with an existing key-to-slot mapping registers that mapping's hash under a name of its own, such as Redis Cluster's CRC16 or Kafka's murmur2.

A factory declares its capabilities for a parsed configuration, because a configuration can change them. `exclusive` provides `fenced` only when its configuration sets `fenced: true`.

```go
package config

import (
 "encoding/json"
 "errors"
 "fmt"

 "go.thesmos.sh/core/errs"
)

// Axis names a kind of registered strategy: a spec field, the planner's constraints
// and goals, which the planner's configuration names, or the hash functions, which a
// partitioner's configuration names. Store and Membership name the roles of the
// process's adapters, which provide capabilities and do not register strategies.
type Axis string

const (
 Partitioning Axis = "partitioning"
 Placement    Axis = "placement"
 Replication  Axis = "replication"
 Ownership    Axis = "ownership"
 Handoff      Axis = "handoff"
 Topology     Axis = "topology"
 Routing      Axis = "routing"
 Constraint   Axis = "constraint"
 Goal         Axis = "goal"
 Hash         Axis = "hash"
 Store        Axis = "store"
 Membership   Axis = "membership"
)

// Capability is a named property that a strategy provides to other axes,
// such as "ordered-keys" or "single-writer".
type Capability string

// Requirement demands that the strategy selected on Axis, or the adapter in the role
// that Axis names, provides Capability.
type Requirement struct {
 Axis       Axis
 Capability Capability
 // Reason is shown to the user when the requirement fails.
 Reason string
}

// Capabilities declares what a strategy provides and what it needs from other axes.
type Capabilities struct {
 Provides []Capability
 Requires []Requirement
 // Prefers lists requirements whose failure Resolve reports as a warning instead of
 // an error, for combinations that work but are probably a mistake.
 Prefers []Requirement
}

// Parsed is a strategy configuration after parsing. Validate returns every invalid
// field joined with errors.Join, or nil when the configuration is valid. A Parsed
// value encodes with encoding/json to its complete configuration, every default
// included, and Parse of that encoding returns an equal value. The root stores that
// encoding in the resolved spec.
type Parsed interface {
 Validate() error
}

// Factory builds strategies of type T, where T is the axis interface, such as
// partition.Partitioner.
type Factory[T any] interface {
 // Name is the name that a spec uses to select the strategy. It is unique per axis.
 Name() string
 // Parse decodes raw into the strategy's configuration type and applies defaults.
 // An empty raw yields the default configuration, and an unknown field is an error.
 // A field that raw omits takes its default, and a field that raw sets keeps its
 // value, an empty list included.
 Parse(raw json.RawMessage) (Parsed, error)
 // Capabilities declares the capabilities that the strategy provides and requires
 // with the configuration cfg, which Parse returned and Validate accepted.
 Capabilities(cfg Parsed) Capabilities
 // New builds a strategy from a configuration that Parse returned and Validate accepted.
 New(cfg Parsed) (T, error)
}

// Support is a registry's support matrix: the strategies that a spec may select, the
// axes on which every registered name is supported, and the capabilities that every
// namespace must have. Resolve checks a spec against it before anything else.
type Support struct {
 // Strategies maps each axis to the names that a spec may select on it. A name that
 // it does not list fails Resolve with ErrUnsupported, even when it is registered.
 Strategies map[Axis][]string
 // Open lists the axes on which every registered name is supported, including the
 // names that an application registers.
 Open []Axis
 // Requires lists the capabilities that every resolved namespace must have.
 Requires []Capability
 // Approvals reports whether a spec may contain an approvals section, and a process a
 // trust anchor.
 Approvals bool
}

// Registry contains registered strategies and presets, and the support matrix that
// Resolve checks. It is safe for concurrent use. There is no global registry.
type Registry struct {
 // unexported fields
}

// ErrDuplicate is returned when a name is registered twice on one axis. It classifies
// as errs.Invalid.
var ErrDuplicate = errs.WithClass(errors.New("config: duplicate registration"), errs.Invalid)

// ErrUnknown is returned when a spec selects a name that is not registered on its
// axis. It classifies as errs.Invalid.
var ErrUnknown = errs.WithClass(errors.New("config: unknown strategy"), errs.Invalid)

// ErrUnsupported is returned for a selection or a preset outside a registry's
// Support. The error that wraps it names the axis and the value. It classifies as
// errs.Unsupported and matches errors.ErrUnsupported.
var ErrUnsupported = errs.WithClass(fmt.Errorf("config: outside the support matrix: %w", errors.ErrUnsupported), errs.Unsupported)

// NewRegistry returns an empty registry whose support matrix is s.
func NewRegistry(s Support) *Registry

// Register adds f to r on axis. It returns ErrDuplicate if axis already has f.Name().
func Register[T any](r *Registry, axis Axis, f Factory[T]) error

// Build returns the strategy that res selected on axis, built by its factory.
// It returns an error if the factory registered on axis does not build a T.
func Build[T any](r *Registry, res *Resolved, axis Axis) (T, error)

// BuildNamed returns the strategy registered as name on axis, built from raw. The
// planner builds the constraints and goals that its configuration names with it. It
// returns ErrUnknown when axis has no factory of that name.
func BuildNamed[T any](r *Registry, axis Axis, name string, raw json.RawMessage) (T, error)
```

The factories of the built-in strategies are in package `builtin`, which imports `config` and every strategy package. The strategy packages do not import `config`, so an application can use a partitioner or a function placer without a registry. `builtin` also declares the support matrix of the first release.

```go
// Package builtin registers the library's built-in strategies and presets, and
// declares the support matrix of the first release.
package builtin

import "go.thesmos.sh/sharder/config"

// Support returns the support matrix of the first release: fixed-slots and singleton
// partitioning, the planner, primary-only replication, exclusive ownership with a
// fence, break-before-make handoff, the embedded topology and watch routing. On the
// constraint, goal and hash axes, it supports every registered name. It requires the
// capability "fenced", and it has no approvals.
func Support() config.Support

// Registry returns a new registry whose support matrix is Support(). It contains every
// built-in strategy, and the built-in presets inside the matrix. Each call returns a
// separate registry, which the caller can extend.
func Registry() *config.Registry
```

An application adds its own strategy the same way the built-ins are added. The support matrix lists the constraint, goal and hash axes as open, so a constraint, a goal or a hash function that an application registers is supported. On every other axis, the matrix lists names, and `Resolve` rejects a spec that selects a name outside the list, even when an application has registered it.

```go
package example

import (
 "go.thesmos.sh/sharder/builtin"
 "go.thesmos.sh/sharder/config"
 "go.thesmos.sh/sharder/placement"
)

// Setup returns a registry with every built-in and the application's residency
// constraint, which keeps each tenant's shards on nodes in the tenant's country.
func Setup(residency config.Factory[placement.Constraint]) (*config.Registry, error) {
 reg := builtin.Registry()
 if err := config.Register(reg, config.Constraint, residency); err != nil {
  return nil, err
 }
 return reg, nil
}
```

Each `Registry` is a value that the application creates and passes to the controller. Separate registries in one process do not share names, and a test can build a registry that contains only fakes, with a support matrix that lists them. No package registers anything from `init`.

### Presets and defaults

```go
package config

// Preset is a named partial spec, from which Resolve fills each field that the spec
// leaves empty, and over whose configuration of a strategy Resolve merges the spec's
// selection of that strategy.
type Preset struct {
 Name        string
 Description string
 Spec        Spec
}

// RegisterPreset adds p to r. It returns ErrDuplicate if the name is taken, and
// ErrUnsupported if p selects a strategy outside r's support matrix.
func RegisterPreset(r *Registry, p Preset) error
```

`Resolve` builds each selection in three steps:

1. A selection that names another strategy than the preset's starts from that strategy's defaults. The preset's configuration of its own strategy does not apply to it.
2. A selection of the preset's strategy, or one without a name, merges its configuration over the preset's configuration of that strategy as a JSON Merge Patch, as RFC 7396 defines it. An object merges key by key, `null` deletes a key, and an array or any other value replaces the preset's value. An empty array, `[]`, is an explicit empty list.
3. The strategy's `Parse` applies its defaults to the merged configuration, so a key that the merge deleted takes the strategy's default.

The root stores the resolved spec, with every selection's configuration expanded: the encoding of what `Parse` returned, every default included. A release that changes a strategy's default therefore does not change a running namespace, whose stored configuration states the value that it runs.

A spec field that is not a selection takes the preset's value when the spec leaves it empty. The budget's numeric fields are pointers, so a spec that sets one to zero gets zero. Every other field is empty when it has its zero value, so a spec that needs such a field set to zero states it through the strategy's explicit option, such as `"replicas": 1`.

Every preset in the support matrix selects `fenced: true` in its ownership configuration, `watch` routing and `PlannedInterruption`, and resolves completely: no field of it takes a value that fails a check.

### Capabilities and cross-axis requirements

A factory declares the capabilities its strategy provides and the capabilities it requires on other axes. The store and membership adapters of the process declare what they provide through `Provider`. The controller passes each adapter's capabilities to `Resolve` under the adapter's role: the axis `store` for the store, and the axis `membership` for the membership. A requirement names the role that must provide a capability, so a store that has leases does not satisfy a requirement on the membership. An adapter that the process passes in both roles provides its capabilities in both. `Resolve` checks every requirement against the strategies the spec selected and the adapters the process has. The capability names are part of the library's public contract.

| Capability | Provided by | Required by |
|---|---|---|
| `ordered-keys` | `key-range` | none of the built-ins. An application's range scans can require it |
| `reshapes` | `hash-range`, `key-range` | none |
| `function` | `rendezvous`, `consistent-hash`, `bounded-load`, `jump`, `maglev` | topology `leaderless`, routing source `compute` |
| `controller` | topologies `embedded`, `service` | partitioners that reshape, placers `planner` and `static`, handoffs `graceful`, `staged` and `elect`, routing sources `watch` and `lookup` |
| `ordinal-nodes` | membership adapters that number their nodes, such as `membership/static` | placer `jump` |
| `single-writer` | `primary-only`, `primary-secondary`, custom models with one writer role | ownership `exclusive`, `overlapping` and `delegated`, handoff `graceful` |
| `exclusive-safe` | handoffs `break-before-make`, `graceful`, `staged` | ownership `exclusive` |
| `delegated-safe` | handoff `elect` | ownership `delegated` |
| `overlap` | handoffs `make-before-break`, `staged` | ownership `overlapping` |
| `writer-epochs` | ownership `exclusive`, `overlapping`, `delegated` | none. Handoff `graceful` prefers it |
| `fenced` | ownership `exclusive`, `overlapping` and `delegated`, when their configuration sets `fenced: true` | the support matrix of the first release |
| `leases` | membership adapters that end a node's session within a bounded time after the node stops renewing it, and store adapters whose elections end the same way | ownership `exclusive` and `overlapping`, from the membership |
| `durable` | store adapters that persist every commit | topology `service`, and `controller.New` unless the process selects a volatile store, from the store |

The capability checks reject these combinations, among others, whatever the support matrix contains:

- `exclusive` ownership with `make-before-break` handoff fails validation, because `make-before-break` does not provide `exclusive-safe`. The check prevents two writers at once.
- A `leaderless` topology with `planner` placement fails validation twice. The planner requires `controller`, and the leaderless topology requires `function`.
- `delegated` ownership with any handoff other than `elect` fails validation, because only `elect` provides `delegated-safe`. The check keeps the core from moving a writer that the application's authority elected.
- A `leaderless` topology with `watch` or `lookup` routing fails validation, because both sources read what a controller commits. Its routers use `compute`.

Validation accepts `affinity` ownership with `graceful` handoff and warns about it. `graceful` prefers `writer-epochs`, and under `affinity` it runs a coordinated primary handoff for a writer that the core does not fence.

A namespace on `membership/static` fails the capability checks inside the support matrix as well, because `exclusive` requires `leases` from the membership, and the static membership does not provide them, whatever the store provides. An `exclusive` ownership configuration without `fenced: true` fails the matrix, which requires `fenced`.

### Resolution

```go
package config

import "go.thesmos.sh/sharder/shard"

// Resolved is a spec after presets, defaults, parsing and cross-axis checks.
type Resolved struct {
 // Spec is the spec with every field filled, and every selection's configuration
 // expanded with its defaults. The root stores it.
 Spec    Spec
 Configs map[Axis]Parsed
 // Mapping is the mapping of the resolved partitioner, which Resolve reads from the
 // partitioner that it builds.
 Mapping shard.Mapping
 // Warnings lists the preferences that the selections do not meet. sharderctl apply
 // prints them, and the spec takes effect regardless.
 Warnings []Warning
}

// Warning is a combination that Resolve accepts but that is probably a mistake.
type Warning struct {
 // Path is the spec path of the selection whose preference failed, such as "handoff".
 Path string
 // Reason is the failed preference's Reason.
 Reason string
}

// Provider is implemented by store and membership adapters. Provides lists the
// capabilities that the adapter offers to the spec's strategies, such as "leases".
// The controller files them under the role in which the process passes the adapter.
type Provider interface {
 Provides() []Capability
}

// Resolve applies the preset to spec, and parses and validates every selection with
// its factory, the constraints, goals and hash functions that configurations name
// included. It parses no selection whose name r's support matrix does not list. It
// checks the result against the support matrix, every cross-axis requirement, and the
// budget's interruption against the handoff protocol, with the capabilities that
// provided lists for the roles Store and Membership. A problem with the support matrix is a selection outside it, an approvals
// section, or a capability that the matrix requires and the resolved namespace lacks.
// When there is any such problem, Resolve returns every one of them joined with
// ErrUnsupported. Otherwise it returns every other problem, a missing preset included,
// joined in one error of class errs.Invalid. Each problem names its spec path, such as
// "placement.config.goals[1].weight". A preference that the selections do not meet
// becomes a Warning in the result.
func Resolve(r *Registry, spec Spec, provided map[Axis][]Capability) (*Resolved, error)
```

`Resolve` reports every problem of one class in one error, so a user fixes a spec in at most two rounds instead of one round per problem. Every error of the package classifies as `errs.Invalid` or `errs.Unsupported` under core's `errs.Classify`, so a generic retry loop never retries a spec that cannot succeed.

### Changing a running namespace

The root of a running namespace reads the spec from the root scope, and the cell controllers read it from there. A new version takes effect in these steps:

1. `sharderctl apply` sends the new spec as a proposal with the next version number to the root, through the protocol's `Submit` call.
2. The root authorises the caller for the kind `spec`, resolves the proposal, builds every strategy of the result, and calls `Change` with the current resolved spec. A strategy that fails to build rejects the proposal with its spec path.
3. On success, the root commits the resolved spec together with the proposal and its caller's identity, and records the new version as the accepted one. `Submit` returns the warnings of `Resolve`.
4. On failure, `Submit` returns the reasons, and the root continues with the current spec.
5. Each cell's controller reads the accepted spec, verifies it, and applies it at its next planning pass. The commit that starts applying it records its version as `shard.Cell.SpecVersion` in the cell's record.

`sharderctl status` prints the accepted version and each cell's applied version, so an operator sees a cell that has not applied the latest spec. `sharderctl` prints what `Submit` returns. It reads the store for `status`, so operators need read credentials for the store, and no write credentials.

```go
package config

// Change reports whether a namespace may move from old to new by a spec edit alone.
// When an edit is not enough, the error classifies as errs.Invalid and names the
// operation that the change needs, such as a migration to a new namespace.
func Change(r *Registry, old, new *Resolved) error
```

In the support matrix, `Change` allows these edits and no others:

| Change | Allowed as an edit | Otherwise |
|---|---|---|
| Placement goals, weights, constraints, limits | Yes | |
| Budget and load model | Yes | |
| Cell bounds | Yes. The root reports the cells above the new bounds | |
| The mapping: the partitioner, the slot count, the hash function or its version, hash tags, or any other field of the partitioner's configuration | No | A migration to a new namespace |
| Every other field | No | A new namespace |

The mapping row compares the new spec's `Resolved.Mapping` with the mapping that the root recorded when it created the namespace. A spec edit cannot change it, because a key whose position changes moves to another shard without its data. A change of `hashTags` alone moves a key such as `user:{42}:cart` to another slot.

The design beyond the support matrix allows more edits. `Resolve` rejects the strategies that these rows need, so no namespace inside the matrix can make them:

| Change | Allowed as an edit | Otherwise |
|---|---|---|
| Replica count | Yes. The planner adds or removes replicas under the budget | |
| Configuration of a reshaping partitioner that leaves the mapping unchanged, such as split thresholds | Yes | |
| Ownership from `exclusive` to `affinity` | Yes | |
| Ownership from `affinity` to `exclusive` | No | An ownership barrier: the core drops every overlapping replica and assigns every shard a new epoch |
| Ownership to or from `delegated` | No | Recreating the namespace, because the core's epochs and the authority's epochs cannot share one fence |
| Approvals section | Yes, by a `spec` proposal that the `spec` policies of the old and the new section both approve | |
| Replication model | No | Recreating the namespace |
| Topology between `embedded` and `service` | Yes. The root moves each cell controller through the handoff protocol | |
| Topology to or from `leaderless` | No | Recreating the namespace |
| Routing source | Yes. Routers switch at their next refresh | |

The store and the membership adapters are not in the spec, so no spec change replaces them. Moving a namespace to another store is a migration to a new namespace.

### Proposals

Every change that an operator requests is a proposal, which `Submit` sends to the controller that applies it. A proposal names its scope and the namespace incarnation that it changes, so a controller that serves many namespaces applies it to the right one, and refuses a delayed proposal of a deleted namespace.

```go
package config

import (
 "time"

 "go.thesmos.sh/core/crypto/sign"
 "go.thesmos.sh/core/epoch"
 "go.thesmos.sh/sharder/shard"
)

//go:generate go tool kanon -type=Proposal,Signed

// OperationKind names a change that an operator proposes.
type OperationKind string

const (
 SpecChange OperationKind = "spec"
 Move       OperationKind = "move"
 Drain      OperationKind = "drain"
 Undrain    OperationKind = "undrain"
 Reshard    OperationKind = "reshard"
 CellSplit  OperationKind = "cell-split"
 // Explain asks for the constraints and goal costs that placed a shard. The cell's
 // controller returns them from Submit and commits nothing.
 Explain OperationKind = "explain"
)

// Proposal is one change that an operator requests.
type Proposal struct {
 // Scope is the scope whose controller applies the proposal: the root scope for a
 // spec, and a cell's scope for an operation on the cell's shards or nodes.
 Scope shard.Scope
 // Incarnation is the namespace incarnation that the proposal changes. It is zero
 // only in the spec proposal that creates the namespace. A controller refuses a
 // proposal for another incarnation.
 Incarnation shard.NamespaceIncarnation
 // ID is unique within the namespace. A controller refuses an ID that its scope has
 // already applied.
 ID   string
 Kind OperationKind
 // Base is the version of the state that the proposal changes. A controller applies
 // the proposal only while that state is at Base.
 Base epoch.Epoch
 // Body is the new spec, or the operation's arguments, as JSON.
 Body []byte
 // Expires is the last instant at which a controller may apply the proposal.
 Expires time.Time
}

// Message appends to dst the bytes that approvers sign: every field of p, framed with
// crypto.NewFramer under the domain "sharder.proposal", version 1.
func (p Proposal) Message(dst []byte) []byte

// Signed is a proposal with its approvers' signatures. Signatures is empty in the
// support matrix, which has no approvals.
type Signed struct {
 Proposal   Proposal
 Signatures []sign.Signature
}
```

### Authorization

Each operation arrives at the controller that applies it through the protocol, which the transport adapter authenticates. The controller authorises every call by the kind of the operation, never by a setting of the whole namespace:

| Call | Authorised when |
|---|---|
| `Submit` of a kind with an approval policy | The proposal has signatures that meet the kind's policy. Approvals are outside the support matrix |
| `Submit` of a kind without an approval policy | The caller is an operator of the namespace |
| `Disrupt` of one node | The caller is that node's registered identity and names the node's current incarnation, or the caller is an operator |
| `Disrupt` of more than one node | The caller is an operator |
| `Done` | The caller is the reservation's owner, the identity that `Disrupt` admitted it for, or an operator |
| A call without an identity | Never, unless the process selects unauthenticated mode |

The operators of a namespace are the identities that `controller.WithOperators` lists. The list comes from the process's own configuration, never from the store, so a writer to the store cannot grant itself operator rights. A process behind a transport that authenticates no caller selects unauthenticated mode explicitly, with `controller.WithUnauthenticated` and the agent's `Config.Unauthenticated`. That mode skips every identity check, and the epoch checks still apply.

Each kind of proposal has a base and a controller that applies it:

| Kind | Base | Applied by | In the support matrix |
|---|---|---|---|
| `spec` | The version of the spec that the proposal replaces | The root | Yes |
| `move` | The shard's epoch | The cell's controller | Yes |
| `drain`, `undrain` | The node's incarnation | The cell's controller | Yes |
| `explain` | The cell's generation | The cell's controller | Yes |
| `reshard` | The version of the spec whose slot count it changes | The root | No. `Submit` returns an error of class `errs.Unsupported` |
| `cell-split` | The version of the cell map | The root | No. `Submit` returns an error of class `errs.Unsupported` |

A controller refuses a proposal when its `Expires` has passed, when its scope has applied its ID, or when the state has moved from its `Base`. It commits the proposal, the identity of the caller that submitted it, and the change in one generation of its scope. The scope keeps the proposal under its ID until `Expires` passes, which bounds replays to the proposal's own lifetime.

### Approvals

Approvals are outside the support matrix. `Resolve` rejects a spec whose approvals section is not empty, `controller.New` rejects `WithTrust` with an error of class `errs.Unsupported`, and `sharderctl` has no `sign`, `submit` or `anchor` command. This section records the design.

A namespace can require that a threshold of approvers sign each change by an operator, as dual control requires. Without approvals, the operators decide how every shard is placed. A spec that drops the constraint that keeps a regulated tenant in one jurisdiction moves the tenant's replicas out of it, and a change from `exclusive` to `affinity` is an edit that removes fencing tokens.

A change is a proposal. The controller that applies it checks the proposal's signatures against the namespace's approvals section, with core's signature threshold, `sign.Policy`, and its table of verifiers, `sign.Resolver`. Approvers sign the bytes that core's `crypto.NewFramer` frames.

```go
package config

import (
 "go.thesmos.sh/core/crypto"
 "go.thesmos.sh/core/crypto/sign"
 "go.thesmos.sh/core/epoch"
)

// Approvals maps each operation kind to the approvers of its proposals, and a proposal
// of a kind without a policy needs an operator instead of signatures.
type Approvals struct {
 // Version increases by one with every accepted change of the section.
 Version  epoch.Epoch                      `json:"version"`
 Policies map[OperationKind]ApprovalPolicy `json:"policies,omitempty"`
}

// ApprovalPolicy requires signatures from at least Threshold of Parties. The party of
// a proposal's requester counts like any other, so a policy that needs k approvers
// besides the requester sets Threshold to k+1.
type ApprovalPolicy struct {
 Threshold int     `json:"threshold"`
 Parties   []Party `json:"parties"`
}

// Party is one approver, which counts toward a threshold only when every one of its
// keys has signed.
type Party struct {
 Name string      `json:"name"`
 Keys []PublicKey `json:"keys"`
}

// PublicKey is one key of a party, in the encoding that its algorithm defines.
type PublicKey struct {
 Algorithm crypto.Algorithm `json:"algorithm"`
 Key       []byte           `json:"key"`
}

// Trust is a process's anchor for the approvals of one namespace. It comes from the
// process's own configuration, never from the store, so that a writer to the store
// cannot replace it.
type Trust struct {
 // Anchor is the SHA-256 digest of the Message of the namespace's first spec
 // proposal. That proposal's approvals section is the first version of the chain.
 Anchor crypto.Digest
 // Resolver builds a verifier for each public key. A key whose algorithm Resolver
 // lacks cannot sign.
 Resolver sign.Resolver
}

// Verify checks that spec is approved under the chain of approvals sections that
// starts at t.Anchor. chain lists, in version order, the signed spec proposal that
// introduced each version of the approvals section, starting with the namespace's
// first spec. It returns an error that classifies as errs.Integrity when the anchor or
// a signature does not match, and errs.Invalid when a proposal does not decode.
func (t Trust) Verify(spec Signed, chain []Signed) error
```

These rules apply to every namespace whose processes have a `Trust`:

1. A process accepts a spec only when the spec's approvals chain starts at the anchor. The namespace's first spec is approved under the `spec` policy of its own approvals section. Every later version of the section is exactly one above the version before it, and is approved under the `spec` policies of both, as TUF requires of a new root.
2. The controller that applies a proposal refuses it when the kind's policy is not met, in addition to the refusals that every proposal gets.
3. The controller checks each policy with `sign.Policy.Check` and excludes no requester. A proposal's requester is not authenticated, so a party that proposed a change could name another key as the requester and approve its own proposal. The ledger's strict profiles count approvers the same way, with three distinct approvers so that at least two of them are not the requester.
4. The root scope keeps the signed proposal of the current spec, and of every version of the approvals section, for as long as the namespace exists.
5. Every controller and every agent verifies each spec it reads, and refuses a spec whose version is below the highest version it has verified. A restarted process has no such floor.
6. Automatic actions do not need approval: failover, emergency placement, periodic rebalancing, resharding by the partitioner, and `Agent.Close`. They act within the spec that the approvers signed.

A process without a `Trust` accepts every spec, as a namespace without approvals does. `Resolve` checks that each policy's threshold is between one and its number of parties, and that no key appears twice. `Trust.Verify` builds each policy with `sign.NewPolicy` and the process's `Resolver`. Operators sign proposals with `sharderctl`, outside the store.

Approvals need one more decision before the support matrix can include them. A signature proves that a spec was approved, not that it is the latest approved spec: rule 5 gives a restarted process no floor, so a writer to the store can return it to an earlier approved spec. Approvals need either a trusted checkpoint outside the store, or a threat model that excludes rollback by a writer to the store.

## Alternatives considered

### Configuration in code only

Build every strategy with Go constructors and functional options, as many Go libraries do. The compiler checks the types. The library then has no parser or schema to maintain.

**Why not:** configuration in code cannot be changed at runtime, stored, compared with the previous version, or edited by an operator. An application that wants type safety can still build the spec in code, because the spec is a Go struct.

### A schema language: JSON Schema or CUE

Describe every strategy's configuration in a schema language and validate specs against the schemas. Cross-axis rules become schema conditions. Editors can autocomplete specs.

**Why not:** the cross-axis rules depend on which strategies are selected, and each strategy's factory already has to parse its configuration into Go types. A second description in a schema language drifts from the Go types. We can generate a JSON Schema from the registry for editors without making it the source of truth.

### Plugins as separate processes

Load strategies as separate processes over RPC, as HashiCorp's go-plugin does, or as shared objects through Go's `plugin` package. A new strategy is then deployed without rebuilding the application.

**Why not:** the planner evaluates goals and constraints once per candidate move, and one pass evaluates many candidates. An RPC per evaluation adds its round-trip time to each of them. go-plugin's own documentation names performance as the advantage of shared libraries over its design. Go's `plugin` package warns that runtime crashes are likely unless the application and its plugins are built with exactly the same toolchain, build tags and flags. A loaded plugin cannot be closed, and plugins work only on Linux, FreeBSD and macOS. Strategies compile into the application binary, as Caddy modules and OpenTelemetry Collector components do.

### A global registry filled from init

Register built-in and third-party strategies into one package-level registry from `init` functions, as `database/sql` drivers and gRPC balancers do. Importing a package for its side effect enables its strategies.

**Why not:**

- Separate configurations in one process share the names of a global registry.
- Tests depend on import order.
- The strategies that a binary contains are not visible at the call site.

`database/sql` and gRPC also disagree on duplicates: `database/sql` panics on a second registration of a name, and gRPC keeps the last registration. The Go wiki's code review guidance keeps side-effect imports to main packages and tests. `builtin.Registry` gives the same convenience with an explicit value. `Register` returns `ErrDuplicate`, as the Kubernetes scheduler's plugin registry returns an error for a duplicate name.

### Zero values as defaults in the budget

Treat a zero `MaxUnavailable` as absent, as every other numeric field of the spec does. The budget then needs no pointers.

**Why not:** a user could not ask for zero planned unavailability when the model's default is higher. Zero and the default are the two values of the budget that matter most, and a spec has to be able to state either one.

### Editable fields of the mapping

Let an edit change the partitioner's fields that look harmless, such as hash tags or the hash function, and freeze only the slot count.

**Why not:** every one of these fields changes positions. Hash tags change which bytes of a key are hashed, and a new hash function or a new version of one moves nearly every key. Freezing the whole mapping removes the need to classify each field, and a migration to a new namespace handles every change the same way.

### Access control on the store instead of approvals

Restrict writes to the root scope with the store's own access control, such as role-based access control, or with a change-management workflow in front of `sharderctl`.

**Why not:**

- The store's access control decides who may write. It does not provide dual control or a record that a node can verify, and a store administrator bypasses it.
- A node cannot verify a workflow's decision, so anyone with write access to the store bypasses the workflow.
- One signature per spec authenticates the spec without dual control.

## Drawbacks

- The spec has 14 top-level fields and seven axes, and the built-in strategies add their own configuration schemas. A user reads one schema per selected strategy.
- Capability names are a second vocabulary next to strategy names. The table has 13 capabilities, and every new strategy must declare its capabilities correctly. A wrong declaration lets an invalid combination through validation. The simulator catches some of these. A missing requirement on an unusual combination can pass validation and fail in production.
- The support matrix is a third check next to the registry and the capabilities. A registered strategy outside it fails `Resolve`, so an application that registers its own partitioner cannot use it until the matrix admits the partitioning axis.
- An application rule that no built-in constraint covers is Go code, so changing it requires a deploy of the application.
- In the support matrix, a spec edit can change only placement, the budget, the load model and the cell bounds. Every other change needs a new namespace, including a change of the lease TTL.
- A factory registered on the wrong axis fails when the controller starts, or when the root builds a spec that selects it, not when the application compiles. `Register` takes the axis as a value, so the compiler cannot match it to the factory's type parameter.
- To use none of a list that its preset configures, a spec states an empty list, `[]`, because a selection of the preset's strategy merges over the preset's configuration. A selection of another strategy loses the preset's configuration of the preset's strategy.
- Every strategy's `Parse` must encode its result with every default and parse that encoding back to an equal value, so the root can store the expanded spec.
- Strict parsing conflicts with rolling upgrades. A spec that uses a field added in a newer release is rejected by controllers that still run the older release. gRPC ignores unknown fields for this reason. Here, a spec may use a new field only after every controller runs a release that supports it, and the controller that rejects such a spec reports the unknown field by name.
- Operator identities are part of each controller process's configuration. Adding or removing an operator changes the configuration of every controller process of the namespace.
- Approvals, outside the matrix, need a signing key for each approver and the anchor in each process's configuration. A change of approvers needs signatures under both the old and the new policy.

## Open questions

None.

## Unresolved and future work

- Approvals are outside the support matrix. Before the matrix includes them, they need a decision on freshness after a restart: a trusted checkpoint outside the store, or a threat model without rollback by a writer to the store.
- Strategies that applications register on axes other than constraints, goals and hash functions are outside the support matrix.
- Per-shard spec overrides beyond replica counts and placement preferences are not proposed here.
- Specs as Kubernetes custom resources are not proposed here. The root reads specs only from the store.
- A JSON Schema of the spec, generated from the registry for editors, is not proposed here.

## References

| What | Where |
|---|---|
| JSON Merge Patch, RFC 7396 | https://www.rfc-editor.org/rfc/rfc7396 |
| gRPC service config, load-balancing policy selection | https://github.com/grpc/grpc/blob/master/doc/service_config.md |
| gRPC proposal A21, service config error handling | https://github.com/grpc/proposal/blob/master/A21-service-config-error-handling.md |
| grpc-go v1.84.0, balancer registration and ConfigParser | https://github.com/grpc/grpc-go/blob/v1.84.0/balancer/balancer.go |
| database/sql in Go 1.27.1, Register | https://github.com/golang/go/blob/go1.27.1/src/database/sql/sql.go |
| Go wiki, code review comments on blank imports | https://go.dev/wiki/CodeReviewComments#import-blank |
| Kubernetes v1.37.1, scheduler plugin registry | https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/scheduler/framework/runtime/registry.go |
| OpenTelemetry Collector v0.161.0, confmap validation | https://github.com/open-telemetry/opentelemetry-collector/blob/v0.161.0/confmap/validation.go |
| Caddy v2 module system | https://caddyserver.com/docs/extending-caddy |
| OpenTelemetry Collector component factories | https://github.com/open-telemetry/opentelemetry-collector |
| Envoy xDS protocol, ACK and NACK | https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol |
| Kubernetes CRD validation rules with CEL | https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#validation-rules |
| Go plugin package | https://pkg.go.dev/plugin |
| HashiCorp go-plugin | https://github.com/hashicorp/go-plugin |
| Redis Cluster specification, hash tags | https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/ |
| The Update Framework specification, §5.2 and §5.3, the trusted root and its rotation | https://theupdateframework.github.io/specification/latest/ |
| go.thesmos.sh/core at 8edf26a: `crypto/sign/policy.go`, `crypto/sign/resolver.go`, `crypto/framer.go`, `errs/class.go` | https://github.com/thesmos-ai/core |
| go.thesmos.sh/ledger RFC-0016, "Configurable integrity": approvers in the strict profiles | `docs/rfc/0016-configurable-integrity.md` in the ledger repository |

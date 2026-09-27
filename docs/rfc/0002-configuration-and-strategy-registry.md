---
rfc: 0002
title: Namespace specs, the strategy registry and validation
author: Roy Klopper
status: Draft
created: 2026-09-27
updated: 2026-09-27
discussion: none
supersedes: none
superseded-by: none
produces-adr: tbd
research: Research-0002
related: RFC-0001, RFC-0003, RFC-0004, RFC-0005, RFC-0006, RFC-0007, RFC-0008, RFC-0009
---

# RFC-0002: Namespace specs, the strategy registry and validation

## Summary

A namespace has one declarative spec, which selects a preset and, per axis, a registered strategy with its own JSON configuration. Strategies register in an explicit `Registry` through a typed factory that parses and validates the strategy's configuration. Before a spec takes effect, `Resolve` applies the preset and defaults and parses every strategy configuration. It also compiles every CEL guardrail with a cost limit and checks every cross-axis requirement. `Resolve` reports all problems at once, each with its spec path. A running namespace accepts a new spec version only after `Change` confirms that the move from the old spec is possible by an edit alone. The controller records a rejected version with its reasons, and the namespace continues on the last accepted spec. A namespace can also require signatures from a threshold of approvers on every spec change and every operator's operation, checked against an anchor in each process's own configuration.

## Motivation

A library that accepts any combination of strategies accepts combinations that cannot work:

- exclusive ownership with a handoff that runs two writers at once
- a leaderless topology with a placer that needs a controller
- a partitioner that splits and merges shards, in a topology without a controller to compute the splits

Each of these combinations fails at runtime as a correctness bug, not at start-up. Misconfiguration is the main cost of configurability, and this RFC specifies validation together with the spec for that reason.

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

```go
// Package config declares namespace specs, the strategy registry, presets and validation.
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
 // Preset names a registered preset. An empty Preset selects "exclusive-worker".
 Preset string `json:"preset,omitempty"`

 Partitioning Selection `json:"partitioning,omitzero"`
 Placement    Selection `json:"placement,omitzero"`
 Replication  Selection `json:"replication,omitzero"`
 Ownership    Selection `json:"ownership,omitzero"`
 Handoff      Selection `json:"handoff,omitzero"`
 Membership   Selection `json:"membership,omitzero"`
 Store        Selection `json:"store,omitzero"`
 Topology     Selection `json:"topology,omitzero"`
 Routing      Selection `json:"routing,omitzero"`

 Budget    Budget     `json:"budget,omitzero"`
 Load      LoadModel  `json:"load,omitzero"`
 Cells     CellPolicy `json:"cells,omitzero"`
 Approvals Approvals  `json:"approvals,omitzero"`
}

// CellPolicy bounds the size of each cell of the namespace. The root reports a cell
// above either bound, and an operator splits it.
type CellPolicy struct {
 // MaxNodes is the most nodes of one cell. Zero selects 5,000.
 MaxNodes int `json:"maxNodes,omitempty"`
 // MaxShards is the most shards of one cell. Zero selects 1,000,000.
 MaxShards int `json:"maxShards,omitempty"`
}

// Selection selects a registered strategy by name. Config is the strategy's
// configuration as raw JSON, in the schema that the strategy's factory defines.
type Selection struct {
 Name   string          `json:"name,omitempty"`
 Config json.RawMessage `json:"config,omitempty"`
}

// Budget bounds how many replicas of a shard may be unavailable at once, and how many
// nodes planned disruptions may take out at once.
type Budget struct {
 // MaxUnavailable is the most replicas of one shard that may be unavailable at once,
 // counting failures, maintenance and transitions. Zero selects the replication
 // model's default, which keeps the shard available under the model's rule.
 MaxUnavailable int `json:"maxUnavailable,omitempty"`
 // MaxConcurrentNodes is the most nodes that planned disruptions may take out at once.
 // Zero selects one.
 MaxConcurrentNodes int `json:"maxConcurrentNodes,omitempty"`
 // DrainRoles lists the roles moved off a node before a planned disruption starts.
 // Replicas in other roles remain on the node and count as unavailable until the
 // node returns.
 DrainRoles []shard.Role `json:"drainRoles,omitempty"`
}

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

A spec that relies on its preset for everything but two fields:

```json
{
  "namespace": "orders",
  "version": 3,
  "preset": "replicated-store",
  "replication": {"name": "primary-secondary", "config": {"replicas": 5}},
  "placement": {
    "name": "planner",
    "config": {
      "constraints": [
        {"name": "guardrail", "config": {"expr": "node.labels['disk'] == 'ssd'"}}
      ]
    }
  }
}
```

### The registry

A strategy registers through a factory for its axis. The factory's type parameter is the axis interface, such as `partition.Partitioner`. `Build` returns an error for a factory whose type parameter does not match the axis, such as a partitioner factory registered on the placement axis, and the controller calls `Build` for every axis when it starts.

```go
package config

import (
 "encoding/json"
 "errors"

 "go.thesmos.sh/core/errs"
)

// Axis names a spec field that selects a registered strategy.
type Axis string

const (
 Partitioning Axis = "partitioning"
 Placement    Axis = "placement"
 Replication  Axis = "replication"
 Ownership    Axis = "ownership"
 Handoff      Axis = "handoff"
 Membership   Axis = "membership"
 Store        Axis = "store"
 Topology     Axis = "topology"
 Routing      Axis = "routing"
)

// Capability is a named property that a strategy provides to other axes,
// such as "ordered-keys" or "single-writer".
type Capability string

// Requirement demands that the strategy selected on Axis provides Capability.
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
}

// Parsed is a strategy configuration after parsing. Validate returns every invalid
// field joined with errors.Join, or nil when the configuration is valid.
type Parsed interface {
 Validate() error
}

// Factory builds strategies of type T, where T is the axis interface, such as
// partition.Partitioner.
type Factory[T any] interface {
 // Name is the name that a spec uses to select the strategy. It is unique per axis.
 Name() string
 // Capabilities declares the strategy's provided and required capabilities.
 Capabilities() Capabilities
 // Parse decodes raw into the strategy's configuration type and applies defaults.
 // An empty raw yields the default configuration, and an unknown field is an error.
 Parse(raw json.RawMessage) (Parsed, error)
 // New builds a strategy from a configuration that Parse returned and Validate accepted.
 New(cfg Parsed) (T, error)
}

// Registry contains registered strategies and presets. It is safe for concurrent use.
// There is no global registry.
type Registry struct {
 // unexported fields
}

// ErrDuplicate is returned when a name is registered twice on one axis. It classifies
// as errs.Invalid.
var ErrDuplicate = errs.WithClass(errors.New("config: duplicate registration"), errs.Invalid)

// ErrUnknown is returned when a spec selects a name that is not registered on its
// axis. It classifies as errs.Invalid.
var ErrUnknown = errs.WithClass(errors.New("config: unknown strategy"), errs.Invalid)

// NewRegistry returns an empty registry.
func NewRegistry() *Registry

// Register adds f to r on axis. It returns ErrDuplicate if axis already has f.Name().
func Register[T any](r *Registry, axis Axis, f Factory[T]) error

// Build returns the strategy that res selected on axis, built by its factory.
// It returns an error if the factory registered on axis does not build a T.
func Build[T any](r *Registry, res *Resolved, axis Axis) (T, error)
```

The factories of the built-in strategies are in package `builtin`, which imports `config` and every strategy package. The strategy packages do not import `config`, so an application can use a partitioner or a function placer without a registry.

```go
// Package builtin registers the library's built-in strategies and presets.
package builtin

import "go.thesmos.sh/sharder/config"

// Registry returns a new registry that contains every built-in strategy and preset.
// Each call returns a separate registry, which the caller can extend.
func Registry() *config.Registry
```

An application adds its own strategy the same way the built-ins are added:

```go
package example

import (
 "go.thesmos.sh/sharder/builtin"
 "go.thesmos.sh/sharder/config"
 "go.thesmos.sh/sharder/partition"
)

// Setup returns a registry with every built-in and the application's tenant partitioner,
// which maps each tenant ID to its own shard.
func Setup(tenant config.Factory[partition.Partitioner]) (*config.Registry, error) {
 reg := builtin.Registry()
 if err := config.Register(reg, config.Partitioning, tenant); err != nil {
  return nil, err
 }
 return reg, nil
}
```

Each `Registry` is a value that the application creates and passes to the controller. Separate registries in one process do not share names, and a test can build a registry that contains only fakes. No package registers anything from `init`.

### Presets and defaults

```go
package config

// Preset is a named partial spec. Resolve copies each field that the spec leaves empty.
type Preset struct {
 Name        string
 Description string
 Spec        Spec
}

// RegisterPreset adds p to r. It returns ErrDuplicate if the name is taken.
func RegisterPreset(r *Registry, p Preset) error
```

Resolution fills a field from three sources in order: the spec, the preset, and the strategy's own default. A preset selects strategies and may contain their configurations. A strategy's `Parse` fills every field that the resulting configuration leaves empty. A field is empty when it has its zero value, so a spec that needs a field set to zero states it through the strategy's explicit option, such as `"replicas": 1`.

### Capabilities and cross-axis requirements

A factory declares the capabilities its strategy provides and the capabilities it requires on other axes. `Resolve` checks every requirement against the strategies the spec selected. The capability names are part of the library's public contract.

| Capability | Provided by | Required by |
|---|---|---|
| `ordered-keys` | `key-range` | none of the built-ins. An application's range scans can require it |
| `reshapes` | `hash-range`, `key-range` | none |
| `function` | `rendezvous`, `consistent-hash`, `bounded-load`, `jump`, `maglev` | topology `leaderless` |
| `controller` | topologies `embedded`, `service` | partitioners that reshape, placers `planner`, `static` and `external`, handoffs `graceful`, `staged` and `elect` |
| `ordinal-nodes` | membership `static`, `kubernetes` | placer `jump` |
| `single-writer` | `primary-only`, `primary-secondary`, custom models with one writer role | ownership `exclusive`, `overlapping` and `delegated`, handoff `graceful` |
| `exclusive-safe` | handoffs `break-before-make`, `graceful`, `staged` | ownership `exclusive` |
| `delegated-safe` | handoff `elect` | ownership `delegated` |
| `overlap` | handoffs `make-before-break`, `staged` | ownership `overlapping` |
| `leases` | stores `memory` and `etcd`, membership `etcd` and `kubernetes` | ownership `exclusive` and `overlapping` |
| `durable` | store `etcd` | topology `service` |

Validation rejects these combinations, among others:

- `exclusive` ownership with `make-before-break` handoff fails validation, because `make-before-break` does not provide `exclusive-safe`. The check prevents two writers at once.
- A `leaderless` topology with `planner` placement fails validation twice. The planner requires `controller`, and the leaderless topology requires `function`.
- `delegated` ownership with any handoff other than `elect` fails validation, because only `elect` provides `delegated-safe`. The check keeps the core from moving a writer that the application's authority elected.

### Resolution

```go
package config

// Resolved is a spec after presets, defaults, parsing and cross-axis checks.
type Resolved struct {
 Spec    Spec
 Configs map[Axis]Parsed
}

// Resolve applies the preset and defaults to spec, parses and validates every
// selection with its factory, compiles every guardrail, and checks every cross-axis
// requirement. The returned error joins every problem found, and each problem
// classifies as errs.Invalid. Each problem names its spec path, such as
// "placement.config.goals[1].weight".
func Resolve(r *Registry, spec Spec) (*Resolved, error)
```

`Resolve` reports every problem in one error, so a user fixes a spec in one round instead of one round per problem. Every error of the package classifies as `errs.Invalid` under core's `errs.Classify`, so a generic retry loop never retries a spec that cannot succeed.

### CEL guardrails

A guardrail is a CEL expression that a placement move must satisfy. The planner evaluates it as a hard constraint. Guardrails express application rules that no built-in constraint covers, such as keeping shards of a regulated tenant in one jurisdiction.

The expression sees three variables:

| Variable | Type | Contents |
|---|---|---|
| `shard` | `map(string, dyn)` | `id`, `load` (a map of metric to value) and `labels` from the partitioner |
| `node` | `map(string, dyn)` | `id`, `labels`, `capacity` and `load` |
| `role` | `string` | The role of the replica being placed |

`Resolve` type-checks each expression and rejects any expression whose output type is not `bool`. It builds each program with a runtime cost limit. An evaluation that exceeds the limit rejects the move and reports the guardrail by name. The default limit is 10,000. Kubernetes allows 1,000,000 per validation call, which its source estimates at about 0.1 seconds. A guardrail runs once per candidate move, so its limit is 100 times lower.

```go
package config

import (
 "fmt"

 "cel.dev/cel-go/cel"
)

// DefaultGuardrailCost is the runtime cost limit of one guardrail evaluation.
const DefaultGuardrailCost = 10_000

// compileGuardrail type-checks expr and returns a program with a runtime cost limit.
func compileGuardrail(expr string, costLimit uint64) (cel.Program, error) {
 env, err := cel.NewEnv(
  cel.Variable("shard", cel.MapType(cel.StringType, cel.DynType)),
  cel.Variable("node", cel.MapType(cel.StringType, cel.DynType)),
  cel.Variable("role", cel.StringType),
 )
 if err != nil {
  return nil, err
 }
 ast, iss := env.Compile(expr)
 if iss.Err() != nil {
  return nil, iss.Err()
 }
 if !ast.OutputType().IsExactType(cel.BoolType) {
  return nil, fmt.Errorf("guardrail %q returns %v, want bool", expr, ast.OutputType())
 }
 return env.Program(ast, cel.CostLimit(costLimit))
}
```

The code uses `cel.dev/cel-go` v0.32.0, whose module path changed from `github.com/google/cel-go` in that release.

### Changing a running namespace

The root of a running namespace reads the spec from the root scope, and the cell controllers read it from there. A new version takes effect in these steps:

1. `sharderctl apply` writes the new spec to the root scope as a proposal with the next version number. In a namespace with approvals, `apply` prints the proposal instead, and `sharderctl submit` sends it with its approvers' signatures.
2. The root checks the proposal's approvals, resolves it, and calls `Change` with the current resolved spec.
3. On success, the root commits the new spec, and each cell controller verifies it and applies it at its next planning pass.
4. On failure, the root records the rejection and its reasons next to the proposal and continues with the current spec. `sharderctl apply` prints the reasons.

```go
package config

// Change reports whether a namespace may move from old to new by a spec edit alone.
// When an edit is not enough, the error classifies as errs.Invalid and describes the
// required operation, such as a resharding.
func Change(r *Registry, old, new *Resolved) error
```

| Change | Allowed as an edit | Otherwise |
|---|---|---|
| Placement goals, weights, constraints, limits | Yes | |
| Replica count | Yes. The planner adds or removes replicas under the budget | |
| Budget and load model | Yes | |
| Partitioning configuration within one strategy, such as split thresholds | Yes | |
| Partitioning strategy, hash function or slot count | No | A resharding operation |
| Ownership from `exclusive` to `affinity` | Yes | |
| Ownership from `affinity` to `exclusive` | No | An ownership barrier: the core drops every overlapping replica and assigns every shard a new epoch |
| Ownership to or from `delegated` | No | Recreating the namespace, because the core's epochs and the authority's epochs cannot share one fence |
| Approvals section | Yes, by a `spec` proposal that the `spec` policies of the old and the new section both approve | |
| Replication model | No | Recreating the namespace |
| Store or membership backend | No | A migration to a new namespace |
| Topology between `embedded` and `service` | Yes. The root moves each cell controller through the handoff protocol | |
| Topology to or from `leaderless` | No | Recreating the namespace |
| Cell bounds | Yes. The root reports the cells above the new bounds | |
| Routing source | Yes. Routers switch at their next refresh | |

### Approvals

A namespace can require that a threshold of approvers sign each change by an operator, as dual control requires. Without approvals, anyone who can write the root scope decides how every shard is placed. A spec that drops the guardrail that keeps a regulated tenant in one jurisdiction moves the tenant's replicas out of it, and a change from `exclusive` to `affinity` is an edit that removes fencing tokens.

A change is a proposal. The controller that applies it checks the proposal's signatures against the namespace's approvals section, with core's signature threshold, `sign.Policy`, and its table of verifiers, `sign.Resolver`. Approvers sign the bytes that core's `crypto.NewFramer` frames.

```go
package config

import (
 "time"

 "go.thesmos.sh/core/crypto"
 "go.thesmos.sh/core/crypto/sign"
 "go.thesmos.sh/core/epoch"
 "go.thesmos.sh/sharder/shard"
)

// OperationKind names a change that an operator proposes.
type OperationKind string

const (
 SpecChange OperationKind = "spec"
 Move       OperationKind = "move"
 Drain      OperationKind = "drain"
 Undrain    OperationKind = "undrain"
 Reshard    OperationKind = "reshard"
 CellSplit  OperationKind = "cell-split"
)

// Approvals maps each operation kind to the approvers of its proposals. A kind without
// a policy does not need approval.
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

// Proposal is one change that an operator requests.
type Proposal struct {
 Namespace shard.Namespace
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

// Signed is a proposal with its approvers' signatures.
type Signed struct {
 Proposal   Proposal
 Signatures []sign.Signature
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

Each kind of proposal has a base and a controller that applies it:

| Kind | Base | Applied by |
|---|---|---|
| `spec` | The version of the spec that the proposal replaces | The root |
| `reshard` | The version of the spec whose slot count it changes | The root |
| `cell-split` | The version of the cell map | The root |
| `move` | The shard's epoch | The cell's controller |
| `drain`, `undrain` | The node's incarnation | The cell's controller |

These rules apply to every namespace whose processes have a `Trust`:

1. A process accepts a spec only when the spec's approvals chain starts at the anchor. The namespace's first spec is approved under the `spec` policy of its own approvals section. Every later version of the section is exactly one above the version before it, and is approved under the `spec` policies of both, as TUF requires of a new root.
2. The controller that applies a proposal refuses it when its `Expires` has passed, when its scope has applied its ID, when the state has moved from its `Base`, or when the kind's policy is not met.
3. The controller checks each policy with `sign.Policy.Check` and excludes no requester. A proposal's requester is not authenticated, so a party that proposed a change could name another key as the requester and approve its own proposal. The ledger's strict profiles count approvers the same way, with three distinct approvers so that at least two of them are not the requester.
4. The controller commits the signed proposal and the change in one generation of its scope. The scope keeps the proposal under its ID until `Expires` passes, which bounds replays to the proposal's own lifetime. The root scope keeps the signed proposal of the current spec, and of every version of the approvals section, for as long as the namespace exists.
5. Every controller and every agent verifies each spec it reads, and refuses a spec whose version is below the highest version it has verified. A restarted process has no such floor.
6. Automatic actions do not need approval: failover, emergency placement, periodic rebalancing, resharding by the partitioner, and `Agent.Close`. They act within the spec that the approvers signed.

A process without a `Trust` accepts every spec, as a namespace without approvals does. `Resolve` checks that each policy's threshold is between one and its number of parties, and that no key appears twice. `Trust.Verify` builds each policy with `sign.NewPolicy` and the process's `Resolver`. Operators sign proposals with `sharderctl`, outside the store.

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

### Access control on the store instead of approvals

Restrict writes to the root scope with the store's own access control, such as etcd's role-based access control, or with a change-management workflow in front of `sharderctl`.

**Why not:**

- The store's access control decides who may write. It does not provide dual control or a record that a node can verify, and a store administrator bypasses it.
- A node cannot verify a workflow's decision, so anyone with write access to the store bypasses the workflow.
- One signature per spec authenticates the spec without dual control.

## Drawbacks

- The spec has 16 top-level fields and nine axes, and the built-in strategies add their own configuration schemas. A user reads one schema per selected strategy.
- Capability names are a second vocabulary next to strategy names. The table has 11 capabilities, and every new strategy must declare its capabilities correctly. A wrong declaration lets an invalid combination through validation. The simulator catches some of these. A missing requirement on an unusual combination can pass validation and fail in production.
- Guardrails move application rules into strings that the Go compiler does not check. `Resolve` type-checks them, but a guardrail that is valid and wrong places shards wrongly without an error.
- `Change` rejects some edits that users expect to work, such as changing the slot count. Each rejected edit needs an operation, which is more code to write and test.
- A factory registered on the wrong axis fails when the controller starts, not when the application compiles. `Register` takes the axis as a value, so the compiler cannot match it to the factory's type parameter.
- `cel.dev/cel-go` becomes a dependency of the `config` package, together with its transitive dependencies.
- Strict parsing conflicts with rolling upgrades. A spec that uses a field added in a newer release is rejected by controllers that still run the older release. gRPC ignores unknown fields for this reason. Here, a spec may use a new field only after every controller runs a release that supports it, and the controller that rejects such a spec reports the unknown field by name.
- Approvals need a signing key for each approver and the anchor in each process's configuration. A change of approvers needs signatures under both the old and the new policy.
- A restarted process has no floor on the spec versions it accepts, so a writer to the store can return a restarted process to an earlier approved spec. Approvals stop unapproved specs, not rollbacks to approved ones.

## Open questions

1. Should specs also be accepted as Kubernetes custom resources, with the root watching them, or only through the store?
2. Is a cost limit of 10,000 per guardrail evaluation the right default?
3. Should `Resolve` warn, not only fail, for valid but suspicious combinations, such as `affinity` ownership with `graceful` handoff?
4. Should the registry export a JSON Schema for editors, generated from the registered factories?

## Unresolved and future work

- Per-shard spec overrides beyond replica counts and placement preferences are not proposed here.

## References

| What | Where |
|---|---|
| gRPC service config, load-balancing policy selection | https://github.com/grpc/grpc/blob/master/doc/service_config.md |
| gRPC proposal A21, service config error handling | https://github.com/grpc/proposal/blob/master/A21-service-config-error-handling.md |
| grpc-go v1.84.0, balancer registration and ConfigParser | https://github.com/grpc/grpc-go/blob/v1.84.0/balancer/balancer.go |
| database/sql in Go 1.27.1, Register | https://github.com/golang/go/blob/go1.27.1/src/database/sql/sql.go |
| Go wiki, code review comments on blank imports | https://go.dev/wiki/CodeReviewComments#import-blank |
| Kubernetes v1.37.1, scheduler plugin registry | https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/scheduler/framework/runtime/registry.go |
| Kubernetes v1.37.1, CEL cost limits | https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/apis/cel/config.go |
| OpenTelemetry Collector v0.161.0, confmap validation | https://github.com/open-telemetry/opentelemetry-collector/blob/v0.161.0/confmap/validation.go |
| Caddy v2 module system | https://caddyserver.com/docs/extending-caddy |
| OpenTelemetry Collector component factories | https://github.com/open-telemetry/opentelemetry-collector |
| Envoy xDS protocol, ACK and NACK | https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol |
| Kubernetes CRD validation rules with CEL | https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#validation-rules |
| cel-go v0.32.0 release notes | https://github.com/cel-expr/cel-go/releases/tag/v0.32.0 |
| Go plugin package | https://pkg.go.dev/plugin |
| HashiCorp go-plugin | https://github.com/hashicorp/go-plugin |
| The Update Framework specification, §5.2 and §5.3, the trusted root and its rotation | https://theupdateframework.github.io/specification/latest/ |
| go.thesmos.sh/core at b1d1a4b: `crypto/sign/policy.go`, `crypto/sign/resolver.go`, `crypto/framer.go` | https://github.com/thesmos-ai/core |
| go.thesmos.sh/ledger RFC-0016, "Configurable integrity": approvers in the strict profiles | `docs/rfc/0016-configurable-integrity.md` in the ledger repository |

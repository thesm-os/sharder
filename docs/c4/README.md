# C4 model

The C4 model describes sharder at three levels of zoom, and deploys it once per topology.

| Kind | Diagram | Shows | Specified in |
|---|---|---|---|
| System context | [sharder](#system-context) | sharder, the people who operate it, and the systems it depends on | [RFC-0001](../rfc/0001-architecture-and-core-model.md) |
| Containers | [sharder](#containers) | the libraries, the adapters, the programs and the store | [RFC-0001](../rfc/0001-architecture-and-core-model.md), [RFC-0007](../rfc/0007-membership-store-and-distribution.md), [RFC-0008](../rfc/0008-agent-router-and-budgets.md) |
| Components | [Controller](#controller) | planning, transitions, proposals, the budget and the root | [RFC-0004](../rfc/0004-placement-and-rebalancing.md), [RFC-0006](../rfc/0006-ownership-handoff-and-fencing.md), [RFC-0009](../rfc/0009-horizontal-scaling-with-cells.md) |
| Components | [Agent](#agent) | callbacks, leases, tokens and reports | [RFC-0008](../rfc/0008-agent-router-and-budgets.md) |
| Components | [Router](#router) | positions, the cell map, the route cache and computed routes | [RFC-0003](../rfc/0003-partitioning-strategies.md), [RFC-0008](../rfc/0008-agent-router-and-budgets.md) |
| Deployment | [Embedded](#embedded-topology), [Service](#service-topology), [Leaderless](#leaderless-topology) | where the containers run in each topology | [RFC-0001](../rfc/0001-architecture-and-core-model.md), [RFC-0009](../rfc/0009-horizontal-scaling-with-cells.md) |

## Layout

- Each kind of diagram has its own directory: `context/`, `containers/`, `components/` and `deployment/`.
- A file is named after the element it zooms into, or the topology it deploys.
- Every diagram includes `c4.iuml`, which contains the C4-PlantUML library and the font settings. Deployment diagrams also include `C4_Deployment`.
- Each `.puml` source has its rendered `.svg` beside it.

## System context

![System context of sharder](context/sharder.svg)

- Application servers embed the agent, and embed the controller in the `embedded` topology. Application clients embed the router.
- The application authority takes part only in the `delegated` ownership mode.
- The application implements the fence over its own storage. sharder calls it to acquire each writer's tenure.

## Containers

![Containers of sharder](containers/sharder.svg)

- The controller, the agent and the router are packages of the root module, which imports only core and the standard library.
- Every call between processes goes through a transport adapter: `httptransport` by default, which sends kanon-encoded bodies over HTTP/2, or `grpc`. Calls within one process go through `protocol.Local`.
- The store adapter implements `store.Store` and `membership.Membership` over a client that the embedder built. The store is one root scope per namespace and one scope per cell, and the store adapter maps each scope to a store cluster.
- `sharderctl` sends every operation to the controller that applies it through `Submit`, and a cluster manager calls `Disrupt` and `Done`. The transport adapter authenticates each caller, and operators do not need store credentials.

## Components

### Controller

![Components of the controller](components/controller.svg)

- The controller process that `Campaign` elects leads the namespace's root. Any controller process runs the cell controllers that a root places on it.
- The transition engine commits each step before it calls a node, so a replacement controller continues each transition at its committed step.
- The proposal handler accepts a proposal with its approvers' signatures, or from an operator identity in the process's own configuration, and commits it with its caller's identity. The budget accepts `Disrupt` and `Done` from operator identities, and from a node that disrupts only itself.

### Agent

![Components of the agent](components/agent.svg)

- The callback service refuses a callback from any identity other than the controller that the cell record names, and a callback at a lower shard or controller epoch.
- In the `delegated` mode, handler dispatch also calls the application's `Elector`. The application reports its authority's replicas to the reporter through `Hosted` and `Released`, and its elections and depositions through `Elected` and `Deposed`.

### Router

![Components of the router](components/router.svg)

- In `watch` mode, the default, the route cache contains every route of each cell the router uses. In `lookup` mode it contains only the routes that the router has used. In `compute` mode, the only mode of the leaderless topology, the route function computes each route and nothing is cached.
- A `fixed-slots` namespace caches routes in an array indexed by slot, and every other partitioner in core's copy-on-write B-tree.

## Deployment

### Embedded topology

![Deployment of the embedded topology](deployment/embedded.svg)

- The root is elected among the application server processes, and each cell's controller runs on a node of its cell.

### Service topology

![Deployment of the service topology](deployment/service.svg)

- `sharderd` processes host the roots and the cell controllers of many namespaces.
- The root scope and the scope of cell `c1` share store cluster A, and cell `c2` has its own cluster B, as the store adapter maps them.

### Leaderless topology

![Deployment of the leaderless topology](deployment/leaderless.svg)

- A leaderless namespace has one cell and no controller. Each agent computes placement with the namespace's function placer. The agent that the placer selects as a shard's writer campaigns in the store's election for that shard, and the term of the election is the shard's epoch.

## Rendering

The SVG files are rendered with PlantUML 1.2026.8 and Liberation Sans. PlantUML writes the width of each label into the SVG, measured with its own font, and Liberation Sans has the advance widths of the Arial and Helvetica fonts that the SVG files request.

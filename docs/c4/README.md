# C4 model

The C4 model describes sharder at three levels of zoom, and deploys it once per topology.

| Kind | Diagram | Shows | Specified in |
|---|---|---|---|
| System context | [sharder](#system-context) | sharder, the people who operate it, and the systems it depends on | [RFC-0001](../rfc/0001-architecture-and-core-model.md) |
| Containers | [sharder](#containers) | the libraries, the adapters, the programs and the store | [RFC-0001](../rfc/0001-architecture-and-core-model.md), [RFC-0007](../rfc/0007-membership-store-and-distribution.md), [RFC-0008](../rfc/0008-agent-router-and-budgets.md) |
| Components | [Controller](#controller) | planning, transitions, the budget and the root | [RFC-0004](../rfc/0004-placement-and-rebalancing.md), [RFC-0006](../rfc/0006-ownership-handoff-and-fencing.md), [RFC-0009](../rfc/0009-horizontal-scaling-with-cells.md) |
| Components | [Agent](#agent) | callbacks, leases, tokens and reports | [RFC-0008](../rfc/0008-agent-router-and-budgets.md) |
| Components | [Router](#router) | positions, the cell map and the route cache | [RFC-0003](../rfc/0003-partitioning-strategies.md), [RFC-0008](../rfc/0008-agent-router-and-budgets.md) |
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
- Every call between processes goes through a transport adapter, such as the gRPC adapter or the kanon binary transport. Calls within one process go through `protocol.Local`.
- The store and membership adapters implement `store.Store` and `membership.Membership` over clients that the embedder built. The store is one root scope per namespace and one scope per cell, and the store adapter maps each scope to a store cluster.
- `sharderctl` writes spec proposals to the root scope, and submits signed proposals and operations to controllers.

## Components

### Controller

![Components of the controller](components/controller.svg)

- The controller process that `Campaign` elects leads the namespace's root. Any controller process runs the cell controllers that a root places on it.
- The transition engine commits each step before it calls a node, so a replacement controller continues each transition at its committed step.

### Agent

![Components of the agent](components/agent.svg)

- The callback service refuses a callback from any identity other than the controller that the cell record names, and a callback at a lower shard or controller epoch.
- In the `delegated` mode, handler dispatch also calls the application's `Elector`, and the application reports elections and depositions to the reporter.

### Router

![Components of the router](components/router.svg)

- In `lookup` mode the route cache contains only the routes that the router has used. In `watch` mode it contains every route of each cell the router uses.
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

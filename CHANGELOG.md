# go.thesmos.sh/sharder

## 0.1.0

### Minor Changes

- fa61a8c: Add `config.ParseNamed` to parse the configuration of a strategy that another configuration selects.
- 33c17a6: Add `controller.Controller.Close` to end the root sessions of a controller process before it stops.
- e8997f6: Add package `budget` with the planned disruptions of a cell, their reservations and their kanon codecs.
- 59b20f8: Add package `builtin` with `Support`, `Register`, `Presets` and a constructor of the factory of each built-in strategy of the support matrix.
- a6cd8c3: Add package `config` with namespace specs, the strategy registry, presets, `Resolve` and `Change`.
- c478711: Add package `controller` with `New`, `Config` and `Controller` to run the roots and the cell controllers of a process.
- 7adebb2: Add package `embedded` with `New`, `Config` and `Process` to run the controller, the agents and the routers of an application process.
- da8ef5e: Add package `fence` with fencing tokens and the interfaces of fenced storage.
- e04f313: Add package `handoff` with the protocol contract, scripts on the sides of a transfer and the lifecycle of a transition, and the protocol `handoff/breakbeforemake`.
- e891dcd: Add package `membership` with the views and the sessions of the nodes of a scope, and the conformance kit `membershiptest`.
- 043b0a5: Add package `ownership` with the configuration of exclusive ownership and its lease.
- c79ad32: Add package `partition` with the partitioners `fixed-slots` and `singleton`, the hash `xxhash64` and the conformance kit `partitiontest`.
- bb28a6e: Add package `placement` with the placer contracts and `Validate`, the planner `placement/planner`, the built-in constraints and goals, and the conformance kit `placementtest`.
- de027a2: Add package `proposal` with the proposals of operators and their kanon codecs.
- 99f9ccc: Add package `protocol` with the calls between controllers and agents, the in-process transport `protocol/local` and the conformance kit `protocoltest`.
- f47ca93: Add package `replica` with `New`, `Agent`, `Handler` and `Runner` to host the replicas of the shards of a node.
- 56dec7e: Add package `replication` with replica state models, their compilation into transition tables, and the model `primary-only`.
- 7271b87: Add package `router` with `New`, `Watcher`, `Do` and `NotOwnerError` to route keys to the replicas of their shards.
- 777b897: Add package `shard` with the value types and kanon codecs that every sharder package shares.
- 5617a81: Add package `store` with the contracts and the records of the store, the in-memory store `store/memory` and the conformance kit `storetest`.
- 9de5dd6: Add `replica.Agent.Observed` with `ObservedRequest` and `ObservedResult` for the refusal of a request that the node does not serve.
- 53d1563: Add `shard.Placement.Incarnation` with the incarnation of the node of the assigned writer.
- f2b193a: Add `proposal.MoveArgs`, `NodeArgs` and `ShardArgs` for the bodies of the operations of a cell, and `proposal.Explanation` and `Violation` for the result of an explain.
- 699c72c: Add the JSON keys `reason`, `retryAfter` and `allowed` to `budget.Decision`.
- 3f0053d: Add `partition.Layout` and `partition.Shard`, with `Layout.Locate` to find the shard of a position.
- 6af1234: Add `shard.RoleCount`, the number of roles.
- d901740: Add `shard.Phase`, the phase of a transition, with its kanon codec.
- d901740: Add the text methods `MarshalText` and `UnmarshalText` to `shard.Role`.
- 125f392: Add the text methods `MarshalText` and `UnmarshalText` to `shard.ID`.
- 33c17a6: Make a deferred `Disrupt` with a deadline wait for its admission, for the end of its reservation, or until one second before its deadline.
- 2c3225f: Make `embedded.Process.Close` end the root sessions of the process through `controller.Controller.Close`.
- 125f392: Make `shard.ID` a 16-byte `id.ID128`, and add `shard.ErrID` for bytes that are not the encoding of a shard ID.

### Patch Changes

- 37cbf7d: Let `constraint.Capacity` admit a load that exceeds its limit by at most a billionth of the limit.
- 2266a66: Update `go.thesmos.sh/core` to v0.19.0.

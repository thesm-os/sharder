# go.thesmos.sh/sharder

## 0.2.0

### Minor Changes

- 458ca91: Add `membershiptest.ErrOperation` for a check that calls an operation of the kit on a membership without it.
- f020380: Add a `Parse` function, a `Values` function, the text methods and an `ErrUnknown` sentinel to each exported enumeration.
- 26996e3: Add `placement.State.PeakCount`, `PeakRoleCount` and `placement.InFlight.Role` for the replicas that the transitions in flight can place on a node.
- 355ea94: Add `placementtest.Constrained` and `ErrUnconstrained` for the check that validates the plans of a placer against its constraints.
- 6f0a9e9: Add `placementtest.PlacerCells` to draw the input of each generated call of `Place` from the cells of `Inputs`.
- aee26b2: Add `replica.Revoker` and `Runner.RevokeShard` to end the work of a tenure when the session of its node ends under `stop`.
- f887eaf: Add `storetest.ErrOperation` for a check that calls an operation of the kit on a store without it.
- 5268c3f: Add `storetest.MutationBuilder`, the generated builder of `store.Mutation`.
- f76f51b: Add `configtest.SpecBuilder` and `configtest.SelectionBuilder`, the generated builders of `config.Spec` and `config.Selection`.
- 679f394: Add `shardtest.PlacementBuilder` and `shardtest.NodeBuilder`, the generated builders of `shard.Placement` and `shard.Node`.
- 355ea94: Add `placementtest.States`, `CellMoves`, `ConstraintCells` and `GoalCells` to draw the calls of the suites from the cells of `Inputs`.
- 9d0dc62: Add `fencetest.AcquirerStub`, the generated double of `fence.Acquirer`.
- cec90b9: Add `handofftest.ProtocolStub`, the generated double of `handoff.Protocol`.
- 355ea94: Add `placementtest.StateStub`, the generated double of the methods of `placement.State`.
- aef9a90: Add `routertest.RouterStub`, the generated double of `router.Router`.
- 04e1e28: Add `configtest.ParsedStub`, `FactoryStub` and `ProviderStub`, the generated doubles of `config.Parsed`, `config.Factory` and `config.Provider`.
- 458ca91: Add `membershiptest.MembershipStub` and `SessionStub`, the generated doubles of `membership.Membership` and `membership.Session`.
- 3add429: Add `partitiontest.PartitionerStub` and `partitiontest.HashStub`, the generated doubles of `partition.Partitioner` and `partition.Hash`.
- 355ea94: Add `placementtest.ConstraintStub`, `GoalStub` and `PlacerStub`, the generated doubles of `placement.Constraint`, `placement.Goal` and `placement.Placer`.
- 0642763: Add `protocoltest.ConnectorStub`, `AgentClientStub` and `ControllerClientStub`, the generated doubles of `protocoltest.Connector`, `protocol.AgentClient` and `protocol.ControllerClient`.
- d0d711d: Add `replicatest.HandlerStub`, `RevokerStub`, `LoadReporterStub` and `OpenerStub`, the generated doubles of `replica.Handler`, `replica.Revoker`, `replica.LoadReporter` and `replica.Opener`.
- f887eaf: Add `storetest.StoreStub`, `ReaderStub`, `WriterStub` and `LeadershipStub`, the generated doubles of `store.Store`, `store.Reader`, `store.Writer` and `store.Leadership`.
- 3add429: Add `partitiontest.RunLayoutSuite`, the suite of `partition.Layout` with a budget of no allocation for `Locate`.
- 355ea94: Add `placementtest.RunStateSuite`, the suite of `placement.State` with a budget of no allocation for each reader and the counted check `RepeatedMovesDoNotAllocate` of `Apply`.
- 0642763: Add `protocoltest.RunMetadataSuite`, the suite of `protocol.Metadata` with a budget of no allocation for `Check`.
- aef9a90: Add `routertest.RunWatcherSuite`, the suite of `router.Watcher` with a budget of no allocation for `Route`.
- c6bfe06: Add `shardtest.RunCellMapSuite`, the suite of `shard.CellMap` with a budget of no allocation for `Locate`.
- 02b944b: **Breaking:** Rename `proposal.ErrUnknownKind` to `proposal.ErrUnknownOperationKind`.
- 08a2ba6: **Breaking:** Rename the lag gauge of a router from `sharder.generation.lag` to `sharder.router.refresh.lag`.
- 76e98aa: **Breaking:** Rename `Valid` to `IsValid` on each exported enumeration.
- 458ca91: **Breaking:** Replace `membershiptest.AssertMembership` and `WithLostRenewals` with the generated suite `RunMembershipSuite` and the interfaces `membershiptest.Backed` and `Faulty`.
- 3add429: **Breaking:** Replace `partitiontest.AssertPartitioner`, `AssertHash` and `Vector` with the generated suites `RunPartitionerSuite` and `RunHashSuite`.
- 355ea94: **Breaking:** Replace `placementtest.AssertConstraint`, `AssertConstraintAllocs`, `AssertGoal`, `AssertGoalAllocs` and `AssertPlacer` with the generated suites `RunConstraintSuite`, `RunGoalSuite` and `RunPlacerSuite`.
- 0642763: **Breaking:** Replace `protocoltest.AssertTransport` with the interface `protocoltest.Connector` and its generated suite `RunConnectorSuite`.
- f887eaf: **Breaking:** Replace `storetest.AssertStore` and its options with the generated suite `RunStoreSuite` and the interfaces `storetest.Faulty`, `Deleter` and `Compactor`.
- 4c1801f: **Breaking:** Return an error that wraps the `ErrUnknown` sentinel of its type from the text methods of each exported enumeration.

### Patch Changes

- b8e250d: Admit each move of a pass and each move proposal against the constraints, the budget and the transitions in flight.
- c3f39e1: Classify an error of `controller.Controller.Do` that wraps `context.DeadlineExceeded` as `errs.Transient`.
- aee26b2: Classify an error of `replica.Agent.Do` that wraps `context.DeadlineExceeded` as `errs.Transient`.
- 26996e3: Count the destination and the fallback of each transition in flight in `constraint.MaxPerNode`.
- 5268c3f: Draw each mutation of the generated calls of `storetest.StoreSuite` from the value of `storetest.NewMutationBuilder`.
- 08a2ba6: Leave the `Refresh` calls that a client cancelled out of the lag gauge of a router.
- e57a51f: Repair the drains and the violations in an emergency pass of the planner that restores no replica.
- 08a2ba6: Replace only the routes of the slots whose node a membership view adds, removes or moves.
- 56020ad: Require `go.dokimi.dev/conform` v0.8.0 and `go.dokimi.dev/stub` v0.2.0 for the generated suites and doubles of the kits.
- 56825cc: Require Go 1.27.3.
- 6908ef1: Return the error of encoding/json from `fixedslots.New` in place of discarding it.
- 554b622: Retry a callback, a fence acquisition or a cleanup that ran past its deadline instead of keeping the deadline as its result.
- 6106cd6: Fix the controller to start a pass when a transition ends with its writer on a node that drains.
- 56825cc: Update `go.dokimi.dev/assert` to v0.10.0.
- 56825cc: Update `go.thesmos.sh/core` to v0.35.0.
- 56825cc: Update `go.thesmos.sh/kanon` to v0.3.0.

## 0.1.1

### Patch Changes

- 2849811: Add each node whose live incarnation failed an add of a shard definitively to the `placement.Input.Faults` of the shard.

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

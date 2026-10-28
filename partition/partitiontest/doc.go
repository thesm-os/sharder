// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Package partitiontest contains the conformance suites of partition.Partitioner,
// partition.Hash and partition.Layout, and the doubles of Partitioner and Hash.
//
// Every partitioner and every hash passes the same checks, so a router and the core rely
// on the laws of package partition under each implementation that a namespace
// registers. [RunPartitionerSuite] applies every check of [PartitionerSuite] to a new
// partitioner of a factory, one partitioner for each check.
// [RunPartitionerSuiteFromHandle] builds each partitioner from the handle of its check.
// [RunHashSuite] applies the checks of [HashSuite] to new hashes in the same way. Each
// check belongs to the method that it constrains.
//
// [LayoutSuite] is the suite of the hot path [partition.Layout.Locate]. Its budget
// states that Locate does not allocate.
//
// # Budgets
//
// [RunHashBudgets] applies the allocation ceiling of Sum64, which [HashSuite] states for
// every hash. [RunPartitionerBudgets] applies the ceilings that the subject of a
// partitioner states, such as one allocation for Position of fixed-slots. Each ceiling
// holds for every input that the call generator of its callable draws.
//
// # Planted defects
//
// Each check has a planted defect: the double of its interface over fixed-slots or
// xxhash64, with the calls that break the law of the check replaced. The test of the
// suite runs each check on its defect, and the check must fail at the assertion whose
// message is its statement. Every other check must pass on the defect.
//
// # Doubles
//
// [PartitionerStub] and [HashStub] are recording doubles of [partition.Partitioner] and
// [partition.Hash]. Each method of a double returns the results of a function, fixed
// results or an armed fault.
//
// # Dependency position
//
// partitiontest imports bytes, context, encoding/json, math, math/rand/v2, slices,
// testing and time of the standard library, go.dokimi.dev/assert with its package prop,
// go.dokimi.dev/conform, go.dokimi.dev/stub, and the packages partition,
// partition/fixedslots, partition/xxhash64 and shard of sharder.
package partitiontest

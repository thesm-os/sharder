// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package examples

import (
	"slices"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/proposal"
)

// Kind is the kind of an example: a term of a spec, or an operation of an operator.
type Kind string

// The kinds of the examples.
const (
	// KindOperation is the kind of an example of an operation of an operator: a kind of
	// proposal, or a disruption of the budget of a cell.
	KindOperation Kind = "operation"
	// KindPreset is the kind of an example of a preset: the field preset of a spec.
	KindPreset Kind = "preset"
	// KindTopology is the kind of an example of a topology: the axis topology of a spec.
	KindTopology Kind = "topology"
)

// The parts of a key.
const (
	// separator joins the kind of an example and the name that the example shows.
	separator = "-"
	// disruption is the name of the example of a disruption of the budget of a cell, the one
	// operation that no kind of proposal covers.
	disruption = "disruption"
)

// Keys returns the key of each example of the catalogue, in the order of their directories:
//   - one example for each preset that builtin.Presets returns.
//   - one example for each topology that builtin.Support lists.
//   - one example for each kind of proposal that proposal.OperationKindValues returns, and one
//     for a disruption.
//
// The key of an example is its kind, a hyphen and the name that the example shows, such as
// preset-exclusive-worker or operation-drain.
func Keys() []string {
	presets := builtin.Presets()
	topologies := builtin.Support().Strategies[config.Topology]
	kinds := proposal.OperationKindValues()
	keys := make([]string, 0, len(presets)+len(topologies)+len(kinds)+1)
	for i := range presets {
		keys = append(keys, string(KindPreset)+separator+string(presets[i].Name))
	}
	for _, name := range topologies {
		keys = append(keys, string(KindTopology)+separator+string(name))
	}
	for _, k := range kinds {
		keys = append(keys, string(KindOperation)+separator+k.String())
	}
	keys = append(keys, string(KindOperation)+separator+disruption)
	slices.Sort(keys)
	return keys
}

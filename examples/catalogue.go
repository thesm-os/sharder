// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package examples

import (
	"slices"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
)

// Kind is the kind of an example: a term of a spec.
type Kind string

// The kinds of the examples.
const (
	// KindPreset is the kind of an example of a preset: the field preset of a spec.
	KindPreset Kind = "preset"
	// KindTopology is the kind of an example of a topology: the axis topology of a spec.
	KindTopology Kind = "topology"
)

// separator joins the kind of an example and the registered name that the example shows.
const separator = "-"

// Keys returns the key of each example that the support matrix requires, in the order of
// their directories: one example for each preset that builtin.Presets returns, and one for
// each topology that builtin.Support lists. The key of an example is its kind, a hyphen and
// the registered name that the example shows, such as preset-exclusive-worker.
func Keys() []string {
	presets := builtin.Presets()
	topologies := builtin.Support().Strategies[config.Topology]
	keys := make([]string, 0, len(presets)+len(topologies))
	for i := range presets {
		keys = append(keys, string(KindPreset)+separator+string(presets[i].Name))
	}
	for _, name := range topologies {
		keys = append(keys, string(KindTopology)+separator+string(name))
	}
	slices.Sort(keys)
	return keys
}

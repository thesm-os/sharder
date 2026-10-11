// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config

import (
	"bytes"
	"errors"
	"fmt"
)

// fixedAxes lists the axes other than partitioning whose selection a spec edit cannot
// change. Change checks partitioning together with the mapping.
var fixedAxes = [...]Axis{Replication, Ownership, Handoff, Topology, Routing}

// Change returns nil when a running namespace may move from the resolved spec from to
// the resolved spec to by a spec edit alone. An edit may change the placement, the
// budget, the load model and the cell bounds. Change does not compare the versions of
// the two specs.
//
// Error modes:
//   - an error that classifies as errs.Invalid, with one problem for each other
//     change. Each problem wraps ErrChange, and its text contains the operation that
//     the change needs. A change of the partitioning or of the mapping needs a
//     migration to a new namespace. A change of the namespace, of the preset or of the
//     selection of another axis needs a new namespace.
func Change(from, to *Resolved) error {
	var problems []error
	if from.Spec.Namespace != to.Spec.Namespace {
		problems = append(problems,
			fmt.Errorf("%w: namespace: a change of the namespace needs a new namespace", ErrChange))
	}
	if from.Spec.Preset != to.Spec.Preset {
		problems = append(problems,
			fmt.Errorf("%w: preset: a change of the preset needs a new namespace", ErrChange))
	}
	m, n := from.Mapping, to.Mapping
	moved := m.Partitioner != n.Partitioner || m.Hash != n.Hash || m.HashVersion != n.HashVersion ||
		!bytes.Equal(m.Config, n.Config)
	p, q := from.Spec.Partitioning, to.Spec.Partitioning
	if moved || p.Name != q.Name || !bytes.Equal(p.Config, q.Config) {
		problems = append(problems,
			fmt.Errorf("%w: partitioning: a change of the mapping needs a migration to a new namespace", ErrChange))
	}
	for _, axis := range fixedAxes {
		a, b := from.Spec.selection(axis), to.Spec.selection(axis)
		if a.Name != b.Name || !bytes.Equal(a.Config, b.Config) {
			problems = append(problems,
				fmt.Errorf("%w: %s: a change of the %s needs a new namespace", ErrChange, axis, axis))
		}
	}
	return errors.Join(problems...)
}

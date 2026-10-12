// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package cell

import (
	"bytes"
	"fmt"

	"go.thesmos.sh/sharder/internal/control"
)

// resolve returns the strategies of the accepted spec raw, whose resolution must give the
// mapping that the root recorded. It keeps the cell bounds of a spec whose strategies
// build. The root accepts each spec with the version after the last one, so the last spec
// that the cell resolves is the latest.
//
// Error modes:
//   - an error that wraps the error of control.Resolve for a raw that does not decode or
//     resolve.
//   - an error that wraps control.ErrSpec for a resolved mapping that differs from the
//     mapping that the root recorded.
//   - an error that wraps the error of control.Build for a spec whose strategies do not
//     build.
func (m *Machine) resolve(raw []byte) (*control.Strategies, error) {
	res, err := control.Resolve(m.registry, m.provided, raw)
	if err != nil {
		return nil, fmt.Errorf("cell: the spec of the root scope: %w", err)
	}
	got, want := &res.Mapping, &m.recorded
	if got.Partitioner != want.Partitioner || got.Hash != want.Hash || got.HashVersion != want.HashVersion ||
		!bytes.Equal(got.Config, want.Config) {

		return nil, fmt.Errorf("%w: version %d resolves the mapping of %s, and the root recorded %s",
			control.ErrSpec, res.Spec.Version, got.Partitioner, want.Partitioner)
	}
	s, err := control.Build(m.registry, res)
	if err != nil {
		return nil, fmt.Errorf("cell: the spec of the root scope: %w", err)
	}
	m.bounds = res.Spec.Cells
	return s, nil
}

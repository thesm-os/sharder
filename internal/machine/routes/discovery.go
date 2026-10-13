// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"go.thesmos.sh/core/id/ulid"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/follow"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/shard"
)

// logMapping is the message of the log of an installed namespace whose positions the router
// cannot compute.
const logMapping = "routes: the router cannot compute the positions of the namespace"

// discovery is one read of the namespace: the root scope, and the scope and the views of
// each cell of the cell map. The router installs a discovery once every cell has its first
// snapshot and its first view, and the routes of the discovery then serve until the router
// installs the next one.
type discovery struct {
	// root reads the root scope.
	root *follow.Follower
	// partitioner is the partitioner of the spec, and nil before the discovery has resolved
	// the spec, or for a spec whose positions the router cannot compute.
	partitioner partition.Partitioner
	// err is the error with which Route refuses the namespace, or nil.
	err error
	// spec is the spec of the namespace as JSON, and nil before the root has created the
	// namespace.
	spec []byte
	// listed lists each cell that the root scope has listed, in the order of the reads. The
	// router keeps the first range of each cell.
	listed []shard.Cell
	// cells lists the read of each cell of listed, in the same order, once the discovery has
	// resolved the spec.
	cells []*cell
	// mapping is the mapping that the root recorded.
	mapping shard.Mapping
	// incarnation is the namespace incarnation, and zero before the root has created the
	// namespace.
	incarnation shard.NamespaceIncarnation
	// resolved is true once the discovery has resolved the spec.
	resolved bool
}

// newDiscovery returns a discovery whose follower reads the root scope.
//
// Error modes:
//   - an error that wraps the error of follow.New.
func (m *Machine) newDiscovery() (*discovery, error) {
	root, err := follow.New(&follow.Config{Rand: m.rand, Tokens: &m.tokens, Scope: shard.Scope{Namespace: m.namespace}})
	if err != nil {
		return nil, fmt.Errorf("routes: the root scope of %s: %w", m.namespace, err)
	}
	return &discovery{root: root}, nil
}

// rooted takes what the follower of the root scope of the discovery d passed. The last page
// of a snapshot and each later generation can complete the namespace and add cells to its
// cell map.
//
// Error modes:
//   - the error of lost for the error that stopped the follower.
//   - the error of discover.
func (m *Machine) rooted(d *discovery, res *follow.Result, out []machine.Effect) ([]machine.Effect, error) {
	if res.Err != nil {
		return m.lost(d, fmt.Errorf("routes: the root scope of %s: %w", m.namespace, res.Err), out)
	}
	if p := res.Page; p != nil {
		for i := range p.Placements {
			d.list(&p.Placements[i])
		}
		if !p.Last {
			return out, nil
		}
		d.spec, d.mapping, d.incarnation = p.State.Spec, p.State.Mapping, p.State.Head.Incarnation
		return m.discover(d, out)
	}
	mu := &res.Event.Mutation
	if mu.Spec != nil {
		d.spec = mu.Spec
	}
	if mu.Mapping != nil {
		d.mapping = *mu.Mapping
	}
	if d.incarnation.IsZero() {
		d.incarnation = mu.ID.Incarnation
	}
	for i := range mu.Put {
		d.list(&mu.Put[i])
	}
	return m.discover(d, out)
}

// list adds the cell of the placement p of the root scope to the cell map of d, when the map
// does not have it. A cell is a shard of the root, and its ID is the canonical text of the
// ID of that shard.
func (d *discovery) list(p *shard.Placement) {
	id := shard.CellID(ulid.Format(p.Shard.ID()))
	if !slices.ContainsFunc(d.listed, func(c shard.Cell) bool { return c.ID == id }) {
		d.listed = append(d.listed, shard.Cell{ID: id, Range: p.Range})
	}
}

// discover resolves the spec once the root has created the namespace: once the root scope
// has the spec and a cell. It then starts the reads of each cell that the discovery does not
// read yet. A rediscovery that finds the installed incarnation ends, and a rediscovery whose
// spec does not apply installs the namespace without its cells.
//
// Error modes:
//   - the error of resolve, for the first read of the namespace.
//   - the error of newCell.
//   - the error of progress.
func (m *Machine) discover(d *discovery, out []machine.Effect) ([]machine.Effect, error) {
	if d.spec == nil || len(d.listed) == 0 {
		return out, nil
	}
	if !d.resolved {
		if m.current != nil && d.incarnation == m.current.incarnation {
			return m.settled(out)
		}
		d.resolved = true
		d.partitioner, d.err = resolve(m.registry, m.provided, d.spec, &d.mapping)
		if d.err != nil {
			if m.current == nil {
				return out, d.err
			}
			m.logger.LogAttrs(context.Background(), slog.LevelError, logMapping, slog.Any(attrError, d.err))
			return m.install(d, out)
		}
	}
	if d.partitioner == nil {
		return out, nil
	}
	for _, l := range d.listed[len(d.cells):] {
		c, err := m.newCell(d, &l)
		if err != nil {
			return out, err
		}
		d.cells = append(d.cells, c)
		out = m.watchViews(d, c, c.follower.Start(out))
	}
	return m.progress(d, out)
}

// resolve returns the partitioner of the spec raw, as the root stores it, resolved with the
// registry r and the capabilities provided. The mapping of the partitioner must be the
// mapping recorded.
//
// Error modes:
//   - an error that wraps ErrMapping and the error of encoding/json for a raw that does not
//     decode.
//   - an error that wraps ErrMapping and the error of config.Resolve or config.Build for a
//     spec whose partitioner r does not build.
//   - an error that wraps ErrMapping for a partitioner whose mapping is not recorded.
func resolve(r *config.Registry, provided map[config.Axis][]config.Capability, raw []byte, recorded *shard.Mapping) (
	partition.Partitioner, error,
) {
	var spec config.Spec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return nil, fmt.Errorf("%w: the spec does not decode: %w", ErrMapping, err)
	}
	res, err := config.Resolve(r, &spec, provided)
	var p partition.Partitioner
	if err == nil {
		p, err = config.Build[partition.Partitioner](r, res, config.Partitioning)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: the spec of namespace %s: %w", ErrMapping, spec.Namespace, err)
	}
	got := p.Mapping()
	if got.Partitioner != recorded.Partitioner || got.Hash != recorded.Hash ||
		got.HashVersion != recorded.HashVersion ||
		!bytes.Equal(got.Config, recorded.Config) {

		return nil, fmt.Errorf(
			"%w: version %d of the spec of namespace %s computes the mapping of %s, and the root recorded %s",
			ErrMapping,
			res.Spec.Version,
			spec.Namespace,
			got.Partitioner,
			recorded.Partitioner,
		)
	}
	return p, nil
}

// progress takes a change of the discovery d: the first snapshot or the first view of a
// cell, a snapshot that replaces the routes of a cell, or a view. The router installs an
// incoming discovery once it is complete, and publishes the routes of the installed one
// again.
//
// Error modes:
//   - the error of install.
func (m *Machine) progress(d *discovery, out []machine.Effect) ([]machine.Effect, error) {
	if d == m.current {
		m.publish()
		return m.settle(out), nil
	}
	if !d.complete() {
		return out, nil
	}
	return m.install(d, out)
}

// complete reports whether every cell of d has its first snapshot and its first view.
func (d *discovery) complete() bool {
	for _, c := range d.cells {
		if c.cache == nil || c.addresses == nil {
			return false
		}
	}
	return true
}

// install makes d the installed discovery. It ends the reads of the discovery that it
// replaces, publishes the routes of d, and replies to each Started that waits.
//
// Error modes:
//   - the error of fsm.Machine.Fire.
func (m *Machine) install(d *discovery, out []machine.Effect) ([]machine.Effect, error) {
	if old := m.current; old != nil {
		out = old.stop(out)
		old.release()
	}
	m.current, m.incoming = d, nil
	m.publish()
	return m.settle(m.answer(nil, out)), m.fire(eventRead)
}

// settled ends the incoming discovery, which has found the installed incarnation. The
// installed routes keep serving.
//
// Error modes:
//   - the error of fsm.Machine.Fire.
func (m *Machine) settled(out []machine.Effect) ([]machine.Effect, error) {
	out = m.incoming.stop(out)
	m.incoming = nil
	return m.settle(out), m.fire(eventRead)
}

// publish publishes the table of the installed discovery. The table has each cell whose
// first snapshot has arrived, in the order of their ranges, with its cache. A cell has no
// route before its first view, because a route lists only the nodes of a view. publish
// costs O(n log n) for the n cells of the discovery.
func (m *Machine) publish() {
	d := m.current
	t := &table{
		partitioner: d.partitioner,
		err:         d.err,
		caches:      make(map[shard.CellID]*cache, len(d.cells)),
		cells:       shard.CellMap{Namespace: m.namespace, Incarnation: d.incarnation},
	}
	for i, c := range d.cells {
		if c.cache != nil {
			t.cells.Cells = append(t.cells.Cells, d.listed[i])
			t.caches[d.listed[i].ID] = c.cache
		}
	}
	slices.SortFunc(t.cells.Cells, func(a, b shard.Cell) int { return bytes.Compare(a.Range.Start, b.Range.Start) })
	m.published.Store(t)
}

// lost takes err, the error that ended a read of the discovery d. shard.ErrIncarnation
// reports a namespace that the root has created again, so the router reads the namespace
// again. Any other error stops the router.
//
// Error modes:
//   - err, when it does not wrap shard.ErrIncarnation.
//   - the errors of reread and rediscover.
func (m *Machine) lost(d *discovery, err error, out []machine.Effect) ([]machine.Effect, error) {
	switch {
	case !errors.Is(err, shard.ErrIncarnation):
		return out, err
	case d == m.incoming:
		return m.reread(out)
	default:
		return m.rediscover(out)
	}
}

// rediscover starts a read of the namespace while the installed routes serve. When a read of
// the namespace runs already, rediscover does not start another one, and the read that runs
// is the rediscovery.
//
// Error modes:
//   - the error of reread.
//   - the error of fsm.Machine.Fire.
func (m *Machine) rediscover(out []machine.Effect) ([]machine.Effect, error) {
	if m.incoming != nil {
		return out, nil
	}
	out, err := m.reread(out)
	return out, errors.Join(err, m.fire(eventRediscover))
}

// reread ends the incoming discovery, if one runs, and starts a new one in its place.
//
// Error modes:
//   - the error of newDiscovery.
func (m *Machine) reread(out []machine.Effect) ([]machine.Effect, error) {
	if old := m.incoming; old != nil {
		out = old.stop(out)
		old.release()
	}
	d, err := m.newDiscovery()
	//dokimi:mutate-skip ror-true: New has checked the Rand and the namespace that follow.New refuses, so newDiscovery does not fail here
	if err == nil {
		m.incoming = d
		out = d.root.Start(out)
	}
	return out, err
}

// stop appends the end of each read of d to out: the Unwatch of the follower of the root
// scope, and of the follower and the watch of the views of each cell.
func (d *discovery) stop(out []machine.Effect) []machine.Effect {
	out = d.root.Stop(out)
	for _, c := range d.cells {
		out = c.follower.Stop(out)
		if c.views != 0 {
			out = append(out, machine.Unwatch{Token: c.views})
		}
	}
	return out
}

// release ends the instruments of each cell of d.
func (d *discovery) release() {
	for _, c := range d.cells {
		c.metrics.release()
	}
}

// cell returns the read of the cell id, and nil when the cell map of d does not have it.
func (d *discovery) cell(id shard.CellID) *cell {
	for _, c := range d.cells {
		if c.scope.Cell == id {
			return c
		}
	}
	return nil
}

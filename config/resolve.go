// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// The defaults of the fields of a spec that the spec and its preset leave empty.
const (
	// defaultMaxUnavailable is the default of Budget.MaxUnavailable. A move of
	// primary-only takes one replica of a shard down for its writer gap.
	defaultMaxUnavailable = 1
	// defaultMaxConcurrentNodes is the default of Budget.MaxConcurrentNodes.
	defaultMaxConcurrentNodes = 1
	// defaultMaxTransitions is the default of Budget.MaxTransitions.
	defaultMaxTransitions = 64
	// defaultMaxNodeTransitions is the default of Budget.MaxNodeTransitions.
	defaultMaxNodeTransitions = 16
	// defaultWindow is the default of LoadModel.Window.
	defaultWindow = shard.Duration(time.Minute)
	// defaultMaxNodes is the default of CellPolicy.MaxNodes.
	defaultMaxNodes = 5000
	// defaultMaxShards is the default of CellPolicy.MaxShards.
	defaultMaxShards = 1000000
)

// Resolved is the result of Resolve. It contains the complete spec, the parsed
// configuration of every axis of the spec and the mapping.
//
// # Concurrency
//
// Resolve returns a Resolved that nothing else refers to. A Resolved is safe for
// concurrent reads.
type Resolved struct {
	// Configs maps each axis of a spec to the configuration that its factory parsed.
	Configs map[Axis]Parsed
	// Mapping is the mapping of the resolved partitioner. Resolve builds the
	// partitioner to read it.
	Mapping shard.Mapping
	// Spec is the spec with every field filled. The configuration of each selection
	// contains every default. The root stores this spec.
	Spec Spec
}

// Resolve fills spec from its preset and the defaults, and parses and validates every
// selection with its factory. It builds the partitioner to read the mapping, and the
// replica state model to read its roles. Resolve checks the result against these
// rules:
//
//   - Every selection is in the support matrix of r.
//   - Every strategy has the capabilities that it requires from the other axes.
//     provided maps the roles Store and Membership to the capabilities of their
//     adapters.
//   - The namespace has every capability that the support matrix requires.
//   - The budget, the load model and the cell bounds are in range.
//   - Every drain role is a role of the model.
//
// Resolve fills an empty DrainRoles with the writer roles of the model.
//
// Resolve merges the configuration of a selection over the configuration of the preset
// when the selection has no name or the name of the strategy of the preset. The merge
// is the JSON Merge Patch of RFC 7396. It merges two objects member by member, and
// deletes a member whose value in the patch is null. Every other value of the patch
// replaces the value of the preset. A merge whose result is null gives the default
// configuration of the strategy.
//
// Resolve starts a selection of another strategy from the defaults of that strategy.
// It parses no selection outside the support matrix.
//
// Error modes:
//   - an error that classifies as errs.Unsupported, with one problem for each
//     selection outside the support matrix of r and for each capability of the matrix
//     that the namespace does not have. Each problem wraps ErrUnsupported.
//   - otherwise, an error that classifies as errs.Invalid, with one problem for each
//     other rule that the spec breaks. Each problem wraps ErrSpec, ErrUnknown or
//     ErrRegistration. Its text contains the place of the problem in the spec, such
//     as budget.maxTransitions.
func Resolve(r *Registry, spec *Spec, provided map[Axis][]Capability) (*Resolved, error) {
	r.mu.RLock()
	preset, known := r.presets[spec.Preset]
	r.mu.RUnlock()
	z := resolution{
		registry: r,
		res: &Resolved{
			Configs: make(map[Axis]Parsed, len(specAxes)),
			Spec:    Spec{Namespace: spec.Namespace, Preset: spec.Preset, Version: spec.Version},
		},
		provides: map[Axis][]Capability{Store: provided[Store], Membership: provided[Membership]},
	}
	if spec.Namespace == "" {
		z.invalid = append(z.invalid, fmt.Errorf("%w: namespace: the spec does not have a namespace", ErrSpec))
	}
	if spec.Preset == "" {
		z.invalid = append(z.invalid, fmt.Errorf("%w: preset: the spec does not have a preset", ErrSpec))
	} else if !known {
		z.invalid = append(z.invalid, fmt.Errorf("%w: preset %q", ErrUnknown, spec.Preset))
	}
	for _, axis := range specAxes {
		z.selection(axis, spec.selection(axis), preset.Spec.selection(axis))
	}
	z.budget(&spec.Budget, &preset.Spec.Budget)
	z.load(&spec.Load, &preset.Spec.Load)
	z.cells(spec.Cells, preset.Spec.Cells)
	if len(z.res.Configs) == len(specAxes) {
		z.capabilities()
		z.build()
	}
	if len(z.unsupported) > 0 {
		return nil, errors.Join(z.unsupported...)
	}
	if len(z.invalid) > 0 {
		return nil, errors.Join(z.invalid...)
	}
	return z.res, nil
}

// resolution is the state of one call of Resolve. It contains the result so far and
// the problems of the spec.
type resolution struct {
	registry *Registry
	res      *Resolved
	// provides maps each axis and each role of an adapter to the capabilities that its
	// strategy or its adapter provides.
	provides map[Axis][]Capability
	// requires lists the requirements of the resolved strategies.
	requires []required
	// unsupported lists the problems with the support matrix.
	unsupported []error
	// invalid lists every other problem.
	invalid []error
}

// required is one requirement of the strategy that a spec selects on axis.
type required struct {
	Requirement
	axis Axis
}

// selection resolves the selection sel of the spec on axis over the selection pre of
// the preset. It records the expanded selection, its configuration and its
// capabilities in the result, or the problem that stops it.
func (z *resolution) selection(axis Axis, sel, pre *Selection) {
	name := cmp.Or(sel.Name, pre.Name)
	if name == "" {
		z.invalid = append(z.invalid, fmt.Errorf("%w: %s: no strategy is selected", ErrSpec, axis))
		return
	}
	support := z.registry.support
	if !slices.Contains(support.Open, axis) && !slices.Contains(support.Strategies[axis], name) {
		z.unsupported = append(z.unsupported, fmt.Errorf("%w: %s %q", ErrUnsupported, axis, name))
		return
	}
	e, ok := z.registry.entryOf(axis, name)
	if !ok {
		z.invalid = append(z.invalid, fmt.Errorf("%w: %s %q", ErrUnknown, axis, name))
		return
	}
	place := axis.String() + ".config"
	raw := sel.Config
	if name == pre.Name {
		merged, err := mergePatch(pre.Config, sel.Config)
		if err != nil {
			z.invalid = append(z.invalid, fmt.Errorf("%w: %s: %w", ErrSpec, place, err))
			return
		}
		raw = merged
	}
	cfg, err := parse(e.parser, raw, place)
	if err != nil {
		z.invalid = append(z.invalid, err)
		return
	}
	expanded, err := json.Marshal(cfg)
	if err != nil {
		z.invalid = append(z.invalid, fmt.Errorf("%w: %s: %w", ErrSpec, place, err))
		return
	}
	*z.res.Spec.selection(axis) = Selection{Name: name, Config: expanded}
	z.res.Configs[axis] = cfg
	caps := e.parser.Capabilities(cfg)
	z.provides[axis] = caps.Provides
	for _, req := range caps.Requires {
		z.requires = append(z.requires, required{Requirement: req, axis: axis})
	}
}

// budget resolves the budget spec of the spec over the budget pre of the preset. build
// fills an empty DrainRoles with the writer roles of the model.
func (z *resolution) budget(spec, pre *Budget) {
	b := &z.res.Spec.Budget
	b.MaxUnavailable = z.count("budget.maxUnavailable", spec.MaxUnavailable, pre.MaxUnavailable,
		defaultMaxUnavailable, 0)
	b.MaxConcurrentNodes = z.count("budget.maxConcurrentNodes", spec.MaxConcurrentNodes, pre.MaxConcurrentNodes,
		defaultMaxConcurrentNodes, 0)
	b.MaxTransitions = z.count("budget.maxTransitions", spec.MaxTransitions, pre.MaxTransitions,
		defaultMaxTransitions, 1)
	b.MaxNodeTransitions = z.count("budget.maxNodeTransitions", spec.MaxNodeTransitions, pre.MaxNodeTransitions,
		defaultMaxNodeTransitions, 1)
	b.NodeCallbacks = z.count("budget.nodeCallbacks", spec.NodeCallbacks, pre.NodeCallbacks,
		*b.MaxNodeTransitions, 1)
	b.Interruption = cmp.Or(spec.Interruption, pre.Interruption)
	if !b.Interruption.Valid() {
		z.invalid = append(z.invalid, fmt.Errorf(
			"%w: budget.interruption: %v is not %v, which every handoff of the support matrix needs",
			ErrSpec, b.Interruption, PlannedInterruption))
	}
	b.DrainRoles = slices.Clone(spec.DrainRoles)
	if len(b.DrainRoles) == 0 {
		b.DrainRoles = slices.Clone(pre.DrainRoles)
	}
}

// count returns the value of one count of the budget. It returns the value of the
// spec, else the value of the preset, else def. It records a problem at path for a
// value below least.
func (z *resolution) count(path string, spec, pre *int, def, least int) *int {
	v := def
	if pre != nil {
		v = *pre
	}
	if spec != nil {
		v = *spec
	}
	if v < least {
		z.invalid = append(z.invalid, fmt.Errorf("%w: %s: %d is below %d", ErrSpec, path, v, least))
	}
	return &v
}

// load resolves the load model spec of the spec over the load model pre of the
// preset. It checks a window before it applies the default.
func (z *resolution) load(spec, pre *LoadModel) {
	l := &z.res.Spec.Load
	l.Window = cmp.Or(spec.Window, pre.Window)
	if l.Window < 0 {
		z.invalid = append(z.invalid, fmt.Errorf("%w: load.window: %v is negative", ErrSpec, time.Duration(l.Window)))
	}
	l.Window = cmp.Or(l.Window, defaultWindow)
	l.Metrics = slices.Clone(spec.Metrics)
	if len(l.Metrics) == 0 {
		l.Metrics = slices.Clone(pre.Metrics)
	}
	for i, m := range l.Metrics {
		if m.Name == "" {
			z.invalid = append(z.invalid,
				fmt.Errorf("%w: load.metrics[%d].name: the metric does not have a name", ErrSpec, i))
			continue
		}
		earlier := l.Metrics[:i]
		if slices.ContainsFunc(earlier, func(o Metric) bool { return o.Name == m.Name }) {
			z.invalid = append(z.invalid,
				fmt.Errorf("%w: load.metrics[%d].name: %q is listed twice", ErrSpec, i, m.Name))
		}
	}
}

// cells resolves the cell bounds spec of the spec over the cell bounds pre of the
// preset. It checks each bound before it applies the default.
func (z *resolution) cells(spec, pre CellPolicy) {
	c := &z.res.Spec.Cells
	c.MaxNodes = cmp.Or(spec.MaxNodes, pre.MaxNodes)
	if c.MaxNodes < 0 {
		z.invalid = append(z.invalid, fmt.Errorf("%w: cells.maxNodes: %d is negative", ErrSpec, c.MaxNodes))
	}
	c.MaxNodes = cmp.Or(c.MaxNodes, defaultMaxNodes)
	c.MaxShards = cmp.Or(spec.MaxShards, pre.MaxShards)
	if c.MaxShards < 0 {
		z.invalid = append(z.invalid, fmt.Errorf("%w: cells.maxShards: %d is negative", ErrSpec, c.MaxShards))
	}
	c.MaxShards = cmp.Or(c.MaxShards, defaultMaxShards)
}

// capabilities checks every requirement of the resolved strategies, and every
// capability that the support matrix requires.
func (z *resolution) capabilities() {
	for _, req := range z.requires {
		if !slices.Contains(z.provides[req.Axis], req.Capability) {
			z.invalid = append(z.invalid, fmt.Errorf("%w: %s requires %v from %s: %s",
				ErrSpec, req.axis, req.Capability, req.Axis, req.Reason))
		}
	}
	for _, c := range z.registry.support.Requires {
		has := false
		for _, provides := range z.provides {
			has = has || slices.Contains(provides, c)
		}
		if !has {
			z.unsupported = append(z.unsupported,
				fmt.Errorf("%w: the namespace does not have the capability %v", ErrUnsupported, c))
		}
	}
}

// build builds the partitioner and reads its mapping. It then builds the replica state
// model. It fills an empty DrainRoles with the writer roles of the model, and checks
// every role of any other DrainRoles against the model.
func (z *resolution) build() {
	p, err := Build[partition.Partitioner](z.registry, z.res, Partitioning)
	switch {
	case err != nil:
		z.invalid = append(z.invalid, err)
	case p == nil:
		z.invalid = append(z.invalid, fmt.Errorf("%w: partitioning: the factory built no partitioner", ErrSpec))
	default:
		z.res.Mapping = p.Mapping()
	}
	m, err := Build[*replication.Compiled](z.registry, z.res, Replication)
	if err != nil {
		z.invalid = append(z.invalid, err)
		return
	}
	if m == nil {
		z.invalid = append(z.invalid, fmt.Errorf("%w: replication: the factory built no model", ErrSpec))
		return
	}
	b := &z.res.Spec.Budget
	if len(b.DrainRoles) == 0 {
		for _, role := range m.Model.Roles {
			if role.Writer {
				b.DrainRoles = append(b.DrainRoles, role.Name)
			}
		}
		return
	}
	for i, d := range b.DrainRoles {
		if !slices.ContainsFunc(m.Model.Roles, func(role replication.Role) bool { return role.Name == d }) {
			z.invalid = append(z.invalid, fmt.Errorf("%w: budget.drainRoles[%d]: %v is not a role of %s",
				ErrSpec, i, d, m.Model.Name))
		}
	}
}

// mergePatch returns the JSON Merge Patch of RFC 7396 of patch over target. It returns
// target for an empty patch. An empty target is an absent value. mergePatch returns
// nil for a result of null, and keeps the text of every number.
//
// Error modes:
//   - an error that wraps the error of encoding/json when target or patch is not
//     JSON.
//   - an error when target or patch continues after its JSON value.
func mergePatch(target, patch json.RawMessage) (json.RawMessage, error) {
	if len(patch) == 0 {
		return target, nil
	}
	p, err := decodeJSON(patch)
	if err != nil {
		return nil, err
	}
	var t any
	if len(target) > 0 {
		if t, err = decodeJSON(target); err != nil {
			return nil, err
		}
	}
	merged := mergeValue(t, p)
	if merged == nil {
		return nil, nil
	}
	out, err := json.Marshal(merged)
	return out, jsonError(err)
}

// decodeJSON decodes the one JSON value of data. It decodes numbers as json.Number.
//
// Error modes:
//   - an error that wraps the error of encoding/json when data is not JSON.
//   - an error of its own when data continues after the value.
func decodeJSON(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, jsonError(err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("config: data follows the JSON value")
	}
	return v, nil
}

// jsonError returns nil for a nil err, and an error that wraps err for any other.
// mergePatch and decodeJSON return the errors of encoding/json through it.
func jsonError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("config: JSON: %w", err)
}

// mergeValue applies the decoded value patch to the decoded value target with the
// MergePatch function of section 2 of RFC 7396. It changes and returns target when
// both are objects.
func mergeValue(target, patch any) any {
	p, ok := patch.(map[string]any)
	if !ok {
		return patch
	}
	t, ok := target.(map[string]any)
	if !ok {
		t = make(map[string]any, len(p))
	}
	for name, value := range p {
		if value == nil {
			delete(t, name)
			continue
		}
		t[name] = mergeValue(t[name], value)
	}
	return t
}

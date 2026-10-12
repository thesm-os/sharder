// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sync"

	"go.thesmos.sh/sharder/shard"
)

// Parsed is the configuration of a strategy after its factory parsed it. encoding/json
// encodes a Parsed as its complete configuration with every default. The Parse of that
// encoding returns an equal value. The resolved spec contains that encoding.
type Parsed interface {
	// Validate returns every invalid field joined with errors.Join, or nil for a
	// valid configuration.
	Validate() error
}

// Factory builds the strategies of type T. T is the interface of an axis, such as
// partition.Partitioner.
//
// # Concurrency
//
// A Factory is safe for concurrent use.
type Factory[T any] interface {
	// Name returns the name with which a spec selects the strategy. The name is
	// unique on its axis.
	Name() shard.StrategyName
	// Parse decodes raw into the configuration of the strategy and applies its
	// defaults. It returns the default configuration for an empty raw, and an error
	// for an unknown field. A field that is absent from raw gets its default. A field
	// that is present in raw keeps its value, also when the value is an empty list.
	Parse(raw json.RawMessage) (Parsed, error)
	// Capabilities returns the capabilities that the strategy provides and requires
	// with the configuration cfg. Parse returned cfg, and Validate accepted it.
	Capabilities(cfg Parsed) Capabilities
	// New builds a strategy from a configuration that Parse returned and Validate
	// accepted.
	New(cfg Parsed) (T, error)
}

// parser is the part of a Factory that does not depend on its type parameter. Resolve
// calls it for every axis.
type parser interface {
	Parse(raw json.RawMessage) (Parsed, error)
	Capabilities(cfg Parsed) Capabilities
}

// entry is one registered factory.
type entry struct {
	// factory is the Factory that Register received. Build and BuildNamed assert it to
	// the Factory of their type parameter.
	factory any
	// parser is the same factory, through the methods that do not depend on its type
	// parameter.
	parser parser
}

// Support is the support matrix of a registry. It contains the strategies that a spec
// may select, the axes on which a spec may select any registered name, and the
// capabilities that every namespace has to have.
//
// # Concurrency
//
// A copy of a Support shares its map and its slices with the original. NewRegistry
// copies them.
type Support struct {
	// Strategies maps each axis to the names that a spec may select on it. Resolve
	// returns an error that wraps ErrUnsupported for any other name, also for a
	// registered one.
	Strategies map[Axis][]shard.StrategyName
	// Open lists the axes on which a spec may select any registered name. The names
	// that an application registers are included.
	Open []Axis
	// Requires lists the capabilities that every resolved namespace has to have.
	Requires []Capability
}

// Registry contains registered strategies and presets, and the support matrix that
// Resolve checks. The package has no global registry. An application creates a
// registry and passes it to the controller. The names of one registry are independent
// of the names of another.
//
// # Concurrency
//
// A Registry is safe for concurrent use. Resolve, Build and BuildNamed call the
// methods of factories without a lock of the registry, so a factory can call
// BuildNamed.
type Registry struct {
	factories map[Axis]map[shard.StrategyName]entry
	presets   map[PresetName]Preset
	support   Support
	mu        sync.RWMutex
}

// NewRegistry returns an empty registry whose support matrix is a copy of s.
func NewRegistry(s Support) *Registry {
	strategies := make(map[Axis][]shard.StrategyName, len(s.Strategies))
	for axis, names := range s.Strategies {
		strategies[axis] = slices.Clone(names)
	}
	return &Registry{
		factories: make(map[Axis]map[shard.StrategyName]entry),
		presets:   make(map[PresetName]Preset),
		support: Support{
			Strategies: strategies,
			Open:       slices.Clone(s.Open),
			Requires:   slices.Clone(s.Requires),
		},
	}
}

// Register adds f to r on axis.
//
// Error modes:
//   - an error that wraps ErrRegistration when axis is not one of the ten axes of
//     strategies, when f is nil, or when f has no name.
//   - an error that wraps ErrDuplicate when axis already has a factory of the name of
//     f.
func Register[T any](r *Registry, axis Axis, f Factory[T]) error {
	if axis < Partitioning || axis > Hash {
		return fmt.Errorf("%w: %s takes no strategies", ErrRegistration, axis)
	}
	if f == nil {
		return fmt.Errorf("%w: a nil factory on %s", ErrRegistration, axis)
	}
	name := f.Name()
	if name == "" {
		return fmt.Errorf("%w: a factory without a name on %s", ErrRegistration, axis)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	names, ok := r.factories[axis]
	if !ok {
		names = make(map[shard.StrategyName]entry)
		r.factories[axis] = names
	}
	if _, ok := names[name]; ok {
		return fmt.Errorf("%w: %s %q", ErrDuplicate, axis, name)
	}
	names[name] = entry{factory: f, parser: f}
	return nil
}

// Build returns a strategy of the selection of res on axis. The factory of the
// selection builds it from the configuration in res.
//
// Error modes:
//   - an error that wraps ErrRegistration when axis has no selection in a spec, or
//     when the factory of the selection does not build a T.
//   - an error that wraps ErrUnknown when r has no factory of the selection on axis.
//   - an error that wraps ErrSpec and the error of New when New fails.
func Build[T any](r *Registry, res *Resolved, axis Axis) (T, error) {
	var zero T
	sel := res.Spec.selection(axis)
	if sel == nil {
		return zero, fmt.Errorf("%w: %s has no selection in a spec", ErrRegistration, axis)
	}
	f, err := factoryOf[T](r, axis, sel.Name)
	if err != nil {
		return zero, err
	}
	s, err := f.New(res.Configs[axis])
	if err != nil {
		return zero, fmt.Errorf("%w: %s %q: %w", ErrSpec, axis, sel.Name, err)
	}
	return s, nil
}

// BuildNamed parses raw with the factory registered as name on axis, and returns the
// strategy that the factory builds from it. The factories of placers and partitioners
// call it to build constraints, goals and hash functions.
//
// Error modes:
//   - an error that wraps ErrRegistration when axis is not one of the ten axes of
//     strategies, or when the factory does not build a T.
//   - an error that wraps ErrUnknown when axis has no factory of the name.
//   - an error that wraps ErrSpec and the error of the factory when Parse, Validate
//     or New fails, or when Parse returns no configuration.
func BuildNamed[T any](r *Registry, axis Axis, name shard.StrategyName, raw json.RawMessage) (T, error) {
	var zero T
	if axis < Partitioning || axis > Hash {
		return zero, fmt.Errorf("%w: %s takes no strategies", ErrRegistration, axis)
	}
	f, err := factoryOf[T](r, axis, name)
	if err != nil {
		return zero, err
	}
	cfg, err := parse(f, raw, fmt.Sprintf("%s %q", axis, name))
	if err != nil {
		return zero, err
	}
	s, err := f.New(cfg)
	if err != nil {
		return zero, fmt.Errorf("%w: %s %q: %w", ErrSpec, axis, name, err)
	}
	return s, nil
}

// ParseNamed parses raw with the factory registered as name on axis, and returns the
// configuration that Validate accepts. The factories of placers and partitioners call
// it to parse the configurations of the constraints, goals and hash functions that
// their own configurations select. The encoding of the result contains every default.
//
// Error modes:
//   - an error that wraps ErrRegistration when axis is not one of the ten axes of
//     strategies.
//   - an error that wraps ErrUnknown when axis has no factory of the name.
//   - an error that wraps ErrSpec and the error of the factory when Parse or Validate
//     fails, or when Parse returns no configuration.
func ParseNamed(r *Registry, axis Axis, name shard.StrategyName, raw json.RawMessage) (Parsed, error) {
	if axis < Partitioning || axis > Hash {
		return nil, fmt.Errorf("%w: %s takes no strategies", ErrRegistration, axis)
	}
	e, ok := r.entryOf(axis, name)
	if !ok {
		return nil, fmt.Errorf("%w: %s %q", ErrUnknown, axis, name)
	}
	return parse(e.parser, raw, fmt.Sprintf("%s %q", axis, name))
}

// factoryOf returns the factory registered as name on axis, as a Factory[T]. It
// returns an error that wraps ErrUnknown when r has no such factory, and one that
// wraps ErrRegistration when the factory does not build a T.
func factoryOf[T any](r *Registry, axis Axis, name shard.StrategyName) (Factory[T], error) {
	e, ok := r.entryOf(axis, name)
	if !ok {
		return nil, fmt.Errorf("%w: %s %q", ErrUnknown, axis, name)
	}
	f, ok := e.factory.(Factory[T])
	if !ok {
		return nil, fmt.Errorf("%w: %s %q does not build a %v", ErrRegistration, axis, name, reflect.TypeFor[T]())
	}
	return f, nil
}

// parse returns the configuration that p parses from raw, after Validate accepts it.
// Resolve and BuildNamed call it. Its errors contain place as the location of raw, such
// as "placement.config".
//
// Error modes:
//   - an error that wraps ErrSpec and the error of Parse or Validate when either fails.
//   - an error that wraps ErrSpec when Parse returns no configuration.
func parse(p parser, raw json.RawMessage, place string) (Parsed, error) {
	cfg, err := p.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrSpec, place, err)
	}
	if cfg == nil {
		return nil, fmt.Errorf("%w: %s: the factory parsed no configuration", ErrSpec, place)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrSpec, place, err)
	}
	return cfg, nil
}

// entryOf returns the factory registered as name on axis, and whether r has it. It
// takes the read lock of r for the lookup alone, so its callers call the factory
// without the lock.
func (r *Registry) entryOf(axis Axis, name shard.StrategyName) (entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.factories[axis][name]
	return e, ok
}

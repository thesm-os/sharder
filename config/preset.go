// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config

import (
	"errors"
	"fmt"
	"slices"
)

// PresetName is the registered name of a preset, such as "exclusive-worker". The
// empty PresetName is no preset.
type PresetName string

// Preset is a named partial spec. Resolve fills each field that a spec leaves empty
// from its preset, and merges a selection of the strategy of the preset over the
// configuration of that strategy in the preset.
//
// # Concurrency
//
// A copy of a Preset shares the slices of its Spec with the original.
type Preset struct {
	// Name is the name under which a spec selects the preset.
	Name PresetName
	// Description is a sentence for people, such as the use case of the preset.
	Description string
	// Spec is the partial spec of the preset. Its Namespace, Preset and Version are
	// empty.
	Spec Spec
}

// RegisterPreset adds p to r. r keeps the slices, the pointers and the configurations of
// p.Spec, so the caller does not modify them after the call.
//
// Error modes:
//   - an error that wraps ErrRegistration when p has no name, or when its Spec has a
//     namespace, a preset or a version.
//   - an error that wraps ErrDuplicate when r has a preset of the same name.
//   - an error that wraps ErrUnsupported for each selection of p outside the support
//     matrix of r.
func RegisterPreset(r *Registry, p *Preset) error {
	if p.Name == "" {
		return fmt.Errorf("%w: a preset without a name", ErrRegistration)
	}
	if p.Spec.Namespace != "" || p.Spec.Preset != "" || p.Spec.Version != 0 {
		return fmt.Errorf("%w: preset %q has a namespace, a preset or a version", ErrRegistration, p.Name)
	}
	var unsupported []error
	for _, axis := range specAxes {
		name := p.Spec.selection(axis).Name
		if name == "" || slices.Contains(r.support.Open, axis) || slices.Contains(r.support.Strategies[axis], name) {
			continue
		}
		unsupported = append(unsupported, fmt.Errorf("%w: preset %q: %s %q", ErrUnsupported, p.Name, axis, name))
	}
	if len(unsupported) > 0 {
		return errors.Join(unsupported...)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.presets[p.Name]; ok {
		return fmt.Errorf("%w: preset %q", ErrDuplicate, p.Name)
	}
	r.presets[p.Name] = *p
	return nil
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package root

import (
	"encoding/json"
	"errors"
	"fmt"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// Capacity is the capacity of a controller process in the root: the most cells that the
// process hosts. The root gives each cell a load of one on the metric of the same name, so
// the capacity constraint keeps the root from placing more cells on a process.
const Capacity = "cells"

// firstVersion is the version of the spec with which the root creates a namespace, and the
// version of the fixed spec of the strategies of the root.
const firstVersion epoch.Epoch = 1

// planned is the configuration of the planner of the root that its spec merges over the
// preset: the capacity constraint and the count goal.
type planned struct {
	// Constraints lists the constraint capacity.
	Constraints []planner.Selection `json:"constraints"`
	// Goals lists the goal count.
	Goals []planner.GoalSelection `json:"goals"`
}

// strategies returns the strategies of the root of the namespace ns, which a spec of the
// namespace does not select. The spec of the root selects the planner with the capacity
// constraint and the count goal over the load metric Capacity. Its other strategies are
// the strategies of the preset builtin.LeaderElection: singleton, primary-only, fenced
// exclusive ownership, and break-before-make with its default timeouts. r builds the
// strategies, and provided lists the capabilities of the store and the membership.
//
// Error modes:
//   - an error that wraps ErrConfig and the errors of config.Resolve and control.Build
//     for a registry without the built-in strategies.
func strategies(r *config.Registry, provided map[config.Axis][]config.Capability, ns shard.Namespace) (
	*control.Strategies, error,
) {
	raw, merr := json.Marshal(&planned{
		Constraints: []planner.Selection{{Name: constraint.CapacityName}},
		Goals:       []planner.GoalSelection{{Name: goal.CountName, Weight: 1}},
	})
	res, rerr := config.Resolve(r, &config.Spec{
		Namespace: ns,
		Preset:    builtin.LeaderElection,
		Placement: config.Selection{Name: planner.Name, Config: raw},
		Load:      config.LoadModel{Metrics: []config.Metric{{Name: Capacity}}},
		Version:   firstVersion,
	}, provided)
	var s *control.Strategies
	var berr error
	if rerr == nil {
		s, berr = control.Build(r, res)
	}
	if err := errors.Join(merr, rerr, berr); err != nil {
		return nil, fmt.Errorf("%w: the strategies of the root: %w", ErrConfig, err)
	}
	return s, nil
}

// namespace returns the mapping and the expanded encoding of the spec s at the first
// version, with which the root creates the namespace of s. The strategies of the spec must
// build, because the controllers of the cells build them.
//
// Error modes:
//   - an error that wraps ErrConfig and the error of config.Resolve for a spec that does
//     not resolve.
//   - an error that wraps ErrConfig and the error of control.Build for a spec whose
//     strategies do not build.
func namespace(r *config.Registry, provided map[config.Axis][]config.Capability, s *config.Spec) (
	shard.Mapping, []byte, error,
) {
	first := *s
	first.Version = firstVersion
	res, err := config.Resolve(r, &first, provided)
	if err == nil {
		_, err = control.Build(r, res)
	}
	var raw []byte
	if err == nil {
		raw, err = json.Marshal(&res.Spec)
	}
	if err != nil {
		return shard.Mapping{}, nil, fmt.Errorf("%w: the spec of the namespace %s: %w", ErrConfig, s.Namespace, err)
	}
	return res.Mapping, raw, nil
}

// acquisition completes the mutation mu of one attempt of the acquisition of the root
// scope at the header head, and returns the namespace incarnation of the root. On a scope
// before its first commit, mu creates the namespace with a new incarnation, the mapping
// and the spec at the first version. Another process can create the namespace first, and
// the next attempt then acquires the scope in its incarnation and takes its spec.
func (m *Machine) acquisition(head store.Head, mu *store.Mutation) shard.NamespaceIncarnation {
	if !head.Incarnation.IsZero() {
		return head.Incarnation
	}
	mapping := m.mapping
	mu.Mapping, mu.Spec = &mapping, m.initial
	return shard.NamespaceIncarnation{ID: m.ids.Generate()}
}

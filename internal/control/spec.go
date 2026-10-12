// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/internal/engine"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// goal is a goal of the planner with its tier and its weight.
type goal struct {
	// goal is the goal.
	goal placement.Goal
	// weight is the factor of the cost of the goal in the sum of its tier.
	weight float64
	// tier is the tier of the goal.
	tier int
}

// Strategies are the strategies that a controller builds from one resolved version of a
// spec: the planner with its constraints and goals, the partitioner, the replica state model
// and the settings of the engine.
//
// The zero Strategies is not usable. Construct one with [Build].
//
// # Concurrency
//
// A Strategies is not modified after Build returns it. Its strategies are safe for
// concurrent use.
type Strategies struct {
	// placer is the placer of the namespace.
	placer placement.Placer
	// partitioner is the partitioner of the namespace.
	partitioner partition.Partitioner
	// model is the replica state model of the namespace.
	model *replication.Compiled
	// constraints are the constraints of the placer, which every pass checks with
	// placement.Validate.
	constraints []placement.Constraint
	// goals are the goals of the placer, which an explain proposal reports.
	goals []goal
	// metrics are the load metrics of the namespace.
	metrics []placement.Metric
	// roles lists the serving roles of the model.
	roles []shard.Role
	// budget is the budget of the namespace, with every field filled.
	budget config.Budget
	// settings are the settings of the engine.
	settings engine.Settings
	// schedule is the schedule of the passes of the placer.
	schedule placement.Schedule
	// version is the version of the spec.
	version epoch.Epoch
	// writer is the writer role of the model.
	writer shard.Role
}

// Resolve decodes the spec raw, as the root stores it, and resolves it with config.Resolve
// on r. provided maps the roles config.Store and config.Membership to the capabilities of
// their adapters.
//
// Error modes:
//   - an error that wraps ErrSpec and the error of encoding/json for a raw that does not
//     decode.
//   - an error that wraps ErrSpec and the error of config.Resolve for a spec that does not
//     resolve.
func Resolve(r *config.Registry, provided map[config.Axis][]config.Capability, raw []byte) (
	*config.Resolved, error,
) {
	var spec config.Spec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return nil, fmt.Errorf("%w: decode: %w", ErrSpec, err)
	}
	res, err := config.Resolve(r, &spec, provided)
	if err != nil {
		return nil, fmt.Errorf("%w: version %d: %w", ErrSpec, spec.Version, err)
	}
	return res, nil
}

// Build builds the strategies of the resolved spec res with the registry r.
//
// Error modes:
//   - an error that wraps ErrSpec for a placer whose configuration is not a configuration
//     of the planner.
//   - an error that wraps ErrSpec and the error of the registry for each strategy that does
//     not build.
func Build(r *config.Registry, res *config.Resolved) (*Strategies, error) {
	pc, ok := res.Configs[config.Placement].(*planner.Config)
	if !ok {
		return nil, fmt.Errorf("%w: version %d selects the placer %s, and a controller runs the planner",
			ErrSpec, res.Spec.Version, res.Spec.Placement.Name)
	}
	s := &Strategies{
		constraints: make([]placement.Constraint, len(pc.Constraints)),
		goals:       make([]goal, len(pc.Goals)),
		metrics:     make([]placement.Metric, 0, len(res.Spec.Load.Metrics)),
		budget:      res.Spec.Budget,
		version:     res.Spec.Version,
	}
	cerrs := make([]error, len(pc.Constraints))
	for i, c := range pc.Constraints {
		s.constraints[i], cerrs[i] = config.BuildNamed[placement.Constraint](r, config.Constraint, c.Name, c.Config)
	}
	gerrs := make([]error, len(pc.Goals))
	for i, g := range pc.Goals {
		var built placement.Goal
		built, gerrs[i] = config.BuildNamed[placement.Goal](r, config.Goal, g.Name, g.Config)
		s.goals[i] = goal{goal: built, weight: g.Weight, tier: g.Tier}
	}
	for _, mt := range res.Spec.Load.Metrics {
		s.metrics = append(s.metrics, placement.Metric{Name: mt.Name, Capacity: mt.Capacity})
	}
	if err := errors.Join(errors.Join(cerrs...), errors.Join(gerrs...), s.build(r, res)); err != nil {
		return nil, fmt.Errorf("%w: version %d: %w", ErrSpec, res.Spec.Version, err)
	}
	return s, nil
}

// build builds the placer, the replica state model, the partitioner and the protocol of res
// with r into s, and fills the settings of the engine from the ownership and the budget of
// res. It returns the errors of the strategies that do not build, joined, and then leaves
// s incomplete.
func (s *Strategies) build(r *config.Registry, res *config.Resolved) error {
	placer, perr := config.Build[placement.Placer](r, res, config.Placement)
	model, merr := config.Build[*replication.Compiled](r, res, config.Replication)
	partitioner, terr := config.Build[partition.Partitioner](r, res, config.Partitioning)
	protocol, herr := config.Build[handoff.Protocol](r, res, config.Handoff)
	own, oerr := config.Build[ownership.Config](r, res, config.Ownership)
	if err := errors.Join(perr, merr, terr, herr, oerr); err != nil {
		return err
	}
	s.placer, s.model, s.partitioner, s.settings.Protocol = placer, model, partitioner, protocol
	s.settings.LockDelay = time.Duration(own.Lease.LockDelay)
	s.settings.MaxTransitions = *s.budget.MaxTransitions
	s.settings.MaxNodeTransitions = *s.budget.MaxNodeTransitions
	s.settings.NodeCallbacks = *s.budget.NodeCallbacks
	s.schedule = placer.Schedule()
	for _, role := range model.Model.Roles {
		s.roles = append(s.roles, role.Name)
		if role.Writer {
			s.writer = role.Name
		}
	}
	return nil
}

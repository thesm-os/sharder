// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replication

import (
	"errors"
	"fmt"
	"slices"

	"go.thesmos.sh/core/fsm"

	"go.thesmos.sh/sharder/shard"
)

// PrimaryOnlyName is the registered name of the built-in model PrimaryOnly, which a
// spec selects on the replication axis.
const PrimaryOnlyName shard.StrategyName = "primary-only"

// Role is a serving role of a model, with the most replicas of one shard that may be
// in it at one instant.
//
// The zero Role is not valid in a model, because no replica serves in
// shard.RoleNone.
type Role struct {
	// Max is the most replicas of one shard that may be in the role at any instant,
	// the replicas whose transition into the role has started included. It is at
	// least one.
	Max int
	// Name is the role, such as shard.RolePrimary. It is not shard.RoleNone,
	// shard.RoleOffline or shard.RoleDropped, because no replica serves in them.
	Name shard.Role
	// Writer reports whether the replicas in the role write the data of the shard. A
	// model has at most one writer role, and its Max is one. The core issues fencing
	// tokens to the replicas in the writer role.
	Writer bool
}

// Transition is one change of role that a model allows. The core starts the pending
// transitions of a shard in ascending Priority, within the Max of every role.
type Transition struct {
	// Priority orders the pending transitions of a shard. The core starts a
	// transition of a lower Priority before one of a higher Priority.
	Priority int
	// From is the role that the replica leaves.
	From shard.Role
	// To is the role that the replica enters.
	To shard.Role
}

// Model declares the serving roles of the replicas of a shard, and the transitions
// between them and shard.RoleOffline.
//
// The zero Model is not valid, because it has no name.
//
// # Concurrency
//
// A copy of a Model shares the elements of Roles and Transitions with the original.
type Model struct {
	// Name is the registered name of the model, such as PrimaryOnlyName.
	Name shard.StrategyName
	// Roles lists the serving roles of the model.
	Roles []Role
	// Transitions lists every change of role that the model allows, to and from
	// shard.RoleOffline included. Compile adds the transition from shard.RoleOffline
	// to shard.RoleDropped.
	Transitions []Transition
}

// PrimaryOnly is the built-in model primary-only. Each shard has one replica, the
// primary, which writes. A move removes the old primary before it adds the new one:
// the transition from shard.RolePrimary to shard.RoleOffline has priority 0, and the
// transition back has priority 1. The caller must not modify PrimaryOnly.
var PrimaryOnly = Model{
	Name:  PrimaryOnlyName,
	Roles: []Role{{Name: shard.RolePrimary, Max: 1, Writer: true}},
	Transitions: []Transition{
		{From: shard.RolePrimary, To: shard.RoleOffline, Priority: 0},
		{From: shard.RoleOffline, To: shard.RolePrimary, Priority: 1},
	},
}

// Compile validates m and builds its transition table with core's fsm. The table
// starts in shard.RoleOffline, adds the transition from shard.RoleOffline to
// shard.RoleDropped, and has shard.RoleDropped as its only terminal state.
// fsm.Builder.Build checks the structure of the table. It reports a role without a
// path from shard.RoleOffline, a role that m does not declare and that a transition
// leaves included. The Compiled contains a copy of m.
//
// Error modes:
//   - an error that joins one error per problem of m, each of which wraps
//     ErrInvalidModel.
//
// These are the problems of a model:
//
//   - The model has no name.
//   - A role is shard.RoleNone, shard.RoleOffline, shard.RoleDropped or a role that is
//     not Valid, because no replica serves in it.
//   - A role is declared twice.
//   - A role has a Max below one, or a writer role has a Max other than one.
//   - The model has more than one writer role.
//   - A transition enters a role that the model does not declare.
//   - fsm.Builder.Build reports a problem of the table.
//   - The table does not contain a declared role.
func (m Model) Compile() (*Compiled, error) {
	var problems []error
	if m.Name == "" {
		problems = append(problems, fmt.Errorf("%w: the model has no name", ErrInvalidModel))
	}

	served := make(map[shard.Role]bool, len(m.Roles))
	writers := 0
	for _, r := range m.Roles {
		if !r.Name.Valid() || r.Name == shard.RoleNone || r.Name == shard.RoleOffline || r.Name == shard.RoleDropped {
			problems = append(problems, fmt.Errorf("%w: no replica serves in the role %v", ErrInvalidModel, r.Name))
			continue
		}
		if served[r.Name] {
			problems = append(problems, fmt.Errorf("%w: the role %v is declared twice", ErrInvalidModel, r.Name))
			continue
		}
		served[r.Name] = true
		if r.Max < 1 {
			problems = append(problems, fmt.Errorf("%w: the role %v has a Max of %d", ErrInvalidModel, r.Name, r.Max))
		}
		if r.Writer {
			writers++
			if r.Max != 1 {
				problems = append(problems,
					fmt.Errorf("%w: the writer role %v has a Max of %d", ErrInvalidModel, r.Name, r.Max))
			}
		}
	}
	if writers > 1 {
		problems = append(problems, fmt.Errorf("%w: the model has %d writer roles", ErrInvalidModel, writers))
	}

	b := fsm.NewBuilder[shard.Role, shard.Role, struct{}](shard.RoleOffline)
	for _, t := range m.Transitions {
		if t.To != shard.RoleOffline && !served[t.To] {
			problems = append(problems,
				fmt.Errorf("%w: a transition to %v, which the model does not declare", ErrInvalidModel, t.To))
			continue
		}
		b.Edge(t.From, t.To, t.To)
	}

	spec, err := b.Edge(shard.RoleOffline, shard.RoleDropped, shard.RoleDropped).Terminal(shard.RoleDropped).Build()
	if err != nil {
		return nil, errors.Join(append(problems, fmt.Errorf("%w: %w", ErrInvalidModel, err))...)
	}
	for _, r := range m.Roles {
		if served[r.Name] && !spec.Known(r.Name) {
			problems = append(problems,
				fmt.Errorf("%w: the table does not contain the role %v", ErrInvalidModel, r.Name))
		}
	}
	if len(problems) > 0 {
		return nil, errors.Join(problems...)
	}

	return &Compiled{
		Spec:  spec,
		Model: Model{Name: m.Name, Roles: slices.Clone(m.Roles), Transitions: slices.Clone(m.Transitions)},
	}, nil
}

// Compiled is a model that Compile validated, with its transition table.
//
// # Concurrency
//
// A Compiled is immutable and safe for concurrent use.
type Compiled struct {
	// Spec is the transition table of the model. Its states are roles, and the event
	// that moves a replica to a role is that role, so the transition from s to t is
	// the edge from s on t to t. Its initial state is shard.RoleOffline, and its only
	// terminal state is shard.RoleDropped.
	Spec *fsm.Spec[shard.Role, shard.Role, struct{}]
	// Model is the copy of the model that Compile validated.
	Model Model
}

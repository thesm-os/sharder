// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replication_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

const (
	// recordedPrimaryOnlyName is the registered name of primary-only, which the specs
	// of namespaces record.
	recordedPrimaryOnlyName = "primary-only"
	// testModel is the name of the models that the tests declare.
	testModel shard.StrategyName = "test-model"
)

// TestModel checks the validation and the transition table of Compile, and the
// built-in model PrimaryOnly.
func TestModel(t *testing.T) {
	t.Parallel()

	t.Run("Compile", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the table of PrimaryOnly", func(t *testing.T) {
			t.Parallel()
			c, err := replication.PrimaryOnly.Compile()
			assert.NoError(t, err, "Compile must build the table of PrimaryOnly")

			var allowed [][2]shard.Role
			for from := shard.RoleNone; from <= shard.RoleLearner; from++ {
				for to := shard.RoleNone; to <= shard.RoleLearner; to++ {
					if c.Spec.Allows(from, to) {
						allowed = append(allowed, [2]shard.Role{from, to})
					}
				}
			}
			expect.Permutation(t, allowed, [][2]shard.Role{
				{shard.RolePrimary, shard.RoleOffline},
				{shard.RoleOffline, shard.RolePrimary},
				{shard.RoleOffline, shard.RoleDropped},
			}, "the table must allow the transitions of primary-only and the drop from offline")
			expect.Equal(t, c.Spec.Initial(), shard.RoleOffline, "the table must start in offline")
			expect.True(t, c.Spec.Terminal(shard.RoleDropped), "dropped must be the terminal state of the table")
		})

		t.Run("returns PrimaryOnly under its recorded name", func(t *testing.T) {
			t.Parallel()
			c, err := replication.PrimaryOnly.Compile()
			assert.NoError(t, err, "Compile must build the table of PrimaryOnly")
			assert.Equal(t, c.Model.Name, recordedPrimaryOnlyName,
				"PrimaryOnly must have the name that the specs of namespaces record")
		})

		t.Run("returns a copy of the model", func(t *testing.T) {
			t.Parallel()
			m := replication.Model{
				Name:  testModel,
				Roles: []replication.Role{{Name: shard.RoleLeader, Max: 1, Writer: true}},
				Transitions: []replication.Transition{
					{From: shard.RoleLeader, To: shard.RoleOffline},
					{From: shard.RoleOffline, To: shard.RoleLeader, Priority: 1},
				},
			}
			c, err := m.Compile()
			assert.NoError(t, err, "Compile must build the table of the model")
			m.Roles[0].Max = 2
			m.Transitions[0].Priority = 2
			assert.Equal(t, c.Model, replication.Model{
				Name:  testModel,
				Roles: []replication.Role{{Name: shard.RoleLeader, Max: 1, Writer: true}},
				Transitions: []replication.Transition{
					{From: shard.RoleLeader, To: shard.RoleOffline},
					{From: shard.RoleOffline, To: shard.RoleLeader, Priority: 1},
				},
			}, "a change of the model after Compile must not change the Compiled")
		})

		// Each row adds roles and transitions to a valid model with the writer role
		// primary, so that the model has the problem that the name of the row states.
		// extra counts the problems that the transitions of the row cause besides it.
		tests := []struct {
			name        string
			roles       []replication.Role
			transitions []replication.Transition
			extra       int
		}{
			{
				name:  "returns ErrInvalidModel for the role RoleNone",
				roles: []replication.Role{{Name: shard.RoleNone, Max: 1}},
				transitions: []replication.Transition{
					{From: shard.RoleOffline, To: shard.RoleNone},
					{From: shard.RoleNone, To: shard.RoleOffline},
				},
				extra: 2,
			},
			{
				name:  "returns ErrInvalidModel for the role RoleNone without a transition",
				roles: []replication.Role{{Name: shard.RoleNone, Max: 1}},
			},
			{
				name:  "returns ErrInvalidModel for the role RoleOffline",
				roles: []replication.Role{{Name: shard.RoleOffline, Max: 1}},
			},
			{
				name:  "returns ErrInvalidModel for the role RoleDropped",
				roles: []replication.Role{{Name: shard.RoleDropped, Max: 1}},
			},
			{
				name:  "returns ErrInvalidModel for a role that is not valid",
				roles: []replication.Role{{Name: shard.RoleLearner + 1, Max: 1}},
				transitions: []replication.Transition{
					{From: shard.RoleOffline, To: shard.RoleLearner + 1},
					{From: shard.RoleLearner + 1, To: shard.RoleOffline},
				},
				extra: 2,
			},
			{
				name:  "returns ErrInvalidModel for a role declared twice",
				roles: []replication.Role{{Name: shard.RolePrimary, Max: 1}},
			},
			{
				name:  "returns ErrInvalidModel for a role whose Max is zero",
				roles: []replication.Role{{Name: shard.RoleSecondary}},
				transitions: []replication.Transition{
					{From: shard.RoleOffline, To: shard.RoleSecondary},
					{From: shard.RoleSecondary, To: shard.RoleOffline},
				},
			},
			{
				name:  "returns ErrInvalidModel for two writer roles",
				roles: []replication.Role{{Name: shard.RoleLeader, Max: 1, Writer: true}},
				transitions: []replication.Transition{
					{From: shard.RoleOffline, To: shard.RoleLeader},
					{From: shard.RoleLeader, To: shard.RoleOffline},
				},
			},
			{
				name:  "returns ErrInvalidModel for a transition from a role that the model does not declare",
				roles: []replication.Role{{Name: shard.RoleLeader, Max: 1}},
				transitions: []replication.Transition{
					{From: shard.RoleOffline, To: shard.RoleLeader},
					{From: shard.RoleLeader, To: shard.RoleOffline},
					{From: shard.RoleSecondary, To: shard.RoleOffline},
				},
			},
			{
				name:  "returns ErrInvalidModel for a transition to a role that the model does not declare",
				roles: []replication.Role{{Name: shard.RoleLeader, Max: 1}},
				transitions: []replication.Transition{
					{From: shard.RoleOffline, To: shard.RoleLeader},
					{From: shard.RoleLeader, To: shard.RoleOffline},
					{From: shard.RoleOffline, To: shard.RoleSecondary},
				},
			},
			{
				name:  "returns ErrInvalidModel for a transition to RoleDropped",
				roles: []replication.Role{{Name: shard.RoleLeader, Max: 1}},
				transitions: []replication.Transition{
					{From: shard.RoleOffline, To: shard.RoleLeader},
					{From: shard.RoleLeader, To: shard.RoleOffline},
					{From: shard.RoleLeader, To: shard.RoleDropped},
				},
			},
			{
				name:        "returns ErrInvalidModel for a role without a transition out of it",
				roles:       []replication.Role{{Name: shard.RoleLeader, Max: 1}},
				transitions: []replication.Transition{{From: shard.RoleOffline, To: shard.RoleLeader}},
			},
			{
				name:        "returns ErrInvalidModel for a role without a path from RoleOffline",
				roles:       []replication.Role{{Name: shard.RoleLeader, Max: 1}},
				transitions: []replication.Transition{{From: shard.RoleLeader, To: shard.RoleOffline}},
			},
			{
				name:  "returns ErrInvalidModel for a transition declared twice",
				roles: []replication.Role{{Name: shard.RoleLeader, Max: 1}},
				transitions: []replication.Transition{
					{From: shard.RoleOffline, To: shard.RoleLeader},
					{From: shard.RoleLeader, To: shard.RoleOffline},
					{From: shard.RoleLeader, To: shard.RoleOffline, Priority: 1},
				},
			},
			{
				name:  "returns ErrInvalidModel for a declared role without a transition",
				roles: []replication.Role{{Name: shard.RoleLeader, Max: 1}},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				m := replication.Model{
					Name:  testModel,
					Roles: append([]replication.Role{{Name: shard.RolePrimary, Max: 1, Writer: true}}, tt.roles...),
					Transitions: append([]replication.Transition{
						{From: shard.RolePrimary, To: shard.RoleOffline},
						{From: shard.RoleOffline, To: shard.RolePrimary, Priority: 1},
					}, tt.transitions...),
				}
				c, err := m.Compile()
				joined := assert.ErrorAs[interface{ Unwrap() []error }](t, err,
					"Compile must join the problems of the model")
				expect.Length(t, joined.Unwrap(), 1+tt.extra, "Compile must return one error per problem of the model")
				expect.ErrorIs(t, err, replication.ErrInvalidModel, "Compile must return ErrInvalidModel")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Compile must classify as Invalid")
				expect.Nil(t, c, "Compile must not return a Compiled with an error")
			})
		}

		t.Run("returns ErrInvalidModel for a model without a name", func(t *testing.T) {
			t.Parallel()
			_, err := replication.Model{
				Roles: []replication.Role{{Name: shard.RolePrimary, Max: 1, Writer: true}},
				Transitions: []replication.Transition{
					{From: shard.RolePrimary, To: shard.RoleOffline},
					{From: shard.RoleOffline, To: shard.RolePrimary, Priority: 1},
				},
			}.Compile()
			assert.ErrorIs(t, err, replication.ErrInvalidModel, "Compile must return ErrInvalidModel")
		})

		t.Run("returns ErrInvalidModel for a writer role whose Max is two", func(t *testing.T) {
			t.Parallel()
			_, err := replication.Model{
				Name:  testModel,
				Roles: []replication.Role{{Name: shard.RolePrimary, Max: 2, Writer: true}},
				Transitions: []replication.Transition{
					{From: shard.RolePrimary, To: shard.RoleOffline},
					{From: shard.RoleOffline, To: shard.RolePrimary, Priority: 1},
				},
			}.Compile()
			assert.ErrorIs(t, err, replication.ErrInvalidModel, "Compile must return ErrInvalidModel")
		})

		t.Run("returns one error per problem of the model", func(t *testing.T) {
			t.Parallel()
			_, err := replication.Model{
				Roles: []replication.Role{{Name: shard.RolePrimary, Max: 2, Writer: true}},
				Transitions: []replication.Transition{
					{From: shard.RolePrimary, To: shard.RoleOffline},
					{From: shard.RoleOffline, To: shard.RolePrimary, Priority: 1},
				},
			}.Compile()
			joined := assert.ErrorAs[interface{ Unwrap() []error }](t, err,
				"Compile must join the problems of the model")
			assert.Length(t, joined.Unwrap(), 2, "Compile must return a problem for the name and one for the writer")
		})
	})
}

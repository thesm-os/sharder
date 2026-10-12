// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin_test

import (
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/goal"
	"go.thesmos.sh/sharder/shard"
)

// TestGoal checks the factories of the seven goals.
func TestGoal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		factory  config.Factory[placement.Goal]
		defaults string
		valid    string
		want     func() (placement.Goal, error)
		invalid  config.Parsed
	}{
		{
			name:     "Balance",
			factory:  builtin.Balance(),
			defaults: `{"metric":""}`,
			valid:    `{"metric":"cpu"}`,
			want: func() (placement.Goal, error) {
				return goal.NewBalance(goal.BalanceConfig{Metric: "cpu"})
			},
			invalid: &goal.BalanceConfig{},
		},
		{
			name:     "Colocate",
			factory:  builtin.Colocate(),
			defaults: `{"group":""}`,
			valid:    `{"group":"tenant"}`,
			want: func() (placement.Goal, error) {
				return goal.NewColocate(goal.ColocateConfig{Group: "tenant"})
			},
			invalid: &goal.ColocateConfig{},
		},
		{
			name:     "Count",
			factory:  builtin.Count(),
			defaults: emptyJSON,
			valid:    `{"roleWeights":{"primary":2}}`,
			want: func() (placement.Goal, error) {
				return goal.NewCount(goal.CountConfig{RoleWeights: map[shard.Role]float64{shard.RolePrimary: 2}})
			},
			invalid: &goal.CountConfig{RoleWeights: map[shard.Role]float64{shard.RoleNone: 1}},
		},
		{
			name:     "Prefer",
			factory:  builtin.Prefer(),
			defaults: `{"label":"","from":""}`,
			valid:    `{"label":"region","from":"home"}`,
			want: func() (placement.Goal, error) {
				return goal.NewPrefer(goal.PreferConfig{Label: "region", From: "home"})
			},
			invalid: &goal.PreferConfig{},
		},
		{
			name:     "Spread",
			factory:  builtin.Spread(),
			defaults: `{"label":""}`,
			valid:    `{"label":"zone"}`,
			want: func() (placement.Goal, error) {
				return goal.NewSpread(goal.SpreadConfig{Label: "zone"})
			},
			invalid: &goal.SpreadConfig{},
		},
		{
			name:     "Stickiness",
			factory:  builtin.Stickiness(),
			defaults: emptyJSON,
			valid:    emptyJSON,
			want: func() (placement.Goal, error) {
				return goal.Stickiness{}, nil
			},
		},
		{
			name:     "Utilisation",
			factory:  builtin.Utilisation(),
			defaults: `{"metric":"","above":0.9}`,
			valid:    `{"metric":"cpu"}`,
			want: func() (placement.Goal, error) {
				return goal.NewUtilisation(goal.UtilisationConfig{Metric: "cpu", Above: 0.9})
			},
			invalid: &goal.UtilisationConfig{Above: 0.9},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("Parse", func(t *testing.T) {
				t.Parallel()

				t.Run("returns the defaults for no configuration", func(t *testing.T) {
					t.Parallel()
					encoded, err := json.Marshal(parse(t, tt.factory, ""))
					assert.NoError(t, err, "the defaults must encode")
					assert.Equal(t, string(encoded), tt.defaults, "Parse must return the defaults of the goal")
				})

				t.Run("returns the configuration of its own encoding", func(t *testing.T) {
					t.Parallel()
					roundTrip(t, tt.factory, parse(t, tt.factory, tt.valid))
				})
			})

			t.Run("Capabilities", func(t *testing.T) {
				t.Parallel()

				t.Run("returns no capabilities", func(t *testing.T) {
					t.Parallel()
					assert.Equal(t, tt.factory.Capabilities(parse(t, tt.factory, tt.valid)), config.Capabilities{},
						"a goal must not provide or require a capability")
				})
			})

			t.Run("New", func(t *testing.T) {
				t.Parallel()

				t.Run("builds the goal of the configuration", func(t *testing.T) {
					t.Parallel()
					got, err := tt.factory.New(parse(t, tt.factory, tt.valid))
					assert.NoError(t, err, "New must build the goal")
					want, err := tt.want()
					assert.NoError(t, err, "the goal of the test must build")
					assert.Equal(t, got, want, "New must build the goal of its configuration")
				})

				if tt.invalid == nil {
					return
				}
				t.Run("returns ErrConfig for a configuration that the goal refuses", func(t *testing.T) {
					t.Parallel()
					_, err := tt.factory.New(tt.invalid)
					assert.ErrorIs(t, err, builtin.ErrConfig, "New must refuse an invalid configuration")
					assert.ErrorIs(t, err, goal.ErrConfig, "New must wrap the error of the constructor")
				})
			})
		})
	}
}

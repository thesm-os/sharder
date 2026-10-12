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
	"go.thesmos.sh/sharder/placement/constraint"
)

// TestConstraint checks the factories of the six constraints.
func TestConstraint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		factory  config.Factory[placement.Constraint]
		defaults string
		valid    string
		want     func() (placement.Constraint, error)
		invalid  config.Parsed
	}{
		{
			name:     "Capacity",
			factory:  builtin.Capacity(),
			defaults: `{"headroom":0}`,
			valid:    `{"headroom":0.2}`,
			want: func() (placement.Constraint, error) {
				return constraint.NewCapacity(constraint.CapacityConfig{Headroom: 0.2})
			},
			invalid: &constraint.CapacityConfig{Headroom: 2},
		},
		{
			name:     "Drain",
			factory:  builtin.Drain(),
			defaults: emptyJSON,
			valid:    emptyJSON,
			want: func() (placement.Constraint, error) {
				return constraint.Drain{}, nil
			},
		},
		{
			name:     "Isolation",
			factory:  builtin.Isolation(),
			defaults: `{"label":"","max":1}`,
			valid:    `{"label":"zone"}`,
			want: func() (placement.Constraint, error) {
				return constraint.NewIsolation(constraint.IsolationConfig{Label: "zone", Max: 1})
			},
			invalid: &constraint.IsolationConfig{Max: 1},
		},
		{
			name:     "Labels",
			factory:  builtin.Labels(),
			defaults: `{"rules":null}`,
			valid:    `{"rules":[{"key":"zone","values":["a"],"operator":"in"}]}`,
			want: func() (placement.Constraint, error) {
				return constraint.NewLabels(constraint.LabelsConfig{Rules: []constraint.Rule{
					{Key: "zone", Values: []string{"a"}, Operator: constraint.In},
				}})
			},
			invalid: &constraint.LabelsConfig{Rules: []constraint.Rule{{Operator: constraint.In}}},
		},
		{
			name:     "MatchLabel",
			factory:  builtin.MatchLabel(),
			defaults: `{"label":""}`,
			valid:    `{"label":"jurisdiction"}`,
			want: func() (placement.Constraint, error) {
				return constraint.NewMatchLabel(constraint.MatchLabelConfig{Label: "jurisdiction"})
			},
			invalid: &constraint.MatchLabelConfig{},
		},
		{
			name:     "MaxPerNode",
			factory:  builtin.MaxPerNode(),
			defaults: `{"max":0}`,
			valid:    `{"max":3}`,
			want: func() (placement.Constraint, error) {
				return constraint.NewMaxPerNode(constraint.MaxPerNodeConfig{Max: 3})
			},
			invalid: &constraint.MaxPerNodeConfig{},
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
					assert.Equal(t, string(encoded), tt.defaults, "Parse must return the defaults of the constraint")
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
						"a constraint must not provide or require a capability")
				})
			})

			t.Run("New", func(t *testing.T) {
				t.Parallel()

				t.Run("builds the constraint of the configuration", func(t *testing.T) {
					t.Parallel()
					got, err := tt.factory.New(parse(t, tt.factory, tt.valid))
					assert.NoError(t, err, "New must build the constraint")
					want, err := tt.want()
					assert.NoError(t, err, "the constraint of the test must build")
					assert.Equal(t, got, want, "New must build the constraint of its configuration")
				})

				if tt.invalid == nil {
					return
				}
				t.Run("returns ErrConfig for a configuration that the constraint refuses", func(t *testing.T) {
					t.Parallel()
					_, err := tt.factory.New(tt.invalid)
					assert.ErrorIs(t, err, builtin.ErrConfig, "New must refuse an invalid configuration")
					assert.ErrorIs(t, err, constraint.ErrConfig, "New must wrap the error of the constructor")
				})
			})
		})
	}
}

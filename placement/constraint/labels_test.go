// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package constraint_test

import (
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/placementtest"
	"go.thesmos.sh/sharder/shard"
)

// recordedRuleJSON pins the encoding of a rule of In for secondaries in the JSON of a
// configuration.
const recordedRuleJSON = `{"key":"region","values":["eu"],"operator":"in","role":"secondary"}`

// TestLabels checks the JSON of a rule, the validation of a LabelsConfig, NewLabels and
// the moves that a Labels rejects.
func TestLabels(t *testing.T) {
	t.Parallel()

	t.Run("Rule", func(t *testing.T) {
		t.Parallel()

		t.Run("encodes as its recorded JSON", func(t *testing.T) {
			t.Parallel()
			r := constraint.Rule{Key: region, Operator: constraint.In, Values: []string{eu}, Role: shard.RoleSecondary}
			assert.RoundTrip(t, func(give constraint.Rule) (string, error) {
				b, err := json.Marshal(&give)
				return string(b), err
			}, func(s string) (constraint.Rule, error) {
				var decoded constraint.Rule
				err := json.Unmarshal([]byte(s), &decoded)
				return decoded, err
			}, r, "a rule must encode and decode through JSON")
			got, err := json.Marshal(&r)
			assert.NoError(t, err, "json.Marshal must encode the rule")
			assert.Equal(t, string(got), recordedRuleJSON, "the JSON of a rule must keep its recorded form")
		})
	})

	t.Run("LabelsConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("Validate", func(t *testing.T) {
			t.Parallel()

			t.Run("returns nil for one rule of each operator", func(t *testing.T) {
				t.Parallel()
				assert.NoError(t, constraint.LabelsConfig{Rules: []constraint.Rule{
					{Key: region, Operator: constraint.In, Values: []string{eu}},
					{Key: region, Operator: constraint.NotIn, Values: []string{us}, Role: shard.RolePrimary},
					{Key: region, Operator: constraint.Exists},
					{Key: jurisdiction, Operator: constraint.NotExists},
				}}.Validate(), "Validate must accept the rules")
			})

			invalid := []struct {
				name string
				give constraint.Rule
			}{
				{
					name: "returns ErrConfig for a rule without a key",
					give: constraint.Rule{Operator: constraint.Exists},
				},
				{name: "returns ErrConfig for the zero Operator", give: constraint.Rule{Key: region}},
				{
					name: "returns ErrConfig for a rule of In without values",
					give: constraint.Rule{Key: region, Operator: constraint.In},
				},
				{
					name: "returns ErrConfig for a rule of NotIn without values",
					give: constraint.Rule{Key: region, Operator: constraint.NotIn},
				},
				{
					name: "returns ErrConfig for a rule of Exists with values",
					give: constraint.Rule{Key: region, Operator: constraint.Exists, Values: []string{eu}},
				},
				{
					name: "returns ErrConfig for a rule of NotExists with values",
					give: constraint.Rule{Key: region, Operator: constraint.NotExists, Values: []string{eu}},
				},
				{
					name: "returns ErrConfig for a role that is not valid",
					give: constraint.Rule{Key: region, Operator: constraint.Exists, Role: shard.RoleLearner + 1},
				},
			}
			for _, tt := range invalid {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					err := constraint.LabelsConfig{Rules: []constraint.Rule{tt.give}}.Validate()
					expect.ErrorIs(t, err, constraint.ErrConfig, "Validate must return ErrConfig")
					expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
				})
			}

			t.Run("returns one error per problem of a rule", func(t *testing.T) {
				t.Parallel()
				joined := assert.ErrorAs[interface{ Unwrap() []error }](
					t,
					constraint.LabelsConfig{
						Rules: []constraint.Rule{{Values: []string{eu}, Role: shard.RoleLearner + 1}},
					}.Validate(),
					"Validate must join the problems of the rule",
				)
				assert.Length(t, joined.Unwrap(), 3, "Validate must report the key, the operator and the role")
			})
		})
	})

	t.Run("NewLabels", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a configuration that Validate refuses", func(t *testing.T) {
			t.Parallel()
			_, err := constraint.NewLabels(constraint.LabelsConfig{Rules: []constraint.Rule{{}}})
			assert.ErrorIs(t, err, constraint.ErrConfig, "NewLabels must refuse the zero Rule")
		})

		t.Run("copies the values of the rules", func(t *testing.T) {
			t.Parallel()
			cfg := constraint.LabelsConfig{Rules: []constraint.Rule{
				{Key: region, Operator: constraint.In, Values: []string{eu}},
			}}
			c := newLabels(t, cfg.Rules...)
			cfg.Rules[0].Values[0] = us
			assert.NoError(t, c.Check(newState(t, cell()), placement.Move{To: n1, Shard: s4, Role: shard.RolePrimary}),
				"a change of the configuration must not change the constraint")
		})
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns LabelsName", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newLabels(t).Name(), constraint.LabelsName, "Name must return the registered name")
		})
	})

	t.Run("Check", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			rule constraint.Rule
			give placement.Move
			want error
		}{
			{
				name: "returns nil for a node with a value of In",
				rule: constraint.Rule{Key: region, Operator: constraint.In, Values: []string{eu}},
				give: placement.Move{To: n1, Shard: s4, Role: shard.RolePrimary},
			},
			{
				name: "returns ErrLabels for a node with another value than In lists",
				rule: constraint.Rule{Key: region, Operator: constraint.In, Values: []string{eu}},
				give: placement.Move{To: n3, Shard: s4, Role: shard.RolePrimary},
				want: constraint.ErrLabels,
			},
			{
				name: "returns ErrLabels for a node without the label of In",
				rule: constraint.Rule{Key: region, Operator: constraint.In, Values: []string{eu}},
				give: placement.Move{To: n4, Shard: s4, Role: shard.RolePrimary},
				want: constraint.ErrLabels,
			},
			{
				name: "returns ErrLabels for a node with a value of NotIn",
				rule: constraint.Rule{Key: region, Operator: constraint.NotIn, Values: []string{us}},
				give: placement.Move{To: n3, Shard: s4, Role: shard.RolePrimary},
				want: constraint.ErrLabels,
			},
			{
				name: "returns nil for a node with another value than NotIn lists",
				rule: constraint.Rule{Key: region, Operator: constraint.NotIn, Values: []string{us}},
				give: placement.Move{To: n1, Shard: s4, Role: shard.RolePrimary},
			},
			{
				name: "returns nil for a node without the label of NotIn",
				rule: constraint.Rule{Key: region, Operator: constraint.NotIn, Values: []string{us}},
				give: placement.Move{To: n4, Shard: s4, Role: shard.RolePrimary},
			},
			{
				name: "returns nil for a node with the label of Exists",
				rule: constraint.Rule{Key: region, Operator: constraint.Exists},
				give: placement.Move{To: n1, Shard: s4, Role: shard.RolePrimary},
			},
			{
				name: "returns ErrLabels for a node without the label of Exists",
				rule: constraint.Rule{Key: region, Operator: constraint.Exists},
				give: placement.Move{To: n4, Shard: s4, Role: shard.RolePrimary},
				want: constraint.ErrLabels,
			},
			{
				name: "returns ErrLabels for a node with the label of NotExists",
				rule: constraint.Rule{Key: region, Operator: constraint.NotExists},
				give: placement.Move{To: n1, Shard: s4, Role: shard.RolePrimary},
				want: constraint.ErrLabels,
			},
			{
				name: "returns nil for a node without the label of NotExists",
				rule: constraint.Rule{Key: region, Operator: constraint.NotExists},
				give: placement.Move{To: n4, Shard: s4, Role: shard.RolePrimary},
			},
			{
				name: "returns nil for a replica in another role than the rule",
				rule: constraint.Rule{Key: region, Operator: constraint.Exists, Role: shard.RoleSecondary},
				give: placement.Move{To: n4, Shard: s4, Role: shard.RolePrimary},
			},
			{
				name: "returns ErrLabels for a replica in the role of the rule",
				rule: constraint.Rule{Key: region, Operator: constraint.Exists, Role: shard.RoleSecondary},
				give: placement.Move{To: n4, Shard: s4, Role: shard.RoleSecondary},
				want: constraint.ErrLabels,
			},
			{
				name: "returns nil for a move that removes a replica",
				rule: constraint.Rule{Key: region, Operator: constraint.Exists},
				give: placement.Move{From: n3, Shard: s2, Role: shard.RolePrimary},
			},
			{
				name: "returns nil for a node that the state does not describe",
				rule: constraint.Rule{Key: region, Operator: constraint.Exists},
				give: placement.Move{From: d1, To: d1, Shard: s3, Role: shard.RolePrimary},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got := newLabels(t, tt.rule).Check(newState(t, cell()), tt.give)
				assert.Equal(t, got, tt.want, "Check must return the error of the rule")
			})
		}

		t.Run("returns ErrLabels for a node that passes one rule and fails the next", func(t *testing.T) {
			t.Parallel()
			c := newLabels(t,
				constraint.Rule{Key: region, Operator: constraint.Exists},
				constraint.Rule{Key: jurisdiction, Operator: constraint.In, Values: []string{us}},
			)
			assert.ErrorIs(t, c.Check(newState(t, cell()), placement.Move{To: n1, Shard: s4, Role: shard.RolePrimary}),
				constraint.ErrLabels, "Check must apply every rule")
		})

		t.Run(
			"returns ErrLabels for a node without the label under a rule of In with the empty value",
			func(t *testing.T) {
				t.Parallel()
				c := newLabels(t, constraint.Rule{Key: region, Operator: constraint.In, Values: []string{""}})
				assert.ErrorIs(
					t,
					c.Check(newState(t, cell()), placement.Move{To: n4, Shard: s4, Role: shard.RolePrimary}),
					constraint.ErrLabels,
					"Check must not read a missing label as the empty value",
				)
			},
		)
	})
}

// TestLabelsConformance checks the laws of placement.Constraint against a Labels.
func TestLabelsConformance(t *testing.T) {
	t.Parallel()
	placementtest.AssertConstraint(t, newLabels(t,
		constraint.Rule{Key: placementtest.Zone, Operator: constraint.NotIn, Values: []string{zoneB}},
		constraint.Rule{Key: placementtest.Zone, Operator: constraint.Exists, Role: shard.RoleSecondary},
	))
}

// TestLabelsAllocs checks that Check of a Labels does not allocate.
func TestLabelsAllocs(t *testing.T) {
	placementtest.AssertConstraintAllocs(t, newLabels(t,
		constraint.Rule{Key: placementtest.Zone, Operator: constraint.In, Values: []string{zoneA}},
	))
}

// BenchmarkLabels reports the cost of Check of a Labels with two rules, and fails when
// it allocates.
func BenchmarkLabels(b *testing.B) {
	b.Run("Check", func(b *testing.B) {
		c := newLabels(b,
			constraint.Rule{Key: region, Operator: constraint.In, Values: []string{eu, us}},
			constraint.Rule{Key: jurisdiction, Operator: constraint.NotIn, Values: []string{us}},
		)
		st := newState(b, cell())
		m := placement.Move{To: n2, Shard: s4, Role: shard.RolePrimary}
		var err error

		contract := bench.Start(b).MaxAllocs(0)
		defer contract.End()

		for contract.Loop() {
			err = c.Check(st, m)
		}

		assert.NoError(b, err, "the benchmark must measure a move that passes both rules")
	})
}

// newLabels returns the constraint labels with rules, and stops tb when NewLabels
// refuses them.
func newLabels(tb assert.TB, rules ...constraint.Rule) *constraint.Labels {
	tb.Helper()
	c, err := constraint.NewLabels(constraint.LabelsConfig{Rules: rules})
	assert.NoError(tb, err, "NewLabels must accept the rules of the test")
	return c
}

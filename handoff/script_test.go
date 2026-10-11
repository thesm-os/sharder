// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package handoff_test

import (
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/handoff"
)

const (
	// addTimeout is the timeout of the adds of the scripts of the tests.
	addTimeout = 30 * time.Second
	// dropTimeout is the timeout of the drops of the scripts of the tests.
	dropTimeout = 10 * time.Second
)

var (
	// dropFrom drops the replica on From.
	dropFrom = handoff.Step{Action: handoff.Drop, Side: handoff.From, Timeout: dropTimeout}
	// commitTo commits To as the writer.
	commitTo = handoff.Step{Action: handoff.Commit, Side: handoff.To}
	// addTo adds the replica on To.
	addTo = handoff.Step{Action: handoff.Add, Side: handoff.To, Timeout: addTimeout}
	// commitFallback commits Fallback as the writer.
	commitFallback = handoff.Step{Action: handoff.Commit, Side: handoff.Fallback}
	// addFallback adds the replica on Fallback.
	addFallback = handoff.Step{Action: handoff.Add, Side: handoff.Fallback, Timeout: addTimeout}
	// dropTo drops the replica on To.
	dropTo = handoff.Step{Action: handoff.Drop, Side: handoff.To, Timeout: dropTimeout}
	// commitWithoutWriter commits the placement without a writer.
	commitWithoutWriter = handoff.Step{Action: handoff.Commit}
)

// TestScript checks the actions of a step and the validation of a script.
func TestScript(t *testing.T) {
	t.Parallel()

	t.Run("Action", func(t *testing.T) {
		t.Parallel()

		t.Run("Valid", func(t *testing.T) {
			t.Parallel()

			t.Run("reports true for Add, Drop and Commit", func(t *testing.T) {
				t.Parallel()
				prop.True(t, handoff.Action.Valid, "every constant of Action must be Valid",
					prop.Using(prop.SampledFrom(handoff.Add, handoff.Drop, handoff.Commit)))
			})

			t.Run("reports false for the zero Action", func(t *testing.T) {
				t.Parallel()
				assert.False(t, handoff.Action(0).Valid(), "the zero Action must not be Valid")
			})

			t.Run("reports false for a value above Commit", func(t *testing.T) {
				t.Parallel()
				prop.False(t, handoff.Action.Valid, "a value above the last constant must not be Valid",
					prop.Using(prop.Integer[handoff.Action](handoff.Commit+1, 255)))
			})
		})

		t.Run("String", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name string
				give handoff.Action
				want string
			}{
				{name: "returns add for Add", give: handoff.Add, want: "add"},
				{name: "returns drop for Drop", give: handoff.Drop, want: "drop"},
				{name: "returns commit for Commit", give: handoff.Commit, want: "commit"},
				{name: "returns the number of the zero Action", give: 0, want: "Action(0)"},
				{name: "returns the number of a value above Commit", give: handoff.Commit + 1, want: "Action(4)"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					assert.Equal(t, tt.give.String(), tt.want, "String must return the spelling of the action")
				})
			}
		})
	})

	t.Run("Validate", func(t *testing.T) {
		t.Parallel()

		valid := []struct {
			name     string
			script   handoff.Script
			transfer handoff.Transfer
		}{
			{
				name: "returns nil for a script with a step on every side",
				script: handoff.Script{
					Steps: []handoff.Step{dropFrom, commitTo, addTo},
					Abort: [][]handoff.Step{nil, nil, {commitFallback, addFallback, dropTo}},
				},
				transfer: everySide,
			},
			{
				name:     "returns nil for a commit without a writer",
				script:   handoff.Script{Steps: []handoff.Step{commitWithoutWriter}, Abort: [][]handoff.Step{nil}},
				transfer: handoff.Transfer{To: nodeB},
			},
			{
				name: "returns nil for a script without a From in a transfer without a From",
				script: handoff.Script{
					Steps: []handoff.Step{commitTo, addTo},
					Abort: [][]handoff.Step{nil, {commitWithoutWriter}},
				},
				transfer: handoff.Transfer{To: nodeB},
			},
		}
		for _, tt := range valid {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.NoError(t, tt.script.Validate(tt.transfer), "Validate must accept the script")
			})
		}

		// Each row makes Validate report one problem per entry of want, in the same order.
		invalid := []struct {
			name     string
			script   handoff.Script
			transfer handoff.Transfer
			want     []string
		}{
			{
				name:     "returns ErrScript for a script without steps",
				transfer: everySide,
				want:     []string{"the script has no steps"},
			},
			{
				name:     "returns ErrScript for fewer abort paths than steps",
				script:   handoff.Script{Steps: []handoff.Step{addTo}},
				transfer: everySide,
				want:     []string{"the script has 0 abort paths for 1 steps"},
			},
			{
				name:     "returns ErrScript for more abort paths than steps",
				script:   handoff.Script{Steps: []handoff.Step{addTo}, Abort: [][]handoff.Step{nil, nil}},
				transfer: everySide,
				want:     []string{"the script has 2 abort paths for 1 steps"},
			},
			{
				name: "returns ErrScript for the zero Action",
				script: handoff.Script{
					Steps: []handoff.Step{{Side: handoff.To, Timeout: addTimeout}},
					Abort: [][]handoff.Step{nil},
				},
				transfer: everySide,
				want:     []string{"step 0: the action Action(0) is not valid"},
			},
			{
				name: "returns ErrScript for an action above Commit",
				script: handoff.Script{
					Steps: []handoff.Step{{Action: handoff.Commit + 1, Side: handoff.To, Timeout: addTimeout}},
					Abort: [][]handoff.Step{nil},
				},
				transfer: everySide,
				want:     []string{"step 0: the action Action(4) is not valid"},
			},
			{
				name:     "returns ErrScript for an add on a side that the transfer does not have",
				script:   handoff.Script{Steps: []handoff.Step{addFallback}, Abort: [][]handoff.Step{nil}},
				transfer: handoff.Transfer{From: nodeA, To: nodeB},
				want:     []string{"step 0: the add needs a node on the side fallback, and the transfer has none"},
			},
			{
				name: "returns ErrScript for a drop on the zero Side",
				script: handoff.Script{
					Steps: []handoff.Step{{Action: handoff.Drop, Timeout: dropTimeout}},
					Abort: [][]handoff.Step{nil},
				},
				transfer: everySide,
				want:     []string{"step 0: the drop needs a node on the side Side(0)"},
			},
			{
				name: "returns ErrScript for an add on a side above Fallback",
				script: handoff.Script{
					Steps: []handoff.Step{{Action: handoff.Add, Side: handoff.Fallback + 1, Timeout: addTimeout}},
					Abort: [][]handoff.Step{nil},
				},
				transfer: everySide,
				want:     []string{"step 0: the add needs a node on the side Side(4)"},
			},
			{
				name: "returns ErrScript for a commit on a side that the transfer does not have",
				script: handoff.Script{
					Steps: []handoff.Step{{Action: handoff.Commit, Side: handoff.From}},
					Abort: [][]handoff.Step{nil},
				},
				transfer: handoff.Transfer{To: nodeB},
				want:     []string{"step 0: the commit needs a node on the side from"},
			},
			{
				name: "returns ErrScript for an add without a timeout",
				script: handoff.Script{
					Steps: []handoff.Step{{Action: handoff.Add, Side: handoff.To}},
					Abort: [][]handoff.Step{nil},
				},
				transfer: everySide,
				want:     []string{"step 0: the timeout 0s of the add is not above zero"},
			},
			{
				name: "returns ErrScript for a drop with a negative timeout",
				script: handoff.Script{
					Steps: []handoff.Step{{Action: handoff.Drop, Side: handoff.From, Timeout: -1}},
					Abort: [][]handoff.Step{nil},
				},
				transfer: everySide,
				want:     []string{"step 0: the timeout -1ns of the drop is not above zero"},
			},
			{
				name: "returns ErrScript for a commit with a timeout",
				script: handoff.Script{
					Steps: []handoff.Step{{Action: handoff.Commit, Side: handoff.To, Timeout: addTimeout}},
					Abort: [][]handoff.Step{nil},
				},
				transfer: everySide,
				want:     []string{"step 0: the timeout 30s of the commit is not zero"},
			},
			{
				name: "returns ErrScript for the steps of an abort path",
				script: handoff.Script{
					Steps: []handoff.Step{dropFrom, commitTo, addTo},
					Abort: [][]handoff.Step{nil, nil, {commitFallback, addFallback, dropTo}},
				},
				transfer: handoff.Transfer{From: nodeA, To: nodeB},
				want: []string{
					"step 0 of the abort path of step 2: the commit needs a node on the side fallback",
					"step 1 of the abort path of step 2: the add needs a node on the side fallback",
				},
			},
			{
				name: "returns one error per broken rule of a step",
				script: handoff.Script{
					Steps: []handoff.Step{{Action: handoff.Add, Side: handoff.Fallback}},
					Abort: [][]handoff.Step{nil},
				},
				transfer: handoff.Transfer{From: nodeA, To: nodeB},
				want: []string{
					"step 0: the add needs a node on the side fallback",
					"step 0: the timeout 0s of the add is not above zero",
				},
			},
			{
				name: "returns the problems of the steps before the problems of the abort paths",
				script: handoff.Script{
					Steps: []handoff.Step{{Action: handoff.Add, Side: handoff.To}},
					Abort: [][]handoff.Step{{{Action: handoff.Drop, Side: handoff.To}}},
				},
				transfer: everySide,
				want: []string{
					"step 0: the timeout 0s of the add is not above zero",
					"step 0 of the abort path of step 0: the timeout 0s of the drop is not above zero",
				},
			},
			{
				name:     "returns the problems of the script before the problems of its steps",
				script:   handoff.Script{Steps: []handoff.Step{{Action: handoff.Add, Side: handoff.To}}},
				transfer: everySide,
				want: []string{
					"the script has 0 abort paths for 1 steps",
					"step 0: the timeout 0s of the add is not above zero",
				},
			},
		}
		for _, tt := range invalid {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				err := tt.script.Validate(tt.transfer)
				expect.ErrorIs(t, err, handoff.ErrScript, "Validate must return ErrScript")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
				joined := assert.ErrorAs[interface{ Unwrap() []error }](t, err,
					"Validate must join the problems of the script")
				assert.Length(t, joined.Unwrap(), len(tt.want), "Validate must return one error per problem")
				for i, problem := range joined.Unwrap() {
					expect.ErrorIs(t, problem, handoff.ErrScript, "each problem must wrap ErrScript")
					expect.Contains(t, problem.Error(), tt.want[i], "each problem must state its step and its rule")
				}
			})
		}
	})
}

// TestScriptAllocs checks the allocation contracts of Action and Script. MaxAllocs
// counts the allocations of the whole process, so the test does not run in parallel.
func TestScriptAllocs(t *testing.T) {
	action := handoff.Commit

	t.Run("Action", func(t *testing.T) {
		t.Run("Valid", func(t *testing.T) {
			var got bool
			expect.MaxAllocs(t, func() { got = action.Valid() }, 0, "Valid must not allocate")
			assert.True(t, got, "the test must measure a Valid action")
		})

		t.Run("String", func(t *testing.T) {
			var got string
			expect.MaxAllocs(t, func() { got = action.String() }, 0, "String of a Valid action must not allocate")
			assert.Equal(t, got, "commit", "the test must measure the spelling of a Valid action")
		})
	})

	t.Run("Validate", func(t *testing.T) {
		script := handoff.Script{
			Steps: []handoff.Step{dropFrom, commitTo, addTo},
			Abort: [][]handoff.Step{nil, nil, {commitFallback, addFallback, dropTo}},
		}
		var err error
		expect.MaxAllocs(t, func() { err = script.Validate(everySide) }, 0,
			"Validate of a valid script must not allocate")
		assert.NoError(t, err, "the test must measure a valid script")
	})
}

// BenchmarkScript reports the cost of Validate, which the core calls before it commits
// each transition, and fails when it allocates.
func BenchmarkScript(b *testing.B) {
	b.Run("Validate", func(b *testing.B) {
		script := handoff.Script{
			Steps: []handoff.Step{dropFrom, commitTo, addTo},
			Abort: [][]handoff.Step{nil, nil, {commitFallback, addFallback, dropTo}},
		}
		var err error

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			err = script.Validate(everySide)
		}

		assert.NoError(b, err, "the benchmark must measure a valid script")
	})
}

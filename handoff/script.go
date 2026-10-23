// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package handoff

import (
	"errors"
	"fmt"
	"time"
)

// Action is the kind of a step of a script, such as [Commit]. A step completes when the
// condition of its action holds, and the engine then starts the next step.
//
// [Action.IsValid] refuses the zero Action.
//
// # Encoding
//
// [Action.String] returns the spelling of an Action, such as "commit", and the form
// "Action(n)" for any other value. [ParseAction] returns the action of a spelling.
//
// # Concurrency
//
// An Action is a comparable value.
//
// # Allocation contract
//
// [Action.IsValid], and [Action.String] of Add, Drop or Commit, do not allocate. String
// allocates the form of any other value.
//
//dokimi:enum text=comment
type Action uint8

const (
	// Add is a step in which the engine calls the callback add on the node of the side
	// of the step. The callback starts a tenure at the epoch of the side. The step
	// completes when the callback succeeds, and the handler of the node returns once the
	// replica serves.
	Add Action = 1 // add
	// Drop is a step in which the engine calls the callback drop on the node of the side
	// of the step. The callback ends the tenure at the epoch of the side. The step
	// completes when the callback succeeds, or once the incarnation of the node that the
	// step addresses has ended. The incarnation has ended when a later incarnation of the
	// node refuses the callback, or when the lease of its session lapses and the lock
	// delay passes.
	Drop Action = 2 // drop
	// Commit is a step in which the engine commits the placement of the shard with the
	// node of the side of the step as its writer. The writer has the epoch of the side.
	// For the zero Side, the engine commits the placement without a writer. The step
	// completes when the commit applies.
	Commit Action = 3 // commit
)

// Step is one step of a script. It is a callback on the node of one side of a transfer,
// or a commit of the placement of the shard.
//
// The zero Step is not valid, because IsValid refuses its Action.
//
// # Concurrency
//
// A Step is a comparable value.
type Step struct {
	// Timeout bounds one attempt of an Add or a Drop. The engine cancels the callback
	// when the timeout passes, and retries the step with the same key. A Commit has no
	// timeout, because the store bounds each commit.
	Timeout time.Duration
	// Action is the kind of the step.
	Action Action
	// Side is the side of the transfer whose node and epoch the step uses. It is the
	// zero Side only in a Commit without a writer.
	Side Side
}

// problems returns one error per rule of a step that s breaks for the transfer t, or nil
// when s keeps every rule. The rules are in the docblock of Script.Validate. For an
// Action that IsValid refuses, problems returns that problem alone, because the other
// rules depend on the action.
func (s Step) problems(t Transfer) []error {
	if !s.Action.IsValid() {
		return []error{fmt.Errorf("the action %v is not valid", s.Action)}
	}
	var problems []error
	commit := s.Action == Commit
	if t.Node(s.Side) == "" && (!commit || s.Side != 0) {
		problems = append(problems,
			fmt.Errorf("the %v needs a node on the side %v, and the transfer has none", s.Action, s.Side))
	}
	if commit {
		if s.Timeout != 0 {
			problems = append(problems, fmt.Errorf("the timeout %v of the commit is not zero", s.Timeout))
		}
	} else if s.Timeout <= 0 {
		problems = append(problems, fmt.Errorf("the timeout %v of the %v is not above zero", s.Timeout, s.Action))
	}
	return problems
}

// Script is the steps of one transfer, with an abort path for each step. Abort[i] lists
// the steps that undo the move when Steps[i] fails after every earlier step completed.
// An abort path without steps ends the transition at once.
//
// The zero Script has no steps, and Validate refuses it.
//
// # Concurrency
//
// A copy of a Script shares its steps with the original.
type Script struct {
	// Steps lists the steps of the move, from the first to the last.
	Steps []Step
	// Abort lists one abort path per element of Steps, in the same order.
	Abort [][]Step
}

// Validate returns an error that joins every problem of s for the transfer t, or nil
// when s is valid. The core validates the script of each transfer before it commits
// the transition. A script that Validate accepts runs steps only on the nodes of t.
// Each step runs at the epoch that the core reserved for its side.
//
// These are the rules of a script:
//
//   - The script has at least one step.
//   - The script has one abort path per step.
//   - IsValid accepts the Action of each step.
//   - The side of each step other than a Commit with the zero Side has a node in t.
//   - An Add and a Drop have a Timeout above zero, and a Commit has no Timeout.
//
// The text of each problem contains the position of its step: its index in Steps, or
// its index in an abort path and the index of the step that the abort path undoes.
//
// Error modes:
//   - an error that joins one error per broken rule, each of which wraps ErrScript.
//
// # Allocation contract
//
// Zero alloc for a valid script.
func (s Script) Validate(t Transfer) error {
	var problems []error
	if len(s.Steps) == 0 {
		problems = append(problems, fmt.Errorf("%w: the script has no steps", ErrScript))
	}
	if len(s.Abort) != len(s.Steps) {
		problems = append(problems,
			fmt.Errorf("%w: the script has %d abort paths for %d steps", ErrScript, len(s.Abort), len(s.Steps)))
	}
	for i, step := range s.Steps {
		for _, p := range step.problems(t) {
			problems = append(problems, fmt.Errorf("%w: step %d: %w", ErrScript, i, p))
		}
	}
	for i, path := range s.Abort {
		for j, step := range path {
			for _, p := range step.problems(t) {
				problems = append(problems,
					fmt.Errorf("%w: step %d of the abort path of step %d: %w", ErrScript, j, i, p))
			}
		}
	}
	return errors.Join(problems...)
}

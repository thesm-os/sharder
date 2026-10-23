// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/machine"
)

// errAttempt is an error of an attempt that a retry does not fix.
var errAttempt = errs.WithClass(errors.New("test: the attempt failed"), errs.Invalid)

// TestAttempt checks the class of the error of one attempt of a call.
func TestAttempt(t *testing.T) {
	t.Parallel()

	t.Run("ClassifyAttempt", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies an error that wraps context.DeadlineExceeded as errs.Transient", func(t *testing.T) {
			t.Parallel()
			err := machine.ClassifyAttempt(fmt.Errorf("test: the handler: %w", context.DeadlineExceeded))
			assert.Equal(t, errs.Classify(err), errs.Transient,
				"an attempt that ran past its deadline must be transient")
		})

		t.Run("classifies an error of another class that wraps context.DeadlineExceeded as errs.Transient",
			func(t *testing.T) {
				t.Parallel()
				late := errs.WithClass(fmt.Errorf("test: the handler: %w", context.DeadlineExceeded), errs.Invalid)
				assert.Equal(t, errs.Classify(machine.ClassifyAttempt(late)), errs.Transient,
					"the deadline of an attempt must take precedence over the class of its error")
			})

		t.Run("keeps context.DeadlineExceeded in the chain of the error", func(t *testing.T) {
			t.Parallel()
			err := machine.ClassifyAttempt(fmt.Errorf("test: the handler: %w", context.DeadlineExceeded))
			assert.ErrorIs(t, err, context.DeadlineExceeded, "the classified error must still wrap the deadline")
		})

		t.Run("returns an error without a deadline unchanged", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, machine.ClassifyAttempt(errAttempt), errAttempt,
				"an error without a deadline must keep its class")
		})

		t.Run("returns an error that wraps context.Canceled unchanged", func(t *testing.T) {
			t.Parallel()
			cancelled := fmt.Errorf("test: the handler: %w", context.Canceled)
			assert.Equal(t, machine.ClassifyAttempt(cancelled), cancelled,
				"the cancellation of an attempt must keep its class")
		})

		t.Run("returns nil for nil", func(t *testing.T) {
			t.Parallel()
			assert.Nil(t, machine.ClassifyAttempt(nil), "an attempt without an error must have no error")
		})
	})
}

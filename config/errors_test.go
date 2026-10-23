// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config_test

import (
	"errors"
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/config"
)

// TestErrors checks the class of each sentinel error of package config.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err   error
		name  string
		class errs.Class
	}{
		{name: "ErrDuplicate", err: config.ErrDuplicate, class: errs.Invalid},
		{name: "ErrUnknown", err: config.ErrUnknown, class: errs.Invalid},
		{name: "ErrUnsupported", err: config.ErrUnsupported, class: errs.Unsupported},
		{name: "ErrSpec", err: config.ErrSpec, class: errs.Invalid},
		{name: "ErrRegistration", err: config.ErrRegistration, class: errs.Invalid},
		{name: "ErrChange", err: config.ErrChange, class: errs.Invalid},
		{name: "ErrUnknownAxis", err: config.ErrUnknownAxis, class: errs.Invalid},
		{name: "ErrUnknownCapability", err: config.ErrUnknownCapability, class: errs.Invalid},
		{name: "ErrUnknownInterruption", err: config.ErrUnknownInterruption, class: errs.Invalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("classifies as its class", func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, errs.Classify(tt.err), tt.class, "the sentinel must classify as its class")
			})
		})
	}

	t.Run("ErrUnsupported", func(t *testing.T) {
		t.Parallel()

		t.Run("matches errors.ErrUnsupported", func(t *testing.T) {
			t.Parallel()
			assert.ErrorIs(t, config.ErrUnsupported, errors.ErrUnsupported,
				"ErrUnsupported must match the sentinel of the standard library")
		})
	})
}

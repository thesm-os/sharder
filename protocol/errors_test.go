// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol_test

import (
	"errors"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/protocol"
)

// TestErrors checks the class of each sentinel error of package protocol, and the class of
// a CallError.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give error
		want errs.Class
	}{
		{name: "ErrUnsupported", give: protocol.ErrUnsupported, want: errs.Unsupported},
		{name: "ErrUnreachable", give: protocol.ErrUnreachable, want: errs.Transient},
		{name: "ErrUnknownCall", give: protocol.ErrUnknownCall, want: errs.Invalid},
		{name: "ErrUnknownPath", give: protocol.ErrUnknownPath, want: errs.Invalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("classifies as "+tt.want.String(), func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, errs.Classify(tt.give), tt.want, "the sentinel must classify as its documented class")
			})
		})
	}

	t.Run("ErrUnsupported", func(t *testing.T) {
		t.Parallel()

		t.Run("matches errors.ErrUnsupported", func(t *testing.T) {
			t.Parallel()
			assert.ErrorIs(t, protocol.ErrUnsupported, errors.ErrUnsupported,
				"ErrUnsupported must match the error of the standard library")
		})
	})

	t.Run("CallError", func(t *testing.T) {
		t.Parallel()

		t.Run("classifies as the class of the error of the server", func(t *testing.T) {
			t.Parallel()
			prop.Equal(t, func(c errs.Class) errs.Class { return errs.Classify(&protocol.CallError{ErrorClass: c}) },
				func(c errs.Class) errs.Class { return c }, "errs.Classify must return the class of the CallError",
				prop.Using(prop.SampledFrom(errs.Unspecified, errs.Transient, errs.Conflict, errs.NotFound,
					errs.Invalid, errs.Unsupported, errs.Denied, errs.Integrity)))
		})
	})
}

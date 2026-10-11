// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol_test

import (
	"errors"
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/protocol"
)

// errorPrefix is the prefix of the text of every error of package protocol.
const errorPrefix = "protocol: "

// TestErrors checks the class and the text of each sentinel error of package protocol.
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

			t.Run("starts its text with the package's name", func(t *testing.T) {
				t.Parallel()
				assert.HasPrefix(t, tt.give.Error(), errorPrefix,
					"the text of the sentinel must start with the name of the package")
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
}

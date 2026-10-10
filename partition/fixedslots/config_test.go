// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fixedslots_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/partition/xxhash64"
)

// TestConfig checks the validation of a Config.
func TestConfig(t *testing.T) {
	t.Parallel()

	t.Run("Validate", func(t *testing.T) {
		t.Parallel()

		valid := []struct {
			name string
			give fixedslots.Config
		}{
			{name: "returns nil for one slot", give: fixedslots.Config{Hash: xxhash64.Name, Slots: 1}},
			{
				name: "returns nil for MaxSlots slots",
				give: fixedslots.Config{Hash: xxhash64.Name, Slots: fixedslots.MaxSlots},
			},
		}
		for _, tt := range valid {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.NoError(t, tt.give.Validate(), "Validate must accept the configuration")
			})
		}

		invalid := []struct {
			name string
			give fixedslots.Config
		}{
			{name: "returns ErrConfig for an empty hash", give: fixedslots.Config{Slots: 1}},
			{name: "returns ErrConfig for zero slots", give: fixedslots.Config{Hash: xxhash64.Name}},
			{
				name: "returns ErrConfig for one slot more than MaxSlots",
				give: fixedslots.Config{Hash: xxhash64.Name, Slots: fixedslots.MaxSlots + 1},
			},
		}
		for _, tt := range invalid {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				err := tt.give.Validate()
				expect.ErrorIs(t, err, fixedslots.ErrConfig, "Validate must return ErrConfig")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
			})
		}

		t.Run("returns one error per problem of the zero Config", func(t *testing.T) {
			t.Parallel()
			joined := assert.ErrorAs[interface{ Unwrap() []error }](t, fixedslots.Config{}.Validate(),
				"Validate must join the problems of the configuration")
			assert.Length(t, joined.Unwrap(), 2, "Validate must return a problem for each of the two fields")
		})
	})
}

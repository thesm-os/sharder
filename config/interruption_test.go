// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config_test

import (
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/config"
)

const (
	// recordedPlanned is the spelling of PlannedInterruption, which the specs of
	// namespaces record.
	recordedPlanned = "planned"
	// unknownInterruption is a word that is not the spelling of an interruption.
	unknownInterruption = "none"
)

// TestInterruption checks Valid, String and the text methods of Interruption.
func TestInterruption(t *testing.T) {
	t.Parallel()

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		t.Run("reports true for PlannedInterruption", func(t *testing.T) {
			t.Parallel()
			assert.True(t, config.PlannedInterruption.Valid(), "PlannedInterruption must be Valid")
		})

		t.Run("reports false for the zero Interruption", func(t *testing.T) {
			t.Parallel()
			assert.False(t, config.Interruption(0).Valid(), "the zero Interruption must not be Valid")
		})

		t.Run("reports false for a value above PlannedInterruption", func(t *testing.T) {
			t.Parallel()
			prop.False(t, config.Interruption.Valid, "a value above the last constant must not be Valid",
				prop.Using(prop.Integer[config.Interruption](config.PlannedInterruption+1, 255)))
		})
	})

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		t.Run("returns planned for PlannedInterruption", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, config.PlannedInterruption.String(), recordedPlanned, "String must return the spelling")
		})

		t.Run("returns the number of a value that is not Valid", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, (config.PlannedInterruption + 1).String(), "Interruption(2)",
				"String must return the number of a value that is not Valid")
		})
	})

	t.Run("MarshalText", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the spelling of the interruption", func(t *testing.T) {
			t.Parallel()
			got, err := config.PlannedInterruption.MarshalText()
			assert.NoError(t, err, "MarshalText must encode a Valid interruption")
			assert.Equal(t, string(got), recordedPlanned, "MarshalText must return the spelling")
		})

		t.Run("returns ErrSpec for the zero Interruption", func(t *testing.T) {
			t.Parallel()
			_, err := config.Interruption(0).MarshalText()
			assert.ErrorIs(t, err, config.ErrSpec, "MarshalText must refuse an interruption that is not Valid")
		})

		t.Run("encodes an interruption in JSON as a string", func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(config.PlannedInterruption)
			assert.NoError(t, err, "encoding/json must encode a Valid interruption")
			assert.Equal(t, string(got), `"`+recordedPlanned+`"`, "encoding/json must write the spelling")
		})
	})

	t.Run("UnmarshalText", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the interruption that MarshalText encodes", func(t *testing.T) {
			t.Parallel()
			text, err := config.PlannedInterruption.MarshalText()
			assert.NoError(t, err, "MarshalText must encode a Valid interruption")
			var got config.Interruption
			assert.NoError(t, got.UnmarshalText(text), "UnmarshalText must decode the spelling")
			assert.Equal(t, got, config.PlannedInterruption, "UnmarshalText must return the encoded interruption")
		})

		t.Run("returns ErrSpec for text that is not the spelling of an interruption", func(t *testing.T) {
			t.Parallel()
			got := config.PlannedInterruption
			err := got.UnmarshalText([]byte(unknownInterruption))
			expect.ErrorIs(t, err, config.ErrSpec, "UnmarshalText must return ErrSpec")
			expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of UnmarshalText must classify as Invalid")
			expect.Equal(t, got, config.PlannedInterruption, "UnmarshalText must leave the interruption unchanged")
		})

		t.Run("decodes a JSON string", func(t *testing.T) {
			t.Parallel()
			var got config.Interruption
			assert.NoError(t, json.Unmarshal([]byte(`"`+recordedPlanned+`"`), &got),
				"encoding/json must decode the spelling of an interruption")
			assert.Equal(t, got, config.PlannedInterruption, "encoding/json must return the interruption")
		})
	})
}

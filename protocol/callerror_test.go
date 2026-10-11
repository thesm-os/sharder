// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol_test

import (
	"encoding/hex"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/protocol"
)

// recordedMessage is the message of recordedCallError.
const recordedMessage = "generation moved"

// recordedCallErrorHex pins the encoding of recordedCallError: field 1 is the class
// Conflict, and field 2 the message.
const recordedCallErrorHex = "0802" + "1210" + "67656e65726174696f6e206d6f766564"

// recordedCallError is the error whose encoding recordedCallErrorHex pins.
var recordedCallError = protocol.CallError{ErrorClass: errs.Conflict, Message: recordedMessage}

// classes generates each class of core's errs.
var classes = prop.SampledFrom(errs.Unspecified, errs.Transient, errs.Conflict, errs.NotFound, errs.Invalid,
	errs.Unsupported, errs.Denied, errs.Integrity)

// TestCallError checks the text, the class and the recorded encoding of a CallError.
func TestCallError(t *testing.T) {
	t.Parallel()

	t.Run("Error", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the message", func(t *testing.T) {
			t.Parallel()
			e := recordedCallError
			assert.Equal(t, e.Error(), recordedMessage, "Error must return the message of the server")
		})
	})

	t.Run("Class", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the class of the error of the server", func(t *testing.T) {
			t.Parallel()
			prop.Equal(t, func(c errs.Class) errs.Class { return errs.Classify(&protocol.CallError{ErrorClass: c}) },
				func(c errs.Class) errs.Class { return c }, "errs.Classify must return the class of the CallError",
				prop.Using(classes))
		})
	})

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a CallError", func(t *testing.T) {
			t.Parallel()
			e := recordedCallError
			got, err := e.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the CallError")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedCallErrorHex,
				"AppendBinary must write fields 1 and 2 as recorded",
			)
		})
	})
}

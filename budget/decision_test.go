// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package budget_test

import (
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/budget"
)

// recordedDeferralJSON pins the JSON of recordedDeferral, which the receipt of a drain
// proposal contains. The time to wait is a count of nanoseconds.
const recordedDeferralJSON = `{"reason":"draining","retryAfter":1000000000,"allowed":false}`

// The recorded encodings of decisions.
const (
	// recordedDeferralHex pins the encoding of recordedDeferral. Field 2 is the reason
	// draining. Field 3 is the time to wait, one second, as the zigzag varint of its
	// nanoseconds.
	recordedDeferralHex = "1208" + "647261696e696e67" + "18" + "80a8d6b907"
	// recordedAllowanceHex pins the encoding of recordedAllowance: field 1 is true.
	recordedAllowanceHex = "0801"

	// recordedReason is the reason of recordedDeferral.
	recordedReason = "draining"
)

var (
	// recordedDeferral is the Decision whose encoding recordedDeferralHex pins.
	recordedDeferral = budget.Decision{Reason: recordedReason, RetryAfter: time.Second}
	// recordedAllowance is the Decision whose encoding recordedAllowanceHex pins.
	recordedAllowance = budget.Decision{Allowed: true}
)

// TestDecision checks the recorded encodings of a deferral and of an allowance.
func TestDecision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		appendName    string
		unmarshalName string
		wantHex       string
		give          budget.Decision
	}{
		{
			appendName:    "appends the recorded encoding of a deferral",
			unmarshalName: "returns the deferral of its recorded encoding",
			give:          recordedDeferral,
			wantHex:       recordedDeferralHex,
		},
		{
			appendName:    "appends the recorded encoding of an allowance",
			unmarshalName: "returns the allowance of its recorded encoding",
			give:          recordedAllowance,
			wantHex:       recordedAllowanceHex,
		},
	}

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		for _, tt := range tests {
			t.Run(tt.appendName, func(t *testing.T) {
				t.Parallel()
				got, err := tt.give.AppendBinary(nil)
				assert.NoError(t, err, "AppendBinary must encode the Decision")
				assert.Equal(t, hex.EncodeToString(got), tt.wantHex, "AppendBinary must write the recorded fields")
			})
		}
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		for _, tt := range tests {
			t.Run(tt.unmarshalName, func(t *testing.T) {
				t.Parallel()
				data, err := hex.DecodeString(tt.wantHex)
				assert.NoError(t, err, "the recorded encoding must be hexadecimal")
				var got budget.Decision
				assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
				assert.Equal(t, got, tt.give, "UnmarshalBinary must return the recorded Decision")
			})
		}
	})

	t.Run("Decision", func(t *testing.T) {
		t.Parallel()

		t.Run("encodes a deferral as the recorded JSON", func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(&recordedDeferral)
			assert.NoError(t, err, "encoding/json must encode a Decision")
			assert.Equal(t, string(got), recordedDeferralJSON, "a Decision must keep its recorded JSON")
		})

		t.Run("decodes a deferral from its recorded JSON", func(t *testing.T) {
			t.Parallel()
			var got budget.Decision
			err := json.Unmarshal([]byte(recordedDeferralJSON), &got)
			assert.NoError(t, err, "encoding/json must decode a Decision")
			assert.Equal(t, got, recordedDeferral, "a Decision must decode from its recorded JSON")
		})
	})
}

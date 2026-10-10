// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package ownership_test

import (
	"encoding/json"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/shard"
)

const (
	// recordedStop is the spelling of Stop, which the specs of namespaces record.
	recordedStop = "stop"
	// recordedContinueFenced is the spelling of ContinueFenced, which the specs of
	// namespaces record.
	recordedContinueFenced = "continue-fenced"
	// unknownAction is a spelling of no action.
	unknownAction = "pause"
)

// TestLease checks the actions on the loss of a lease, and the validity of a lease.
func TestLease(t *testing.T) {
	t.Parallel()

	t.Run("OnLeaseLoss", func(t *testing.T) {
		t.Parallel()

		t.Run("Valid", func(t *testing.T) {
			t.Parallel()

			t.Run("reports true for Stop and ContinueFenced", func(t *testing.T) {
				t.Parallel()
				prop.True(t, ownership.OnLeaseLoss.Valid, "every constant of OnLeaseLoss must be Valid",
					prop.Using(prop.SampledFrom(ownership.Stop, ownership.ContinueFenced)))
			})

			t.Run("reports false for the zero OnLeaseLoss", func(t *testing.T) {
				t.Parallel()
				assert.False(t, ownership.OnLeaseLoss(0).Valid(), "the zero OnLeaseLoss must not be Valid")
			})

			t.Run("reports false for a value above ContinueFenced", func(t *testing.T) {
				t.Parallel()
				prop.False(t, ownership.OnLeaseLoss.Valid, "a value above the last constant must not be Valid",
					prop.Using(prop.Integer[ownership.OnLeaseLoss](ownership.ContinueFenced+1, 255)))
			})
		})

		t.Run("String", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name string
				give ownership.OnLeaseLoss
				want string
			}{
				{name: "returns stop for Stop", give: ownership.Stop, want: recordedStop},
				{
					name: "returns continue-fenced for ContinueFenced",
					give: ownership.ContinueFenced, want: recordedContinueFenced,
				},
				{name: "returns the number of a value that is not Valid", give: 3, want: "OnLeaseLoss(3)"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					assert.Equal(t, tt.give.String(), tt.want, "String must return the recorded spelling of the action")
				})
			}
		})

		t.Run("MarshalText", func(t *testing.T) {
			t.Parallel()

			t.Run("returns the spelling of the action", func(t *testing.T) {
				t.Parallel()
				got, err := ownership.ContinueFenced.MarshalText()
				assert.NoError(t, err, "MarshalText must encode a Valid action")
				assert.Equal(t, string(got), recordedContinueFenced, "MarshalText must return the spelling")
			})

			t.Run("returns ErrConfig for the zero OnLeaseLoss", func(t *testing.T) {
				t.Parallel()
				_, err := ownership.OnLeaseLoss(0).MarshalText()
				assert.ErrorIs(t, err, ownership.ErrConfig, "MarshalText must refuse an action that is not Valid")
			})

			t.Run("encodes an action in JSON as a string", func(t *testing.T) {
				t.Parallel()
				got, err := json.Marshal(ownership.Stop)
				assert.NoError(t, err, "encoding/json must encode a Valid action")
				assert.Equal(t, string(got), `"`+recordedStop+`"`, "encoding/json must write the spelling")
			})
		})

		t.Run("UnmarshalText", func(t *testing.T) {
			t.Parallel()

			t.Run("returns the action that MarshalText encodes", func(t *testing.T) {
				t.Parallel()
				prop.RoundTrip(
					t,
					ownership.OnLeaseLoss.MarshalText,
					func(text []byte) (ownership.OnLeaseLoss, error) {
						var o ownership.OnLeaseLoss
						err := o.UnmarshalText(text)
						return o, err
					},
					"UnmarshalText must return the action that MarshalText encodes",
					prop.Using(prop.SampledFrom(ownership.Stop, ownership.ContinueFenced)),
				)
			})

			t.Run("returns ErrConfig for the spelling of no action", func(t *testing.T) {
				t.Parallel()
				got := ownership.Stop
				err := got.UnmarshalText([]byte(unknownAction))
				expect.ErrorIs(t, err, ownership.ErrConfig, "UnmarshalText must return ErrConfig")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of UnmarshalText must classify as Invalid")
				expect.Equal(t, got, ownership.Stop, "UnmarshalText must leave the action unchanged")
			})

			t.Run("decodes a JSON string", func(t *testing.T) {
				t.Parallel()
				var got ownership.OnLeaseLoss
				assert.NoError(t, json.Unmarshal([]byte(`"`+recordedContinueFenced+`"`), &got),
					"encoding/json must decode the spelling of an action")
				assert.Equal(t, got, ownership.ContinueFenced, "encoding/json must return the action of the spelling")
			})
		})
	})

	t.Run("Validity", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give ownership.Lease
			want time.Duration
		}{
			{
				name: "returns the TTL less the drift",
				give: ownership.Lease{TTL: shard.Duration(10 * time.Second), MaxDriftRate: 0.01},
				want: 9900 * time.Millisecond,
			},
			{
				name: "returns the TTL for a drift rate of zero",
				give: ownership.Lease{TTL: shard.Duration(10 * time.Second)},
				want: 10 * time.Second,
			},
			{
				name: "rounds down to a nanosecond",
				give: ownership.Lease{TTL: 3, MaxDriftRate: 0.5},
				want: 1,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.give.Validity(), tt.want, "Validity must return the TTL less the drift")
			})
		}
	})
}

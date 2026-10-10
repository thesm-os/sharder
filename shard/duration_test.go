// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard_test

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/shard"
)

// TestDuration checks the text methods of a Duration, and the JSON form that they
// give it.
func TestDuration(t *testing.T) {
	t.Parallel()

	t.Run("MarshalText", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Go duration string", func(t *testing.T) {
			t.Parallel()
			got, err := shard.Duration(90 * time.Second).MarshalText()
			assert.NoError(t, err, "MarshalText must encode a Duration")
			assert.Equal(t, string(got), "1m30s", "MarshalText must return the Go duration string")
		})

		t.Run("encodes a Duration in JSON as a string", func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(shard.Duration(30 * time.Second))
			assert.NoError(t, err, "encoding/json must encode a Duration")
			assert.Equal(t, string(got), `"30s"`, "encoding/json must write the Go duration string")
		})
	})

	t.Run("UnmarshalText", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the duration of a Go duration string", func(t *testing.T) {
			t.Parallel()
			var got shard.Duration
			assert.NoError(t, got.UnmarshalText([]byte("1h2m3.5s")), "UnmarshalText must decode a duration string")
			assert.Equal(t, got, shard.Duration(time.Hour+2*time.Minute+3500*time.Millisecond),
				"UnmarshalText must return the duration of the string")
		})

		t.Run("returns the duration that MarshalText encodes", func(t *testing.T) {
			t.Parallel()
			prop.RoundTrip(
				t,
				func(d shard.Duration) ([]byte, error) { return d.MarshalText() },
				func(text []byte) (shard.Duration, error) {
					var d shard.Duration
					err := d.UnmarshalText(text)
					return d, err
				},
				"UnmarshalText must return the duration that MarshalText encodes",
				prop.Using(prop.Integer[shard.Duration](math.MinInt64, math.MaxInt64)),
			)
		})

		tests := []struct {
			name string
			give string
		}{
			{name: "returns ErrDuration for a number without a unit", give: "30"},
			{name: "returns ErrDuration for an unknown unit", give: "30y"},
			{name: "returns ErrDuration for empty text", give: ""},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got := shard.Duration(time.Second)
				err := got.UnmarshalText([]byte(tt.give))
				expect.ErrorIs(t, err, shard.ErrDuration, "UnmarshalText must return ErrDuration")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of UnmarshalText must classify as Invalid")
				expect.Equal(t, got, shard.Duration(time.Second), "UnmarshalText must leave the Duration unchanged")
			})
		}

		t.Run("decodes a JSON string", func(t *testing.T) {
			t.Parallel()
			var got shard.Duration
			assert.NoError(t, json.Unmarshal([]byte(`"10s"`), &got), "encoding/json must decode a duration string")
			assert.Equal(t, got, shard.Duration(10*time.Second), "encoding/json must return the duration of the string")
		})

		t.Run("refuses a JSON number", func(t *testing.T) {
			t.Parallel()
			var got shard.Duration
			_ = assert.ErrorAs[*json.UnmarshalTypeError](t, json.Unmarshal([]byte(`10`), &got),
				"encoding/json must refuse a number for a Duration")
		})
	})
}

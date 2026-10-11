// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package breakbeforemake_test

import (
	"encoding/json"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/handoff/breakbeforemake"
	"go.thesmos.sh/sharder/shard"
)

const (
	// addTimeout is the timeout of the adds of the protocol of the tests.
	addTimeout = 30 * time.Second
	// dropTimeout is the timeout of the drops of the protocol of the tests.
	dropTimeout = 10 * time.Second
	// recordedConfigJSON is the JSON form of testConfig, which the specs of namespaces
	// record.
	recordedConfigJSON = `{"addTimeout":"30s","dropTimeout":"10s"}`
)

// testConfig is the configuration of the protocol of the tests.
var testConfig = breakbeforemake.Config{
	AddTimeout:  shard.Duration(addTimeout),
	DropTimeout: shard.Duration(dropTimeout),
}

// TestConfig checks the JSON form and the validation of a Config.
func TestConfig(t *testing.T) {
	t.Parallel()

	t.Run("encodes the recorded JSON form", func(t *testing.T) {
		t.Parallel()
		got, err := json.Marshal(&testConfig)
		assert.NoError(t, err, "encoding/json must encode a Config")
		assert.Equal(t, string(got), recordedConfigJSON, "encoding/json must write the keys and the durations")
	})

	t.Run("decodes the recorded JSON form", func(t *testing.T) {
		t.Parallel()
		var got breakbeforemake.Config
		assert.NoError(t, json.Unmarshal([]byte(recordedConfigJSON), &got), "encoding/json must decode a Config")
		assert.Equal(t, got, testConfig, "encoding/json must return the recorded Config")
	})

	t.Run("Validate", func(t *testing.T) {
		t.Parallel()

		valid := []struct {
			name string
			give breakbeforemake.Config
		}{
			{name: "returns nil for timeouts above zero", give: testConfig},
			{
				name: "returns nil for timeouts of one nanosecond",
				give: breakbeforemake.Config{AddTimeout: 1, DropTimeout: 1},
			},
		}
		for _, tt := range valid {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.NoError(t, tt.give.Validate(), "Validate must accept the configuration")
			})
		}

		// Each row makes Validate report one problem per entry of want, in the same order.
		invalid := []struct {
			name string
			give breakbeforemake.Config
			want []string
		}{
			{
				name: "returns ErrConfig for an add timeout of zero",
				give: breakbeforemake.Config{DropTimeout: shard.Duration(dropTimeout)},
				want: []string{"the add timeout 0s is not above zero"},
			},
			{
				name: "returns ErrConfig for a negative add timeout",
				give: breakbeforemake.Config{AddTimeout: -1, DropTimeout: shard.Duration(dropTimeout)},
				want: []string{"the add timeout -1ns is not above zero"},
			},
			{
				name: "returns ErrConfig for a drop timeout of zero",
				give: breakbeforemake.Config{AddTimeout: shard.Duration(addTimeout)},
				want: []string{"the drop timeout 0s is not above zero"},
			},
			{
				name: "returns ErrConfig for a negative drop timeout",
				give: breakbeforemake.Config{AddTimeout: shard.Duration(addTimeout), DropTimeout: -1},
				want: []string{"the drop timeout -1ns is not above zero"},
			},
			{
				name: "returns ErrConfig for each timeout of the zero Config",
				want: []string{"the add timeout 0s", "the drop timeout 0s"},
			},
		}
		for _, tt := range invalid {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				err := tt.give.Validate()
				expect.ErrorIs(t, err, breakbeforemake.ErrConfig, "Validate must return ErrConfig")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
				joined := assert.ErrorAs[interface{ Unwrap() []error }](t, err,
					"Validate must join the problems of the configuration")
				assert.Length(t, joined.Unwrap(), len(tt.want), "Validate must return one error per problem")
				for i, problem := range joined.Unwrap() {
					expect.Contains(t, problem.Error(), tt.want[i], "each error must name the timeout of its problem")
				}
			})
		}
	})
}

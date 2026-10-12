// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin_test

import (
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/replication"
)

// TestReplication checks the factory of the replica state model primary-only.
func TestReplication(t *testing.T) {
	t.Parallel()

	t.Run("PrimaryOnly", func(t *testing.T) {
		t.Parallel()

		t.Run("Parse", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name string
				give string
			}{
				{name: "returns ErrConfig for a replica count", give: `{"replicas":3}`},
				{name: "returns ErrConfig for scaling", give: `{"scale":{"metric":"cpu"}}`},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					_, err := builtin.PrimaryOnly().Parse(json.RawMessage(tt.give))
					assert.ErrorIs(t, err, builtin.ErrConfig, "Parse must refuse every field of primary-only")
					assert.Equal(t, errs.Classify(err), errs.Invalid, "the refusal must classify as Invalid")
				})
			}
		})

		t.Run("Capabilities", func(t *testing.T) {
			t.Parallel()

			t.Run("provides single-writer", func(t *testing.T) {
				t.Parallel()
				f := builtin.PrimaryOnly()
				assert.Equal(t, f.Capabilities(parse(t, f, "")),
					config.Capabilities{Provides: []config.Capability{config.SingleWriter}},
					"primary-only must provide single-writer")
			})
		})

		t.Run("New", func(t *testing.T) {
			t.Parallel()

			t.Run("compiles the model primary-only", func(t *testing.T) {
				t.Parallel()
				f := builtin.PrimaryOnly()
				got, err := f.New(parse(t, f, ""))
				assert.NoError(t, err, "New must compile primary-only")
				assert.Equal(t, got.Model, replication.PrimaryOnly, "New must compile the model primary-only")
			})
		})
	})
}

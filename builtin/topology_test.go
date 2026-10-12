// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin_test

import (
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
)

// TestTopology checks the factory of the topology embedded.
func TestTopology(t *testing.T) {
	t.Parallel()

	t.Run("Embedded", func(t *testing.T) {
		t.Parallel()

		t.Run("Parse", func(t *testing.T) {
			t.Parallel()

			t.Run("returns ErrConfig for a field", func(t *testing.T) {
				t.Parallel()
				_, err := builtin.Embedded().Parse(json.RawMessage(`{"controllers":2}`))
				assert.ErrorIs(t, err, builtin.ErrConfig, "Parse must refuse every field of embedded")
			})
		})

		t.Run("Capabilities", func(t *testing.T) {
			t.Parallel()

			t.Run("provides controller", func(t *testing.T) {
				t.Parallel()
				f := builtin.Embedded()
				assert.Equal(t, f.Capabilities(parse(t, f, "")),
					config.Capabilities{Provides: []config.Capability{config.Controller}},
					"embedded must provide controller")
			})
		})

		t.Run("New", func(t *testing.T) {
			t.Parallel()

			t.Run("builds the topology", func(t *testing.T) {
				t.Parallel()
				f := builtin.Embedded()
				_, err := f.New(parse(t, f, ""))
				assert.NoError(t, err, "New must build embedded")
			})
		})
	})
}

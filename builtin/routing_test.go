// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin_test

import (
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
)

// TestRouting checks the factory of the routing source watch.
func TestRouting(t *testing.T) {
	t.Parallel()

	t.Run("Watch", func(t *testing.T) {
		t.Parallel()

		t.Run("Parse", func(t *testing.T) {
			t.Parallel()

			t.Run("returns ErrConfig for hold", func(t *testing.T) {
				t.Parallel()
				_, err := builtin.Watch().Parse(json.RawMessage(`{"hold":"5s"}`))
				assert.ErrorIs(t, err, builtin.ErrConfig, "Parse must refuse the field hold")
				assert.Equal(t, errs.Classify(err), errs.Invalid, "the refusal must classify as Invalid")
			})
		})

		t.Run("Capabilities", func(t *testing.T) {
			t.Parallel()

			t.Run("requires a controller from the topology", func(t *testing.T) {
				t.Parallel()
				f := builtin.Watch()
				got := f.Capabilities(parse(t, f, "")).Requires
				assert.Length(t, got, 1, "watch must have one requirement")
				expect.Equal(t, got[0].Axis, config.Topology, "watch must require from the topology")
				expect.Equal(t, got[0].Capability, config.Controller, "watch must require a controller")
			})
		})

		t.Run("New", func(t *testing.T) {
			t.Parallel()

			t.Run("builds the routing source", func(t *testing.T) {
				t.Parallel()
				f := builtin.Watch()
				_, err := f.New(parse(t, f, ""))
				assert.NoError(t, err, "New must build watch")
			})
		})
	})
}

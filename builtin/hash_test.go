// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin_test

import (
	"encoding/json"
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/xxhash64"
)

// TestHash checks the factory of the hash xxhash64.
func TestHash(t *testing.T) {
	t.Parallel()

	t.Run("XXHash64", func(t *testing.T) {
		t.Parallel()

		t.Run("Parse", func(t *testing.T) {
			t.Parallel()

			t.Run("returns ErrConfig for a field", func(t *testing.T) {
				t.Parallel()
				_, err := builtin.XXHash64().Parse(json.RawMessage(`{"seed":1}`))
				assert.ErrorIs(t, err, builtin.ErrConfig, "Parse must refuse every field of xxhash64")
			})
		})

		t.Run("Capabilities", func(t *testing.T) {
			t.Parallel()

			t.Run("returns no capabilities", func(t *testing.T) {
				t.Parallel()
				f := builtin.XXHash64()
				assert.Equal(t, f.Capabilities(parse(t, f, "")), config.Capabilities{},
					"xxhash64 must not provide or require a capability")
			})
		})

		t.Run("New", func(t *testing.T) {
			t.Parallel()

			t.Run("builds xxhash64", func(t *testing.T) {
				t.Parallel()
				f := builtin.XXHash64()
				got, err := f.New(parse(t, f, ""))
				assert.NoError(t, err, "New must build xxhash64")
				assert.Equal(t, got, partition.Hash(xxhash64.Hash{}), "New must return xxhash64")
			})
		})
	})
}

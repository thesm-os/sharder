// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package examples_test

import (
	"os"
	"strings"
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/examples"
)

// The parts of the module that the tests read.
const (
	// readme is the file of the catalogue.
	readme = "README.md"
	// internal is the directory of the applications of the examples, which is not an example.
	internal = "internal"
	// rowPrefix starts a row of the table of the catalogue that names an example.
	rowPrefix = "| `"
	// quote ends the directory of a row.
	quote = "`"
)

// TestCatalogue checks the directories of the examples and the table of README.md against
// the keys that the support matrix requires.
func TestCatalogue(t *testing.T) {
	t.Parallel()

	t.Run("Keys", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the key of the directory of each example", func(t *testing.T) {
			t.Parallel()
			entries, err := os.ReadDir(".")
			assert.NoError(t, err, "the directory of the module reads")
			var dirs []string
			for _, e := range entries {
				if e.IsDir() && e.Name() != internal {
					dirs = append(dirs, e.Name())
				}
			}
			assert.Equal(t, dirs, examples.Keys(), "each supported name must have one example, and each example a name")
		})

		t.Run("returns the directory of each row of the table of README.md", func(t *testing.T) {
			t.Parallel()
			raw, err := os.ReadFile(readme)
			assert.NoError(t, err, "README.md reads")
			var rows []string
			for line := range strings.Lines(string(raw)) {
				if rest, ok := strings.CutPrefix(line, rowPrefix); ok {
					dir, _, _ := strings.Cut(rest, quote)
					rows = append(rows, dir)
				}
			}
			assert.Equal(t, rows, examples.Keys(), "the table of README.md must list each example once, in order")
		})
	})
}

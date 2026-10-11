// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package memory_test

import (
	"encoding/hex"
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/page"

	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// TestToken checks the page tokens of Read.
func TestToken(t *testing.T) {
	t.Parallel()

	t.Run("Read", func(t *testing.T) {
		t.Parallel()

		inc := hex.EncodeToString(incA.Bytes())
		tests := []struct {
			name string
			give string
		}{
			{name: "returns ErrToken for a token without three parts", give: inc + ".1"},
			{name: "returns ErrToken for a token whose incarnation is not hexadecimal", give: "zz.1.AA"},
			{name: "returns ErrToken for a token whose incarnation is not an identifier", give: "0a0b.1.AA"},
			{name: "returns ErrToken for a token whose generation is not a decimal number", give: inc + ".one.AA"},
			{name: "returns ErrToken for a token whose start is not unpadded base64url", give: inc + ".1.!!"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				s := open(t)
				commit(t, s, incA, &store.Mutation{Put: placements})
				_, _, err := s.Read(t.Context(), scope, incA, store.Query{}, page.Page{Token: tt.give, Limit: 1})
				assert.ErrorIs(t, err, memory.ErrToken, "Read returns ErrToken for a token that Read did not write")
			})
		}

		t.Run("returns the next page of a scope that a writer without an incarnation created", func(t *testing.T) {
			t.Parallel()
			s := open(t)
			commit(t, s, shard.NamespaceIncarnation{}, &store.Mutation{Put: placements})
			_, first, err := s.Read(
				t.Context(),
				scope,
				shard.NamespaceIncarnation{},
				store.Query{},
				page.Page{Limit: 1},
			)
			assert.NoError(t, err, "Read returns the first page of the scope")
			token := first.NextPage()
			_, next, err := s.Read(t.Context(), scope, shard.NamespaceIncarnation{}, store.Query{},
				page.Page{Token: token, Limit: 1})
			assert.NoError(t, err, "Read returns the page of a token of the zero incarnation")
			var got []shard.Placement
			for p, err := range next.Seq(t.Context()) {
				assert.NoError(t, err, "the cursor yields the placements of its page")
				got = append(got, p)
			}
			assert.Equal(t, got, placements[1:], "the next page lists the placement after the first page")
		})
	})
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package memory

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/shard"
)

// tokenSeparator separates the parts of the text of a token.
const tokenSeparator = "."

// tokenParts is the number of parts of the text of a token.
const tokenParts = 3

// token is the position of a later page of a Read: the incarnation and the generation
// of the read, and the start of the last placement of the page before it.
type token struct {
	// after is the start of the last placement of the page before.
	after []byte
	// incarnation is the namespace incarnation of the scope at the read.
	incarnation shard.NamespaceIncarnation
	// generation is the generation of the read.
	generation epoch.Epoch
}

// parseToken returns the token of the text that token.String wrote. An empty incarnation is
// the zero incarnation of a scope that a writer without an incarnation created.
//
// Error modes:
//   - an error that wraps ErrToken for any other text.
func parseToken(text string) (token, error) {
	parts := strings.Split(text, tokenSeparator)
	if len(parts) != tokenParts {
		return token{}, fmt.Errorf("%w: %q has %d parts", ErrToken, text, len(parts))
	}
	raw, err := hex.DecodeString(parts[0])
	if err != nil {
		return token{}, fmt.Errorf("%w: %q: %w", ErrToken, text, err)
	}
	var inc id.ID
	if len(raw) > 0 {
		if inc, err = id.FromBytes(raw); err != nil {
			return token{}, fmt.Errorf("%w: %q: %w", ErrToken, text, err)
		}
	}
	gen, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return token{}, fmt.Errorf("%w: %q: %w", ErrToken, text, err)
	}
	after, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return token{}, fmt.Errorf("%w: %q: %w", ErrToken, text, err)
	}
	return token{after: after, incarnation: shard.NamespaceIncarnation{ID: inc}, generation: epoch.Epoch(gen)}, nil
}

// String returns the text of t. The text has three parts, separated by periods:
//   - the incarnation in hexadecimal.
//   - the generation in decimal.
//   - the start in unpadded URL-safe base64.
func (t *token) String() string {
	return hex.EncodeToString(t.incarnation.Bytes()) + tokenSeparator +
		strconv.FormatUint(uint64(t.generation), 10) + tokenSeparator +
		base64.RawURLEncoding.EncodeToString(t.after)
}

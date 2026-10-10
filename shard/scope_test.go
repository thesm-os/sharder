// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard_test

import (
	"encoding/hex"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"

	"go.thesmos.sh/sharder/shard"
)

// recordedScopeHex pins the encoding of recordedScope, which every store call and
// every protocol message persists: field 1 is the namespace orders, and field 2 the
// cell c1.
const recordedScopeHex = "0a06" + "6f7264657273" +
	"1202" + "6331"

// recordedScope is the scope whose encoding recordedScopeHex pins.
var recordedScope = shard.Scope{Namespace: "orders", Cell: "c1"}

// TestScope checks the recorded encoding of a Scope.
func TestScope(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Scope", func(t *testing.T) {
			t.Parallel()
			s := recordedScope
			got, err := s.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Scope")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedScopeHex,
				"AppendBinary must write fields 1 and 2 as recorded",
			)
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Scope of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			var got shard.Scope
			assert.NoError(
				t,
				got.UnmarshalBinary(decodeHex(t, recordedScopeHex)),
				"UnmarshalBinary must decode the recorded encoding",
			)
			assert.Equal(t, got, recordedScope, "UnmarshalBinary must return the recorded Scope")
		})
	})
}

// TestScopeAllocs checks the allocation contract of Scope. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestScopeAllocs(t *testing.T) {
	namespace, cell := recordedScope.Namespace, recordedScope.Cell
	root := shard.Scope{Namespace: namespace}

	t.Run("constructs a Scope without an allocation", func(t *testing.T) {
		var got shard.Scope
		expect.MaxAllocs(
			t,
			func() { got = shard.Scope{Namespace: namespace, Cell: cell} },
			0,
			"constructing a Scope must not allocate",
		)
		assert.Equal(t, got, recordedScope, "the test must measure the construction of the Scope")
	})

	t.Run("compares two Scopes without an allocation", func(t *testing.T) {
		var got bool
		expect.MaxAllocs(t, func() { got = recordedScope == root }, 0, "comparing two Scopes must not allocate")
		assert.False(t, got, "the test must compare two Scopes that differ")
	})
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol_test

import (
	"math"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/protocol"
)

// The values that the tests of Metadata pin.
const (
	// recordedCurrent is the value of Current in this release, which every call of the
	// release states.
	recordedCurrent = 1
	// futureFeature is a feature that this release does not implement.
	futureFeature protocol.Feature = "future"
)

// TestMetadata checks Current and Check.
func TestMetadata(t *testing.T) {
	t.Parallel()

	t.Run("Current", func(t *testing.T) {
		t.Parallel()

		t.Run("is the recorded major version of the release", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, protocol.Current, recordedCurrent, "Current must be the major version that calls state")
		})
	})

	t.Run("Check", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil for the current version without a required feature", func(t *testing.T) {
			t.Parallel()
			md := protocol.Metadata{Version: protocol.Current}
			assert.NoError(t, md.Check(), "Check must accept a call of the current version")
		})

		t.Run("returns ErrUnsupported for a version other than Current", func(t *testing.T) {
			t.Parallel()
			prop.ErrorIs(t, func(v protocol.Version) error { return protocol.Metadata{Version: v}.Check() },
				protocol.ErrUnsupported, "Check must refuse a call of another major version",
				prop.Using(prop.OneOf(prop.Just[protocol.Version](0),
					prop.Integer[protocol.Version](protocol.Current+1, math.MaxUint32))))
		})

		t.Run("returns ErrUnsupported for a required feature", func(t *testing.T) {
			t.Parallel()
			md := protocol.Metadata{Version: protocol.Current, Requires: []protocol.Feature{futureFeature}}
			assert.ErrorIs(t, md.Check(), protocol.ErrUnsupported, "Check must refuse a call that requires a feature")
		})
	})
}

// TestMetadataAllocs checks the allocation contract of Check. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestMetadataAllocs(t *testing.T) {
	md := protocol.Metadata{Version: protocol.Current}

	t.Run("Check", func(t *testing.T) {
		var err error
		expect.MaxAllocs(t, func() { err = md.Check() }, 0, "Check of a supported call must not allocate")
		assert.NoError(t, err, "the test must measure a supported call")
	})
}

// BenchmarkMetadata reports the cost of Check, which every server and every transport
// calls for every call, and fails when it allocates.
func BenchmarkMetadata(b *testing.B) {
	md := protocol.Metadata{Version: protocol.Current}

	b.Run("Check", func(b *testing.B) {
		var err error

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			err = md.Check()
		}

		assert.NoError(b, err, "the benchmark must measure a supported call")
	})
}

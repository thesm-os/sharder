// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package singleton_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/shard"
)

const (
	// recordedName is the registered name of the partitioner, which the mappings of
	// namespaces record.
	recordedName = "singleton"
	// recordedConfig is the configuration of the partitioner, which the mappings of
	// namespaces record.
	recordedConfig = "{}"
	// labelKey labels the key that a property generates.
	labelKey = "key"
)

// TestPartitioner checks the position, the mapping and the span of singleton.
func TestPartitioner(t *testing.T) {
	t.Parallel()

	t.Run("Position", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the empty position for every key", func(t *testing.T) {
			t.Parallel()
			prop.ForAll(t, "Position returns the empty position for every key", func(c *prop.Case) {
				key := c.Draw(prop.Bytes(prop.MaxSize(64)), labelKey)
				assert.Empty(c, singleton.Partitioner{}.Position(nil, key), "Position must return the empty position")
			})
		})
	})

	t.Run("Mapping", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the recorded mapping", func(t *testing.T) {
			t.Parallel()
			assert.Equal(
				t,
				singleton.Partitioner{}.Mapping(),
				shard.Mapping{Partitioner: recordedName, Config: []byte(recordedConfig)},
				"Mapping must return the mapping that namespaces record",
			)
		})
	})

	t.Run("Initial", func(t *testing.T) {
		t.Parallel()

		t.Run("returns one span whose range is the range", func(t *testing.T) {
			t.Parallel()
			r := shard.Range{Start: []byte("a"), End: []byte("m")}
			spans, err := singleton.Partitioner{}.Initial(r)
			assert.NoError(t, err, "Initial must divide the range")
			assert.Equal(t, spans, []partition.Span{{Range: r}}, "Initial must return one span of the range")
		})
	})
}

// newPartitioner returns the zero Partitioner, the subject of the suite of
// partition.Partitioner.
func newPartitioner() singleton.Partitioner {
	return singleton.Partitioner{}
}

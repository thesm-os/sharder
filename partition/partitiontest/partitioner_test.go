// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package partitiontest_test

import (
	"bytes"
	"testing"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/partitiontest"
	"go.thesmos.sh/sharder/shard"
)

const (
	// quartersName is the registered name of the partitioner quarters.
	quartersName = "quarters"
	// quartersConfig is the configuration of quarters as JSON. quarters has no field
	// to configure.
	quartersConfig = "{}"
)

// quarterBounds are the positions at which quarters divides the position space into
// four spans.
var quarterBounds = [][]byte{{0x40}, {0x80}, {0xc0}}

// quarters is a partitioner that keeps every law of partition.Partitioner. The
// position of a key is the key itself. Initial divides a range at each of
// quarterBounds that lies inside the range, so the position space has four spans.
type quarters struct{}

// Position appends key to dst.
func (quarters) Position(dst, key []byte) []byte {
	return append(dst, key...)
}

// Mapping returns the name and the configuration of quarters. The mapping does not
// contain a hash.
func (quarters) Mapping() shard.Mapping {
	return shard.Mapping{Partitioner: quartersName, Config: []byte(quartersConfig)}
}

// Initial divides r at each bound of quarterBounds that is above the start of r and
// inside r. It returns no error.
func (quarters) Initial(r shard.Range) ([]partition.Span, error) {
	spans := []partition.Span{{Range: shard.Range{Start: r.Start}}}
	for _, b := range quarterBounds {
		if bytes.Compare(b, r.Start) > 0 && r.Contains(b) {
			spans[len(spans)-1].Range.End = b
			spans = append(spans, partition.Span{Range: shard.Range{Start: b}})
		}
	}
	spans[len(spans)-1].Range.End = r.End
	return spans, nil
}

// TestPartitioner checks that quarters passes the checks of the suite of
// partition.Partitioner.
func TestPartitioner(t *testing.T) {
	t.Parallel()

	partitiontest.RunPartitionerSuite(t, func() quarters { return quarters{} })
}

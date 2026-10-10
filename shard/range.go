// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=Range

import "bytes"

// Range is the half-open interval [Start, End) of positions, in the order of
// [bytes.Compare]. The ranges of the cells of a namespace, and of the shards of a
// cell, are Ranges.
//
// The zero Range is the whole position space.
//
// # Encoding
//
// kanon generates the codec of Range. Start is field 1, and End is field 2. The codec
// encodes a nil and an empty byte slice alike, so an empty Start and an empty End
// each mean the same range whether they are nil or not.
//
// # Concurrency
//
// A copy of a Range shares the bytes of Start and End with the original.
//
// # Allocation contract
//
// Construction and [Range.Contains] do not allocate.
type Range struct {
	// Start is the first position of the range. An empty Start is the first position
	// of the position space.
	Start []byte
	// End is the first position after the range. An empty End extends the range to
	// the end of the position space.
	End []byte
}

// Contains reports whether pos is in r: at or above Start, and below End unless End
// is empty.
//
// # Allocation contract
//
// Zero alloc.
func (r Range) Contains(pos []byte) bool {
	return bytes.Compare(pos, r.Start) >= 0 && (len(r.End) == 0 || bytes.Compare(pos, r.End) < 0)
}

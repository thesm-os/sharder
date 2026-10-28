// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package partitiontest

import (
	"bytes"
	"encoding/json"
	"slices"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/partition/xxhash64"
	"go.thesmos.sh/sharder/shard"
)

// The labels of the values that the checks draw. A counterexample shows each value
// under its label.
const (
	// labelPrefix labels the bytes that dst contains before Position appends.
	labelPrefix = "prefix"
	// labelKey labels a key.
	labelKey = "key"
	// labelSpare labels the room of dst beyond the room for the position.
	labelSpare = "spare"
	// labelFirst labels the index of the first span of a range.
	labelFirst = "first"
	// labelLast labels the index of the last span of a range.
	labelLast = "last"
)

// The bounds of the values that the checks draw.
const (
	// maxPrefix is the largest number of bytes of a prefix.
	maxPrefix = 8
	// maxKey is the largest number of bytes of a key.
	maxKey = 64
	// maxSpare is the largest room of dst beyond the room for the position.
	maxSpare = 4
)

// fakeSlots is the number of slots of the verified fake of the planted defects of
// Partitioner.
const fakeSlots = 4

// driftCalls is the number of calls after which a drifting planted defect returns
// other results. Each check of a method other than the check of determinism calls the
// method at most driftCalls times on one subject.
const driftCalls = 2

// PositionIsDeterministic checks that Position returns the same position for the same
// key.
//
//dokimi:check Partitioner.Position defect=driftsPositions
func PositionIsDeterministic(h *PartitionerHandle, c *prop.Case) {
	s := h.Subject()
	key := c.Draw(prop.Bytes(prop.MaxSize(maxKey)), labelKey)
	assert.Deterministic(h, func(key []byte) ([]byte, error) { return s.Position(nil, key), nil }, key,
		h.Statement())
}

// driftsPositions returns a planted defect whose Position adds one to the last byte of
// the position after driftCalls calls. One goroutine runs a check, so the count of the
// calls has no lock.
func driftsPositions(h *PartitionerHandle) partition.Partitioner {
	fake := newFake(h)
	calls := 0
	return NewPartitionerStub(h, PartitionerStubDelegateTo(fake),
		WithPartitionerPosition(func(dst, key []byte) []byte {
			calls++
			pos := fake.Position(dst, key)
			if calls > driftCalls {
				pos[len(pos)-1]++
			}
			return pos
		}))
}

// PositionAppendsToDst checks that Position appends the position of the key to dst.
//
//dokimi:check Partitioner.Position defect=overwritesDst
func PositionAppendsToDst(h *PartitionerHandle, c *prop.Case) {
	s := h.Subject()
	prefix := c.Draw(prop.Bytes(prop.MaxSize(maxPrefix)), labelPrefix)
	key := c.Draw(prop.Bytes(prop.MaxSize(maxKey)), labelKey)
	want := append(bytes.Clone(prefix), s.Position(nil, key)...)
	assert.Equal(h, s.Position(bytes.Clone(prefix), key), want, h.Statement(), assert.EquateEmpty())
}

// overwritesDst returns a planted defect whose Position writes the position over the
// bytes of dst.
func overwritesDst(h *PartitionerHandle) partition.Partitioner {
	fake := newFake(h)
	return NewPartitionerStub(h, PartitionerStubDelegateTo(fake),
		WithPartitionerPosition(func(dst, key []byte) []byte { return fake.Position(dst[:0], key) }))
}

// PositionAppendsIntoTheRoomOfDst checks that Position appends the position into the
// array of dst when dst has room for the position.
//
//dokimi:check Partitioner.Position defect=ignoresRoom
func PositionAppendsIntoTheRoomOfDst(h *PartitionerHandle, c *prop.Case) {
	s := h.Subject()
	prefix := c.Draw(prop.Bytes(prop.MaxSize(maxPrefix)), labelPrefix)
	key := c.Draw(prop.Bytes(prop.MaxSize(maxKey)), labelKey)
	spare := c.Draw(prop.Integer(0, maxSpare), labelSpare)
	dst := append(make([]byte, 0, len(prefix)+len(s.Position(nil, key))+spare), prefix...)
	got := s.Position(dst, key)
	// got starts at the address of dst when Position appends into the array of dst.
	assert.Equal(h, got[:0], dst[:0], h.Statement(), assert.ByIdentity())
}

// ignoresRoom returns a planted defect whose Position appends the position to dst
// without its room, so the position goes into a new array.
func ignoresRoom(h *PartitionerHandle) partition.Partitioner {
	fake := newFake(h)
	return NewPartitionerStub(h, PartitionerStubDelegateTo(fake),
		WithPartitionerPosition(func(dst, key []byte) []byte { return fake.Position(slices.Clip(dst), key) }))
}

// MappingIsDeterministic checks that Mapping returns an equal mapping on every call.
//
//dokimi:check Partitioner.Mapping defect=driftsMappings
func MappingIsDeterministic(h *PartitionerHandle) {
	s := h.Subject()
	assert.Deterministic(h, func(struct{}) (shard.Mapping, error) { return s.Mapping(), nil }, struct{}{},
		h.Statement())
}

// driftsMappings returns a planted defect whose Mapping adds one to the hash version
// after driftCalls calls.
func driftsMappings(h *PartitionerHandle) partition.Partitioner {
	fake := newFake(h)
	calls := 0
	return NewPartitionerStub(h, PartitionerStubDelegateTo(fake),
		WithPartitionerMapping(func() shard.Mapping {
			calls++
			m := fake.Mapping()
			if calls > driftCalls {
				m.HashVersion++
			}
			return m
		}))
}

// MappingContainsTheName checks that the mapping contains the name of the partitioner.
//
//dokimi:check Partitioner.Mapping defect=dropsTheName
func MappingContainsTheName(h *PartitionerHandle) {
	assert.NotEmpty(h, h.Subject().Mapping().Partitioner, h.Statement())
}

// dropsTheName returns a planted defect whose mapping does not contain the name of the
// partitioner.
func dropsTheName(h *PartitionerHandle) partition.Partitioner {
	fake := newFake(h)
	return NewPartitionerStub(h, PartitionerStubDelegateTo(fake),
		WithPartitionerMapping(func() shard.Mapping {
			m := fake.Mapping()
			m.Partitioner = ""
			return m
		}))
}

// ConfigIsJSON checks that the configuration in the mapping is JSON.
//
//dokimi:check Partitioner.Mapping defect=truncatesConfigs
func ConfigIsJSON(h *PartitionerHandle) {
	assert.True(h, json.Valid(h.Subject().Mapping().Config), h.Statement())
}

// truncatesConfigs returns a planted defect whose mapping contains the configuration
// without its last byte, which is not JSON.
func truncatesConfigs(h *PartitionerHandle) partition.Partitioner {
	fake := newFake(h)
	return NewPartitionerStub(h, PartitionerStubDelegateTo(fake),
		WithPartitionerMapping(func() shard.Mapping {
			m := fake.Mapping()
			m.Config = m.Config[:len(m.Config)-1]
			return m
		}))
}

// MappingVersionsItsHash checks that the mapping contains a hash version exactly when it
// contains the name of a hash.
//
//dokimi:check Partitioner.Mapping defect=dropsHashVersions
func MappingVersionsItsHash(h *PartitionerHandle) {
	m := h.Subject().Mapping()
	assert.Equal(h, m.HashVersion == 0, m.Hash == "", h.Statement())
}

// dropsHashVersions returns a planted defect whose mapping contains the name of its hash
// without a hash version.
func dropsHashVersions(h *PartitionerHandle) partition.Partitioner {
	fake := newFake(h)
	return NewPartitionerStub(h, PartitionerStubDelegateTo(fake),
		WithPartitionerMapping(func() shard.Mapping {
			m := fake.Mapping()
			m.HashVersion = 0
			return m
		}))
}

// InitialPartitionsThePositionSpace checks that Initial returns spans that partition the
// position space.
//
//dokimi:check Partitioner.Initial defect=dropsSpans
func InitialPartitionsThePositionSpace(h *PartitionerHandle) {
	spans, err := h.Subject().Initial(shard.Range{})
	assert.NoError(h, err, "Initial must divide the position space")
	assertPartition(h, spans, shard.Range{}, h.Statement())
}

// InitialPartitionsRangesBetweenBounds checks that Initial returns spans that partition a
// range between the bounds of two spans of the position space. A cell starts and ends
// at the bounds of shards, so Initial divides each such range.
//
//dokimi:check Partitioner.Initial defect=dropsSpans
func InitialPartitionsRangesBetweenBounds(h *PartitionerHandle, c *prop.Case) {
	s := h.Subject()
	spans, err := s.Initial(shard.Range{})
	assert.NoError(h, err, "Initial must divide the position space")
	assert.NotEmpty(h, spans, "Initial must return a span of the position space")
	first := c.Draw(prop.Integer(0, len(spans)-1), labelFirst)
	last := c.Draw(prop.Integer(first, len(spans)-1), labelLast)
	r := shard.Range{Start: spans[first].Range.Start, End: spans[last].Range.End}
	got, err := s.Initial(r)
	assert.NoError(h, err, "Initial must divide a range between the bounds of two of its spans")
	assertPartition(h, got, r, h.Statement())
}

// dropsSpans returns a planted defect whose Initial leaves the second span out of the
// spans of the position space. The position space is also a range between the bounds
// of two of its spans, so the defect breaks both checks of a partition.
func dropsSpans(h *PartitionerHandle) partition.Partitioner {
	fake := newFake(h)
	return NewPartitionerStub(h, PartitionerStubDelegateTo(fake),
		WithPartitionerInitial(func(r shard.Range) ([]partition.Span, error) {
			spans, err := fake.Initial(r)
			if err == nil && len(r.Start) == 0 && len(r.End) == 0 {
				spans = slices.Delete(spans, 1, 2)
			}
			return spans, err
		}))
}

// InitialIsDeterministic checks that Initial returns equal spans on every call.
//
//dokimi:check Partitioner.Initial defect=driftsSpans
func InitialIsDeterministic(h *PartitionerHandle) {
	assert.Deterministic(h, h.Subject().Initial, shard.Range{}, h.Statement())
}

// driftsSpans returns a planted defect whose Initial returns one span of the whole range
// after driftCalls calls.
func driftsSpans(h *PartitionerHandle) partition.Partitioner {
	fake := newFake(h)
	calls := 0
	return NewPartitionerStub(h, PartitionerStubDelegateTo(fake),
		WithPartitionerInitial(func(r shard.Range) ([]partition.Span, error) {
			calls++
			if calls > driftCalls {
				return []partition.Span{{Range: r}}, nil
			}
			return fake.Initial(r)
		}))
}

// newFake returns the verified fake that the planted defects of Partitioner build on. The
// fake is fixed-slots over fakeSlots slots of xxhash64 without hash tags, and newFake
// stops tb when New returns an error.
func newFake(tb assert.TB) *fixedslots.Partitioner {
	tb.Helper()
	p, err := fixedslots.New(fixedslots.Config{Hash: xxhash64.Name, Slots: fakeSlots}, xxhash64.Hash{})
	assert.NoError(tb, err, "New must build the verified fake")
	return p
}

// assertPartition stops tb unless spans partition r, without gaps or overlaps. Each
// assertion passes msg as its message. It checks these properties in order:
//
//   - spans is not empty.
//   - The first span starts at the start of r.
//   - The last span ends at the end of r.
//   - Each span ends where the next span starts.
//   - The starts ascend, so no span before the last is empty.
//   - The last span contains its own start, so it is not empty either.
func assertPartition(tb assert.TB, spans []partition.Span, r shard.Range, msg string) {
	tb.Helper()

	assert.NotEmpty(tb, spans, msg)
	assert.Equal(tb, spans[0].Range.Start, r.Start, msg, assert.EquateEmpty())
	last := spans[len(spans)-1].Range
	assert.Equal(tb, last.End, r.End, msg, assert.EquateEmpty())
	assert.Pairwise(tb, spans, func(earlier, later partition.Span) bool {
		return bytes.Equal(earlier.Range.End, later.Range.Start)
	}, msg)
	assert.Pairwise(tb, spans, func(earlier, later partition.Span) bool {
		return bytes.Compare(earlier.Range.Start, later.Range.Start) < 0
	}, msg)
	assert.True(tb, last.Contains(last.Start), msg)
}

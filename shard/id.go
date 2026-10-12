// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

import (
	"fmt"

	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"
)

// ID is the identity of a shard in a namespace incarnation. The core assigns it with
// core's id/ulid when it commits the first placement of the shard. The ID of a shard
// never changes, and no later shard of the namespace incarnation gets it. The range
// of positions of a shard is the Range of its placement. The range is not part of the
// ID.
//
// An ID contains the 16 bytes of its ULID as an [id.ID128], so a map keyed by ID hashes
// and compares 16 bytes. [id.ID128.ID] returns the identifier as an [id.ID].
//
// The zero ID is no shard. The core never assigns it, because a ULID starts with the
// millisecond of its creation.
//
// # Order
//
// [ID.Compare] orders IDs by their bytes. A ULID starts with the millisecond of its
// creation, so IDs of different milliseconds compare in creation order. The planner and
// the repair cursor examine shards in this order.
//
// # Encoding
//
// An ID encodes as the 16 bytes of its ULID, and the zero ID encodes as no bytes. The
// binary methods of [id.ID] write the same bytes for the same identifier.
// [id.ID128.String] returns a diagnostic form. [ID.MarshalText] returns the canonical
// form of the ULID, which [go.thesmos.sh/core/id/ulid.Format] returns, so encoding/json
// writes an ID as that text.
//
// # Concurrency
//
// An ID is a comparable value. A copy is independent of the original.
//
// # Allocation contract
//
// Construction, comparison, a lookup in a map keyed by ID, [ID.UnmarshalBinary] of a
// valid encoding, and [ID.AppendBinary] into a buffer with room do not allocate.
type ID struct{ id.ID128 }

// IsZero reports whether i is the zero ID. The zero ID is no shard, and the codecs of
// the package leave out a field that contains it.
//
// # Allocation contract
//
// Zero alloc.
func (i ID) IsZero() bool {
	return i == ID{}
}

// Compare returns -1, 0 or +1 by the order of the bytes of i and other. The zero ID
// orders before every other ID. slices.SortFunc sorts IDs with the method expression
// ID.Compare.
//
// # Allocation contract
//
// Zero alloc.
func (i ID) Compare(other ID) int {
	return i.ID128.Compare(other.ID128)
}

// AppendBinary appends the encoding of i to dst and returns the extended slice. The
// encoding is the 16 bytes of the ULID of i, or no bytes for the zero ID. The error
// is always nil. Implements encoding.BinaryAppender.
//
// # Allocation contract
//
// Zero alloc when dst has room for 16 more bytes.
func (i ID) AppendBinary(dst []byte) ([]byte, error) {
	return i.AppendKanon(dst), nil
}

// MarshalBinary returns the encoding that AppendBinary appends in a new slice.
// Implements encoding.BinaryMarshaler.
//
// # Allocation contract
//
// One allocation for the returned slice, and none for the zero ID.
func (i ID) MarshalBinary() ([]byte, error) {
	return i.AppendKanon(make([]byte, 0, i.SizeKanon())), nil
}

// SizeKanon returns the length of the encoding of i. The length is 16, and 0 for the
// zero ID. Implements go.thesmos.sh/kanon.Sizer.
//
// # Allocation contract
//
// Zero alloc.
func (i ID) SizeKanon() int {
	return i.wide().SizeKanon()
}

// ExactKanon declares ID a go.thesmos.sh/kanon.Exact type. AppendBinary returns no
// error and appends SizeKanon bytes, and UnmarshalBinary accepts only the bytes that
// AppendBinary writes for the ID that it decodes. No code calls the method.
func (ID) ExactKanon() {}

// AppendKanon appends the encoding of i that AppendBinary describes to dst, and returns
// the extended slice. Implements go.thesmos.sh/kanon.Appender.
//
// # Allocation contract
//
// Zero alloc when dst has room for 16 more bytes.
func (i ID) AppendKanon(dst []byte) []byte {
	return i.wide().AppendKanon(dst)
}

// UnmarshalBinary sets i to the ID that data encodes. No bytes encode the zero ID, and
// the 16 bytes of a ULID encode the ID of that ULID. It leaves i unchanged when it
// returns an error. Implements encoding.BinaryUnmarshaler.
//
// Error modes:
//   - an error that wraps ErrID and id.ErrSize for data whose length is not 0, 16, 20
//     or 32.
//   - an error that wraps ErrID for the 20 or 32 bytes of a wider identifier, and for
//     16 zero bytes. The zero ID encodes as no bytes, so 16 zero bytes encode no ID.
//
// # Allocation contract
//
// Zero alloc for a valid encoding.
func (i *ID) UnmarshalBinary(data []byte) error {
	var w id.ID
	if err := w.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("%w: %w", ErrID, err)
	}
	if w.IsZero() {
		*i = ID{}
		return nil
	}
	k, ok := w.As128()
	//dokimi:mutate-skip lcr-right: As128 returns the zero ID128 with false, so k is zero when ok is false
	if !ok || k == (id.ID128{}) {
		return fmt.Errorf("%w: the %d bytes %x", ErrID, len(data), data)
	}
	*i = ID{k}
	return nil
}

// MarshalText returns the canonical form of the ULID of i: 26 characters of Crockford's
// base 32. It returns no text for the zero ID. The error is always nil. Implements
// encoding.TextMarshaler.
func (i ID) MarshalText() ([]byte, error) {
	if i.IsZero() {
		return nil, nil
	}
	return []byte(ulid.Format(i.ID())), nil
}

// UnmarshalText sets i to the ID of the canonical form of a ULID, and to the zero ID for
// no text. It leaves i unchanged when it returns an error. Implements
// encoding.TextUnmarshaler.
//
// Error modes:
//   - an error that wraps ErrID and the error of ulid.ParseULID for text that is not the
//     canonical form of a ULID.
//   - an error that wraps ErrID for the text of the zero ULID, because the zero ID is no
//     shard and has no text.
func (i *ID) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		*i = ID{}
		return nil
	}
	u, err := ulid.ParseULID(string(text))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrID, err)
	}
	return i.UnmarshalBinary(u.Bytes())
}

// wide returns the identifier of i as an id.ID. It returns the zero id.ID for the zero
// ID, and the identifier of the 16 bytes of i for every other ID. The binary methods of
// ID write what the binary methods of that id.ID write.
func (i ID) wide() (w id.ID) {
	if !i.IsZero() {
		w = i.ID()
	}
	return
}

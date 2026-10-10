// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

import "go.thesmos.sh/core/id"

// ID is the identity of a shard in a namespace incarnation. The core assigns it with
// core's id/ulid when it commits the first placement of the shard. The ID of a shard
// never changes, and no later shard of the namespace incarnation gets it. The range
// of positions of a shard is the Range of its placement, which is no part of the ID.
//
// The zero ID is no shard.
//
// # Order
//
// [id.ID.Compare] orders IDs by their bytes. A ULID starts with the millisecond of
// its creation, so IDs of different milliseconds compare in creation order. The
// planner and the repair cursor examine shards in this order.
//
// # Encoding
//
// An ID encodes through the binary methods of the [id.ID] that it embeds. The
// encoding is the 16 bytes of the ULID, and the zero ID encodes as no bytes.
// [id.ID.String] returns a diagnostic form. [go.thesmos.sh/core/id/ulid.Format]
// returns the canonical form of a ULID.
//
// # Concurrency
//
// An ID is a comparable value. A copy is independent of the original.
//
// # Allocation contract
//
// Construction, comparison, a lookup in a map keyed by ID, and [id.ID.AppendBinary]
// into a buffer with room do not allocate.
type ID struct{ id.ID }

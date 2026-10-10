// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

import "go.thesmos.sh/core/id"

// Namespace is the name of one independently configured sharded application or key
// space. The application chooses it. The empty Namespace is no namespace.
type Namespace string

// NamespaceIncarnation is the identity of one creation of a namespace. The root
// assigns it with core's id/ulid when it creates the namespace, and it never changes
// afterwards. A namespace that is deleted and created again under the same name has
// a new incarnation, so no scope, fence token, operation or message of the old
// incarnation matches the new one.
//
// The zero NamespaceIncarnation is the value of a sender that does not know the
// incarnation yet, such as the spec proposal that creates the namespace, or a reader
// that has not read the root scope.
//
// # Encoding
//
// A NamespaceIncarnation encodes through the binary methods of the [id.ID] that it
// embeds. The encoding is the 16 bytes of the ULID, and the zero incarnation encodes
// as no bytes. [id.ID.String] returns a diagnostic form.
// [go.thesmos.sh/core/id/ulid.Format] returns the canonical form of a ULID.
//
// # Concurrency
//
// A NamespaceIncarnation is a comparable value. A copy is independent of the
// original.
//
// # Allocation contract
//
// Construction, comparison, and [id.ID.AppendBinary] into a buffer with room do not
// allocate.
type NamespaceIncarnation struct{ id.ID }

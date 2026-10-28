// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocoltest_test

import "go.thesmos.sh/sharder/protocol"

// newMetadata returns the metadata of a caller of the current major version that requires
// no feature, the subject of the suite of Metadata. Check of such metadata does not
// allocate.
func newMetadata() protocol.Metadata {
	return protocol.Metadata{Version: protocol.Current}
}

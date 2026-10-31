// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

module go.thesmos.sh/sharder

go 1.27.3

require (
	github.com/cespare/xxhash/v2 v2.3.0
	go.dokimi.dev/assert v0.10.0
	go.dokimi.dev/conform v0.8.0
	go.dokimi.dev/stub v0.2.0
	go.thesmos.sh/core v0.35.0
	go.thesmos.sh/kanon v0.3.1
)

require go.dokimi.dev/fault v0.2.0 // indirect

tool go.thesmos.sh/kanon/cmd/kanon

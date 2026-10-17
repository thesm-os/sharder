// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

module go.thesmos.sh/sharder

go 1.27.2

require (
	github.com/cespare/xxhash/v2 v2.3.0
	go.dokimi.dev/assert v0.2.0
	go.thesmos.sh/core v0.20.0
	go.thesmos.sh/kanon v0.2.0
)

require (
	github.com/google/go-cmp v0.7.0 // indirect
	go.thesmos.sh/testkit v0.10.0 // indirect
)

tool go.thesmos.sh/kanon/cmd/kanon

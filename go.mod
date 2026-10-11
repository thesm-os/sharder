// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

module go.thesmos.sh/sharder

go 1.27.2

require (
	github.com/cespare/xxhash/v2 v2.3.0
	go.dokimi.dev/assert v0.2.0
	go.thesmos.sh/core v0.11.1-0.20261011153228-44e0ab6bf9dc
	go.thesmos.sh/kanon v0.2.0
)

tool go.thesmos.sh/kanon/cmd/kanon

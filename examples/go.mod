// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

module go.thesmos.sh/sharder/examples

go 1.27.3

require (
	go.dokimi.dev/assert v0.10.0
	go.thesmos.sh/core v0.35.0
	go.thesmos.sh/sharder v0.1.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.dokimi.dev/conform v0.8.0 // indirect
	go.dokimi.dev/fault v0.2.0 // indirect
	go.dokimi.dev/stub v0.2.0 // indirect
	go.thesmos.sh/kanon v0.3.1 // indirect
)

replace go.thesmos.sh/sharder => ../

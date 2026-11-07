// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

module go.thesmos.sh/sharder

go 1.27.3

require (
	github.com/cespare/xxhash/v2 v2.3.0
	go.dokimi.dev/assert v0.17.0
	go.dokimi.dev/conform v0.12.1
	go.dokimi.dev/stub v0.2.1
	go.thesmos.sh/core v0.38.0
	go.thesmos.sh/kanon v0.5.0
)

require (
	github.com/google/jsonschema-go v0.4.3 // indirect
	go.dokimi.dev/fault v0.2.1 // indirect
	go.dokimi.dev/medium v0.1.0 // indirect
	go.dokimi.dev/spec v0.4.0 // indirect
)

tool go.thesmos.sh/kanon/cmd/kanon

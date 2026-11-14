// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

module go.thesmos.sh/sharder/examples

go 1.27.4

require (
	go.dokimi.dev/assert v0.22.0
	go.thesmos.sh/core v0.48.0
	go.thesmos.sh/sharder v0.1.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/google/jsonschema-go v0.4.3 // indirect
	go.dokimi.dev/conform v0.17.0 // indirect
	go.dokimi.dev/fault v0.2.4 // indirect
	go.dokimi.dev/host v0.2.1 // indirect
	go.dokimi.dev/medium v0.2.1 // indirect
	go.dokimi.dev/spec v0.5.1 // indirect
	go.dokimi.dev/stub v0.3.3 // indirect
	go.thesmos.sh/kanon v0.5.0 // indirect
)

replace go.thesmos.sh/sharder => ../

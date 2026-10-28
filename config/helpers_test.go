// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config_test

import (
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/config/configtest"
)

// newSpec returns a spec of the namespace of the tests in which only the namespace and
// preset are set.
func newSpec(preset config.PresetName) *config.Spec {
	return new(configtest.NewSpecBuilder().
		WithNamespace(testSpace).
		WithPreset(preset).
		Build())
}

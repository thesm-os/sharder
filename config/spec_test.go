// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config_test

import (
	"encoding/json"
	"testing"
	"time"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/partition/singleton"
	"go.thesmos.sh/sharder/shard"
)

// The recorded encodings of specs that the root stores.
const (
	// recordedSpecJSON is the recorded encoding of recordedSpec. It has the field names
	// of every section in the order of the fields of a Spec. It does not contain the
	// selections that recordedSpec leaves empty.
	recordedSpecJSON = `{"namespace":"orders","preset":"exclusive-worker",` +
		`"partitioning":{"name":"singleton"},` +
		`"placement":{"name":"planner","config":{"goals":["count"],"limit":5}},` +
		`"load":{"metrics":[{"name":"cpu"},{"name":"bytes","capacity":"disk"}],"window":"30s"},` +
		`"budget":{"maxUnavailable":0,"maxConcurrentNodes":2,"maxTransitions":8,"maxNodeTransitions":4,` +
		`"nodeCallbacks":3,"drainRoles":["primary"],"interruption":"planned"},` +
		`"cells":{"maxNodes":10,"maxShards":100},"version":3}`

	// emptySpecJSON is the recorded encoding of the zero Spec.
	emptySpecJSON = `{"namespace":"","preset":"","version":0}`
)

// recordedSpec is the spec of the encoding recordedSpecJSON.
var recordedSpec = config.Spec{
	Namespace:    testSpace,
	Preset:       workerPreset,
	Partitioning: config.Selection{Name: singleton.Name},
	Placement:    config.Selection{Name: plannerName, Config: json.RawMessage(plannerConfig)},
	Load: config.LoadModel{
		Metrics: []config.Metric{{Name: cpuMetric}, {Name: bytesMetric, Capacity: diskCapacity}},
		Window:  shard.Duration(30 * time.Second),
	},
	Budget: config.Budget{
		MaxUnavailable:     new(0),
		MaxConcurrentNodes: new(2),
		MaxTransitions:     new(8),
		MaxNodeTransitions: new(4),
		NodeCallbacks:      new(3),
		DrainRoles:         []shard.Role{shard.RolePrimary},
		Interruption:       config.PlannedInterruption,
	},
	Cells:   config.CellPolicy{MaxNodes: 10, MaxShards: 100},
	Version: 3,
}

// TestSpec checks the JSON encoding of a Spec.
func TestSpec(t *testing.T) {
	t.Parallel()

	t.Run("Spec", func(t *testing.T) {
		t.Parallel()

		t.Run("encodes in JSON with the recorded field names", func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(recordedSpec)
			assert.NoError(t, err, "encoding/json must encode the spec")
			assert.Equal(t, string(got), recordedSpecJSON, "encoding/json must write the recorded field names")
		})

		t.Run("encodes the zero Spec without its empty sections", func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(config.Spec{})
			assert.NoError(t, err, "encoding/json must encode the zero spec")
			assert.Equal(t, string(got), emptySpecJSON, "encoding/json must leave out every empty section")
		})

		t.Run("decodes the JSON that encoding/json encodes", func(t *testing.T) {
			t.Parallel()
			assert.RoundTrip(t,
				func(s config.Spec) ([]byte, error) {
					return json.Marshal(s)
				},
				func(data []byte) (config.Spec, error) {
					var s config.Spec
					err := json.Unmarshal(data, &s)
					return s, err
				},
				recordedSpec, "encoding/json must decode the spec that it encodes")
		})
	})
}

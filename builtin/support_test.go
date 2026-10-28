// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/config/configtest"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// The built-in strategies of the design outside the support matrix.
const (
	hashRange        shard.StrategyName = "hash-range"
	keyRange         shard.StrategyName = "key-range"
	explicit         shard.StrategyName = "explicit"
	rendezvous       shard.StrategyName = "rendezvous"
	consistentHash   shard.StrategyName = "consistent-hash"
	boundedLoad      shard.StrategyName = "bounded-load"
	jump             shard.StrategyName = "jump"
	maglev           shard.StrategyName = "maglev"
	static           shard.StrategyName = "static"
	secondaryOnly    shard.StrategyName = "secondary-only"
	primarySecondary shard.StrategyName = "primary-secondary"
	affinity         shard.StrategyName = "affinity"
	overlapping      shard.StrategyName = "overlapping"
	delegated        shard.StrategyName = "delegated"
	makeBeforeBreak  shard.StrategyName = "make-before-break"
	graceful         shard.StrategyName = "graceful"
	staged           shard.StrategyName = "staged"
	elect            shard.StrategyName = "elect"
	service          shard.StrategyName = "service"
	leaderless       shard.StrategyName = "leaderless"
	lookup           shard.StrategyName = "lookup"
	compute          shard.StrategyName = "compute"
)

// The names under which the tests register strategies of an application.
const (
	appPartitioner shard.StrategyName = "app-partitioner"
	appConstraint  shard.StrategyName = "app-capacity"
)

// The parts of the support table of the README.
const (
	// readmeName is the name of the README in the root of the repository.
	readmeName = "README.md"
	// tableHeader is the header of the support table, with its separator row.
	tableHeader = "| Axis | Supported |\n|---|---|\n"
	// openAxis is the cell of an axis on which a spec selects any registered name.
	openAxis = "Every registered name"
	// presetsRow is the first cell of the row of the presets.
	presetsRow = "Presets"
	// requiresRow is the first cell of the row of the required capabilities.
	requiresRow = "Required capabilities"
)

// TestSupport checks the support matrix, and the specs that Resolve refuses under it.
func TestSupport(t *testing.T) {
	t.Parallel()

	t.Run("Support", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the matrix of the support table of the README", func(t *testing.T) {
			t.Parallel()
			readme, err := os.ReadFile(filepath.Join("..", readmeName))
			assert.NoError(t, err, "the README of the repository must be readable")
			assert.Contains(t, string(readme), supportTable(builtin.Support(), builtin.Presets()),
				"the support table of the README must list the matrix of Support")
		})

		t.Run("returns a new matrix on each call", func(t *testing.T) {
			t.Parallel()
			want := builtin.Support()
			got := builtin.Support()
			got.Strategies[config.Partitioning][0] = appPartitioner
			got.Open[0] = config.Partitioning
			got.Requires[0] = config.Durable
			assert.Equal(t, builtin.Support(), want, "a change of a matrix must not change the next matrix")
		})

		excluded := []named{
			{axis: config.Partitioning, name: hashRange},
			{axis: config.Partitioning, name: keyRange},
			{axis: config.Partitioning, name: explicit},
			{axis: config.Placement, name: rendezvous},
			{axis: config.Placement, name: consistentHash},
			{axis: config.Placement, name: boundedLoad},
			{axis: config.Placement, name: jump},
			{axis: config.Placement, name: maglev},
			{axis: config.Placement, name: static},
			{axis: config.Replication, name: secondaryOnly},
			{axis: config.Replication, name: primarySecondary},
			{axis: config.Ownership, name: affinity},
			{axis: config.Ownership, name: overlapping},
			{axis: config.Ownership, name: delegated},
			{axis: config.Handoff, name: makeBeforeBreak},
			{axis: config.Handoff, name: graceful},
			{axis: config.Handoff, name: staged},
			{axis: config.Handoff, name: elect},
			{axis: config.Topology, name: service},
			{axis: config.Topology, name: leaderless},
			{axis: config.Routing, name: lookup},
			{axis: config.Routing, name: compute},
		}
		for _, tt := range excluded {
			t.Run("excludes "+string(tt.name)+" from "+tt.axis.String(), func(t *testing.T) {
				t.Parallel()
				_, err := config.Resolve(newRegistry(t), selecting(tt.axis, tt.name), provided)
				assert.ErrorIs(t, err, config.ErrUnsupported, "Resolve must refuse a strategy outside the matrix")
				assert.Equal(t, errs.Classify(err), errs.Unsupported, "the refusal must classify as Unsupported")
				assert.Contains(t, err.Error(), tt.axis.String()+` "`+string(tt.name)+`"`,
					"the refusal must name the axis and the strategy")
			})
		}

		t.Run("requires an ownership with a fence", func(t *testing.T) {
			t.Parallel()
			spec := &config.Spec{
				Namespace: testSpace,
				Preset:    builtin.ExclusiveWorker,
				Ownership: config.Selection{Config: json.RawMessage(`{"fenced":false}`)},
			}
			_, err := config.Resolve(newRegistry(t), spec, provided)
			assert.ErrorIs(t, err, config.ErrUnsupported, "Resolve must refuse an exclusive ownership without a fence")
			assert.Equal(t, errs.Classify(err), errs.Unsupported, "the refusal must classify as Unsupported")
		})

		t.Run("excludes a partitioner that an application registers", func(t *testing.T) {
			t.Parallel()
			r := newRegistry(t)
			err := config.Register(r, config.Partitioning,
				renamed[partition.Partitioner](t, builtin.Singleton(), appPartitioner))
			assert.NoError(t, err, "the registry must take the partitioner of the application")
			_, err = config.Resolve(r, selecting(config.Partitioning, appPartitioner), provided)
			assert.ErrorIs(t, err, config.ErrUnsupported, "Resolve must refuse a partitioner of the application")
		})

		t.Run("includes a constraint that an application registers", func(t *testing.T) {
			t.Parallel()
			r := newRegistry(t)
			err := config.Register(r, config.Constraint,
				renamed[placement.Constraint](t, builtin.Capacity(), appConstraint))
			assert.NoError(t, err, "the registry must take the constraint of the application")
			spec := &config.Spec{
				Namespace: testSpace,
				Preset:    builtin.ExclusiveWorker,
				Placement: config.Selection{
					Config: json.RawMessage(fmt.Sprintf(`{"constraints":[{"name":%q}]}`, appConstraint)),
				},
			}
			_, err = config.Resolve(r, spec, provided)
			assert.NoError(t, err, "Resolve must accept a constraint of the application")
		})
	})
}

// renamed returns a double of the factory f under the name name, as an application
// registers the strategies of a factory under another name.
func renamed[T any](tb assert.TB, f config.Factory[T], name shard.StrategyName) *configtest.FactoryStub[T] {
	tb.Helper()
	return configtest.NewFactoryStub(tb, configtest.FactoryStubDelegateTo(f),
		configtest.WithFactoryName[T](func() shard.StrategyName { return name }))
}

// selecting returns a spec of the preset exclusive-worker that selects name on axis.
func selecting(axis config.Axis, name shard.StrategyName) *config.Spec {
	spec := &config.Spec{Namespace: testSpace, Preset: builtin.ExclusiveWorker}
	sel := config.Selection{Name: name}
	switch axis {
	case config.Partitioning:
		spec.Partitioning = sel
	case config.Placement:
		spec.Placement = sel
	case config.Replication:
		spec.Replication = sel
	case config.Ownership:
		spec.Ownership = sel
	case config.Handoff:
		spec.Handoff = sel
	case config.Topology:
		spec.Topology = sel
	default:
		spec.Routing = sel
	}
	return spec
}

// supportTable returns the support table of the README for the matrix s and the presets.
// The table has one row per axis of strategies in the order of the axes, a row of the
// presets and a row of the capabilities that s requires.
func supportTable(s config.Support, presets []config.Preset) string {
	var b strings.Builder
	b.WriteString(tableHeader)
	for axis := config.Partitioning; axis <= config.Hash; axis++ {
		cell := openAxis
		if !slices.Contains(s.Open, axis) {
			cell = quoted(s.Strategies[axis])
		}
		b.WriteString("| `" + axis.String() + "` | " + cell + " |\n")
	}
	names := make([]config.PresetName, len(presets))
	for i, p := range presets {
		names[i] = p.Name
	}
	b.WriteString("| " + presetsRow + " | " + quoted(names) + " |\n")
	b.WriteString("| " + requiresRow + " | " + quoted(s.Requires) + " |\n")
	return b.String()
}

// quoted returns the spellings of values in code spans, separated by commas. fmt spells
// a name as its text and a capability with its String method.
func quoted[T any](values []T) string {
	spans := make([]string, len(values))
	for i, v := range values {
		spans[i] = "`" + fmt.Sprint(v) + "`"
	}
	return strings.Join(spans, ", ")
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package config_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/config"
)

// TestPreset checks the registration of presets.
func TestPreset(t *testing.T) {
	t.Parallel()

	t.Run("RegisterPreset", func(t *testing.T) {
		t.Parallel()

		t.Run("adds a preset that Resolve resolves", func(t *testing.T) {
			t.Parallel()
			r := newRegistry(t, newSupport())
			p := newPreset()
			p.Name = tunedPreset
			p.Spec.Routing = config.Selection{}
			p.Spec.Placement = config.Selection{Name: balanceName}
			assert.NoError(t, config.RegisterPreset(r, p), "RegisterPreset must accept a preset inside the matrix")
			spec := &config.Spec{Namespace: testSpace, Preset: tunedPreset, Routing: config.Selection{Name: watchName}}
			res, err := config.Resolve(r, spec, testProvided)
			assert.NoError(t, err, "Resolve must resolve a spec of the preset")
			assert.Equal(t, res.Spec.Placement.Name, balanceName, "Resolve must take the placer of the preset")
		})

		t.Run("returns ErrDuplicate for a second preset of one name", func(t *testing.T) {
			t.Parallel()
			err := config.RegisterPreset(newRegistry(t, newSupport()), newPreset())
			assert.ErrorIs(t, err, config.ErrDuplicate, "RegisterPreset must refuse a second preset of one name")
		})

		invalid := []struct {
			change func(p *config.Preset)
			name   string
		}{
			{
				name:   "returns ErrRegistration for a preset without a name",
				change: func(p *config.Preset) { p.Name = "" },
			},
			{
				name:   "returns ErrRegistration for a preset with a namespace",
				change: func(p *config.Preset) { p.Spec.Namespace = testSpace },
			},
			{
				name:   "returns ErrRegistration for a preset with a preset",
				change: func(p *config.Preset) { p.Spec.Preset = workerPreset },
			},
			{
				name:   "returns ErrRegistration for a preset with a version",
				change: func(p *config.Preset) { p.Spec.Version = 1 },
			},
		}
		for _, tt := range invalid {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				p := newPreset()
				p.Name = tunedPreset
				tt.change(p)
				err := config.RegisterPreset(config.NewRegistry(newSupport()), p)
				assert.ErrorIs(t, err, config.ErrRegistration, "RegisterPreset must refuse the preset of the case")
			})
		}

		t.Run("returns ErrUnsupported for each selection outside the support matrix", func(t *testing.T) {
			t.Parallel()
			p := newPreset()
			p.Spec.Ownership = config.Selection{Name: overlappingName}
			p.Spec.Handoff = config.Selection{Name: zoneName}
			err := config.RegisterPreset(config.NewRegistry(newSupport()), p)
			assert.ErrorIs(t, err, config.ErrUnsupported, "RegisterPreset must refuse a selection outside the matrix")
			expect.Contains(t, err.Error(), string(overlappingName), "the error must contain the ownership")
			expect.Contains(t, err.Error(), string(zoneName), "the error must contain the handoff")
			expect.Equal(t, errs.Classify(err), errs.Unsupported, "the error must classify as Unsupported")
		})
	})
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package ownership_test

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/shard"
)

// recordedConfigJSON is the JSON form of fencedConfig, which the specs of namespaces
// record.
const recordedConfigJSON = `{"lease":{"ttl":"10s","renew":"3s","lockDelay":"0s","maxDriftRate":0.01,` +
	`"onLoss":"continue-fenced"},"fenced":true}`

// fencedConfig is a valid configuration of a namespace whose writes go through a
// fence. Its lease is valid for 9.9 seconds after a renewal.
var fencedConfig = ownership.Config{
	Lease: ownership.Lease{
		TTL:          shard.Duration(10 * time.Second),
		Renew:        shard.Duration(3 * time.Second),
		MaxDriftRate: 0.01,
		OnLoss:       ownership.ContinueFenced,
	},
	Fenced: true,
}

// TestConfig checks the JSON form and the validation of a Config.
func TestConfig(t *testing.T) {
	t.Parallel()

	t.Run("encodes the recorded JSON form", func(t *testing.T) {
		t.Parallel()
		got, err := json.Marshal(&fencedConfig)
		assert.NoError(t, err, "encoding/json must encode a Config")
		assert.Equal(t, string(got), recordedConfigJSON, "encoding/json must write the keys and the spellings")
	})

	t.Run("decodes the recorded JSON form", func(t *testing.T) {
		t.Parallel()
		var got ownership.Config
		assert.NoError(t, json.Unmarshal([]byte(recordedConfigJSON), &got), "encoding/json must decode a Config")
		assert.Equal(t, got, fencedConfig, "encoding/json must return the recorded Config")
	})

	t.Run("Validate", func(t *testing.T) {
		t.Parallel()

		valid := []struct {
			name string
			give func(c *ownership.Config)
		}{
			{name: "returns nil for a fenced configuration that continues", give: func(*ownership.Config) {}},
			{
				name: "returns nil for an unfenced configuration that stops",
				give: func(c *ownership.Config) {
					c.Fenced, c.Lease.OnLoss, c.Lease.LockDelay = false, ownership.Stop, c.Lease.TTL
				},
			},
			{
				name: "returns nil for a renewal interval one nanosecond below the validity",
				give: func(c *ownership.Config) { c.Lease.Renew = shard.Duration(c.Lease.Validity() - 1) },
			},
			{
				name: "returns nil for a drift rate of zero",
				give: func(c *ownership.Config) { c.Lease.MaxDriftRate = 0 },
			},
		}
		for _, tt := range valid {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				c := fencedConfig
				tt.give(&c)
				assert.NoError(t, c.Validate(), "Validate must accept the configuration")
			})
		}

		// Each row changes fencedConfig so that Validate reports one problem per entry of
		// want, in the same order.
		invalid := []struct {
			name string
			give func(c *ownership.Config)
			want []string
		}{
			{
				name: "returns ErrConfig for a TTL of zero",
				give: func(c *ownership.Config) { c.Lease.TTL = 0 },
				want: []string{"TTL", "renewal interval"},
			},
			{
				name: "returns ErrConfig for a negative drift rate",
				give: func(c *ownership.Config) { c.Lease.MaxDriftRate = -0.01 },
				want: []string{"drift rate"},
			},
			{
				name: "returns ErrConfig for a drift rate of one",
				give: func(c *ownership.Config) { c.Lease.MaxDriftRate = 1 },
				want: []string{"drift rate"},
			},
			{
				name: "returns ErrConfig for a drift rate that is not a number",
				give: func(c *ownership.Config) { c.Lease.MaxDriftRate = math.NaN() },
				want: []string{"drift rate"},
			},
			{
				name: "returns ErrConfig for a renewal interval of zero",
				give: func(c *ownership.Config) { c.Lease.Renew = 0 },
				want: []string{"renewal interval"},
			},
			{
				name: "returns ErrConfig for a renewal interval equal to the validity",
				give: func(c *ownership.Config) { c.Lease.Renew = shard.Duration(c.Lease.Validity()) },
				want: []string{"renewal interval"},
			},
			{
				name: "returns ErrConfig for a negative lock delay",
				give: func(c *ownership.Config) { c.Lease.LockDelay = -1 },
				want: []string{"lock delay"},
			},
			{
				name: "returns ErrConfig for the zero OnLeaseLoss",
				give: func(c *ownership.Config) { c.Lease.OnLoss = 0 },
				want: []string{"action on lease loss"},
			},
			{
				name: "returns ErrConfig for an OnLeaseLoss above ContinueFenced",
				give: func(c *ownership.Config) { c.Lease.OnLoss = ownership.ContinueFenced + 1 },
				want: []string{"action on lease loss"},
			},
			{
				name: "returns ErrConfig for ContinueFenced without a fence",
				give: func(c *ownership.Config) { c.Fenced = false },
				want: []string{"without a fence"},
			},
		}
		for _, tt := range invalid {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				c := fencedConfig
				tt.give(&c)
				err := c.Validate()
				expect.ErrorIs(t, err, ownership.ErrConfig, "Validate must return ErrConfig")
				expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Validate must classify as Invalid")
				joined := assert.ErrorAs[interface{ Unwrap() []error }](t, err,
					"Validate must join the problems of the configuration")
				assert.Length(t, joined.Unwrap(), len(tt.want), "Validate must return one error per problem")
				for i, problem := range joined.Unwrap() {
					expect.Contains(t, problem.Error(), tt.want[i], "each error must name the field of its problem")
				}
			})
		}
	})
}

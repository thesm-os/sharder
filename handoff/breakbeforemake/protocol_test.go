// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package breakbeforemake_test

import (
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/handoff/breakbeforemake"
	"go.thesmos.sh/sharder/shard"
)

const (
	// recordedName is the name of the protocol, which specs and transitions record.
	recordedName shard.StrategyName = "break-before-make"
	// recordedVersion is the version of the scripts of the protocol, which transitions
	// record.
	recordedVersion uint32 = 1
	// nodeA is the node that a transfer of the tests takes the replica from.
	nodeA shard.NodeID = "node-a"
	// nodeB is the node that a transfer of the tests gives the replica to.
	nodeB shard.NodeID = "node-b"
	// nodeF is the node that becomes the writer on the abort path of a transfer of the
	// tests.
	nodeF shard.NodeID = "node-f"
)

var (
	// dropFrom drops the replica on From.
	dropFrom = handoff.Step{Action: handoff.Drop, Side: handoff.From, Timeout: dropTimeout}
	// commitTo commits To as the writer.
	commitTo = handoff.Step{Action: handoff.Commit, Side: handoff.To}
	// addTo adds the replica on To.
	addTo = handoff.Step{Action: handoff.Add, Side: handoff.To, Timeout: addTimeout}
	// toFallback is the abort path of the add for a transfer with a Fallback.
	toFallback = []handoff.Step{
		{Action: handoff.Commit, Side: handoff.Fallback},
		{Action: handoff.Add, Side: handoff.Fallback, Timeout: addTimeout},
		{Action: handoff.Drop, Side: handoff.To, Timeout: dropTimeout},
	}
	// withoutWriter is the abort path of the add for a transfer without a Fallback.
	withoutWriter = []handoff.Step{{Action: handoff.Commit}}
	// everySide is a transfer with a node on every side.
	everySide = handoff.Transfer{From: nodeA, To: nodeB, Fallback: nodeF}
)

// TestProtocol checks the construction, the identity and the scripts of the protocol.
func TestProtocol(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		t.Run("returns a protocol for a valid configuration", func(t *testing.T) {
			t.Parallel()
			p, err := breakbeforemake.New(testConfig)
			assert.NoError(t, err, "New must accept a valid configuration")
			assert.NotNil(t, p, "New must return a protocol")
		})

		t.Run("returns ErrConfig for a configuration that is not valid", func(t *testing.T) {
			t.Parallel()
			p, err := breakbeforemake.New(breakbeforemake.Config{})
			expect.ErrorIs(t, err, breakbeforemake.ErrConfig, "New must return the error of Validate")
			expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of New must classify as Invalid")
			expect.Nil(t, p, "New must not return a protocol")
		})
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		t.Run("returns break-before-make", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newProtocol(t).Name(), recordedName, "Name must return the recorded name")
		})
	})

	t.Run("Version", func(t *testing.T) {
		t.Parallel()

		t.Run("returns 1", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, newProtocol(t).Version(), recordedVersion, "Version must return the recorded version")
		})
	})

	t.Run("Script", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			give handoff.Transfer
			want handoff.Script
		}{
			{
				name: "returns three steps for a transfer with every side",
				give: everySide,
				want: handoff.Script{
					Steps: []handoff.Step{dropFrom, commitTo, addTo},
					Abort: [][]handoff.Step{nil, nil, toFallback},
				},
			},
			{
				name: "returns an abort path without a writer for a transfer without a Fallback",
				give: handoff.Transfer{From: nodeA, To: nodeB},
				want: handoff.Script{
					Steps: []handoff.Step{dropFrom, commitTo, addTo},
					Abort: [][]handoff.Step{nil, nil, withoutWriter},
				},
			},
			{
				name: "returns no drop for a transfer without a From",
				give: handoff.Transfer{To: nodeB, Fallback: nodeF},
				want: handoff.Script{
					Steps: []handoff.Step{commitTo, addTo},
					Abort: [][]handoff.Step{nil, toFallback},
				},
			},
			{
				name: "returns no drop and an abort path without a writer for a first placement",
				give: handoff.Transfer{To: nodeB},
				want: handoff.Script{
					Steps: []handoff.Step{commitTo, addTo},
					Abort: [][]handoff.Step{nil, withoutWriter},
				},
			},
			{
				name: "returns three steps for a From and a To on one node",
				give: handoff.Transfer{From: nodeA, To: nodeA},
				want: handoff.Script{
					Steps: []handoff.Step{dropFrom, commitTo, addTo},
					Abort: [][]handoff.Step{nil, nil, withoutWriter},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got, err := newProtocol(t).Script(tt.give)
				assert.NoError(t, err, "Script must script a transfer with a To")
				assert.Equal(t, got, tt.want, "Script must return the steps and the abort paths of the transfer",
					assert.EquateEmpty())
			})
		}

		t.Run("returns ErrTransfer for a transfer without a To", func(t *testing.T) {
			t.Parallel()
			got, err := newProtocol(t).Script(handoff.Transfer{From: nodeA, Fallback: nodeF})
			expect.ErrorIs(t, err, handoff.ErrTransfer, "Script must return ErrTransfer")
			expect.Equal(t, errs.Classify(err), errs.Invalid, "the error of Script must classify as Invalid")
			expect.Empty(t, got.Steps, "Script must return no steps")
		})

		t.Run("returns a script that passes Validate for every transfer with a To", func(t *testing.T) {
			t.Parallel()
			p := newProtocol(t)
			sides := prop.SampledFrom(shard.NodeID(""), nodeA, nodeB, nodeF)
			destinations := prop.SampledFrom(nodeA, nodeB, nodeF)
			prop.ForAll(t, "every script of break-before-make must pass Validate", func(c *prop.Case) {
				transfer := handoff.Transfer{
					From:     c.Draw(sides, "from"),
					To:       c.Draw(destinations, "to"),
					Fallback: c.Draw(sides, "fallback"),
				}
				s, err := p.Script(transfer)
				assert.NoError(c, err, "Script must script a transfer with a To")
				assert.NoError(c, s.Validate(transfer), "Validate must accept the script of the transfer")
			})
		})

		t.Run("returns the same script on every call", func(t *testing.T) {
			t.Parallel()
			assert.Deterministic(t, newProtocol(t).Script, everySide, "Script must be deterministic")
		})
	})
}

// TestProtocolAllocs checks the allocation contract of Script. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestProtocolAllocs(t *testing.T) {
	p := newProtocol(t)

	t.Run("Script", func(t *testing.T) {
		var (
			got handoff.Script
			err error
		)
		expect.MaxAllocs(t, func() { got, err = p.Script(everySide) }, 0, "Script must not allocate")
		assert.NoError(t, err, "the test must measure a transfer with a To")
		assert.Length(t, got.Steps, 3, "the test must measure the script of a transfer with every side")
	})
}

// BenchmarkProtocol reports the cost of Script, which the core calls for each transition
// that it starts or resumes, and fails when it allocates.
func BenchmarkProtocol(b *testing.B) {
	p := newProtocol(b)

	b.Run("Script", func(b *testing.B) {
		var (
			got handoff.Script
			err error
		)

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got, err = p.Script(everySide)
		}

		assert.NoError(b, err, "the benchmark must measure a transfer with a To")
		assert.Length(b, got.Steps, 3, "the benchmark must measure the script of a transfer with every side")
	})
}

// newProtocol returns the protocol of the tests, whose timeouts are addTimeout and
// dropTimeout.
func newProtocol(tb testing.TB) *breakbeforemake.Protocol {
	tb.Helper()
	p, err := breakbeforemake.New(testConfig)
	assert.NoError(tb, err, "New must accept the configuration of the tests")
	return p
}

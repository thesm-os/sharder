// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol_test

import (
	"encoding/hex"
	"testing"
	"time"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// recordedObservedHex is the encoding of the replica of recordedReport. Field 1 is the
// shard, field 2 the role primary, field 3 the epoch 7, field 4 the status serving, and
// field 10 the instant 9 seconds after the Unix epoch.
const recordedObservedHex = "0a10" + recordedShardHex + "1003" + "1807" + "2002" + "52020812"

// recordedReportHex pins the encoding of recordedReport. Field 1 is the scope, field 2 the
// incarnation, field 3 the node, field 4 the node incarnation 3, field 5 the sequence 5,
// and field 6 the replicas, each with its length.
const recordedReportHex = "0a0c" + recordedScopeHex + "1210" + recordedIncarnationHex + "1a026e31" + "2003" + "2805" +
	"321d" + "1c" + recordedObservedHex

// recordedAckHex pins the encoding of recordedAck: field 1 is the sequence 5, field 2 the
// generation 6, and field 3 the controller epoch 9.
const recordedAckHex = "0805" + "1006" + "1809"

// recordedReport is the report whose encoding recordedReportHex pins.
var recordedReport = protocol.Report{
	Scope:           recordedScope,
	Incarnation:     recordedIncarnation,
	Node:            recordedNode,
	NodeIncarnation: 3,
	Sequence:        5,
	Replicas: []shard.Observed{{
		Shard:    recordedShard,
		Role:     shard.RolePrimary,
		Epoch:    7,
		Status:   shard.StatusServing,
		Reported: time.Unix(9, 0).UTC(),
	}},
}

// recordedAck is the ack whose encoding recordedAckHex pins.
var recordedAck = protocol.Ack{Sequence: 5, Generation: 6, ControllerEpoch: 9}

// TestReport checks the recorded encodings of a Report and an Ack.
func TestReport(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Report", func(t *testing.T) {
			t.Parallel()
			r := recordedReport
			got, err := r.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Report")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedReportHex,
				"AppendBinary must write fields 1 to 6 as recorded",
			)
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Report of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(recordedReportHex)
			assert.NoError(t, err, "the recorded encoding must be hexadecimal")
			var got protocol.Report
			assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
			assert.Equal(t, got, recordedReport, "UnmarshalBinary must return the recorded Report")
		})
	})

	t.Run("Ack", func(t *testing.T) {
		t.Parallel()

		t.Run("AppendBinary", func(t *testing.T) {
			t.Parallel()

			t.Run("appends the recorded encoding of an Ack", func(t *testing.T) {
				t.Parallel()
				a := recordedAck
				got, err := a.AppendBinary(nil)
				assert.NoError(t, err, "AppendBinary must encode the Ack")
				assert.Equal(
					t,
					hex.EncodeToString(got),
					recordedAckHex,
					"AppendBinary must write fields 1 to 3 as recorded",
				)
			})
		})
	})
}

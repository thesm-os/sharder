// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard_test

import (
	"encoding/hex"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/shard"
)

// The recorded encodings of reports.
const (
	// recordedParentHex pins the encoding of recordedParent: the 16 bytes of its
	// identifier.
	recordedParentHex = "2122232425262728292a2b2c2d2e2f30"

	// recordedObservedHex pins the encoding of recordedObserved, which every report
	// of a node contains. Field 1 is the shard, field 2 the role primary, 3, field 3
	// the epoch 2, and field 4 the status serving, 2. Field 5 is the load cpu=0.5, as
	// the key with its length and the little-endian bits of the float. Field 6 is the
	// fault transient, 1, field 7 the cause disk, and field 8 the range. Field 9 is
	// one parent, as an element with the length of its identifier. Field 10 is the
	// instant of the report, one second after the Unix epoch in UTC.
	recordedObservedHex = "0a10" + recordedIDHex +
		"1003" +
		"1802" +
		"2002" +
		"2a0c" + "03" + "637075" + "000000000000e03f" +
		"3001" +
		"3a04" + "6469736b" +
		"4206" + recordedRangeHex +
		"4a11" + "10" + recordedParentHex +
		"5202" + "0802"

	// onlyFaultHex is the encoding of a report whose only field other than zero is the
	// fault transient: field 6 alone, without field 4 of the zero Status.
	onlyFaultHex = "3001"

	// unknownStatusHex is field 4 with the status 5, which no constant has.
	unknownStatusHex = "2005"

	// unknownObservedRoleHex is field 2 with the role 8, which no constant has.
	unknownObservedRoleHex = "1008"

	// unknownFaultHex is field 6 with the fault 3, which no constant has.
	unknownFaultHex = "3003"
)

// recordedParent is the parent shard of recordedObserved.
var recordedParent = shard.ID{ID: id.New128([16]byte{
	0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
	0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30,
})}

// recordedObserved is the report whose encoding recordedObservedHex pins.
var recordedObserved = shard.Observed{
	Shard:    recordedID,
	Role:     shard.RolePrimary,
	Epoch:    2,
	Status:   shard.StatusServing,
	Load:     shard.Load{"cpu": 0.5},
	Fault:    shard.FaultTransient,
	Cause:    "disk",
	Range:    recordedRange,
	Parents:  []shard.ID{recordedParent},
	Reported: time.Unix(1, 0).UTC(),
}

// TestObserved checks the recorded encoding of an Observed, and the refusal of a
// Role, a Status or a Fault that is not Valid.
func TestObserved(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of an Observed", func(t *testing.T) {
			t.Parallel()
			o := recordedObserved
			got, err := o.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Observed")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedObservedHex,
				"AppendBinary must write fields 1 to 10 as recorded",
			)
		})

		t.Run("appends no field for the zero Status", func(t *testing.T) {
			t.Parallel()
			o := shard.Observed{Fault: shard.FaultTransient}
			got, err := o.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode a report of the zero Status")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				onlyFaultHex,
				"AppendBinary must leave out the field of the zero Status",
			)
		})

		tests := []struct {
			name string
			give shard.Observed
			want error
		}{
			{
				name: "returns ErrUnknownRole for a Role that is not Valid",
				give: shard.Observed{Role: shard.RoleLearner + 1, Status: shard.StatusServing},
				want: shard.ErrUnknownRole,
			},
			{
				name: "returns ErrUnknownStatus for a Status other than zero that is not Valid",
				give: shard.Observed{Status: shard.StatusFailed + 1},
				want: shard.ErrUnknownStatus,
			},
			{
				name: "returns ErrUnknownFault for a Fault that is not Valid",
				give: shard.Observed{Status: shard.StatusServing, Fault: shard.FaultPermanent + 1},
				want: shard.ErrUnknownFault,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				_, err := tt.give.AppendBinary(nil)
				assert.ErrorIs(t, err, tt.want, "AppendBinary must refuse a value outside its enumeration")
			})
		}
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Observed of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			var got shard.Observed
			assert.NoError(t, got.UnmarshalBinary(decodeHex(t, recordedObservedHex)),
				"UnmarshalBinary must decode the recorded encoding")
			assert.Equal(t, got, recordedObserved, "UnmarshalBinary must return the recorded Observed")
		})

		tests := []struct {
			name string
			give string
			want error
		}{
			{
				name: "returns ErrUnknownRole for an encoded Role that is not Valid",
				give: unknownObservedRoleHex,
				want: shard.ErrUnknownRole,
			},
			{
				name: "returns ErrUnknownStatus for an encoded Status that is not Valid",
				give: unknownStatusHex,
				want: shard.ErrUnknownStatus,
			},
			{
				name: "returns ErrUnknownFault for an encoded Fault that is not Valid",
				give: unknownFaultHex,
				want: shard.ErrUnknownFault,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				var got shard.Observed
				assert.ErrorIs(t, got.UnmarshalBinary(decodeHex(t, tt.give)), tt.want,
					"UnmarshalBinary must refuse a value outside its enumeration")
			})
		}
	})
}

// TestObservedAllocs checks the allocation contract of Observed. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestObservedAllocs(t *testing.T) {
	o := recordedObserved

	t.Run("constructs an Observed without an allocation", func(t *testing.T) {
		var got shard.Observed
		expect.MaxAllocs(t, func() {
			got = shard.Observed{
				Shard: o.Shard, Role: o.Role, Epoch: o.Epoch, Status: o.Status, Load: o.Load, Fault: o.Fault,
				Cause: o.Cause, Range: o.Range, Parents: o.Parents, Reported: o.Reported,
			}
		}, 0, "constructing an Observed must not allocate")
		assert.Equal(t, got, recordedObserved, "the test must measure the construction of the Observed")
	})
}

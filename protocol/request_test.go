// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol_test

import (
	"encoding/hex"
	"testing"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/bench"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// The encodings of the identities and the values that the recorded messages contain.
const (
	// recordedIncarnationHex is the encoding of recordedIncarnation: the 16 bytes of its
	// identifier.
	recordedIncarnationHex = "0102030405060708090a0b0c0d0e0f10"
	// recordedShardHex is the encoding of recordedShard: the 16 bytes of its identifier.
	recordedShardHex = "1112131415161718191a1b1c1d1e1f20"
	// recordedDisruptionHex is the encoding of recordedDisruption: the 16 bytes of its
	// identifier.
	recordedDisruptionHex = "2122232425262728292a2b2c2d2e2f30"
	// recordedScopeHex is the encoding of recordedScope: field 1 is the namespace orders,
	// and field 2 the cell c1.
	recordedScopeHex = "0a066f7264657273" + "12026331"
	// recordedOperationHex is the encoding of recordedOperation: field 1 is the
	// incarnation, field 2 the shard, and field 3 the sequence 4.
	recordedOperationHex = "0a10" + recordedIncarnationHex + "1210" + recordedShardHex + "1804"
	// recordedRangeHex is the encoding of recordedRange: field 1 is the start a, and
	// field 2 the end b.
	recordedRangeHex = "0a0161" + "120162"
)

// The names that the recorded messages contain.
const (
	recordedNamespace shard.Namespace = "orders"
	recordedCell      shard.CellID    = "c1"
	recordedNode      shard.NodeID    = "n1"
)

// The identities that the recorded messages contain.
var (
	// recordedIncarnation is the namespace incarnation of the recorded messages.
	recordedIncarnation = shard.NamespaceIncarnation{ID: id.New128([16]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	})}
	// recordedShard is the shard of the recorded messages.
	recordedShard = shard.ID{ID: id.New128([16]byte{
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
	})}
	// recordedDisruption is the disruption of the recorded end of a reservation.
	recordedDisruption = budget.ID{ID: id.New128([16]byte{
		0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
		0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30,
	})}
	// recordedScope is the scope of the recorded messages.
	recordedScope = shard.Scope{Namespace: recordedNamespace, Cell: recordedCell}
	// recordedOperation is the transition of the recorded request.
	recordedOperation = shard.OperationID{Incarnation: recordedIncarnation, Shard: recordedShard, Sequence: 4}
	// recordedRange is the range of the shard of the recorded request.
	recordedRange = shard.Range{Start: []byte("a"), End: []byte("b")}
)

// recordedRequestHex pins the encoding of recordedRequest. Field 1 is the callback drop,
// field 2 the scope, field 3 the operation, field 4 the path abort, field 5 the step 1 in
// zigzag, field 6 the node, field 7 the node incarnation 3, field 8 the shard, field 9 the
// range, field 10 the role primary, field 11 the epoch 7, and field 12 the controller
// epoch 9.
const recordedRequestHex = "0802" + "120c" + recordedScopeHex + "1a26" + recordedOperationHex + "2002" + "2802" +
	"32026e31" + "3803" + "4210" + recordedShardHex + "4a06" + recordedRangeHex + "5003" + "5807" + "6009"

// recordedDigestHex pins the digest of recordedRequest: the SHA-256 digest of its encoding
// without field 12.
const recordedDigestHex = "eb2473357eb917726676a299448cba4165333dcbef26483a1af3af901f57811d"

// recordedKeyHex pins the encoding of recordedKey. Field 1 is the operation, field 2 the
// path abort, field 3 the step 1 in zigzag, field 4 the node, field 5 the node incarnation
// 3, and field 6 the epoch 7.
const recordedKeyHex = "0a26" + recordedOperationHex + "1002" + "1802" + "22026e31" + "2803" + "3007"

// recordedRequest is the request whose encoding recordedRequestHex pins.
var recordedRequest = protocol.Request{
	Call:            protocol.Drop,
	Scope:           recordedScope,
	Operation:       recordedOperation,
	Path:            protocol.Abort,
	Step:            1,
	Node:            recordedNode,
	NodeIncarnation: 3,
	Shard:           recordedShard,
	Range:           recordedRange,
	Role:            shard.RolePrimary,
	Epoch:           7,
	ControllerEpoch: 9,
}

// recordedKey is the key of recordedRequest, whose encoding recordedKeyHex pins.
var recordedKey = protocol.Key{
	Operation:       recordedOperation,
	Path:            protocol.Abort,
	Step:            1,
	Node:            recordedNode,
	NodeIncarnation: 3,
	Epoch:           7,
}

// TestRequest checks the recorded encodings of a Request and a Key, and Key and Digest of a
// Request.
func TestRequest(t *testing.T) {
	t.Parallel()

	t.Run("AppendBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("appends the recorded encoding of a Request", func(t *testing.T) {
			t.Parallel()
			r := recordedRequest
			got, err := r.AppendBinary(nil)
			assert.NoError(t, err, "AppendBinary must encode the Request")
			assert.Equal(
				t,
				hex.EncodeToString(got),
				recordedRequestHex,
				"AppendBinary must write fields 1 to 12 as recorded",
			)
		})
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the Request of the recorded encoding", func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(recordedRequestHex)
			assert.NoError(t, err, "the recorded encoding must be hexadecimal")
			var got protocol.Request
			assert.NoError(t, got.UnmarshalBinary(data), "UnmarshalBinary must decode the recorded encoding")
			assert.Equal(t, got, recordedRequest, "UnmarshalBinary must return the recorded Request")
		})
	})

	t.Run("Key", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the recorded key of the recorded request", func(t *testing.T) {
			t.Parallel()
			r := recordedRequest
			assert.Equal(t, r.Key(), recordedKey, "Key must return the key of the request")
		})

		t.Run("AppendBinary", func(t *testing.T) {
			t.Parallel()

			t.Run("appends the recorded encoding of a Key", func(t *testing.T) {
				t.Parallel()
				k := recordedKey
				got, err := k.AppendBinary(nil)
				assert.NoError(t, err, "AppendBinary must encode the Key")
				assert.Equal(
					t,
					hex.EncodeToString(got),
					recordedKeyHex,
					"AppendBinary must write fields 1 to 6 as recorded",
				)
			})
		})
	})

	t.Run("Digest", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the SHA-256 digest of the encoding without the controller epoch", func(t *testing.T) {
			t.Parallel()
			r := recordedRequest
			got, err := r.Digest()
			assert.NoError(t, err, "Digest must encode the Request")
			assert.Equal(
				t,
				hex.EncodeToString(got.Bytes()),
				recordedDigestHex,
				"Digest must return the recorded digest",
			)
		})

		t.Run("returns the same digest for every controller epoch", func(t *testing.T) {
			t.Parallel()
			r := recordedRequest
			want, err := r.Digest()
			assert.NoError(t, err, "Digest must encode the Request")
			prop.ForAll(t, "Digest does not depend on the controller epoch", func(c *prop.Case) {
				resent := recordedRequest
				resent.ControllerEpoch = c.Draw(prop.Integer[epoch.Epoch](0, 1<<40), "controller epoch")
				got, err := resent.Digest()
				assert.NoError(c, err, "Digest must encode the Request")
				assert.Equal(c, got, want, "Digest must return the digest of the payload")
			})
		})

		t.Run("does not modify the request", func(t *testing.T) {
			t.Parallel()
			r := recordedRequest
			var err error
			assert.Pure(t, func() protocol.Request { return r }, func() { _, err = r.Digest() },
				"Digest must not modify the request")
			assert.NoError(t, err, "Digest must encode the Request")
		})

		t.Run("returns ErrUnknownRole for a request whose role is not Valid", func(t *testing.T) {
			t.Parallel()
			r := recordedRequest
			r.Role = shard.RoleLearner + 1
			_, err := r.Digest()
			assert.ErrorIs(t, err, shard.ErrUnknownRole, "Digest must return the error of the codec")
		})
	})
}

// TestRequestAllocs checks the allocation contracts of Key and Digest. MaxAllocs counts the
// allocations of the whole process, so the test does not run in parallel.
func TestRequestAllocs(t *testing.T) {
	r := recordedRequest

	t.Run("Key", func(t *testing.T) {
		var got protocol.Key
		expect.MaxAllocs(t, func() { got = r.Key() }, 0, "Key must not allocate")
		assert.Equal(t, got, recordedKey, "the test must measure the key of the request")
	})

	t.Run("Digest", func(t *testing.T) {
		var err error
		expect.MaxAllocs(t, func() { _, err = r.Digest() }, 0, "Digest of a short request must not allocate")
		assert.NoError(t, err, "the test must measure the digest of an encodable request")
	})
}

// BenchmarkRequest reports the costs of Key and Digest, which an agent calls for every
// callback, and fails when they allocate.
func BenchmarkRequest(b *testing.B) {
	r := recordedRequest

	b.Run("Key", func(b *testing.B) {
		var got protocol.Key

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			got = r.Key()
		}

		assert.Equal(b, got, recordedKey, "the benchmark must measure the key of the request")
	})

	b.Run("Digest", func(b *testing.B) {
		var err error

		c := bench.Start(b).MaxAllocs(0)
		defer c.End()

		for c.Loop() {
			_, err = r.Digest()
		}

		assert.NoError(b, err, "the benchmark must measure the digest of an encodable request")
	})
}

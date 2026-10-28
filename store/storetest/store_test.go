// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package storetest_test

import (
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/conform"
	"go.thesmos.sh/core/clock/fake"

	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
	"go.thesmos.sh/sharder/store/storetest"
)

// limits are the limits of the stores of the test. The check of Fits generates mutations on
// both sides of them, and every other check commits mutations below them.
var limits = store.Limits{MaxPlacements: 8, MaxBytes: 4096}

// origin is the start of the clock of each store of the test.
var origin = time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC)

// points maps each loss of the suite to the point of the memory store that loses a commit
// there.
var points = map[storetest.Loss]memory.Point{
	storetest.LoseRequest: memory.PointCommitApply,
	storetest.LoseReply:   memory.PointCommitAck,
}

// faultyStore is a memory store that loses the commits of the losses of the suite. It
// deletes scopes and compacts its generations as the memory store does.
type faultyStore struct {
	*memory.Store
}

// LoseCommit arms the fault of the memory store at the point of l.
func (s faultyStore) LoseCommit(l storetest.Loss) {
	s.FailAt(points[l])
}

// TestStore checks that the memory store, the verified fake of the planted defects of the
// suite, passes the checks of the suite of store.Store.
func TestStore(t *testing.T) {
	t.Parallel()

	storetest.RunStoreSuiteFromHandle(t,
		func(h *storetest.StoreHandle) faultyStore {
			s := faultyStore{Store: memory.New(fake.New(origin), memory.WithLimits(limits))}
			h.Cleanup(func() { assert.NoError(h, s.Close(), "Close returns nil") })
			return s
		},
		// The memory store does not keep anything beyond its process, so its restart
		// returns the same store. The check of restarts then runs without a second store.
		conform.Restartable(func(_ *storetest.StoreHandle, s store.Store) store.Store { return s }),
	)
}

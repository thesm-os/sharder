// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package placementtest

import (
	"slices"
	"sync/atomic"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// copied keeps the latest copy of copiesReplicas on the heap.
var copied atomic.Pointer[[]shard.Replica]

// RepeatedMovesDoNotAllocate checks that Apply does not allocate for a relocation of a
// replica and the relocation back, once both moves ran.
//
//dokimi:check State.Apply defect=copiesReplicas counted=true
func RepeatedMovesDoNotAllocate(h *StateHandle) {
	st := h.Subject()
	there, ok := relocation(st)
	assert.True(h, ok, "the state must have a replica that can move to another accepting node")
	back := placement.Move{From: there.To, To: there.From, Shard: there.Shard, Role: there.Role}
	assert.NoError(h, st.Apply(there), "Apply must accept the relocation")
	assert.NoError(h, st.Apply(back), "Apply must accept the relocation back")
	var err error
	assert.MaxAllocs(h, func() {
		if err = st.Apply(there); err == nil {
			err = st.Apply(back)
		}
	}, 0, h.Statement())
	assert.NoError(h, err, "the check must count two moves that Apply accepts")
}

// copiesReplicas returns a planted defect over the state of a cell of two accepting
// nodes and one shard with its primary on the first node. Its Apply copies the replicas
// of the shard of each move before it applies the move.
func copiesReplicas(h *StateHandle) StateMethods {
	sh := shard.ID{ID128: id.ID128{15: 1}}
	st, err := placement.NewState(&placement.Input{
		Model:  replication.PrimaryOnly,
		Layout: partition.Layout{Shards: []partition.Shard{{ID: sh}}},
		Current: shard.Assignment{Placements: map[shard.ID]shard.Placement{
			sh: {Shard: sh, Replicas: []shard.Replica{{Node: acceptingPrefix + "0", Role: shard.RolePrimary}}},
		}},
		Nodes: []shard.Node{{ID: acceptingPrefix + "0"}, {ID: acceptingPrefix + "1"}},
	})
	assert.NoError(h, err, "NewState must accept the cell of the planted defect")
	return NewStateStub(h, StateStubBenchMode(), StateStubDelegateTo(st),
		WithStateApply(func(m placement.Move) error {
			replicas := slices.Clone(st.Replicas(m.Shard))
			copied.Store(&replicas)
			return st.Apply(m)
		}))
}

// relocation returns the first move of st that relocates a replica from an accepting
// node to another accepting node without a replica of the shard, and reports whether st
// has such a move. It reads the shards in the order of Shards and the nodes in the order
// of Nodes. Apply accepts the move, and then the move back.
func relocation(st StateMethods) (placement.Move, bool) {
	for _, sh := range st.Shards() {
		if _, ok := st.InFlight(sh); ok {
			continue
		}
		replicas := st.Replicas(sh)
		for _, r := range replicas {
			if _, ok := st.Node(r.Node); !ok {
				continue
			}
			for _, n := range st.Nodes() {
				if !slices.ContainsFunc(replicas, func(o shard.Replica) bool { return o.Node == n }) {
					return placement.Move{From: r.Node, To: n, Shard: sh, Role: r.Role}, true
				}
			}
		}
	}
	return placement.Move{}, false
}

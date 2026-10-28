// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routestest

import (
	"fmt"
	"slices"
	"sync/atomic"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/shard"
)

// defectSlots is the number of slots of the cell of the router of the planted defect of
// ViewsAllocateOnlyMovedRoutes.
const defectSlots = 4

// copied keeps the latest copy of copiesViews on the heap.
var copied atomic.Pointer[[]shard.Node]

// ViewsAllocateOnlyMovedRoutes checks that a view of a cell allocates only the route and the
// endpoints of each slot whose node it moves.
//
//dokimi:check Machine.Step defect=copiesViews counted=true
func ViewsAllocateOnlyMovedRoutes(h *MachineHandle) {
	s := h.Subject()
	v, ok := s.(Viewed)
	if !ok {
		assert.Fault(h, fmt.Errorf("%w: %T", ErrUnviewed, s))
		return
	}
	for _, hosting := range []bool{false, true} {
		views, moved := v.Views(hosting)
		events := [2]machine.Event{views[0], views[1]}
		out := make([]machine.Effect, 0, 1)
		i := 0
		assert.MaxAllocs(h, func() {
			out = s.Step(events[i%2], out[:0])
			i++
		}, 2*uint64(moved), h.Statement())
		assert.Empty(h, out, "the check must count views that the router applies")
	}
}

// copiesViews returns a planted defect over a router of Serve with defectSlots slots. Its
// Step copies the nodes of each view before the router applies the view.
func copiesViews(h *MachineHandle) MachineMethods {
	s := Serve(h, defectSlots)
	return viewedStub{
		MachineMethods: NewMachineStub(h, MachineStubBenchMode(), MachineStubDelegateTo(s.Machine),
			WithMachineStep(func(ev machine.Event, out []machine.Effect) []machine.Effect {
				if v, ok := ev.(machine.Viewed); ok {
					listed := slices.Clone(v.View.Nodes)
					copied.Store(&listed)
				}
				return s.Step(ev, out)
			})),
		Viewed: s,
	}
}

// viewedStub is a double of Machine whose cell takes the views of a router of Serve. Each
// planted defect of Machine is a viewedStub, so it implements every interface of the kit.
type viewedStub struct {
	MachineMethods
	Viewed
}

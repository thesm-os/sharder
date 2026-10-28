// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routestest_test

import (
	"testing"

	"go.dokimi.dev/assert"

	"go.thesmos.sh/sharder/internal/machine/routes/routestest"
)

// The cell and the key of the tests of Serve.
const (
	// cellSlots is the number of slots of the cell.
	cellSlots = 4
	// key is a key that the tests route.
	key = "key"
)

// TestServed checks the router of Serve and the views of its cell.
func TestServed(t *testing.T) {
	t.Parallel()

	t.Run("Serve", func(t *testing.T) {
		t.Parallel()

		t.Run("returns a router with a route of one endpoint for a key", func(t *testing.T) {
			t.Parallel()
			got, err := routestest.Serve(t, cellSlots).Route(t.Context(), []byte(key))
			assert.NoError(t, err, "the router must route a key of the cell")
			assert.Length(t, got.Replicas, 1, "the route must list the endpoint of node A")
		})
	})

	t.Run("Views", func(t *testing.T) {
		t.Parallel()

		t.Run("returns views that move every slot for the node of the replicas", func(t *testing.T) {
			t.Parallel()
			s := routestest.Serve(t, cellSlots)
			before, err := s.Route(t.Context(), []byte(key))
			assert.NoError(t, err, "the router must route the key before the view")
			views, moved := s.Views(true)
			assert.Equal(t, moved, cellSlots, "the views must move the node of every slot")
			assert.Empty(t, s.Step(views[0], nil), "the router must apply the view without an effect")
			after, err := s.Route(t.Context(), []byte(key))
			assert.NoError(t, err, "the router must route the key after the view")
			assert.NotEqual(t, after.Replicas, before.Replicas, "the view must give the endpoint another address")
		})

		t.Run("returns views that move no slot for a node without replicas", func(t *testing.T) {
			t.Parallel()
			s := routestest.Serve(t, cellSlots)
			before, err := s.Route(t.Context(), []byte(key))
			assert.NoError(t, err, "the router must route the key before the view")
			views, moved := s.Views(false)
			assert.Equal(t, moved, 0, "the views must move no slot")
			assert.Empty(t, s.Step(views[0], nil), "the router must apply the view without an effect")
			after, err := s.Route(t.Context(), []byte(key))
			assert.NoError(t, err, "the router must route the key after the view")
			assert.Equal(t, after.Replicas, before.Replicas, "the view must keep the published route",
				assert.ByIdentity())
		})

		t.Run("returns a second view that gives the node its address again", func(t *testing.T) {
			t.Parallel()
			s := routestest.Serve(t, cellSlots)
			before, err := s.Route(t.Context(), []byte(key))
			assert.NoError(t, err, "the router must route the key before the views")
			views, _ := s.Views(true)
			for _, v := range views {
				assert.Empty(t, s.Step(v, nil), "the router must apply each view without an effect")
			}
			after, err := s.Route(t.Context(), []byte(key))
			assert.NoError(t, err, "the router must route the key after the views")
			assert.Equal(t, after.Replicas, before.Replicas, "the second view must restore the address of the endpoint")
		})
	})
}

// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package routestest_test

import "go.thesmos.sh/sharder/internal/machine/routes/routestest"

// slots is the number of slots of the cell of the router of the suite of Machine.
const slots = 1000

// newServed returns a router of routestest.Serve with slots slots, the subject of the suite
// of Machine.
func newServed(h *routestest.MachineHandle) *routestest.Served {
	return routestest.Serve(h, slots)
}

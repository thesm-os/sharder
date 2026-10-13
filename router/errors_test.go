// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package router_test

import (
	"fmt"
	"testing"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/router"
)

// The prefixes of the texts of the sentinel errors of package router.
const (
	// routerPrefix starts the text of every error that package router declares.
	routerPrefix = "router: "
	// routesPrefix starts the text of every error that package router takes from the machine
	// of the router.
	routesPrefix = "routes: "
)

// TestErrors checks the class and the text of each error of package router.
func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err    error
		name   string
		prefix string
		class  errs.Class
	}{
		{name: "ErrConfig", err: router.ErrConfig, prefix: routerPrefix, class: errs.Invalid},
		{name: "ErrClosed", err: router.ErrClosed, prefix: routesPrefix, class: errs.Invalid},
		{name: "ErrNoRoute", err: router.ErrNoRoute, prefix: routesPrefix, class: errs.Transient},
		{name: "ErrMapping", err: router.ErrMapping, prefix: routesPrefix, class: errs.Unsupported},
		{name: "ErrCell", err: router.ErrCell, prefix: routesPrefix, class: errs.NotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("classifies as "+tt.class.String(), func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, errs.Classify(tt.err), tt.class, "the sentinel must have its class")
			})

			t.Run("starts its text with the name of the package that declares it", func(t *testing.T) {
				t.Parallel()
				assert.HasPrefix(t, tt.err.Error(), tt.prefix,
					"the text of the sentinel must start with the name of the package that declares it")
			})
		})
	}

	t.Run("NotOwnerError", func(t *testing.T) {
		t.Parallel()

		t.Run("Error", func(t *testing.T) {
			t.Parallel()

			t.Run("returns a text with every field of the error", func(t *testing.T) {
				t.Parallel()
				e := &router.NotOwnerError{
					Cell:        cellID,
					Hint:        nodeB.ID,
					Shard:       shards[0],
					Epoch:       3,
					Generation:  7,
					Incarnation: incA,
				}
				assert.Equal(t, e.Error(), "router: the server does not serve shard "+shards[0].String()+
					" of cell "+string(cellID)+" at epoch 3: incarnation "+incA.String()+
					`, generation 7, hint "node-b"`, "the text must name every field of the error")
			})
		})

		t.Run("Class", func(t *testing.T) {
			t.Parallel()

			t.Run("returns errs.Conflict", func(t *testing.T) {
				t.Parallel()
				err := fmt.Errorf("test: the call: %w", &router.NotOwnerError{Cell: cellID})
				assert.Equal(t, errs.Classify(err), errs.Conflict, "a redirect must classify as errs.Conflict")
			})
		})
	})
}

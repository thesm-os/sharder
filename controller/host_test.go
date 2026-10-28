// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package controller_test

import (
	"context"
	"testing"
	"testing/synctest"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/controller"
	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/follow"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/store"
)

// TestHost checks how a controller process hosts the cells that the root of a namespace
// places on it.
func TestHost(t *testing.T) {
	t.Parallel()

	t.Run("Do", func(t *testing.T) {
		t.Parallel()

		t.Run("starts the controller of the cell at the epoch of an add", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				assert.NoError(
					t,
					r.host(t, p, procA.ID, firstEpoch, 1),
					"the add must start the controller of the cell",
				)
				got := record(t, f, cellScope)
				assert.Equal(t, got.Controller, procA.Address,
					"the controller of the process must have acquired the scope of the cell when Do returns")
				assert.Equal(t, got.Epoch, firstEpoch, "the controller of the cell must have the epoch of the add")
			})
		})

		t.Run("keeps the controller of the cell for an add of its term", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				r.place(t, procA.ID, firstEpoch)
				first := r.request(t, protocol.Add, procA.ID, firstEpoch, 1)
				second := r.request(t, protocol.Add, procA.ID, firstEpoch, 2)
				synctest.Wait()
				f.mem.StartOutage()
				assert.ErrorIs(t, p.ctrl.Do(t.Context(), rootMetadata, first), context.DeadlineExceeded,
					"an add whose acquisition does not apply in time must return the error of its context")
				f.mem.EndOutage()
				f.clock.Advance(follow.RetryMax)
				synctest.Wait()
				assert.NoError(t, p.ctrl.Do(t.Context(), rootMetadata, second),
					"a later add of the term must return once the controller of the term serves")
				assert.Equal(t, p.metrics.Bindings(unplacedInstrument, cellAttrs...), 1,
					"the process must start one controller of the term")
			})
		})

		t.Run("replaces the controller of the cell at an older term", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				assert.NoError(t, r.host(t, p, procA.ID, firstEpoch, 1), "the first add starts a controller")
				assert.NoError(t, r.host(t, p, procA.ID, secondEpoch, 2),
					"the add of a newer term must start the controller of that term")
				assert.Equal(t, record(t, f, cellScope).Epoch, secondEpoch,
					"the controller of the newer term must have acquired the scope of the cell")
				synctest.Wait()
				assert.Equal(t, p.metrics.Bound(unplacedInstrument, cellAttrs...), 1,
					"the process must run one controller of the cell")
			})
		})

		t.Run("starts the controller of a newer term once the controller of the older term has stopped",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					r := f.newRoot(t)
					p := f.start(t, procA, spec(namespace))
					synctest.Wait()
					f.faults.block()
					assert.NoError(t, r.host(t, p, procA.ID, firstEpoch, 1), "the first add starts a controller")
					r.place(t, procA.ID, secondEpoch)
					add := r.request(t, protocol.Add, procA.ID, secondEpoch, 2)
					ctx, cancel := context.WithTimeout(t.Context(), callFor)
					defer cancel()
					assert.ErrorIs(
						t,
						p.ctrl.Do(ctx, rootMetadata, add),
						context.DeadlineExceeded,
						"the add of the newer term must return the error of its context while the older controller runs",
					)
					f.faults.unblock()
					synctest.Wait()
					f.clock.Advance(agent.RetryMax)
					synctest.Wait()
					assert.NoError(t, p.ctrl.Do(t.Context(), rootMetadata, add),
						"a repeated add of the newer term must return once the older controller has stopped")
					assert.Equal(t, record(t, f, cellScope).Epoch, secondEpoch,
						"the controller of the newer term must acquire the scope once the older controller has stopped")
					assert.Equal(t, p.metrics.Bound(unplacedInstrument, cellAttrs...), 1,
						"the process must run one controller of the cell")
				})
			})

		t.Run("stops the controller of the cell at a drop", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				assert.NoError(t, r.host(t, p, procA.ID, firstEpoch, 1), "the add starts a controller")
				drop := r.request(t, protocol.Drop, procA.ID, firstEpoch, 2)
				assert.NoError(t, p.ctrl.Do(t.Context(), rootMetadata, drop), "the drop must stop the controller")
				assert.Equal(t, p.metrics.Bound(unplacedInstrument, cellAttrs...), 0,
					"the controller of the cell must have stopped when the drop returns")
			})
		})

		t.Run("stops hosting the cell at a drop", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				assert.NoError(t, r.host(t, p, procA.ID, firstEpoch, 1), "the add starts a controller")
				_, err := p.ctrl.Disrupt(t.Context(), operatorMetadata, disruption(procB.ID, 1))
				assert.NoError(t, err, "the controller of the cell decides the disruption")
				drop := r.request(t, protocol.Drop, procA.ID, firstEpoch, 2)
				assert.NoError(t, p.ctrl.Do(t.Context(), rootMetadata, drop), "the drop must stop the controller")
				_, err = p.ctrl.Disrupt(t.Context(), operatorMetadata, disruption(procB.ID, 2))
				expect.That(t, err).
					ErrorIs(controller.ErrNotServing, "a call must find no controller of a dropped cell").
					ErrorIsNot(driver.ErrStopped, "a call must not go to the stopped run of a dropped cell")
			})
		})

		t.Run("returns the cause of its context when the controller of the cell does not stop in time",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					r := f.newRoot(t)
					p := f.start(t, procA, spec(namespace))
					synctest.Wait()
					f.faults.block()
					assert.NoError(t, r.host(t, p, procA.ID, firstEpoch, 1), "the add starts a controller")
					drop := r.request(t, protocol.Drop, procA.ID, firstEpoch, 2)
					assert.ErrorIs(t, p.ctrl.Do(t.Context(), rootMetadata, drop), context.DeadlineExceeded,
						"a drop must return the error of its context when the controller has not stopped")
					f.faults.unblock()
					again := r.request(t, protocol.Drop, procA.ID, firstEpoch, 3)
					assert.NoError(t, p.ctrl.Do(t.Context(), rootMetadata, again),
						"a later drop must return once the controller has stopped")
					assert.Equal(t, p.metrics.Bound(unplacedInstrument, cellAttrs...), 0,
						"the controller of the cell must have stopped")
				})
			})

		t.Run("returns the error of an acquisition that a newer controller has fenced", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				r.fence(t)
				assert.ErrorIs(t, r.host(t, p, procA.ID, firstEpoch, 1), epoch.ErrFenced,
					"the add must return the error of the acquisition")
			})
		})

		t.Run("keeps the process running after an acquisition that fails", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				f.faults.broken.Store(true)
				assert.ErrorIs(t, r.host(t, p, procA.ID, firstEpoch, 1), errBroken,
					"the add must return the error of the acquisition")
				f.faults.broken.Store(false)
				synctest.Wait()
				streamtest.Blocks(t, p.errc, "Run must not return after an acquisition that fails")
				assert.NoError(t, r.host(t, p, procA.ID, secondEpoch, 2), "a later add must start a controller")
			})
		})

		t.Run("keeps the process running after a newer controller fences the controller of the cell",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					r := f.newRoot(t)
					p := f.start(t, procA, spec(namespace))
					synctest.Wait()
					assert.NoError(t, r.host(t, p, procA.ID, firstEpoch, 1), "the add starts a controller")
					r.fence(t)
					_, err := p.ctrl.Disrupt(t.Context(), operatorMetadata, disruption(procB.ID, 1))
					assert.HasError(t, err, "the fenced controller must not decide the disruption")
					synctest.Wait()
					streamtest.Blocks(t, p.errc, "Run must not return after a newer controller fenced a cell")
					assert.Equal(
						t,
						p.metrics.Bound(unplacedInstrument, cellAttrs...),
						0,
						"the fenced controller of the cell must have stopped",
					)
				})
			})

		t.Run("stops the process when the controller of the cell fails after it serves", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				assert.NoError(t, r.host(t, p, procA.ID, firstEpoch, 1), "the add starts a controller")
				f.faults.broken.Store(true)
				_, err := p.ctrl.Disrupt(t.Context(), operatorMetadata, disruption(procB.ID, 1))
				assert.HasError(t, err, "the failed controller must not decide the disruption")
				assert.ErrorIs(t, p.result(t), errBroken, "Run must return the error of the controller of the cell")
			})
		})

		t.Run("stops the process when a commit of the controller of the cell does not fit the store",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					r := f.newRoot(t)
					p := f.start(t, procA, spec(namespace))
					synctest.Wait()
					assert.NoError(t, r.host(t, p, procA.ID, firstEpoch, 1), "the add starts a controller")
					f.faults.tight.Store(true)
					_, err := p.ctrl.Disrupt(t.Context(), operatorMetadata, disruption(procB.ID, 1))
					assert.HasError(t, err, "the failed controller must not decide the disruption")
					assert.ErrorIs(t, p.result(t), store.ErrTooLarge,
						"Run must return the error of the bound of a commit of the cell")
				})
			})
	})

	t.Run("Run", func(t *testing.T) {
		t.Parallel()

		t.Run("stops the controller of each cell before it returns", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				assert.NoError(t, r.host(t, p, procA.ID, firstEpoch, 1), "the add starts a controller")
				assert.ErrorIs(t, p.halt(t), context.Canceled, "Run returns the cause of its context")
				assert.Equal(t, p.metrics.Bound(unplacedInstrument, cellAttrs...), 0,
					"Run must stop the controller of the cell")
			})
		})

		t.Run("returns ErrNotServing for a cell once Run has returned", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				assert.NoError(t, r.host(t, p, procA.ID, firstEpoch, 1), "the add starts a controller")
				assert.ErrorIs(t, p.halt(t), context.Canceled, "Run returns the cause of its context")
				_, err := p.ctrl.Disrupt(t.Context(), operatorMetadata, disruption(procB.ID, 1))
				assert.ErrorIs(t, err, controller.ErrNotServing,
					"a call to a cell must return ErrNotServing once Run has returned")
				assert.Equal(t, errs.Classify(err), errs.Transient,
					"a call to a cell must return a transient error once Run has returned")
			})
		})
	})
}

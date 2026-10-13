// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package controller_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/history"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/controller"
	"go.thesmos.sh/sharder/internal/control"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The proposals of the history of concurrent spec proposals.
const (
	// submitters is the number of clients that submit spec proposals at once.
	submitters = 4
	// submissions is the number of spec proposals of each client.
	submissions = 3
)

// unsupported is metadata of a major version that this release does not implement.
var unsupported = protocol.Metadata{Caller: operator}

// TestServer checks how a controller process passes each call to the machine that serves
// it.
func TestServer(t *testing.T) {
	t.Parallel()

	t.Run("Do", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the error of Metadata.Check before any other check", func(t *testing.T) {
			t.Parallel()
			err := idle(t).Do(t.Context(), unsupported, &protocol.Request{Scope: cellScope})
			assert.ErrorIs(t, err, protocol.ErrUnsupported, "Do must return the error of Metadata.Check")
		})

		t.Run("returns ErrScope for a callback of a cell", func(t *testing.T) {
			t.Parallel()
			err := idle(t).Do(t.Context(), rootMetadata, &protocol.Request{Scope: cellScope})
			assert.ErrorIs(t, err, controller.ErrScope, "Do must refuse a callback of a cell")
		})

		t.Run("returns ErrNotServing before Run", func(t *testing.T) {
			t.Parallel()
			err := idle(t).Do(t.Context(), rootMetadata, &protocol.Request{Scope: rootScope})
			assert.ErrorIs(t, err, controller.ErrNotServing, "Do must return ErrNotServing before Run")
		})

		t.Run("returns ErrNotServing for a namespace that the process does not run", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				err := p.ctrl.Do(
					t.Context(),
					rootMetadata,
					&protocol.Request{Scope: shard.Scope{Namespace: otherNamespace}},
				)
				assert.ErrorIs(t, err, controller.ErrNotServing, "Do must return ErrNotServing for another namespace")
			})
		})

		t.Run("returns ErrNotServing once Run has returned", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				r.place(t, procA.ID, firstEpoch)
				req := r.request(t, protocol.Add, procA.ID, firstEpoch, 1)
				assert.ErrorIs(t, p.halt(t), context.Canceled, "Run returns the cause of its context")
				assert.ErrorIs(t, p.ctrl.Do(t.Context(), rootMetadata, req), controller.ErrNotServing,
					"Do must return ErrNotServing once Run has returned")
			})
		})

		t.Run("runs a callback of any caller in an unauthenticated process", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				cfg := f.config(procA, noop.New())
				cfg.Unauthenticated = true
				p := f.launch(t, cfg, spec(namespace))
				synctest.Wait()
				r.place(t, procA.ID, firstEpoch)
				req := r.request(t, protocol.Add, procA.ID, firstEpoch, 1)
				assert.NoError(t, p.ctrl.Do(t.Context(), operatorMetadata, req),
					"an unauthenticated process must run the callback of a caller other than the root")
				stranger := protocol.Metadata{Caller: procB.Identity, Version: protocol.Current}
				_, err := p.ctrl.Disrupt(t.Context(), stranger, disruption(procB.ID, 1))
				assert.NoError(
					t,
					err,
					"an unauthenticated controller of a cell must decide the disruption of any caller",
				)
			})
		})

		t.Run("returns the error with which the agent of the root refuses a callback", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				r.place(t, procA.ID, firstEpoch)
				req := r.request(t, protocol.Add, procA.ID, firstEpoch, 1)
				assert.ErrorIs(t, p.ctrl.Do(t.Context(), operatorMetadata, req), agent.ErrDenied,
					"Do must return the error of the agent of the root")
			})
		})
	})

	t.Run("Report", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the error of Metadata.Check before any other check", func(t *testing.T) {
			t.Parallel()
			_, err := idle(t).Report(t.Context(), unsupported, &protocol.Report{Scope: cellScope})
			assert.ErrorIs(t, err, protocol.ErrUnsupported, "Report must return the error of Metadata.Check")
		})

		t.Run("passes a report of the root scope to the root", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				_, err := p.ctrl.Report(t.Context(), operatorMetadata, &protocol.Report{Scope: rootScope})
				assert.ErrorIs(t, err, control.ErrCall, "Report must return the error of the root")
			})
		})

		t.Run("passes a report of a cell to the controller of the cell", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				_, p := hosting(t)
				_, err := p.ctrl.Report(t.Context(), operatorMetadata,
					&protocol.Report{Scope: cellScope, Node: procB.ID, Incarnation: incA})
				assert.ErrorIs(t, err, control.ErrStale, "Report must return the error of the controller of the cell")
			})
		})

		t.Run("returns ErrNotServing for a cell that the process does not host", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				_, err := p.ctrl.Report(t.Context(), operatorMetadata, &protocol.Report{Scope: cellScope})
				assert.ErrorIs(t, err, controller.ErrNotServing,
					"Report must return ErrNotServing for a cell that the process does not host")
			})
		})

		t.Run("returns ErrNotServing for a namespace that the process does not run", func(t *testing.T) {
			t.Parallel()
			_, err := idle(t).Report(t.Context(), operatorMetadata, &protocol.Report{Scope: cellScope})
			assert.ErrorIs(t, err, controller.ErrNotServing,
				"Report must return ErrNotServing for a namespace that the process does not run")
		})
	})

	t.Run("Submit", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the error of Metadata.Check before any other check", func(t *testing.T) {
			t.Parallel()
			_, err := idle(t).Submit(t.Context(), unsupported, &proposal.Proposal{Scope: cellScope})
			assert.ErrorIs(t, err, protocol.ErrUnsupported, "Submit must return the error of Metadata.Check")
		})

		t.Run("passes a proposal of the root scope to the root", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				_, err := p.ctrl.Submit(t.Context(), operatorMetadata, &proposal.Proposal{Scope: rootScope})
				assert.ErrorIs(t, err, control.ErrNotServing,
					"Submit must return the error of a root that does not lead")
			})
		})

		t.Run("returns the error with which the root refuses a proposal", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				p := f.start(t, procA, spec(namespace))
				inc := leading(t, f)
				stranger := protocol.Metadata{Caller: procB.Identity, Version: protocol.Current}
				_, err := p.ctrl.Submit(t.Context(), stranger, respec(t, inc, 1, 0, 0))
				assert.ErrorIs(
					t,
					err,
					control.ErrDenied,
					"the root must refuse the proposal of a caller that is not an operator",
				)
			})
		})

		t.Run("returns ErrNotServing for a cell that the process does not host", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				_, err := p.ctrl.Submit(t.Context(), operatorMetadata, drain(t, procB.ID))
				assert.ErrorIs(t, err, controller.ErrNotServing,
					"Submit must return ErrNotServing for a cell that the process does not host")
			})
		})

		t.Run("passes a proposal of a cell to the controller of the cell", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				_, p := hosting(t)
				_, err := p.ctrl.Submit(t.Context(), operatorMetadata, drain(t, procB.ID))
				assert.ErrorIs(t, err, control.ErrStale, "Submit must return the error of the controller of the cell")
			})
		})

		t.Run("applies one of the concurrent spec proposals of each version", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				p := f.start(t, procA, spec(namespace))
				inc := leading(t, f)
				h := history.New()
				outcomes := history.Concurrently(submitters, waitFor, func(client int) (any, error) {
					for i := range submissions {
						base := epoch.Epoch(i + 1)
						call := h.Invoke(client, "submit", []any{base})
						receipt, err := p.ctrl.Submit(t.Context(), operatorMetadata, respec(t, inc, base, client, i))
						switch {
						case err == nil:
							call.OK(receipt.Applied)
						case errors.Is(err, control.ErrStale):
							call.OK(false)
						default:
							call.Unknown(err)
						}
					}
					return nil, nil
				})
				for _, o := range outcomes {
					assert.True(t, o.Finished, "every client must finish its proposals")
				}
				var final []epoch.Epoch
				history.Linearizable(t, h, versions, "the root must apply one spec proposal of each version",
					history.Final(&final), history.Whole())
				assert.Length(t, final, 1, "the history must leave one version")
				assert.Equal(t, accepted(t, f, inc), final[0], "the root scope must have the version of the history")
			})
		})
	})

	t.Run("Disrupt", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the error of Metadata.Check before any other check", func(t *testing.T) {
			t.Parallel()
			_, err := idle(t).Disrupt(t.Context(), unsupported, disruption(procB.ID, 1))
			assert.ErrorIs(t, err, protocol.ErrUnsupported, "Disrupt must return the error of Metadata.Check")
		})

		t.Run("passes a disruption to the controller of its cell", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f, p := hosting(t)
				_, err := p.ctrl.Disrupt(t.Context(), operatorMetadata, disruption(procB.ID, 1))
				assert.NoError(t, err, "the controller of the cell must decide the disruption")
				assert.Length(t, reservations(t, f), 1, "the controller of the cell must reserve the disruption")
			})
		})

		t.Run("returns ErrNotServing for a cell that the process does not host", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				_, err := p.ctrl.Disrupt(t.Context(), operatorMetadata, disruption(procB.ID, 1))
				assert.ErrorIs(t, err, controller.ErrNotServing,
					"Disrupt must return ErrNotServing for a cell that the process does not host")
			})
		})

		t.Run("returns a deferral without a RetryAfter WaitMargin before the deadline of ctx", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f, p := hosting(t)
				d := disruption(procB.ID, 1)
				d.Negotiable = false
				d.Start, d.End = origin.Add(time.Hour), origin.Add(2*time.Hour)
				ctx, cancel := context.WithDeadline(t.Context(), origin.Add(time.Minute))
				defer cancel()
				decided := make(chan budget.Decision, 1)
				go func() {
					decision, err := p.ctrl.Disrupt(ctx, operatorMetadata, d)
					assert.NoError(t, err, "the controller of the cell must decide the disruption")
					decided <- decision
				}()
				synctest.Wait()
				streamtest.Blocks(t, decided, "the deferral must wait while its deadline is ahead")
				f.clock.Advance(time.Minute - machine.WaitMargin)
				got := received(t, decided, "Disrupt returns")
				expect.False(t, got.Allowed, "the controller must defer a disruption before its start")
				expect.Equal(
					t,
					got.RetryAfter,
					time.Duration(0),
					"the deferral at the deadline must have no RetryAfter",
				)
			})
		})
	})

	t.Run("Done", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the error of Metadata.Check before any other check", func(t *testing.T) {
			t.Parallel()
			err := idle(t).Done(t.Context(), unsupported, &protocol.DoneArgs{Scope: cellScope})
			assert.ErrorIs(t, err, protocol.ErrUnsupported, "Done must return the error of Metadata.Check")
		})

		t.Run("passes the end of a reservation to the controller of its cell", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f, p := hosting(t)
				d := disruption(procB.ID, 1)
				_, err := p.ctrl.Disrupt(t.Context(), operatorMetadata, d)
				assert.NoError(t, err, "the controller of the cell reserves the disruption")
				err = p.ctrl.Done(
					t.Context(),
					operatorMetadata,
					&protocol.DoneArgs{Scope: cellScope, Incarnation: incA, ID: d.ID},
				)
				assert.NoError(t, err, "the controller of the cell must end the reservation")
				assert.Empty(t, reservations(t, f), "the controller of the cell must release the reservation")
			})
		})

		t.Run("returns the error with which the controller of the cell refuses the end", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				_, p := hosting(t)
				d := disruption(procB.ID, 1)
				_, err := p.ctrl.Disrupt(t.Context(), operatorMetadata, d)
				assert.NoError(t, err, "the controller of the cell reserves the disruption")
				stranger := protocol.Metadata{Caller: procB.Identity, Version: protocol.Current}
				err = p.ctrl.Done(
					t.Context(),
					stranger,
					&protocol.DoneArgs{Scope: cellScope, Incarnation: incA, ID: d.ID},
				)
				assert.ErrorIs(t, err, control.ErrDenied, "Done must return the error of the controller of the cell")
			})
		})

		t.Run("returns ErrNotServing for a cell that the process does not host", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				err := p.ctrl.Done(t.Context(), operatorMetadata, &protocol.DoneArgs{Scope: cellScope})
				assert.ErrorIs(t, err, controller.ErrNotServing,
					"Done must return ErrNotServing for a cell that the process does not host")
			})
		})
	})
}

// versions is the sequential specification of the spec proposals of a namespace. Its state
// is the version of the accepted spec. A proposal of the version after its base applies when
// its base is the accepted version, and the root refuses it as stale otherwise.
var versions = history.Spec[epoch.Epoch]{
	Initial: func() epoch.Epoch { return 1 },
	Next: func(v epoch.Epoch, op history.Operation) []epoch.Epoch {
		base, _ := op.Args[0].(epoch.Epoch)
		if base == v && op.Returned(true) {
			return []epoch.Epoch{v + 1}
		}
		if base != v && op.Returned(false) {
			return []epoch.Epoch{v}
		}
		return nil
	},
}

// idle returns a controller of the process procA that has not run.
func idle(tb cleaner) *controller.Controller {
	tb.Helper()
	ctrl, err := controller.New(newFixture(tb).config(procA, noop.New()))
	assert.NoError(tb, err, "New accepts the configuration of the process")
	return ctrl
}

// hosting starts the process procA under the root of the tests, and places the cell of the
// tests on it. It returns the fixture and the process once the controller of the cell
// serves.
func hosting(tb cleaner) (*fixture, *process) {
	tb.Helper()
	f := newFixture(tb)
	r := f.newRoot(tb)
	p := f.start(tb, procA, spec(namespace))
	synctest.Wait()
	assert.NoError(tb, r.host(tb, p, procA.ID, firstEpoch, 1), "the add starts the controller of the cell")
	return f, p
}

// reservations returns the reservations of the scope of the cell of the tests.
func reservations(tb assert.TB, f *fixture) []budget.Reservation {
	tb.Helper()
	state, cursor, err := f.st.Read(assert.Context(tb), cellScope, incA, store.Query{}, page.Page{})
	assert.NoError(tb, err, "the store reads the scope of the cell")
	assert.NoError(tb, cursor.Close(), "the cursor closes")
	return state.Reservations
}

// leading waits until the root machine of a process leads the root scope of the tests, and
// returns the incarnation of the namespace that it created.
func leading(tb assert.TB, f *fixture) shard.NamespaceIncarnation {
	tb.Helper()
	synctest.Wait()
	head, err := f.st.Head(assert.Context(tb), rootScope)
	assert.NoError(tb, err, "the store returns the header of the root scope")
	assert.False(tb, head.Incarnation.IsZero(), "the root must have created the namespace")
	return head.Incarnation
}

// respec returns the proposal of the spec of the tests at the version after base, in the
// namespace incarnation inc. The proposal i of client has an ID of its own.
func respec(tb assert.TB, inc shard.NamespaceIncarnation, base epoch.Epoch, client, i int) *proposal.Proposal {
	tb.Helper()
	s := spec(namespace)
	s.Version = base + 1
	body, err := json.Marshal(&s)
	assert.NoError(tb, err, "the spec encodes")
	return &proposal.Proposal{
		Scope:       rootScope,
		Expires:     origin.Add(time.Hour),
		Body:        body,
		Base:        base,
		Incarnation: inc,
		ID:          proposal.ID{ID: id.New128([id.Size128]byte{0: 0x9e, 14: byte(client), 15: byte(i)})},
		Kind:        proposal.SpecChange,
	}
}

// drain returns the proposal of a drain of the node n in the scope of the cell of the tests.
func drain(tb assert.TB, n shard.NodeID) *proposal.Proposal {
	tb.Helper()
	body, err := json.Marshal(&proposal.NodeArgs{Node: n})
	assert.NoError(tb, err, "the arguments of the drain encode")
	return &proposal.Proposal{
		Scope:       cellScope,
		Expires:     origin.Add(time.Hour),
		Body:        body,
		Incarnation: incA,
		ID:          proposal.ID{ID: id.New128([id.Size128]byte{0: 0xd2, 15: 1})},
		Kind:        proposal.Drain,
	}
}

// accepted returns the version of the accepted spec in the root scope of the namespace
// incarnation inc.
func accepted(tb assert.TB, f *fixture, inc shard.NamespaceIncarnation) epoch.Epoch {
	tb.Helper()
	state, cursor, err := f.st.Read(assert.Context(tb), rootScope, inc, store.Query{}, page.Page{})
	assert.NoError(tb, err, "the store reads the root scope")
	assert.NoError(tb, cursor.Close(), "the cursor closes")
	var s config.Spec
	assert.NoError(tb, json.Unmarshal(state.Spec, &s), "the accepted spec decodes")
	return s.Version
}

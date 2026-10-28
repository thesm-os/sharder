// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package driver_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/coretest/telemetrytest"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/machinetest"
	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
	"go.thesmos.sh/sharder/store/storetest"
)

// waitFor bounds each wait of a test for an event or a result. Inside a bubble of
// testing/synctest, the wait runs on the virtual time of the bubble.
const waitFor = 10 * time.Second

// machineEvents is the capacity of the channel on which the machine of a fixture passes its
// events.
const machineEvents = 64

// namespace is the namespace of the tests.
const namespace shard.Namespace = "orders"

// term is the fence epoch of the writer of the commits of the tests.
const term epoch.Epoch = 1

// The scope, the incarnation and the clock of the tests.
var (
	// scope is the root scope of the namespace of the tests.
	scope = shard.Scope{Namespace: namespace}
	// inc is the namespace incarnation of the tests.
	inc = shard.NamespaceIncarnation{ID: id.New128([id.Size128]byte{0: 0x1a, 15: 0x0a})}
	// origin is the start of the clock of the tests.
	origin = time.Date(2026, time.October, 12, 0, 0, 0, 0, time.UTC)
)

// The errors of the machines and the dependencies of the tests.
var (
	// errBroken is an error that a retry does not fix.
	errBroken = errs.WithClass(errors.New("test: broken"), errs.Invalid)
	// errBusy is an error that a retry can fix.
	errBusy = errs.WithClass(errors.New("test: busy"), errs.Transient)
	// errCause is the cause with which a test ends a context.
	errCause = errors.New("test: the test ends the context")
)

// cleaner is an assert.TB that runs functions when its test ends, such as a
// *testing.T or a *prop.Case.
type cleaner interface {
	assert.TB
	// Cleanup registers f to run when the test ends.
	Cleanup(f func())
}

// fixture is a double of a machine and the configuration of a driver that runs it.
type fixture struct {
	// machine is the machine of the configuration. Its Step passes each event to events, and
	// appends the effects that the script of the fixture returns for the event. The script
	// runs on the goroutine of the run. The double records no call, so it keeps no event and
	// no effect alive.
	machine *machinetest.MachineStub
	// events receives each event that the run passes to the machine.
	events chan machine.Event
	// cfg is the configuration of the driver.
	cfg *driver.Config
	// store is the store of the configuration, and its membership is the membership of the
	// configuration.
	store *memory.Store
	// clock is the clock of the configuration.
	clock *fake.Clock
	// logs keeps the records of the logger of the configuration.
	logs *telemetrytest.Records
}

// newFixture returns the fixture of a machine whose Step runs script. The store closes when
// tb ends.
func newFixture(tb cleaner, script func(ev machine.Event) []machine.Effect) *fixture {
	tb.Helper()
	clk := fake.New(origin)
	s := memory.New(clk)
	tb.Cleanup(func() { assert.NoError(tb, s.Close(), "the store closes") })
	logs := telemetrytest.NewRecords(slog.LevelDebug)
	events := make(chan machine.Event, machineEvents)
	m := machinetest.NewMachineStub(tb, machinetest.MachineStubBenchMode(), machinetest.WithMachineStep(
		func(ev machine.Event, out []machine.Effect) []machine.Effect {
			events <- ev
			return append(out, script(ev)...)
		}))
	return &fixture{
		machine: m,
		events:  events,
		cfg: &driver.Config{
			Machine:    m,
			Reader:     s,
			Store:      s,
			Membership: s.Membership(),
			Clock:      clk,
			Logger:     slog.New(logs),
		},
		store: s,
		clock: clk,
		logs:  logs,
	}
}

// driver returns the driver of the configuration of f, and fails tb when New refuses it.
func (f *fixture) driver(tb assert.TB) *driver.Driver {
	tb.Helper()
	d, err := driver.New(f.cfg)
	assert.NoError(tb, err, "New accepts the configuration of the fixture")
	return d
}

// results runs the driver of f until the machine has passed n events after Started, then
// stops the run, and returns those events.
func (f *fixture) results(t *testing.T, n int) []machine.Event {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	errc := start(ctx, f.driver(t))
	received(t, f.events, "the run passes Started")
	evs := make([]machine.Event, 0, n)
	for range n {
		evs = append(evs, received(t, f.events, "the run passes the result of an effect"))
	}
	stopped(t, cancel, errc)
	return evs
}

// stopped ends the context of a run with cancel, and fails tb when Run does not return the
// error of the ended context.
func stopped(tb assert.TB, cancel context.CancelFunc, errc <-chan error) {
	tb.Helper()
	cancel()
	assert.ErrorIs(tb, received(tb, errc, "Run returns"), context.Canceled, "Run must return the cause of its context")
}

// start runs d with ctx on a goroutine of its own, and returns the channel that receives
// the result of Run.
func start(ctx context.Context, d *driver.Driver) <-chan error {
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	return errc
}

// outcome is the result of one Call: the value and the error that it returned.
type outcome[T any] struct {
	value T
	err   error
}

// call calls d.Call with ctx and event on a goroutine of its own, and returns the channel
// that receives the result of the call.
func call[T machine.Result](
	ctx context.Context,
	d *driver.Driver,
	event func(machine.ReplyToken) machine.Event,
) <-chan outcome[T] {
	c := make(chan outcome[T], 1)
	go func() {
		v, err := d.Call[T](ctx, event)
		c <- outcome[T]{value: v, err: err}
	}()
	return c
}

// received receives the value that ch delivers, and fails tb with msg when ch delivers
// nothing within waitFor.
func received[T any](tb assert.TB, ch <-chan T, msg string) T {
	tb.Helper()
	v, ok := streamtest.Receive(tb, ch, waitFor, msg)
	assert.True(tb, ok, msg)
	return v
}

// blocked waits until ctx ends, and returns its cause. A double of a call that the
// Timeout of its effect must end calls it.
func blocked(ctx context.Context) error {
	<-ctx.Done()
	return context.Cause(ctx)
}

// serve is the script of a probe that replies to Started without an error, and returns no
// effect for any other event.
func serve(ev machine.Event) []machine.Effect {
	if s, ok := ev.(machine.Started); ok {
		return []machine.Effect{machine.Reply[struct{}]{To: s.Reply}}
	}
	return nil
}

// performing returns the script of a probe that replies to Started without an error and
// returns effects with the reply, and that returns no effect for any other event.
func performing(effects ...machine.Effect) func(ev machine.Event) []machine.Effect {
	return func(ev machine.Event) []machine.Effect {
		if s, ok := ev.(machine.Started); ok {
			return append([]machine.Effect{machine.Reply[struct{}]{To: s.Reply}}, effects...)
		}
		return nil
	}
}

// logged returns the level and the errors of the records of logs: the value of each
// attribute whose value is an error.
func logged(logs *telemetrytest.Records) ([]slog.Level, []error) {
	records := logs.All()
	levels := make([]slog.Level, 0, len(records))
	var errs []error
	for _, r := range records {
		levels = append(levels, r.Level)
		r.Attrs(func(a slog.Attr) bool {
			if err, ok := a.Value.Any().(error); ok {
				errs = append(errs, err)
			}
			return true
		})
	}
	return levels, errs
}

// mutation returns the mutation of the commit seq of the writer of the tests, with the
// placements puts.
func mutation(seq uint64, puts ...shard.Placement) *store.Mutation {
	return new(storetest.NewMutationBuilder().
		WithID(store.CommitID{Incarnation: inc, Term: term, Seq: seq}).
		WithPut(puts...).
		Build())
}

// commit applies the commit seq to the scope of the tests in s, with the placements puts,
// and fails t when the store refuses it. The commit expects the generation seq-1.
func commit(t *testing.T, s *memory.Store, seq uint64, puts ...shard.Placement) {
	t.Helper()
	_, err := s.Writer(scope, inc, term).Commit(t.Context(), epoch.Epoch(seq-1), mutation(seq, puts...))
	assert.NoError(t, err, "the store applies the commit")
}

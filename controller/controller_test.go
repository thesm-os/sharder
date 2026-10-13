// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package controller_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/controller"
	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// errCause is the cause with which a test ends the context of Run.
var errCause = errors.New("test: the test ends the context")

// durable is a store that provides the capability durable, as a store that persists every
// commit does.
type durable struct {
	*faulty
}

// Provides returns the capability durable.
func (durable) Provides() []config.Capability {
	return []config.Capability{config.Durable}
}

// The labels of the cases of the property of Run.
const (
	// labelLost counts a case in which the network lost the reply of an add of the root.
	labelLost = "a lost reply of an add"
	// labelMoved counts a case in which two processes controlled the cell.
	labelMoved = "a cell that moves to another process"
	// labelOutage counts a case with an outage of the store.
	labelOutage = "an outage of the store"
)

// The bounds of the property of Run.
const (
	// settleRounds bounds the rounds of the settle of a case.
	settleRounds = 40
	// settleStep is the time by which each round of the settle advances the clock.
	settleStep = 5 * time.Second
	// callFor bounds each call of a step of a case.
	callFor = time.Minute
)

// elapses lists the times by which a step of the property of Run advances the clock: a
// backoff, a renewal of a lease, a part of a lease, and more than the TTL of a lease.
var elapses = []time.Duration{100 * time.Millisecond, time.Second, 5 * time.Second, 15 * time.Second}

// nodes lists the nodes of the two processes of the property of Run.
var nodes = [2]shard.Node{procA, procB}

// lossy is the server of the callbacks of one controller process on the network of the
// property of Run. Once the test arms it, it drops the reply of the next add of the root.
type lossy struct {
	// ctrl is the controller of the last process of the node.
	ctrl atomic.Pointer[controller.Controller]
	// lose is true while the next add loses its reply.
	lose atomic.Bool
	// lost counts the replies that the server dropped.
	lost atomic.Int64
}

var _ protocol.AgentServer = (*lossy)(nil)

// Do passes r to the controller of the process. Once the test has armed l, it drops the reply
// of an add and returns protocol.ErrUnreachable, as a transport that loses a reply does.
func (l *lossy) Do(ctx context.Context, md protocol.Metadata, r *protocol.Request) error {
	err := l.ctrl.Load().Do(ctx, md, r)
	if r.Call == protocol.Add && l.lose.CompareAndSwap(true, false) {
		l.lost.Add(1)
		return fmt.Errorf("test: the reply of the add is lost: %w", protocol.ErrUnreachable)
	}
	return err
}

// submission is the input of a step that submits a spec proposal.
type submission struct {
	// process is the index of the node of the process that receives the proposal.
	process int
	// base is the version of the spec that the proposal follows.
	base epoch.Epoch
}

// system is the two controller processes of a case of the property of Run, over one store
// and one network.
type system struct {
	// f is the store, the network and the clock of the processes.
	f *fixture
	// procs lists the process of each node, and nil for a node whose process the case has
	// stopped.
	procs [2]*process
	// servers lists the server of the callbacks of each node.
	servers [2]*lossy
	// calls counts the proposals and the disruptions of the case, so each has an ID of its
	// own.
	calls byte
	// outage is true during an outage of the store.
	outage bool
}

// newSystem returns a system whose two processes run.
func newSystem(c *prop.Case) *system {
	c.Helper()
	s := &system{f: newFixture(c)}
	for i := range s.servers {
		s.servers[i] = &lossy{}
		s.start(c, i)
	}
	return s
}

// start starts the process of the node i.
func (s *system) start(c *prop.Case, i int) {
	c.Helper()
	p := s.f.start(c, nodes[i], spec(namespace))
	s.servers[i].ctrl.Store(p.ctrl)
	s.f.net.AddAgent(namespace, nodes[i].ID, s.servers[i])
	s.procs[i] = p
}

// stop stops the process of the node i, and checks that its Run returns the cause of its
// context.
func (s *system) stop(c *prop.Case, i int) {
	c.Helper()
	assert.ErrorIs(c, s.procs[i].halt(c), context.Canceled, "Run must return the cause of its context")
	s.procs[i] = nil
}

// toggle starts an outage of the store, or ends the outage that runs.
func (s *system) toggle(c *prop.Case) {
	if s.outage {
		s.f.st.EndOutage()
	} else {
		c.Classify(labelOutage)
		s.f.st.StartOutage()
	}
	s.outage = !s.outage
}

// running returns the indices of the nodes whose process runs.
func (s *system) running() []int {
	var is []int
	for i, p := range s.procs {
		if p != nil {
			is = append(is, i)
		}
	}
	return is
}

// stopped returns the indices of the nodes whose process the case has stopped.
func (s *system) stopped() []int {
	var is []int
	for i, p := range s.procs {
		if p == nil {
			is = append(is, i)
		}
	}
	return is
}

// incarnation returns the incarnation of the namespace. It reports false during an outage
// and before the root has created the namespace.
func (s *system) incarnation(c *prop.Case) (shard.NamespaceIncarnation, bool) {
	head, err := s.f.st.Head(c.Context(), rootScope)
	return head.Incarnation, err == nil && !head.Incarnation.IsZero()
}

// cells returns the cells that the root of the namespace incarnation inc has placed.
func (s *system) cells(c *prop.Case, inc shard.NamespaceIncarnation) []shard.CellID {
	c.Helper()
	ctx := c.Context()
	_, cursor, err := s.f.st.Read(ctx, rootScope, inc, store.Query{}, page.Page{})
	assert.NoError(c, err, "the store reads the root scope")
	var ids []shard.CellID
	for p, err := range cursor.Seq(ctx) {
		assert.NoError(c, err, "the cursor yields the placements of the root scope")
		ids = append(ids, shard.CellID(ulid.Format(p.Shard.ID())))
	}
	assert.NoError(c, cursor.Close(), "the cursor closes")
	return ids
}

// submit submits the proposal of the spec of the tests at the version after in.base to the
// last process of the node in.process. A proposal that applies returns a receipt that
// states it.
func (s *system) submit(c *prop.Case, in submission) {
	c.Helper()
	inc, ok := s.incarnation(c)
	if !ok {
		return
	}
	s.calls++
	ctx, cancel := context.WithTimeout(c.Context(), callFor)
	defer cancel()
	p := respec(c, inc, in.base, in.process, int(s.calls))
	p.Expires = s.f.clock.Time().Add(time.Hour)
	receipt, err := s.servers[in.process].ctrl.Load().Submit(ctx, operatorMetadata, p)
	if err == nil {
		assert.True(c, receipt.Applied, "a spec proposal that the root accepts must apply")
	}
}

// disrupt passes a disruption of the other node to the last process of the node i, for the
// first cell of the namespace.
func (s *system) disrupt(c *prop.Case, i int) {
	c.Helper()
	inc, ok := s.incarnation(c)
	if !ok {
		return
	}
	ids := s.cells(c, inc)
	if len(ids) == 0 {
		return
	}
	s.calls++
	d := disruption(nodes[1-i].ID, s.calls)
	d.Scope, d.Incarnation = shard.Scope{Namespace: namespace, Cell: ids[0]}, inc
	d.Start = s.f.clock.Time()
	d.End = d.Start.Add(time.Hour)
	ctx, cancel := context.WithTimeout(c.Context(), callFor)
	defer cancel()
	_, err := s.servers[i].ctrl.Load().Disrupt(ctx, operatorMetadata, d)
	assert.NotEqual(c, errs.Classify(err), errs.Invalid, "a process must not refuse a disruption as invalid")
}

// check checks the invariants of the system:
//   - Each process that the case started runs until the case stops it.
//   - Each process runs one controller of each cell at most.
//   - Each term of each scope has one controller.
func (s *system) check(c *prop.Case) {
	c.Helper()
	for _, p := range s.procs {
		if p != nil {
			assert.InRange(c, p.cells.most(), 0, 1, "a process must run one controller of each cell at most")
			streamtest.Blocks(c, p.errc, "a process must run until the case stops it")
		}
	}
	inc, ok := s.incarnation(c)
	if !ok {
		return
	}
	terms(c, s.f, rootScope, inc)
	for _, id := range s.cells(c, inc) {
		terms(c, s.f, shard.Scope{Namespace: namespace, Cell: id}, inc)
	}
}

// settle ends the outage and starts every process. It then advances the clock until a
// running process controls the cell, and labels the case.
func (s *system) settle(c *prop.Case) {
	c.Helper()
	if s.outage {
		s.toggle(c)
	}
	for _, i := range s.stopped() {
		s.start(c, i)
	}
	for i := range s.servers {
		s.servers[i].lose.Store(false)
		if s.servers[i].lost.Load() > 0 {
			c.Classify(labelLost)
		}
	}
	for range settleRounds {
		synctest.Wait()
		if s.hosted(c) {
			break
		}
		s.f.clock.Advance(settleStep)
	}
	assert.True(c, s.hosted(c), "a running process must control the cell once the store is available")
	inc, _ := s.incarnation(c)
	ids := s.cells(c, inc)
	addresses := make(map[string]bool)
	for _, r := range records(c, s.f, shard.Scope{Namespace: namespace, Cell: ids[0]}, inc) {
		addresses[r.Controller] = true
	}
	if len(addresses) > 1 {
		c.Classify(labelMoved)
	}
}

// hosted reports whether the root has placed one cell, and the controller of its record is
// a running process that runs one controller of the cell.
func (s *system) hosted(c *prop.Case) bool {
	c.Helper()
	inc, ok := s.incarnation(c)
	if !ok {
		return false
	}
	ids := s.cells(c, inc)
	if len(ids) != 1 {
		return false
	}
	rec := record(c, s.f, shard.Scope{Namespace: namespace, Cell: ids[0]})
	for i, p := range s.procs {
		if p != nil && nodes[i].Address == rec.Controller && p.cells.controllers(ids[0]) == 1 {
			return true
		}
	}
	return false
}

// machine returns the stateful machine of the system. The actions read the processes that
// run and the outage, which only the inputs of the steps change.
func (s *system) machine() stateful.Machine[struct{}] {
	return stateful.Machine[struct{}]{
		Actions: []stateful.Action[struct{}]{
			{
				Name: "elapse",
				Input: func(c *prop.Case, _ struct{}) any {
					return c.Draw(prop.SampledFrom(elapses...), "duration")
				},
				Run: func(_ *prop.Case, _ int, in any) {
					s.f.clock.Advance(in.(time.Duration))
					synctest.Wait()
				},
			},
			{
				Name:    "stop",
				Enabled: func(struct{}) bool { return len(s.running()) > 0 },
				Input: func(c *prop.Case, _ struct{}) any {
					return c.Draw(prop.SampledFrom(s.running()...), "process")
				},
				Run: func(c *prop.Case, _ int, in any) {
					s.stop(c, in.(int))
					synctest.Wait()
				},
			},
			{
				Name:    "start",
				Enabled: func(struct{}) bool { return len(s.stopped()) > 0 },
				Input: func(c *prop.Case, _ struct{}) any {
					return c.Draw(prop.SampledFrom(s.stopped()...), "process")
				},
				Run: func(c *prop.Case, _ int, in any) {
					s.start(c, in.(int))
					synctest.Wait()
				},
			},
			{
				Name: "outage",
				Run: func(c *prop.Case, _ int, _ any) {
					s.toggle(c)
					synctest.Wait()
				},
			},
			{
				Name: "lose",
				Input: func(c *prop.Case, _ struct{}) any {
					return c.Draw(prop.Integer(0, len(nodes)-1), "process")
				},
				Run: func(_ *prop.Case, _ int, in any) {
					s.servers[in.(int)].lose.Store(true)
				},
			},
			{
				Name: "submit",
				Input: func(c *prop.Case, _ struct{}) any {
					return submission{
						process: c.Draw(prop.Integer(0, len(nodes)-1), "process"),
						base:    c.Draw(prop.SampledFrom[epoch.Epoch](1, 2, 3), "base"),
					}
				},
				Run: func(c *prop.Case, _ int, in any) {
					s.submit(c, in.(submission))
					synctest.Wait()
				},
			},
			{
				Name: "disrupt",
				Input: func(c *prop.Case, _ struct{}) any {
					return c.Draw(prop.Integer(0, len(nodes)-1), "process")
				},
				Run: func(c *prop.Case, _ int, in any) {
					s.disrupt(c, in.(int))
					synctest.Wait()
				},
			},
		},
		Invariant: func(c *prop.Case, _ struct{}) { s.check(c) },
		Settle:    func(c *prop.Case, _ struct{}) { s.settle(c) },
	}
}

// TestController checks the configuration and the run of a controller process.
func TestController(t *testing.T) {
	t.Parallel()

	t.Run("New", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give func(c *controller.Config)
			name string
		}{
			{
				name: "returns ErrConfig for a config without a store",
				give: func(c *controller.Config) { c.Store = nil },
			},
			{
				name: "returns ErrConfig for a config without a membership",
				give: func(c *controller.Config) { c.Membership = nil },
			},
			{
				name: "returns ErrConfig for a config without an agent client",
				give: func(c *controller.Config) { c.Agents = nil },
			},
			{
				name: "returns ErrConfig for a config without a clock",
				give: func(c *controller.Config) { c.Clock = nil },
			},
			{
				name: "returns ErrConfig for a config without a source of randomness",
				give: func(c *controller.Config) { c.Rand = nil },
			},
			{
				name: "returns ErrConfig for a config without a reporter",
				give: func(c *controller.Config) { c.Reporter = nil },
			},
			{
				name: "returns ErrConfig for a config without a registry",
				give: func(c *controller.Config) { c.Registry = nil },
			},
			{
				name: "returns ErrConfig for a config without a logger",
				give: func(c *controller.Config) { c.Logger = nil },
			},
			{name: "returns ErrConfig for a node without an ID", give: func(c *controller.Config) { c.Node.ID = "" }},
			{
				name: "returns ErrConfig for a node without an address",
				give: func(c *controller.Config) { c.Node.Address = "" },
			},
			{
				name: "returns ErrConfig for a node with a cell",
				give: func(c *controller.Config) { c.Node.Cell = cellID },
			},
			{
				name: "returns ErrConfig for a negative cell capacity",
				give: func(c *controller.Config) { c.CellCapacity = -1 },
			},
			{
				name: "returns ErrConfig for a store without the capability durable",
				give: func(c *controller.Config) { c.Volatile = false },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				cfg := newFixture(t).config(procA, noop.New())
				tt.give(cfg)
				_, err := controller.New(cfg)
				assert.ErrorIs(t, err, controller.ErrConfig, "New must refuse the configuration")
			})
		}

		t.Run("returns ErrConfig for a nil config", func(t *testing.T) {
			t.Parallel()
			_, err := controller.New(nil)
			assert.ErrorIs(t, err, controller.ErrConfig, "New must refuse a nil configuration")
		})

		t.Run("returns a controller of a store that provides the capability durable", func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			cfg := f.config(procA, noop.New())
			cfg.Store, cfg.Volatile = durable{faulty: f.st}, false
			_, err := controller.New(cfg)
			assert.NoError(t, err, "New must take a durable store without Volatile")
		})
	})

	t.Run("Run", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrRun when Run has been called before", func(t *testing.T) {
			t.Parallel()
			ctrl := idle(t)
			assert.HasError(t, ctrl.Run(t.Context()), "the first Run refuses a run without a spec")
			assert.ErrorIs(t, ctrl.Run(t.Context(), spec(namespace)), controller.ErrRun,
				"Run must refuse a second call")
		})

		t.Run("returns ErrConfig for a run without a spec", func(t *testing.T) {
			t.Parallel()
			assert.ErrorIs(t, idle(t).Run(t.Context()), controller.ErrConfig, "Run must refuse a run without a spec")
		})

		t.Run("returns ErrConfig for two specs of one namespace", func(t *testing.T) {
			t.Parallel()
			err := idle(t).Run(t.Context(), spec(namespace), spec(namespace))
			assert.ErrorIs(t, err, controller.ErrConfig, "Run must refuse two specs of one namespace")
		})

		t.Run("returns the cause of its context once the context ends", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctrl := idle(t)
				ctx, cancel := context.WithCancelCause(t.Context())
				errc := make(chan error, 1)
				go func() { errc <- ctrl.Run(ctx, spec(namespace)) }()
				synctest.Wait()
				cancel(errCause)
				assert.ErrorIs(
					t,
					received(t, errc, "Run returns"),
					errCause,
					"Run must return the cause of its context",
				)
			})
		})

		t.Run("returns the cause of a context that ended before the call", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancelCause(t.Context())
			cancel(errCause)
			assert.ErrorIs(t, idle(t).Run(ctx, spec(namespace)), errCause, "Run must return the cause of its context")
		})

		t.Run("leaves no goroutine once it returns", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				ctrl, err := controller.New(f.config(procA, noop.New()))
				assert.NoError(t, err, "New accepts the configuration of the process")
				ctx, stop := context.WithCancel(t.Context())
				done := assert.NoGoroutineLeaks(t, "Run must end every goroutine that it starts")
				errc := make(chan error, 1)
				go func() { errc <- ctrl.Run(ctx, spec(namespace)) }()
				synctest.Wait()
				p := &process{ctrl: ctrl}
				assert.NoError(t, r.host(t, p, procA.ID, firstEpoch, 1), "the add starts the controller of the cell")
				stop()
				assert.ErrorIs(t, received(t, errc, "Run returns"), context.Canceled,
					"Run returns the cause of its context")
				done()
			})
		})

		t.Run("elects one controller per term of each scope under stops, outages and lost replies",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					prop.ForAll(t, "each term of each scope has one controller, each process runs one controller "+
						"of a cell at most, and a running process controls the cell once the store is available",
						func(c *prop.Case) {
							stateful.Steps(c, newSystem(c).machine())
						})
				})
			})

		t.Run("places the cell of the namespace on the process", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.start(t, procA, spec(namespace))
				synctest.Wait()
				cell := placed(t, f)
				assert.Equal(t, record(t, f, shard.Scope{Namespace: namespace, Cell: cell}).Controller, procA.Address,
					"the controller of the process must control the cell that the root placed on it")
			})
		})

		t.Run("returns nil once Close has stopped it", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				assert.NoError(t, p.ctrl.Close(t.Context()), "Close ends the session of the process")
				assert.NoError(t, p.result(t), "Run must return nil once Close has stopped it")
			})
		})

		t.Run("returns ErrRun after Close", func(t *testing.T) {
			t.Parallel()
			ctrl := idle(t)
			assert.NoError(t, ctrl.Close(t.Context()), "a Close before Run returns nil")
			assert.ErrorIs(t, ctrl.Run(t.Context(), spec(namespace)), controller.ErrRun,
				"Run must refuse a run after Close")
			assert.NoError(t, ctrl.Close(t.Context()), "a Close after a refused Run must return nil")
		})
	})

	t.Run("Close", func(t *testing.T) {
		t.Parallel()

		t.Run("ends the session of the process in the root scope", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				r.session(t, procA.ID)
				assert.NoError(t, p.ctrl.Close(t.Context()), "Close ends the session of the process")
				v := view(t, f, rootScope)
				_, ok := v.Node(procA.ID)
				assert.False(t, ok, "the view of the root must not list a process that has closed")
			})
		})

		t.Run("returns once Run has returned", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				r := f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				f.st.block()
				assert.NoError(t, r.host(t, p, procA.ID, firstEpoch, 1), "the add starts a controller")
				closed := make(chan error, 1)
				go func() { closed <- p.ctrl.Close(t.Context()) }()
				synctest.Wait()
				streamtest.Blocks(t, closed, "Close must wait while the controller of the cell runs")
				f.st.unblock()
				assert.NoError(t, received(t, closed, "Close returns"), "Close must return once Run has returned")
			})
		})

		t.Run("returns the error of a Leave that fails", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				f.st.StartOutage()
				assert.ErrorIs(
					t,
					p.ctrl.Close(t.Context()),
					memory.ErrOutage,
					"Close must return the error of the Leave",
				)
			})
		})

		t.Run("returns the cause of its context when the context ends before a Leave returns", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				f.st.stalled.Store(true)
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				assert.ErrorIs(t, p.ctrl.Close(ctx), context.DeadlineExceeded,
					"Close must return the cause of its context")
			})
		})

		t.Run("returns nil before the agent of the root serves", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.st.StartOutage()
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				assert.NoError(t, p.ctrl.Close(t.Context()), "an agent of the root that does not serve has no session")
			})
		})

		t.Run("returns nil once the run has ended", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.newRoot(t)
				p := f.start(t, procA, spec(namespace))
				synctest.Wait()
				assert.ErrorIs(t, p.halt(t), context.Canceled, "Run returns the cause of its context")
				assert.NoError(t, p.ctrl.Close(t.Context()), "a Close after the run must return nil")
			})
		})

		t.Run("returns nil after a run whose context ended before the call", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctrl := idle(t)
				ctx, cancel := context.WithCancelCause(t.Context())
				cancel(errCause)
				assert.ErrorIs(t, ctrl.Run(ctx, spec(namespace)), errCause, "Run returns the cause of its context")
				assert.NoError(t, ctrl.Close(t.Context()), "a Close must not wait for a machine that never ran")
			})
		})
	})
}

// records returns the record of each commit of the scope s that changed the record of its
// controller, in the order of the commits.
func records(c *prop.Case, f *fixture, s shard.Scope, inc shard.NamespaceIncarnation) []shard.Cell {
	c.Helper()
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	var rs []shard.Cell
	for ev, err := range f.st.Watch(ctx, s, inc, 0, "") {
		assert.NoError(c, err, "the store yields the generations of the scope")
		if ev.Mutation.Cell != nil {
			rs = append(rs, *ev.Mutation.Cell)
		}
	}
	return rs
}

// terms checks that each term of the scope s has one controller in the records of the scope.
func terms(c *prop.Case, f *fixture, s shard.Scope, inc shard.NamespaceIncarnation) {
	c.Helper()
	byTerm := make(map[epoch.Epoch]string)
	for _, r := range records(c, f, s, inc) {
		if addr, ok := byTerm[r.Epoch]; ok {
			assert.Equal(c, r.Controller, addr, "each term of a scope must have one controller")
		}
		byTerm[r.Epoch] = r.Controller
	}
}

// placed returns the cell that the root of the namespace of the tests has placed.
func placed(tb assert.TB, f *fixture) shard.CellID {
	tb.Helper()
	ctx := assert.Context(tb)
	head, err := f.st.Head(ctx, rootScope)
	assert.NoError(tb, err, "the store returns the header of the root scope")
	_, cursor, err := f.st.Read(ctx, rootScope, head.Incarnation, store.Query{}, page.Page{})
	assert.NoError(tb, err, "the store reads the root scope")
	var cells []shard.CellID
	for p, err := range cursor.Seq(ctx) {
		assert.NoError(tb, err, "the cursor yields the placements of the root scope")
		cells = append(cells, shard.CellID(ulid.Format(p.Shard.ID())))
	}
	assert.NoError(tb, cursor.Close(), "the cursor closes")
	assert.Length(tb, cells, 1, "the root must have placed one cell")
	return cells[0]
}
